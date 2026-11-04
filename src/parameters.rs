//! Arrow-to-BigQuery query parameter conversion.
//!
//! A [`ParameterPlan`] derives each bound field's BigQuery type once per binding ([`types`]) and
//! renders every row against it ([`values`]); [`inferred`] maps the parameters a dry run infers
//! back to Arrow.

mod inferred;
mod types;
mod values;

use adbc_core::error::{Error, Result};
use arrow_array::RecordBatch;
use arrow_schema::{Field, Fields, Schema};

use crate::error::invalid_argument;
use crate::options::string_enum;
use crate::rest::model::{QueryParameter, QueryParameterType, QueryParameterValue};
use crate::types::{ValueAt, case_duplicate, validate_schema_nesting};

pub(crate) use inferred::parameter_schema;
use types::parameter_type;
use values::parameter_value;

string_enum! {
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub(crate) enum ParameterMode {
        Named => (
            "named",
            ["adbc.bigquery.sql.query.parameter_mode_named"]
        ),
        #[default]
        Positional => (
            "positional",
            ["adbc.bigquery.sql.query.parameter_mode_positional"]
        ),
    }
}

impl ParameterMode {
    pub(crate) const fn as_bigquery_value(self) -> &'static str {
        match self {
            Self::Named => "NAMED",
            Self::Positional => "POSITIONAL",
        }
    }

    /// The name a parameter bound from `field` is sent under; a positional one has none.
    fn name_of(self, field: &Field) -> String {
        match self {
            Self::Named => field.name().clone(),
            Self::Positional => String::new(),
        }
    }

    /// Names the option to change when BigQuery refused the query's placeholders for being the
    /// other style than the parameters sent: `SELECT @x` bound in the default positional mode gets
    /// a bare "Named parameters are not supported", and only the driver knows which mode it sent.
    /// The match is on BigQuery's exact wording, so a changed wording loses the hint, not the
    /// error.
    pub(crate) fn explain_refusal(self, mut failure: Error) -> Error {
        let (refusal, remedy) = match self {
            Self::Positional => ("Named parameters are not supported", Self::Named),
            Self::Named => ("Positional parameters are not supported", Self::Positional),
        };
        if failure.message.contains(refusal) {
            failure.message = format!(
                "{}; set {} to \"{}\"",
                failure.message,
                crate::BigQueryStatementOption::ParameterMode.as_str(),
                remedy.as_str()
            );
        }
        failure
    }
}

/// The parameter types of a bound schema, derived once and reused for every row of its stream.
pub(crate) struct ParameterPlan {
    mode: ParameterMode,
    types: Vec<QueryParameterType>,
}

impl ParameterPlan {
    pub(crate) fn try_new(schema: &Schema, mode: ParameterMode) -> Result<Self> {
        // Bound the nesting depth before `parameter_type` and `parameter_value` recurse into the
        // caller-supplied schema; a stack overflow would abort the host process.
        validate_schema_nesting(schema.fields(), "bound BigQuery parameter")?;
        validate_parameter_fields(schema.fields(), mode)?;
        let types = schema
            .fields()
            .iter()
            .enumerate()
            // The refusals name the Arrow field; only this knows which bound parameter it is,
            // the only handle a positional binding has.
            .map(|(index, field)| {
                parameter_type(field).map_err(|mut failure| {
                    failure.message =
                        format!("bound BigQuery parameter {index}: {}", failure.message);
                    failure
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { mode, types })
    }

    /// The parameters of `batch`'s row `row`, which must exist, with `batch` carrying the schema
    /// the plan was built from. `stream_row` is the row's position in the whole bound stream,
    /// which is what an error names.
    pub(crate) fn row(
        &self,
        batch: &RecordBatch,
        row: usize,
        stream_row: usize,
    ) -> Result<Vec<QueryParameter>> {
        batch
            .schema_ref()
            .fields()
            .iter()
            .zip(batch.columns())
            .zip(&self.types)
            .enumerate()
            .map(|(index, ((field, array), parameter_type))| {
                let root = ParameterRoot {
                    index,
                    name: field.name(),
                    row: stream_row,
                };
                Ok(QueryParameter {
                    name: self.mode.name_of(field),
                    parameter_type: Some(parameter_type.clone()),
                    parameter_value: Some(parameter_value(
                        &ValueAt::Root(&root),
                        parameter_type,
                        array.as_ref(),
                        row,
                    )?),
                })
            })
            .collect()
    }
}

/// Typed null parameters for a dry run, which reads no bound row.
pub(crate) fn parameters_for_schema(
    schema: &Schema,
    mode: ParameterMode,
) -> Result<Vec<QueryParameter>> {
    let ParameterPlan { mode, types } = ParameterPlan::try_new(schema, mode)?;
    Ok(schema
        .fields()
        .iter()
        .zip(types)
        .map(|(field, parameter_type)| QueryParameter {
            name: mode.name_of(field),
            parameter_type: Some(parameter_type),
            parameter_value: Some(QueryParameterValue::default()),
        })
        .collect())
}

/// The longest named parameter name BigQuery accepts, in characters, not bytes; 128 is rejected
/// with "Query parameter name is too long: ..., must be less than 128 characters" (observed, not
/// documented). Length is the only constraint: names with spaces, backticks, control characters,
/// leading digits, non-ASCII letters or reserved keywords work when the SQL quotes them
/// (`` @`my param` ``), so nothing else may be rejected here.
const MAX_PARAMETER_NAME_CHARACTERS: usize = 127;

fn validate_parameter_fields(fields: &Fields, mode: ParameterMode) -> Result<()> {
    if mode != ParameterMode::Named {
        return Ok(());
    }

    for (index, field) in fields.iter().enumerate() {
        if field.name().is_empty() {
            return Err(invalid_argument(format!(
                "bound BigQuery parameter {index} has an empty field name; name the field after \
                 the @parameter it binds"
            )));
        }
        let characters = field.name().chars().count();
        if characters > MAX_PARAMETER_NAME_CHARACTERS {
            return Err(invalid_argument(format!(
                "named BigQuery parameter field {:?} is {characters} characters long; it cannot \
                 exceed {MAX_PARAMETER_NAME_CHARACTERS} characters",
                field.name()
            )));
        }
    }
    if let Some(((first, first_name), (duplicate, duplicate_name))) = case_duplicate(fields) {
        return Err(invalid_argument(format!(
            "named BigQuery parameter field {duplicate_name:?} at parameter {duplicate} \
             duplicates {first_name:?} at parameter {first}; BigQuery parameter names are \
             case-insensitive and must be unique"
        )));
    }
    Ok(())
}

/// The [`ValueAt`] root of the parameter walk: the bound parameter's position, its field name and
/// the row.
struct ParameterRoot<'a> {
    index: usize,
    name: &'a str,
    row: usize,
}

impl std::fmt::Display for ParameterRoot<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "bound BigQuery parameter {} {:?} at row {}",
            self.index, self.name, self.row
        )
    }
}

#[cfg(test)]
mod tests;
