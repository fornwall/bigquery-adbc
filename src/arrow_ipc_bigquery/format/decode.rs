//! Checked decoding of the Arrow IPC metadata BigQuery sends.
//!
//! Every byte read here comes from the service, so the FlatBuffer walk is hand-written against
//! the vtable slots in the parent module: no `root_unchecked`, no generated `Follow`, and every
//! offset and length checked before it is used.

use crate::types::MAX_SCHEMA_NESTING_DEPTH;

use super::{
    DateUnit, Field, FormatError, I64Pairs, IntervalUnit, IpcType, KeyValue, MessageHeader,
    Precision, RecordBatchMessage, SchemaMessage, TimeUnit, slot,
};

/// Decode and fully validate one Arrow IPC `Message` FlatBuffer.
#[cfg(test)]
pub(crate) fn decode_message(bytes: &[u8]) -> Result<MessageHeader<'_>, FormatError> {
    Ok(decode_message_and_body_length(bytes)?.0)
}

/// Decode and fully validate one Arrow IPC `Message` FlatBuffer, returning its header and the
/// body length its envelope declares.
pub(crate) fn decode_message_and_body_length(
    bytes: &[u8],
) -> Result<(MessageHeader<'_>, i64), FormatError> {
    let message = Table::root(bytes)?;
    match message.scalar_i16(slot::MESSAGE_VERSION, 0)? {
        3 | 4 => {}
        value => return Err(FormatError::unsupported("metadata version", value)),
    }
    let body_length = message.scalar_i64(slot::MESSAGE_BODY_LENGTH, 0)?;
    if body_length < 0 {
        return Err(FormatError::invalid(
            "negative Arrow IPC message body length",
        ));
    }
    // Bounds-checked but not walked: no caller reads a message's own metadata, and following
    // aliased entries would cost every record batch.
    message.table_vector(slot::MESSAGE_METADATA)?;

    let header_type = message.scalar_u8(slot::MESSAGE_HEADER_TYPE, 0)?;
    let header = message
        .table(slot::MESSAGE_HEADER)?
        .ok_or_else(|| FormatError::missing("message header"))?;
    let header = match header_type {
        1 => {
            let mut budget = ExpansionBudget(bytes.len().saturating_mul(EXPANSION_FACTOR));
            MessageHeader::Schema(parse_schema(header, &mut budget)?)
        }
        // The checked envelope and header table are enough to identify a feature this driver
        // always rejects. Parsing its nested record batch would only allocate descriptors that no
        // BigQuery path can consume.
        2 => {
            return Err(FormatError::Unsupported(
                "Arrow IPC dictionary batches are not supported: BigQuery's documented Arrow \
                 mappings do not use dictionary encoding"
                    .to_owned(),
            ));
        }
        3 => MessageHeader::RecordBatch(parse_record_batch(header, body_length)?),
        // Tensor and SparseTensor are outside the BigQuery driver surface.
        value => return Err(FormatError::unsupported("message header", value)),
    };
    Ok((header, body_length))
}

/// How many times its own size a schema's vector entries and strings may add up to.
///
/// A writer that stores each entry and string once cannot exceed 1. Sharing repeated strings, as
/// FlatBuffers' `create_shared_string` does, is the only honest way past it.
const EXPANSION_FACTOR: usize = 4;

/// How many more bytes of vector entries and strings a schema may expand to.
///
/// FlatBuffer offsets may alias, so a vector can list one field table many times, that table's
/// children can do the same, and every alias can name the same long string. A few hundred bytes
/// nested to the depth limit would otherwise expand to billions of fields, and a megabyte of
/// aliases of one long name to tens of gigabytes of validated, copied names. Each four-byte entry
/// and each string's bytes are charged before they are read.
struct ExpansionBudget(usize);

impl ExpansionBudget {
    fn charge(&mut self, bytes: usize) -> Result<(), FormatError> {
        self.0 = self.0.checked_sub(bytes).ok_or_else(|| {
            FormatError::invalid(
                "Arrow IPC schema expands to more fields, metadata and strings than its bytes \
                 can hold (aliased FlatBuffer offsets)",
            )
        })?;
        Ok(())
    }

    fn charge_entries(&mut self, vector: TableVector<'_>) -> Result<(), FormatError> {
        self.charge(vector.len().saturating_mul(4))
    }
}

fn parse_schema<'a>(
    table: Table<'a>,
    budget: &mut ExpansionBudget,
) -> Result<SchemaMessage<'a>, FormatError> {
    if table.scalar_i16(slot::SCHEMA_ENDIANNESS, 0)? != 0 {
        return Err(FormatError::Unsupported(
            "big-endian Arrow IPC schemas are not supported".to_owned(),
        ));
    }
    if let Some(features) = table.scalar_vector::<8>(slot::SCHEMA_FEATURES)? {
        for feature in features {
            match i64::from_le_bytes(*feature) {
                0..=2 => {}
                value => return Err(FormatError::unsupported("schema feature", value)),
            }
        }
    }
    let fields = table
        .table_vector(slot::SCHEMA_FIELDS)?
        .ok_or_else(|| FormatError::missing("schema fields"))?;
    let fields = parse_fields(fields, 0, budget)?;
    let metadata = parse_metadata(table, slot::SCHEMA_METADATA, budget)?;
    Ok(SchemaMessage { fields, metadata })
}

fn parse_fields<'a>(
    vector: TableVector<'a>,
    depth: usize,
    budget: &mut ExpansionBudget,
) -> Result<Vec<Field<'a>>, FormatError> {
    if depth >= MAX_SCHEMA_NESTING_DEPTH {
        return Err(FormatError::invalid(format!(
            "Arrow IPC schema nesting exceeds the driver limit of {MAX_SCHEMA_NESTING_DEPTH} levels"
        )));
    }
    budget.charge_entries(vector)?;
    let mut fields = Vec::with_capacity(vector.len());
    for table in vector.tables() {
        let table = table?;
        let name = table.string(slot::FIELD_NAME, budget)?.unwrap_or("");
        let nullable = table.scalar_bool(slot::FIELD_NULLABLE, false)?;
        let type_tag = table.scalar_u8(slot::FIELD_TYPE_TYPE, 0)?;
        let type_table = table
            .table(slot::FIELD_TYPE)?
            .ok_or_else(|| FormatError::missing("field type union value"))?;
        let data_type = parse_type(type_tag, type_table, budget).map_err(|error| match error {
            FormatError::Unsupported(message) => {
                FormatError::Unsupported(format!("field {name:?} has an {message}"))
            }
            invalid => invalid,
        })?;
        // Presence alone decides: documented BigQuery mappings never carry dictionaries.
        if table.table(slot::FIELD_DICTIONARY)?.is_some() {
            return Err(FormatError::Unsupported(format!(
                "field {name:?} is dictionary-encoded, which BigQuery's documented Arrow mappings \
                 never use"
            )));
        }
        // Writers emit an empty `children` vector for every leaf; recursing into it would charge
        // the leaf a nesting level and refuse a schema exactly at the limit.
        let children = match table.table_vector(slot::FIELD_CHILDREN)? {
            Some(children) if children.len() != 0 => parse_fields(children, depth + 1, budget)?,
            _ => Vec::new(),
        };
        let metadata = parse_metadata(table, slot::FIELD_METADATA, budget)?;
        validate_child_count(data_type, children.len())?;
        fields.push(Field {
            name,
            nullable,
            data_type,
            children,
            metadata,
        });
    }
    Ok(fields)
}

fn validate_child_count(data_type: IpcType<'_>, children: usize) -> Result<(), FormatError> {
    let valid = match data_type {
        IpcType::List => children == 1,
        IpcType::Struct => true,
        _ => children == 0,
    };
    if valid {
        Ok(())
    } else {
        Err(FormatError::invalid(format!(
            "invalid child count {children} for Arrow IPC type {data_type:?}"
        )))
    }
}

fn parse_type<'a>(
    tag: u8,
    table: Table<'a>,
    budget: &mut ExpansionBudget,
) -> Result<IpcType<'a>, FormatError> {
    Ok(match tag {
        2 => IpcType::Int {
            bit_width: table.scalar_i32(slot::TYPE_VALUE_0, 0)?,
            is_signed: table.scalar_bool(slot::TYPE_VALUE_1, false)?,
        },
        3 => IpcType::FloatingPoint(match table.scalar_i16(slot::TYPE_VALUE_0, 0)? {
            0 => Precision::Half,
            1 => Precision::Single,
            2 => Precision::Double,
            value => return Err(FormatError::unsupported("floating-point precision", value)),
        }),
        4 => IpcType::Binary,
        5 => IpcType::Utf8,
        6 => IpcType::Bool,
        7 => IpcType::Decimal {
            precision: table.scalar_i32(slot::TYPE_VALUE_0, 0)?,
            scale: table.scalar_i32(slot::TYPE_VALUE_1, 0)?,
            bit_width: table.scalar_i32(slot::TYPE_VALUE_2, 128)?,
        },
        8 => IpcType::Date(match table.scalar_i16(slot::TYPE_VALUE_0, 1)? {
            0 => DateUnit::Day,
            1 => DateUnit::Millisecond,
            value => return Err(FormatError::unsupported("date unit", value)),
        }),
        9 => IpcType::Time {
            unit: parse_time_unit(table.scalar_i16(slot::TYPE_VALUE_0, 1)?)?,
            bit_width: table.scalar_i32(slot::TYPE_VALUE_1, 32)?,
        },
        10 => IpcType::Timestamp {
            unit: parse_time_unit(table.scalar_i16(slot::TYPE_VALUE_0, 0)?)?,
            timezone: table.string(slot::TYPE_VALUE_1, budget)?,
        },
        11 => IpcType::Interval(match table.scalar_i16(slot::TYPE_VALUE_0, 0)? {
            0 => IntervalUnit::YearMonth,
            1 => IntervalUnit::DayTime,
            2 => IntervalUnit::MonthDayNano,
            value => return Err(FormatError::unsupported("interval unit", value)),
        }),
        12 => IpcType::List,
        13 => IpcType::Struct,
        15 => IpcType::FixedSizeBinary(table.scalar_i32(slot::TYPE_VALUE_0, 0)?),
        19 => IpcType::LargeBinary,
        20 => IpcType::LargeUtf8,
        23 => IpcType::BinaryView,
        24 => IpcType::Utf8View,
        // Neither documented Storage Read nor the CSV seed's scalar canonicalizer uses the rest:
        // Null has no BigQuery type, and unions and run-end encoding are outside BigQuery's type
        // system. Refusing those two also keeps the record-batch buffer traversal unambiguous.
        tag => {
            let name = match tag {
                1 => "Null",
                14 => "Union",
                16 => "FixedSizeList",
                17 => "Map",
                18 => "Duration",
                21 => "LargeList",
                22 => "RunEndEncoded",
                25 => "ListView",
                26 => "LargeListView",
                // NONE, or a member added to `Schema.fbs` after this list.
                _ => return Err(FormatError::unsupported("type tag", tag)),
            };
            return Err(FormatError::unsupported("type", name));
        }
    })
}

fn parse_time_unit(value: i16) -> Result<TimeUnit, FormatError> {
    match value {
        0 => Ok(TimeUnit::Second),
        1 => Ok(TimeUnit::Millisecond),
        2 => Ok(TimeUnit::Microsecond),
        3 => Ok(TimeUnit::Nanosecond),
        value => Err(FormatError::unsupported("time unit", value)),
    }
}

fn parse_metadata<'a>(
    table: Table<'a>,
    slot: u16,
    budget: &mut ExpansionBudget,
) -> Result<Vec<KeyValue<'a>>, FormatError> {
    let Some(vector) = table.table_vector(slot)? else {
        return Ok(Vec::new());
    };
    budget.charge_entries(vector)?;
    let mut metadata = Vec::with_capacity(vector.len());
    for table in vector.tables() {
        let table = table?;
        metadata.push(KeyValue {
            key: table
                .string(slot::KEY_VALUE_KEY, budget)?
                .ok_or_else(|| FormatError::missing("custom metadata key"))?,
            value: table.string(slot::KEY_VALUE_VALUE, budget)?,
        });
    }
    Ok(metadata)
}

fn parse_record_batch(
    table: Table<'_>,
    body_length: i64,
) -> Result<RecordBatchMessage<'_>, FormatError> {
    let length = table.scalar_i64(slot::RECORD_LENGTH, 0)?;
    if length < 0 {
        return Err(FormatError::invalid(
            "negative Arrow IPC record-batch length",
        ));
    }
    let nodes = table
        .pair_vector(slot::RECORD_NODES)?
        .ok_or_else(|| FormatError::missing("record-batch field nodes"))?;
    for (length, null_count) in nodes.iter() {
        if length < 0 || null_count < 0 || null_count > length {
            return Err(FormatError::invalid("invalid Arrow IPC field node"));
        }
    }
    let buffers = table
        .pair_vector(slot::RECORD_BUFFERS)?
        .ok_or_else(|| FormatError::missing("record-batch buffers"))?;
    // A body is its buffers written end to end, in order. Holding writers to that bounds decoding
    // by the body's size: buffers aliasing one range would have the reader validate, align or
    // decompress the same bytes once per buffer. Empty buffers occupy nothing and may sit anywhere.
    let mut previous_end = 0;
    for (index, (offset, length)) in buffers.iter().enumerate() {
        let Some(end) = offset
            .checked_add(length)
            .filter(|&end| offset >= 0 && length >= 0 && end <= body_length)
        else {
            return Err(FormatError::invalid(
                "Arrow IPC buffer lies outside the message body",
            ));
        };
        if length == 0 {
            continue;
        }
        if offset < previous_end {
            return Err(FormatError::invalid(format!(
                "Arrow IPC buffer {index} starts at body offset {offset}, before the buffer \
                 preceding it ends at {previous_end}"
            )));
        }
        previous_end = end;
    }
    let lz4_compressed = table
        .table(slot::RECORD_COMPRESSION)?
        .map(|compression| {
            match compression.scalar_i8(slot::COMPRESSION_METHOD, 0)? {
                0 => {}
                value => return Err(FormatError::unsupported("compression method", value)),
            }
            // 0 is LZ4_FRAME, 1 is ZSTD. A ZSTD decoder would need a C toolchain, which this
            // module avoids, so every codec but LZ4_FRAME is refused here.
            match compression.scalar_i8(slot::COMPRESSION_CODEC, 0)? {
                0 => Ok(true),
                value => Err(FormatError::unsupported("compression codec", value)),
            }
        })
        .transpose()?
        .unwrap_or(false);
    // Only the view types have variadic buffers, and no Storage Read batch carries those.
    let counts = table
        .scalar_vector::<8>(slot::RECORD_VARIADIC_COUNTS)?
        .unwrap_or_default();
    if counts.iter().any(|count| i64::from_le_bytes(*count) < 0) {
        return Err(FormatError::invalid(
            "negative Arrow IPC variadic buffer count",
        ));
    }
    if !counts.is_empty() {
        return Err(FormatError::Unsupported(
            "variadic Arrow IPC buffers are outside the BigQuery Storage Read type mapping"
                .to_owned(),
        ));
    }
    Ok(RecordBatchMessage {
        length,
        nodes,
        buffers,
        lz4_compressed,
    })
}

// A small checked FlatBuffer table view. All arithmetic and all reads are bounds checked.
#[derive(Clone, Copy)]
struct Table<'a> {
    bytes: &'a [u8],
    position: usize,
    vtable: usize,
    vtable_len: usize,
    object_len: usize,
}

impl<'a> Table<'a> {
    fn root(bytes: &'a [u8]) -> Result<Self, FormatError> {
        let offset = read_u32(bytes, 0)? as usize;
        Self::new(bytes, offset)
    }

    fn new(bytes: &'a [u8], position: usize) -> Result<Self, FormatError> {
        let relative = read_i32(bytes, position)?;
        let distance = usize::try_from(relative.unsigned_abs())
            .map_err(|_| FormatError::invalid("Arrow IPC vtable offset does not fit in usize"))?;
        let vtable = if relative >= 0 {
            position.checked_sub(distance)
        } else {
            position.checked_add(distance)
        }
        .ok_or_else(|| FormatError::invalid("invalid Arrow IPC FlatBuffer vtable offset"))?;
        let vtable_len = read_u16(bytes, vtable)? as usize;
        let object_len = read_u16(
            bytes,
            vtable
                .checked_add(2)
                .ok_or_else(|| FormatError::invalid("overflowing Arrow IPC vtable"))?,
        )? as usize;
        if vtable_len < 4 || !vtable_len.is_multiple_of(2) || object_len < 4 {
            return Err(FormatError::invalid(
                "invalid Arrow IPC FlatBuffer table header",
            ));
        }
        checked_range(bytes, vtable, vtable_len)?;
        checked_range(bytes, position, object_len)?;
        Ok(Self {
            bytes,
            position,
            vtable,
            vtable_len,
            object_len,
        })
    }

    fn field(&self, slot: u16) -> Result<Option<usize>, FormatError> {
        let slot = slot as usize;
        if slot + 2 > self.vtable_len {
            return Ok(None);
        }
        let offset = read_u16(
            self.bytes,
            self.vtable
                .checked_add(slot)
                .ok_or_else(|| FormatError::invalid("overflowing Arrow IPC vtable slot"))?,
        )? as usize;
        if offset == 0 {
            return Ok(None);
        }
        if offset >= self.object_len {
            return Err(FormatError::invalid(
                "Arrow IPC FlatBuffer field is outside its table",
            ));
        }
        Ok(Some(offset))
    }

    /// The absolute position of a `size`-byte scalar slot, bounds-checked against the table.
    fn scalar_field(&self, slot: u16, size: usize) -> Result<Option<usize>, FormatError> {
        let Some(offset) = self.field(slot)? else {
            return Ok(None);
        };
        if offset
            .checked_add(size)
            .is_none_or(|end| end > self.object_len)
        {
            return Err(FormatError::invalid(
                "Arrow IPC scalar field extends beyond its table",
            ));
        }
        self.position
            .checked_add(offset)
            .map(Some)
            .ok_or_else(|| FormatError::invalid("overflowing Arrow IPC table field"))
    }

    fn scalar_i8(&self, slot: u16, default: i8) -> Result<i8, FormatError> {
        self.scalar_field(slot, 1)?
            .map_or(Ok(default), |at| read_i8(self.bytes, at))
    }

    fn scalar_u8(&self, slot: u16, default: u8) -> Result<u8, FormatError> {
        self.scalar_field(slot, 1)?
            .map_or(Ok(default), |at| read_u8(self.bytes, at))
    }

    fn scalar_bool(&self, slot: u16, default: bool) -> Result<bool, FormatError> {
        match self.scalar_u8(slot, u8::from(default))? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(FormatError::invalid("invalid Arrow IPC boolean value")),
        }
    }

    fn scalar_i16(&self, slot: u16, default: i16) -> Result<i16, FormatError> {
        self.scalar_field(slot, 2)?
            .map_or(Ok(default), |at| read_i16(self.bytes, at))
    }

    fn scalar_i32(&self, slot: u16, default: i32) -> Result<i32, FormatError> {
        self.scalar_field(slot, 4)?
            .map_or(Ok(default), |at| read_i32(self.bytes, at))
    }

    fn scalar_i64(&self, slot: u16, default: i64) -> Result<i64, FormatError> {
        self.scalar_field(slot, 8)?
            .map_or(Ok(default), |at| read_i64(self.bytes, at))
    }

    fn target(&self, slot: u16) -> Result<Option<usize>, FormatError> {
        self.scalar_field(slot, 4)?
            .map(|at| {
                let offset = read_u32(self.bytes, at)? as usize;
                if offset == 0 {
                    return Err(FormatError::invalid("zero Arrow IPC FlatBuffer offset"));
                }
                at.checked_add(offset)
                    .ok_or_else(|| FormatError::invalid("overflowing Arrow IPC FlatBuffer offset"))
            })
            .transpose()
    }

    fn table(&self, slot: u16) -> Result<Option<Self>, FormatError> {
        self.target(slot)?
            .map(|at| Table::new(self.bytes, at))
            .transpose()
    }

    fn string(
        &self,
        slot: u16,
        budget: &mut ExpansionBudget,
    ) -> Result<Option<&'a str>, FormatError> {
        self.target(slot)?
            .map(|at| read_string(self.bytes, at, budget))
            .transpose()
    }

    /// The vector of table offsets at `slot`.
    fn table_vector(&self, slot: u16) -> Result<Option<TableVector<'a>>, FormatError> {
        self.target(slot)?
            .map(|at| {
                let offsets = read_vector::<4>(self.bytes, at)?;
                Ok(TableVector {
                    bytes: self.bytes,
                    // The elements start right after the four-byte length `read_vector` read.
                    start: at + 4,
                    offsets,
                })
            })
            .transpose()
    }

    /// The vector of `N`-byte scalars or structs at `slot`, as little-endian element bytes.
    fn scalar_vector<const N: usize>(
        &self,
        slot: u16,
    ) -> Result<Option<&'a [[u8; N]]>, FormatError> {
        self.target(slot)?
            .map(|at| read_vector(self.bytes, at))
            .transpose()
    }

    /// The vector of `FieldNode` or `Buffer` structs at `slot`, each split into its two i64s.
    fn pair_vector(&self, slot: u16) -> Result<Option<I64Pairs<'a>>, FormatError> {
        Ok(self
            .scalar_vector::<16>(slot)?
            .map(|pairs| I64Pairs(pairs.as_flattened().as_chunks::<8>().0.as_chunks::<2>().0)))
    }
}

/// The elements of the FlatBuffer vector whose length prefix is at `position`.
fn read_vector<const N: usize>(bytes: &[u8], position: usize) -> Result<&[[u8; N]], FormatError> {
    let len = read_u32(bytes, position)? as usize;
    position
        .checked_add(4)
        .zip(len.checked_mul(N))
        .and_then(|(start, byte_len)| bytes.get(start..start.checked_add(byte_len)?))
        .map(|elements| elements.as_chunks::<N>().0)
        .ok_or_else(|| FormatError::invalid("truncated Arrow IPC FlatBuffer vector"))
}

/// A bounds-checked FlatBuffer vector of offsets to tables.
#[derive(Clone, Copy, Debug)]
struct TableVector<'a> {
    bytes: &'a [u8],
    /// The absolute position of the first offset, which each offset is relative to.
    start: usize,
    offsets: &'a [[u8; 4]],
}

impl<'a> TableVector<'a> {
    fn len(self) -> usize {
        self.offsets.len()
    }

    /// Each element, followed to the table it points at.
    fn tables(self) -> impl Iterator<Item = Result<Table<'a>, FormatError>> {
        self.offsets.iter().enumerate().map(move |(index, offset)| {
            // `read_vector` bounds-checked every element, so its position cannot overflow.
            let element = self.start + 4 * index;
            match u32::from_le_bytes(*offset) as usize {
                0 => Err(FormatError::invalid("zero Arrow IPC table-vector offset")),
                offset => Table::new(
                    self.bytes,
                    element.checked_add(offset).ok_or_else(|| {
                        FormatError::invalid("overflowing Arrow IPC table-vector offset")
                    })?,
                ),
            }
        })
    }
}

fn checked_range(bytes: &[u8], start: usize, len: usize) -> Result<(), FormatError> {
    if start.checked_add(len).is_some_and(|end| end <= bytes.len()) {
        Ok(())
    } else {
        Err(FormatError::invalid("truncated Arrow IPC FlatBuffer"))
    }
}

/// Reads the string at `position`, charging its length to `budget` before its bytes are checked.
fn read_string<'a>(
    bytes: &'a [u8],
    position: usize,
    budget: &mut ExpansionBudget,
) -> Result<&'a str, FormatError> {
    let len = read_u32(bytes, position)? as usize;
    // The contents, then the NUL terminator FlatBuffers writes after every string.
    let terminated = position
        .checked_add(4)
        .and_then(|start| bytes.get(start..start.checked_add(len)?.checked_add(1)?))
        .ok_or_else(|| FormatError::invalid("truncated Arrow IPC string"))?;
    let Some((0, contents)) = terminated.split_last() else {
        return Err(FormatError::invalid("unterminated Arrow IPC string"));
    };
    budget.charge(len)?;
    std::str::from_utf8(contents).map_err(|_| FormatError::invalid("non-UTF-8 Arrow IPC string"))
}

macro_rules! read_scalar {
    ($name:ident, $type:ty, $size:expr) => {
        fn $name(bytes: &[u8], position: usize) -> Result<$type, FormatError> {
            let end = position
                .checked_add($size)
                .ok_or_else(|| FormatError::invalid("overflowing Arrow IPC scalar"))?;
            let value = bytes
                .get(position..end)
                .ok_or_else(|| FormatError::invalid("truncated Arrow IPC FlatBuffer scalar"))?;
            Ok(<$type>::from_le_bytes(
                value.try_into().expect("fixed-size slice"),
            ))
        }
    };
}

read_scalar!(read_i8, i8, 1);
read_scalar!(read_u8, u8, 1);
read_scalar!(read_u16, u16, 2);
read_scalar!(read_i16, i16, 2);
read_scalar!(read_u32, u32, 4);
read_scalar!(read_i32, i32, 4);
read_scalar!(read_i64, i64, 8);

#[cfg(test)]
mod tests;
