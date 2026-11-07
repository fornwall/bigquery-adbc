//! The subset of the Arrow IPC FlatBuffer format that BigQuery uses.
//!
//! The generated Arrow bindings are more than seven thousand lines and include IPC files,
//! tensors, sparse tensors, unions, and other features that the BigQuery driver never puts on
//! the wire. This module holds the one copy of the wire vocabulary (vtable slots and message
//! types); `decode` parses it with checked byte access and `encode` writes it.

mod decode;
mod encode;

use std::fmt;

use arrow_schema::{IntervalUnit, TimeUnit};

#[cfg(test)]
pub(super) use decode::decode_message;
pub(super) use decode::decode_message_and_body_length;
#[cfg(test)]
pub(super) use encode::{
    encode_compressed_record_batch_fixture, encode_dictionary_batch_fixture, encode_record_batch,
    encode_schema_fixture, encode_schema_with_body_fixture, encode_variadic_record_batch_fixture,
};
pub(super) use encode::{encode_record_batch_into, encode_schema, noncanonical_type};

// FlatBuffers vtable offsets.  These are byte offsets within a vtable, not field ordinals.
mod slot {
    pub(super) const MESSAGE_VERSION: u16 = 4;
    pub(super) const MESSAGE_HEADER_TYPE: u16 = 6;
    pub(super) const MESSAGE_HEADER: u16 = 8;
    pub(super) const MESSAGE_BODY_LENGTH: u16 = 10;
    pub(super) const MESSAGE_METADATA: u16 = 12;

    pub(super) const SCHEMA_ENDIANNESS: u16 = 4;
    pub(super) const SCHEMA_FIELDS: u16 = 6;
    pub(super) const SCHEMA_METADATA: u16 = 8;
    pub(super) const SCHEMA_FEATURES: u16 = 10;

    pub(super) const FIELD_NAME: u16 = 4;
    pub(super) const FIELD_NULLABLE: u16 = 6;
    pub(super) const FIELD_TYPE_TYPE: u16 = 8;
    pub(super) const FIELD_TYPE: u16 = 10;
    pub(super) const FIELD_DICTIONARY: u16 = 12;
    pub(super) const FIELD_CHILDREN: u16 = 14;
    pub(super) const FIELD_METADATA: u16 = 16;

    pub(super) const KEY_VALUE_KEY: u16 = 4;
    pub(super) const KEY_VALUE_VALUE: u16 = 6;

    pub(super) const RECORD_LENGTH: u16 = 4;
    pub(super) const RECORD_NODES: u16 = 6;
    pub(super) const RECORD_BUFFERS: u16 = 8;
    pub(super) const RECORD_COMPRESSION: u16 = 10;
    pub(super) const RECORD_VARIADIC_COUNTS: u16 = 12;

    pub(super) const COMPRESSION_CODEC: u16 = 4;
    pub(super) const COMPRESSION_METHOD: u16 = 6;

    pub(super) const TYPE_VALUE_0: u16 = 4;
    pub(super) const TYPE_VALUE_1: u16 = 6;
    pub(super) const TYPE_VALUE_2: u16 = 8;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum FormatError {
    /// Bytes or a schema that break the Arrow IPC format.
    Invalid(String),
    /// Well-formed Arrow IPC that uses a feature BigQuery never puts on the wire. Callers report it
    /// as `NotYetImplemented`, so a Storage Read session schema needing one falls back to REST.
    Unsupported(String),
}

impl FormatError {
    #[cold]
    #[inline(never)]
    fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }

    #[cold]
    #[inline(never)]
    fn missing(name: &str) -> Self {
        Self::Invalid(format!("Arrow IPC metadata is missing required {name}"))
    }

    #[cold]
    #[inline(never)]
    fn unsupported(what: &str, value: impl fmt::Display) -> Self {
        Self::Unsupported(format!("unsupported Arrow IPC {what} {value}"))
    }
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (Self::Invalid(message) | Self::Unsupported(message)) = self;
        f.write_str(message)
    }
}

#[derive(Debug)]
pub(super) enum MessageHeader<'a> {
    Schema(SchemaMessage<'a>),
    RecordBatch(RecordBatchMessage<'a>),
}

#[derive(Debug)]
pub(super) struct SchemaMessage<'a> {
    pub fields: Vec<Field<'a>>,
    pub metadata: Vec<KeyValue<'a>>,
}

#[derive(Debug)]
pub(super) struct Field<'a> {
    pub name: &'a str,
    pub nullable: bool,
    pub data_type: IpcType<'a>,
    pub children: Vec<Self>,
    pub metadata: Vec<KeyValue<'a>>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct KeyValue<'a> {
    pub key: &'a str,
    pub value: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Precision {
    Half,
    Single,
    Double,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DateUnit {
    Day,
    Millisecond,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IpcType<'a> {
    Int {
        bit_width: i32,
        is_signed: bool,
    },
    FloatingPoint(Precision),
    Binary,
    Utf8,
    Bool,
    Decimal {
        precision: i32,
        scale: i32,
        bit_width: i32,
    },
    Date(DateUnit),
    Time {
        unit: TimeUnit,
        bit_width: i32,
    },
    Timestamp {
        unit: TimeUnit,
        timezone: Option<&'a str>,
    },
    Interval(IntervalUnit),
    List,
    Struct,
    FixedSizeBinary(i32),
    LargeBinary,
    LargeUtf8,
    BinaryView,
    Utf8View,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FieldNode {
    pub length: i64,
    pub null_count: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Buffer {
    pub offset: i64,
    pub length: i64,
}

#[derive(Debug)]
pub(super) struct RecordBatchMessage<'a> {
    pub length: i64,
    pub nodes: I64Pairs<'a>,
    pub buffers: I64Pairs<'a>,
    /// Whether the body's buffers are LZ4-frame compressed: the only codec `create_read_session`
    /// requests and this module decodes. Other codecs are refused while parsing metadata.
    pub lz4_compressed: bool,
}

/// A FlatBuffer vector of two-i64 structs, `FieldNode` or `Buffer`, each as little-endian bytes.
///
/// Record-batch parsing validates every element's meaning before exposing this borrowed view.
#[derive(Clone, Copy, Debug)]
pub(super) struct I64Pairs<'a>(&'a [[[u8; 8]; 2]]);

impl I64Pairs<'_> {
    pub(super) fn len(self) -> usize {
        self.0.len()
    }

    pub(super) fn get(self, index: usize) -> Option<(i64, i64)> {
        self.0.get(index).map(i64_pair)
    }

    fn iter(self) -> impl Iterator<Item = (i64, i64)> {
        self.0.iter().map(i64_pair)
    }
}

fn i64_pair([first, second]: &[[u8; 8]; 2]) -> (i64, i64) {
    (i64::from_le_bytes(*first), i64::from_le_bytes(*second))
}
