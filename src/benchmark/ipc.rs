//! Storage API IPC fixtures: scalar columns, nested list/struct columns and booleans.

use std::cell::RefCell;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
};
use arrow_buffer::{Buffer, OffsetBuffer};
use arrow_schema::{ArrowError, Field};

use crate::arrow_ipc_bigquery::reader::decode_record_batch_message;
use crate::arrow_ipc_bigquery::writer::Writer;

/// One canonical batch and its uncompressed Storage API record-batch message.
#[derive(Debug)]
pub struct IpcFixture {
    batch: RecordBatch,
    encoded: Buffer,
}

impl IpcFixture {
    /// Builds `rows` rows with two scalar leaves and `depth` layers of `LIST<STRUCT>` nesting.
    /// Each list has two elements; nullable leaves exercise validity handling as well.
    pub fn new(rows: usize, depth: u32) -> Self {
        let leaf_rows = rows * 2_usize.pow(depth);
        let numbers = Int64Array::from_iter(
            (0..leaf_rows).map(|index| (!index.is_multiple_of(7)).then_some(42)),
        );
        let text = StringArray::from_iter_values(std::iter::repeat_n("IPC value", leaf_rows));
        let mut batch = RecordBatch::try_from_iter([
            ("number", Arc::new(numbers) as ArrayRef),
            ("text", Arc::new(text) as ArrayRef),
        ])
        .expect("the benchmark scalar arrays agree");
        for _ in 0..depth {
            let values = Arc::new(StructArray::from(batch));
            let item = Arc::new(Field::new("item", values.data_type().clone(), false));
            let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(2, values.len() / 2));
            let list = ListArray::new(item, offsets, values, None);
            batch = RecordBatch::try_from_iter([("items", Arc::new(list) as ArrayRef)])
                .expect("the benchmark list array is valid");
        }
        Self::from_batch(batch)
    }

    /// Builds a nullable boolean column to exercise bit-packed values and validity together.
    pub fn booleans(rows: usize) -> Self {
        let values = BooleanArray::from_iter(
            (0..rows).map(|index| (!index.is_multiple_of(7)).then_some(index.is_multiple_of(3))),
        );
        let batch = RecordBatch::try_from_iter([("flag", Arc::new(values) as ArrayRef)])
            .expect("the benchmark boolean array is valid");
        Self::from_batch(batch)
    }

    fn from_batch(batch: RecordBatch) -> Self {
        let encoded = Writer::new()
            .plan_batch(&batch)
            .expect("the benchmark batch is canonical")
            .write()
            .expect("the benchmark batch fits in memory")
            .into();
        Self { batch, encoded }
    }

    /// The same fixture with its message re-hosted at a misaligned base address.
    ///
    /// `Buffer::from(Vec<u8>)` is 64-byte aligned in practice, which costs the decoder nothing. A
    /// real Storage Read message is a zero-copy slice of its protobuf frame, so its buffers inherit
    /// an arbitrary offset; shifting by a byte count that is not a multiple of eight reproduces
    /// that.
    pub fn misaligned(&self, offset: usize) -> Self {
        let mut shifted = vec![0_u8; offset];
        shifted.extend_from_slice(&self.encoded);
        let encoded = Buffer::from(shifted).slice(offset);
        assert_ne!(
            encoded.as_ptr().align_offset(8),
            0,
            "the shifted fixture must actually be misaligned"
        );
        Self {
            batch: self.batch.clone(),
            encoded,
        }
    }

    /// Selects a row window, retaining its nonzero offsets as ingest chunking does.
    pub fn sliced(&self, offset: usize, length: usize) -> Self {
        Self::from_batch(self.batch.slice(offset, length))
    }

    /// The uncompressed record-batch message the writer produced for this fixture's batch.
    pub fn message(&self) -> &[u8] {
        &self.encoded
    }

    /// The same batch carried by `message`, one complete record-batch message another encoder
    /// produced. The in-tree writer never compresses, so this is how a bench reaches the
    /// LZ4_FRAME buffers every Storage Read session asks BigQuery for.
    pub fn with_message(&self, message: Vec<u8>) -> Self {
        Self {
            batch: self.batch.clone(),
            encoded: message.into(),
        }
    }

    /// Decodes the same complete message each iteration, without schema parsing or network I/O.
    pub fn decode(&self) -> Result<RecordBatch, ArrowError> {
        decode_record_batch_message(&self.encoded, self.batch.schema())
    }

    /// Creates a writer reused across iterations, as one Storage Write stream reuses its scratch.
    pub fn encoder(&self) -> impl Fn() -> Result<Vec<u8>, ArrowError> + '_ {
        let writer = RefCell::new(Writer::new());
        move || writer.borrow_mut().plan_batch(&self.batch)?.write()
    }

    /// Checks fixture round trips outside the measured region.
    pub fn validate(&self) {
        assert_eq!(
            self.decode().expect("the benchmark IPC must decode"),
            self.batch
        );
    }
}
