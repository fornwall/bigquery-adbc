//! Microbenchmark for Storage API IPC metadata and body work, including nested arrays.

mod support;

use std::io::Write;

use adbc_driver_bigquery::benchmark::IpcFixture;
use arrow_ipc::{
    BodyCompressionBuilder, CompressionType, MessageBuilder, MessageHeader, MetadataVersion,
    RecordBatchBuilder,
};
use flatbuffers::FlatBufferBuilder;
use lz4_flex::frame::FrameEncoder;

use support::{print_header, run_case};

fn main() {
    print_header();
    for rows in [1, 1024] {
        for (name, depth) in [("scalar", 0), ("nested", 1), ("deep", 3)] {
            let fixture = IpcFixture::new(rows, depth);
            fixture.validate();
            run_case(
                &format!("{name}/decode"),
                rows,
                || (),
                |()| fixture.decode().expect("the benchmark IPC must decode"),
            );
            // A zero-copy slice of a protobuf frame is rarely 8-byte aligned, and every body
            // buffer inherits its offset.
            let misaligned = fixture.misaligned(1);
            misaligned.validate();
            run_case(
                &format!("{name}/decode-misaligned"),
                rows,
                || (),
                |()| misaligned.decode().expect("the benchmark IPC must decode"),
            );
            let lz4 = fixture.with_message(bigquery_lz4(fixture.message()));
            lz4.validate();
            run_case(
                &format!("{name}/decode-lz4"),
                rows,
                || (),
                |()| lz4.decode().expect("the benchmark IPC must decode"),
            );
            let encode = fixture.encoder();
            run_case(
                &format!("{name}/encode"),
                rows,
                || (),
                |()| encode().expect("the benchmark batch must encode"),
            );
        }
    }
    for rows in [1024, 65_536] {
        for (name, depth) in [("scalar", 0), ("nested", 1), ("deep", 3)] {
            let fixture = IpcFixture::new(rows + 1, depth).sliced(1, rows);
            fixture.validate();
            let encode = fixture.encoder();
            run_case(
                &format!("{name}/encode-sliced"),
                rows,
                || (),
                |()| encode().expect("the sliced benchmark batch must encode"),
            );
        }
    }
    for rows in [1024, 65_536, 1_048_576] {
        let fixture = IpcFixture::booleans(rows + 1).sliced(1, rows);
        fixture.validate();
        let encode = fixture.encoder();
        run_case(
            "boolean/encode-sliced",
            rows,
            || (),
            |()| encode().expect("the sliced boolean batch must encode"),
        );
    }
}

/// `message`, an uncompressed record-batch message, with its buffers compressed as BigQuery
/// compresses them: each nonempty buffer becomes its length and one LZ4 frame of independent
/// 64 KiB blocks without checksums, with a block LZ4 cannot shrink stored verbatim. Unlike
/// `arrow-ipc`, BigQuery never falls back to the uncompressed sentinel.
fn bigquery_lz4(message: &[u8]) -> Vec<u8> {
    let (prefix, rest) = message
        .split_first_chunk::<8>()
        .expect("a framed message starts with its eight-byte prefix");
    let [_, _, _, _, a, b, c, d] = *prefix;
    let metadata_length = usize::try_from(u32::from_le_bytes([a, b, c, d])).expect("a u32 fits");
    let (metadata, body) = rest
        .split_at_checked(metadata_length)
        .expect("the writer wrote the metadata its prefix declares");
    let batch = arrow_ipc::root_as_message(metadata)
        .expect("the writer wrote a valid message")
        .header_as_record_batch()
        .expect("the message is a record batch");

    let mut compressed = Vec::new();
    let mut buffers = Vec::new();
    for buffer in batch.buffers().expect("a record batch lists its buffers") {
        let start = usize::try_from(buffer.offset()).expect("a valid buffer offset");
        let length = usize::try_from(buffer.length()).expect("a valid buffer length");
        let raw = body
            .get(start..start + length)
            .expect("the buffer lies in the body");
        let offset = compressed.len();
        if !raw.is_empty() {
            compressed.extend_from_slice(&i64::try_from(length).expect("fits").to_le_bytes());
            let mut encoder = FrameEncoder::new(&mut compressed);
            encoder
                .write_all(raw)
                .expect("writing to a vector cannot fail");
            encoder.finish().expect("writing to a vector cannot fail");
        }
        let written = compressed.len() - offset;
        compressed.resize(compressed.len().next_multiple_of(8), 0);
        buffers.push(arrow_ipc::Buffer::new(
            i64::try_from(offset).expect("fits"),
            i64::try_from(written).expect("fits"),
        ));
    }

    let mut builder = FlatBufferBuilder::new();
    let nodes: Vec<_> = batch
        .nodes()
        .expect("a record batch lists its nodes")
        .iter()
        .copied()
        .collect();
    let nodes = builder.create_vector(&nodes);
    let buffers = builder.create_vector(&buffers);
    let mut compression = BodyCompressionBuilder::new(&mut builder);
    compression.add_codec(CompressionType::LZ4_FRAME);
    let compression = compression.finish();
    let mut header = RecordBatchBuilder::new(&mut builder);
    header.add_length(batch.length());
    header.add_nodes(nodes);
    header.add_buffers(buffers);
    header.add_compression(compression);
    let header = header.finish();
    let mut envelope = MessageBuilder::new(&mut builder);
    envelope.add_version(MetadataVersion::V5);
    envelope.add_header_type(MessageHeader::RecordBatch);
    envelope.add_header(header.as_union_value());
    envelope.add_bodyLength(i64::try_from(compressed.len()).expect("fits"));
    let envelope = envelope.finish();
    builder.finish(envelope, None);

    let metadata = builder.finished_data();
    let padded = metadata.len().next_multiple_of(8);
    let mut framed = vec![0xff; 4];
    framed.extend_from_slice(&u32::try_from(padded).expect("fits").to_le_bytes());
    framed.extend_from_slice(metadata);
    framed.resize(8 + padded, 0);
    framed.extend_from_slice(&compressed);
    framed
}
