use std::io::Write;

use lz4_flex::frame::{BlockMode, BlockSize, FrameEncoder, FrameInfo};

use super::*;

/// One Arrow body buffer: the declared length, then `payload` as one LZ4 frame under `info`.
fn compressed(info: FrameInfo, payload: &[u8]) -> Vec<u8> {
    let mut encoded = i64::try_from(payload.len()).unwrap().to_le_bytes().to_vec();
    let mut encoder = FrameEncoder::with_frame_info(info, &mut encoded);
    encoder.write_all(payload).unwrap();
    encoder.finish().unwrap();
    encoded
}

/// `encoded` with its length prefix replaced by `length`.
fn declaring(mut encoded: Vec<u8>, length: usize) -> Vec<u8> {
    encoded[..COMPRESSION_PREFIX_LENGTH]
        .copy_from_slice(&i64::try_from(length).unwrap().to_le_bytes());
    encoded
}

/// Text that compresses, longer than one 64 KiB block, whose later blocks repeat earlier ones
/// so a linked frame's matches reach back into the block before.
fn multi_block_payload() -> Vec<u8> {
    (0..200_000_u32)
        .map(|index| b"Storage Read"[index as usize % 12] ^ u8::try_from(index / 70_000).unwrap())
        .collect()
}

/// Pseudo-random bytes, which LZ4 stores verbatim in blocks flagged uncompressed.
fn incompressible(length: usize) -> Vec<u8> {
    let mut state = 0x2545_f491_u32;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// Every frame option `lz4_flex` writes: both block modes, several block sizes, both checksums
/// and the content size. BigQuery sends independent 64 KiB blocks without checksums
/// (observed, not documented); the rest cover other writers the format allows.
#[test]
fn every_frame_layout_decodes_to_its_payload() {
    let payload = multi_block_payload();
    for block_mode in [BlockMode::Independent, BlockMode::Linked] {
        for block_size in [BlockSize::Max64KB, BlockSize::Max256KB, BlockSize::Max4MB] {
            for checksums in [false, true] {
                let info = FrameInfo::new()
                    .block_mode(block_mode)
                    .block_size(block_size)
                    .block_checksums(checksums)
                    .content_checksum(checksums)
                    .content_size(checksums.then_some(payload.len() as u64));
                let decoded =
                    decompress_buffer(Buffer::from(compressed(info.clone(), &payload))).unwrap();
                assert_eq!(decoded.as_slice(), payload.as_slice(), "{info:?}");
            }
        }
    }
}

/// LZ4 stores a block it cannot shrink verbatim, flagged by the block size's high bit.
#[test]
fn an_incompressible_block_is_stored_and_copied() {
    let payload = incompressible(4096);
    let encoded = compressed(FrameInfo::new(), &payload);
    // The eight-byte Arrow prefix, then magic, FLG, BD and the header checksum.
    let block_header = u32::from_le_bytes(encoded[15..19].try_into().unwrap());
    assert_ne!(
        block_header & BLOCK_UNCOMPRESSED,
        0,
        "the block must be stored"
    );
    let decoded = decompress_buffer(Buffer::from(encoded)).unwrap();
    assert_eq!(decoded.as_slice(), payload.as_slice());
}

/// The second pair puts a stored 64 KiB block right after a compressed block that decoded
/// short of the 64 KiB zeroed for it, so the stored block lands partly on that scratch.
#[test]
fn concatenated_frames_decode_as_one_buffer() {
    for (first, second) in [
        (b"first frame, ".to_vec(), b"second frame".to_vec()),
        (vec![b'a'; 1000], incompressible(100_000)),
    ] {
        let mut encoded = compressed(FrameInfo::new(), &first);
        let tail = compressed(FrameInfo::new(), &second);
        encoded.extend_from_slice(&tail[COMPRESSION_PREFIX_LENGTH..]);
        let encoded = declaring(encoded, first.len() + second.len());
        let decoded = decompress_buffer(Buffer::from(encoded)).unwrap();
        assert_eq!(decoded.as_slice(), [first, second].concat());
    }
}

/// A linked block reaches back only within its own frame, so a linked frame decodes the same
/// whatever frame precedes it in the buffer.
#[test]
fn a_linked_frame_after_another_frame_does_not_reach_into_it() {
    let payload = multi_block_payload();
    let mut encoded = compressed(FrameInfo::new(), &payload);
    let second = compressed(FrameInfo::new().block_mode(BlockMode::Linked), &payload);
    encoded.extend_from_slice(&second[COMPRESSION_PREFIX_LENGTH..]);
    let encoded = declaring(encoded, payload.len() * 2);
    let decoded = decompress_buffer(Buffer::from(encoded)).unwrap();
    assert_eq!(&decoded[..payload.len()], payload.as_slice());
    assert_eq!(&decoded[payload.len()..], payload.as_slice());
}

/// Each refusal of a malformed frame, by the message naming what was wrong with it.
#[test]
fn each_malformed_frame_is_refused_by_name() {
    let payload = multi_block_payload();
    let checked = FrameInfo::new()
        .block_checksums(true)
        .content_checksum(true)
        .content_size(Some(payload.len() as u64));
    let valid = compressed(checked, &payload);
    // Past the eight-byte Arrow prefix: magic 8..12, FLG 12, BD 13, content size 14..22,
    // header checksum 22, then the first block's size 23..27 and its data from 27.
    let first_block = u32::from_le_bytes(valid[23..27].try_into().unwrap()) as usize;
    let corrupt = |at: usize, value: u8| {
        let mut encoded = valid.clone();
        encoded[at] = value;
        encoded
    };

    for (encoded, expected) in [
        (
            corrupt(8, 0),
            "LZ4 Arrow IPC buffer holds magic number 0x184d2200 where an LZ4 frame (0x184d2204) belongs",
        ),
        (
            corrupt(12, valid[12] ^ 0b1000_0000),
            "LZ4 Arrow IPC buffer has unsupported frame version 3",
        ),
        (
            corrupt(12, valid[12] | FLG_RESERVED),
            "LZ4 Arrow IPC buffer sets reserved frame descriptor bits",
        ),
        (
            corrupt(12, valid[12] | FLG_DICTIONARY_ID),
            "LZ4 Arrow IPC buffer names a dictionary, which Arrow IPC compression never uses",
        ),
        (
            corrupt(13, 0x30),
            "LZ4 Arrow IPC buffer has invalid frame block size code 3",
        ),
        (
            corrupt(22, valid[22] ^ 1),
            "LZ4 Arrow IPC buffer fails its frame header checksum",
        ),
        (
            corrupt(27 + first_block, valid[27 + first_block] ^ 1),
            "LZ4 Arrow IPC buffer fails a block checksum",
        ),
        (
            corrupt(valid.len() - 1, valid[valid.len() - 1] ^ 1),
            "LZ4 Arrow IPC buffer fails its content checksum",
        ),
        (
            valid[..valid.len() - 2].to_vec(),
            "LZ4 Arrow IPC buffer is truncated in its content checksum",
        ),
        (
            // Declaring one byte keeps the expansion bound from refusing the stub first.
            [&1_i64.to_le_bytes(), &valid[8..30]].concat(),
            "LZ4 Arrow IPC buffer is truncated in its block",
        ),
    ] {
        let error = decompress_buffer(Buffer::from(encoded)).unwrap_err();
        assert_eq!(error.to_string(), format!("Ipc error: {expected}"));
    }

    // The header checksum covers the content size, so a forged size needs a matching one.
    let mut wrong_content_size = valid.clone();
    wrong_content_size[14..22].copy_from_slice(&1_u64.to_le_bytes());
    let mut hasher = XxHash32::with_seed(0);
    hasher.write(&wrong_content_size[12..22]);
    wrong_content_size[22] = hasher.finish_32().to_le_bytes()[1];
    assert_eq!(
        decompress_buffer(Buffer::from(wrong_content_size))
            .unwrap_err()
            .to_string(),
        format!(
            "Ipc error: LZ4 Arrow IPC frame decoded to {} bytes, but its header declares 1",
            payload.len()
        )
    );
}

#[test]
fn lz4_uncompressed_sentinel_is_zero_copy() {
    let payload = Buffer::from(b"not worth compressing".as_slice());
    let mut encoded = UNCOMPRESSED_BUFFER.to_le_bytes().to_vec();
    encoded.extend_from_slice(&payload);
    let decoded = decompress_buffer(Buffer::from(encoded)).unwrap();
    assert_eq!(decoded.as_slice(), payload.as_slice());
}

/// The expansion bound still lets a length prefix ask for 255 times its frame, so the output is
/// zeroed only one block ahead of what has decoded: a prefix that overstates its frame costs
/// address space, not memory. Both block kinds are covered: a compressible payload, then an
/// incompressible one that LZ4 stores verbatim.
#[test]
fn an_overstated_length_prefix_zeroes_at_most_one_block_ahead() {
    for payload in [multi_block_payload(), incompressible(100_000)] {
        let encoded = compressed(FrameInfo::new(), &payload);
        let mut input = &encoded[COMPRESSION_PREFIX_LENGTH..];
        let mut output = Vec::new();
        let written = decode_frame(&mut input, &mut output, 0, usize::MAX).unwrap();
        assert_eq!(&output[..written], payload.as_slice());
        // `FrameInfo::new()` writes 64 KiB blocks.
        assert!(output.len() < written + (64 << 10), "{}", output.len());
    }
}

/// The companion to the bound above: a buffer compressible to near the format's limit must
/// still decode, so the ceiling may not be tightened past 255x.
#[test]
fn a_highly_compressible_lz4_buffer_stays_within_the_expansion_bound() {
    let payload = vec![0_u8; 1 << 20];
    let encoded = compressed(FrameInfo::new(), &payload);
    let frame_length = encoded.len() - COMPRESSION_PREFIX_LENGTH;
    assert!(payload.len() < frame_length * LZ4_MAX_EXPANSION_RATIO);

    let decoded = decompress_buffer(Buffer::from(encoded)).unwrap();
    assert_eq!(decoded.as_slice(), payload.as_slice());
}

/// The remaining `decompress_buffer` refusals, by text: a buffer too short to hold its own
/// length prefix, a negative declared length that is not the uncompressed sentinel, a frame that
/// decodes short of or past what it declared, and a prefix no payload of that size could reach.
/// That last bound stops a few hundred wire bytes asking for gibibytes before the frame is read.
#[test]
fn every_compressed_buffer_refusal_names_what_was_wrong() {
    // One byte past what the frame holds and far below the expansion ceiling, so nothing
    // refuses it before the frame is decoded.
    let decodes_short = declaring(compressed(FrameInfo::new(), b"five!"), 6);
    let small = compressed(FrameInfo::new(), b"small");
    let frame_length = small.len() - COMPRESSION_PREFIX_LENGTH;
    let ceiling = frame_length * LZ4_MAX_EXPANSION_RATIO;

    for (encoded, expected) in [
        (
            b"\x01\x02\x03".to_vec(),
            format!(
                "Ipc error: compressed Arrow IPC buffer is shorter than its \
                 {COMPRESSION_PREFIX_LENGTH}-byte length prefix"
            ),
        ),
        (
            (-2_i64).to_le_bytes().to_vec(),
            "Ipc error: compressed Arrow IPC buffer has invalid uncompressed length -2".to_owned(),
        ),
        (
            decodes_short,
            "Ipc error: LZ4 Arrow IPC buffer decoded to 5 bytes, expected 6".to_owned(),
        ),
        (
            declaring(compressed(FrameInfo::new(), b"longer than declared"), 3),
            "Ipc error: LZ4 Arrow IPC buffer exceeds its declared uncompressed length 3".to_owned(),
        ),
        (
            declaring(small, ceiling + 1),
            format!(
                "Ipc error: LZ4 Arrow IPC buffer declares {} uncompressed bytes, more than the \
                 {ceiling} its {frame_length} compressed bytes could produce",
                ceiling + 1,
            ),
        ),
    ] {
        let error = decompress_buffer(Buffer::from(encoded)).unwrap_err();
        assert_eq!(error.to_string(), expected);
    }
}
