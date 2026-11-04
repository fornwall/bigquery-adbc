//! Arrow IPC `LZ4_FRAME` buffer decompression, decoded straight into each buffer's allocation.
//!
//! A compressed body buffer is an eight-byte uncompressed length followed by one LZ4 frame
//! ([format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md)). Because the decoded
//! length is known up front, every block is decoded from the input slice directly into its place
//! in the output. `lz4_flex::frame::FrameDecoder`, a streaming `Read`, would instead allocate
//! block-sized source and destination buffers per frame (128 KiB for BigQuery's 64 KiB blocks,
//! however small the Arrow buffer) and copy every block through both.

use std::hash::Hasher;

use arrow_buffer::Buffer;
use arrow_schema::ArrowError;
use lz4_flex::block::DecompressError;
use twox_hash::XxHash32;

use super::ipc_error;

const COMPRESSION_PREFIX_LENGTH: usize = 8;
const UNCOMPRESSED_BUFFER: i64 = -1;
/// The most an LZ4 frame payload can expand, used to reject a length prefix that no payload of the
/// accompanying size could produce. The bound is the format's, not a tunable: the cheapest
/// maximally expanding sequence is a token byte, a two-byte offset and a run of match-length
/// extension bytes worth at most 255 output bytes each, so the ratio approaches 255 from below and
/// never reaches it.
const LZ4_MAX_EXPANSION_RATIO: usize = 255;

const FRAME_MAGIC: u32 = 0x184D_2204;
const FLG_VERSION_MASK: u8 = 0b1100_0000;
const FLG_VERSION_01: u8 = 0b0100_0000;
const FLG_INDEPENDENT_BLOCKS: u8 = 0b0010_0000;
const FLG_BLOCK_CHECKSUMS: u8 = 0b0001_0000;
const FLG_CONTENT_SIZE: u8 = 0b0000_1000;
const FLG_CONTENT_CHECKSUM: u8 = 0b0000_0100;
const FLG_RESERVED: u8 = 0b0000_0010;
const FLG_DICTIONARY_ID: u8 = 0b0000_0001;
const BD_BLOCK_SIZE_MASK: u8 = 0b0111_0000;
const BLOCK_UNCOMPRESSED: u32 = 0x8000_0000;
/// How far back a match in a linked block may reach into the blocks before it.
const WINDOW_SIZE: usize = 64 * 1024;

/// Decodes one body buffer of a record batch whose metadata declares `LZ4_FRAME` compression.
pub(crate) fn decompress_buffer(encoded: Buffer) -> Result<Buffer, ArrowError> {
    if encoded.is_empty() {
        return Ok(encoded);
    }
    let (prefix, input) = encoded
        .split_first_chunk::<COMPRESSION_PREFIX_LENGTH>()
        .ok_or_else(|| {
            ipc_error(format!(
                "compressed Arrow IPC buffer is shorter than its {COMPRESSION_PREFIX_LENGTH}-byte length prefix"
            ))
        })?;
    let declared = i64::from_le_bytes(*prefix);
    if declared == UNCOMPRESSED_BUFFER {
        return Ok(encoded.slice(COMPRESSION_PREFIX_LENGTH));
    }
    let expected = usize::try_from(declared).map_err(|_| {
        ipc_error(format!(
            "compressed Arrow IPC buffer has invalid uncompressed length {declared}"
        ))
    })?;
    if expected == 0 {
        return Ok(Buffer::from([]));
    }

    // `declared` is the sender's number: reserving it unchecked would let a few hundred bytes ask
    // for gibibytes. See `LZ4_MAX_EXPANSION_RATIO` for why a larger value is always malformed.
    let ceiling = input.len().saturating_mul(LZ4_MAX_EXPANSION_RATIO);
    if expected > ceiling {
        return Err(ipc_error(format!(
            "LZ4 Arrow IPC buffer declares {expected} uncompressed bytes, more than the {ceiling} \
             its {} compressed bytes could produce",
            input.len()
        )));
    }
    // Reserving is only address space; bytes are zeroed as blocks are about to fill them, so a
    // prefix that overstates its frame never commits more memory than the frame decodes to.
    let mut output = Vec::new();
    output.try_reserve_exact(expected).map_err(|source| {
        ArrowError::MemoryError(format!(
            "could not reserve {expected} bytes for an LZ4 Arrow IPC buffer: {source}"
        ))
    })?;

    // Concatenated frames are one valid LZ4 stream, so they decode as one buffer.
    let mut input = input;
    let mut written = 0;
    while !input.is_empty() {
        written = decode_frame(&mut input, &mut output, written, expected)?;
    }
    if written != expected {
        return Err(ipc_error(format!(
            "LZ4 Arrow IPC buffer decoded to {written} bytes, expected {expected}"
        )));
    }
    // `output` never grows past `expected`, so having written that much, it holds exactly that.
    Ok(Buffer::from_vec(output))
}

/// Decodes the frame at the start of `input` into `output` from `frame_start` on, advancing
/// `input` past it and returning where the frame's output ends.
///
/// `output` holds the decoded bytes, then possibly zeroed scratch that a block was allowed to fill
/// but did not; it never grows past `expected`, and every byte in it is zeroed at most once.
fn decode_frame(
    input: &mut &[u8],
    output: &mut Vec<u8>,
    frame_start: usize,
    expected: usize,
) -> Result<usize, ArrowError> {
    let magic = u32::from_le_bytes(take(input, "frame magic number")?);
    if magic != FRAME_MAGIC {
        return Err(ipc_error(format!(
            "LZ4 Arrow IPC buffer holds magic number {magic:#010x} where an LZ4 frame \
             ({FRAME_MAGIC:#010x}) belongs"
        )));
    }
    let [flg, bd] = take(input, "frame descriptor")?;
    if flg & FLG_VERSION_MASK != FLG_VERSION_01 {
        return Err(ipc_error(format!(
            "LZ4 Arrow IPC buffer has unsupported frame version {}",
            flg >> 6
        )));
    }
    if flg & FLG_RESERVED != 0 || bd & !BD_BLOCK_SIZE_MASK != 0 {
        return Err(ipc_error(
            "LZ4 Arrow IPC buffer sets reserved frame descriptor bits",
        ));
    }
    if flg & FLG_DICTIONARY_ID != 0 {
        return Err(ipc_error(
            "LZ4 Arrow IPC buffer names a dictionary, which Arrow IPC compression never uses",
        ));
    }
    let max_block_size = match bd >> 4 {
        4 => 64 << 10,
        5 => 256 << 10,
        6 => 1 << 20,
        7 => 4 << 20,
        code => {
            return Err(ipc_error(format!(
                "LZ4 Arrow IPC buffer has invalid frame block size code {code}"
            )));
        }
    };
    let content_size = if flg & FLG_CONTENT_SIZE == 0 {
        None
    } else {
        Some(u64::from_le_bytes(take(input, "frame content size")?))
    };
    let [header_checksum] = take(input, "frame header checksum")?;
    let mut hasher = XxHash32::with_seed(0);
    hasher.write(&[flg, bd]);
    if let Some(content_size) = content_size {
        hasher.write(&content_size.to_le_bytes());
    }
    // The header checksum is the second byte of the descriptor's xxHash32.
    let [_, expected_checksum, _, _] = hasher.finish_32().to_le_bytes();
    if expected_checksum != header_checksum {
        return Err(ipc_error(
            "LZ4 Arrow IPC buffer fails its frame header checksum",
        ));
    }

    let linked = flg & FLG_INDEPENDENT_BLOCKS == 0;
    let mut written = frame_start;
    loop {
        let block_header = u32::from_le_bytes(take(input, "block size")?);
        if block_header == 0 {
            break;
        }
        let length = (block_header & !BLOCK_UNCOMPRESSED) as usize;
        if length > max_block_size {
            return Err(ipc_error(format!(
                "LZ4 Arrow IPC buffer has a {length}-byte block in a frame of at most \
                 {max_block_size}-byte blocks"
            )));
        }
        let block = take_slice(input, length, "block")?;
        if flg & FLG_BLOCK_CHECKSUMS != 0 {
            let checksum = u32::from_le_bytes(take(input, "block checksum")?);
            if XxHash32::oneshot(0, block) != checksum {
                return Err(ipc_error("LZ4 Arrow IPC buffer fails a block checksum"));
            }
        }

        let exceeds = || {
            ipc_error(format!(
                "LZ4 Arrow IPC buffer exceeds its declared uncompressed length {expected}"
            ))
        };
        // `written` only ever grows by what fit below `expected` and within `output`, so none of
        // these subtractions wraps.
        written += if block_header & BLOCK_UNCOMPRESSED != 0 {
            if length > expected - written {
                return Err(exceeds());
            }
            // Overwrite the zeroed scratch, if any, and append the rest without zeroing it first.
            let (overwrite, append) = block.split_at((output.len() - written).min(length));
            let (scratch, _) = output.split_at_mut(written).1.split_at_mut(overwrite.len());
            scratch.copy_from_slice(overwrite);
            output.extend_from_slice(append);
            length
        } else {
            let room = (expected - written).min(max_block_size);
            if output.len() < written + room {
                output.resize(written + room, 0);
            }
            let limited = room < max_block_size;
            let (decoded, rest) = output.split_at_mut(written);
            let target = rest.split_at_mut(room).0;
            let decompressed = if linked {
                // A linked block may match into the 64 KiB before it, but not across frames.
                let window = decoded.len().saturating_sub(WINDOW_SIZE).max(frame_start);
                lz4_flex::block::decompress_into_with_dict(
                    block,
                    target,
                    decoded.split_at(window).1,
                )
            } else {
                lz4_flex::block::decompress_into(block, target)
            };
            decompressed.map_err(|source| match source {
                DecompressError::OutputTooSmall { .. } if limited => exceeds(),
                source => ipc_error(format!(
                    "LZ4 Arrow IPC buffer has an invalid block: {source}"
                )),
            })?
        };
    }

    let frame = output.get(frame_start..written).unwrap_or_default();
    if flg & FLG_CONTENT_CHECKSUM != 0 {
        let checksum = u32::from_le_bytes(take(input, "content checksum")?);
        if XxHash32::oneshot(0, frame) != checksum {
            return Err(ipc_error("LZ4 Arrow IPC buffer fails its content checksum"));
        }
    }
    if let Some(content_size) = content_size
        && u64::try_from(frame.len()).ok() != Some(content_size)
    {
        return Err(ipc_error(format!(
            "LZ4 Arrow IPC frame decoded to {} bytes, but its header declares {content_size}",
            frame.len()
        )));
    }
    Ok(written)
}

/// Splits the next `N` bytes off `input`, or names the part of the frame it was cut short in.
fn take<const N: usize>(input: &mut &[u8], what: &str) -> Result<[u8; N], ArrowError> {
    let (head, rest) = input
        .split_first_chunk::<N>()
        .ok_or_else(|| truncated(what))?;
    *input = rest;
    Ok(*head)
}

fn take_slice<'a>(input: &mut &'a [u8], length: usize, what: &str) -> Result<&'a [u8], ArrowError> {
    let (head, rest) = input
        .split_at_checked(length)
        .ok_or_else(|| truncated(what))?;
    *input = rest;
    Ok(head)
}

fn truncated(what: &str) -> ArrowError {
    ipc_error(format!("LZ4 Arrow IPC buffer is truncated in its {what}"))
}

#[cfg(test)]
mod tests;
