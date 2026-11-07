//! The ADBC C ABI, transcribed from `arrow-adbc/c/include/arrow-adbc/adbc.h`.
//!
//! Field order in [`AdbcDriver`] is the vtable layout the driver manager indexes into. Never
//! reorder; append at the end, mirroring the header.

#![allow(
    non_snake_case,
    reason = "field and function names mirror the C header verbatim"
)]

use std::os::raw::{c_char, c_int, c_void};

pub(crate) use adbc_core::constants::ADBC_STATUS_OK;
#[cfg(test)]
pub(crate) use adbc_core::constants::{
    ADBC_STATUS_ALREADY_EXISTS, ADBC_STATUS_CANCELLED, ADBC_STATUS_INTEGRITY, ADBC_STATUS_INTERNAL,
    ADBC_STATUS_INVALID_ARGUMENT, ADBC_STATUS_INVALID_DATA, ADBC_STATUS_INVALID_STATE,
    ADBC_STATUS_IO, ADBC_STATUS_NOT_FOUND, ADBC_STATUS_NOT_IMPLEMENTED, ADBC_STATUS_TIMEOUT,
    ADBC_STATUS_UNAUTHENTICATED, ADBC_STATUS_UNAUTHORIZED, ADBC_STATUS_UNKNOWN,
};
pub(crate) use adbc_core::error::AdbcStatusCode;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;

pub(crate) const ADBC_VERSION_1_0_0: c_int = 1_000_000;
pub(crate) const ADBC_VERSION_1_1_0: c_int = 1_001_000;

/// Sentinel in [`AdbcError::vendor_code`] marking the struct as 1.1.0-sized, so the 1.1.0 fields
/// may be accessed. 1.0.0 callers leave a real vendor code or zero here.
pub(crate) const ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA: i32 = i32::MIN;

#[repr(C)]
#[derive(Debug)]
pub(crate) struct AdbcError {
    pub(crate) message: *mut c_char,
    pub(crate) vendor_code: i32,
    pub(crate) sqlstate: [c_char; 5],
    pub(crate) release: Option<unsafe extern "C" fn(*mut Self)>,
    /// Added in ADBC 1.1.0.
    pub(crate) private_data: *mut c_void,
    /// Added in ADBC 1.1.0.
    pub(crate) private_driver: *const AdbcDriver,
}

/// An empty 1.1.0 error, as the header's macro of the same name initializes one.
pub(crate) const ADBC_ERROR_INIT: AdbcError = AdbcError {
    message: std::ptr::null_mut(),
    vendor_code: ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA,
    sqlstate: [0; 5],
    release: None,
    private_data: std::ptr::null_mut(),
    private_driver: std::ptr::null(),
};

/// The 1.0.0 prefix of [`AdbcError`], for writing back into a caller-allocated struct that has no
/// room for the 1.1.0 tail.
#[repr(C)]
#[derive(Debug)]
pub(crate) struct AdbcErrorV100 {
    pub(crate) message: *mut c_char,
    pub(crate) vendor_code: i32,
    pub(crate) sqlstate: [c_char; 5],
    pub(crate) release: Option<unsafe extern "C" fn(*mut Self)>,
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct AdbcErrorDetail {
    pub(crate) key: *const c_char,
    pub(crate) value: *const u8,
    pub(crate) value_length: usize,
}

#[repr(C)]
#[derive(Debug, Default)]
pub(crate) struct AdbcDatabase {
    pub(crate) private_data: *mut c_void,
    pub(crate) private_driver: *const AdbcDriver,
}

#[repr(C)]
#[derive(Debug, Default)]
pub(crate) struct AdbcConnection {
    pub(crate) private_data: *mut c_void,
    pub(crate) private_driver: *const AdbcDriver,
}

#[repr(C)]
#[derive(Debug, Default)]
pub(crate) struct AdbcStatement {
    pub(crate) private_data: *mut c_void,
    pub(crate) private_driver: *const AdbcDriver,
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct AdbcPartitions {
    pub(crate) num_partitions: usize,
    pub(crate) partitions: *mut *const u8,
    pub(crate) partition_lengths: *mut usize,
    pub(crate) private_data: *mut c_void,
    pub(crate) release: Option<unsafe extern "C" fn(*mut Self)>,
}

/// A vtable slot taking only its object and an error.
type Call<H> = Option<unsafe extern "C" fn(*mut H, *mut AdbcError) -> AdbcStatusCode>;
/// `Adbc*SetOption` and its double and integer forms, where `T` is the value.
type SetOption<H, T> =
    Option<unsafe extern "C" fn(*mut H, *const c_char, T, *mut AdbcError) -> AdbcStatusCode>;
type SetOptionBytes<H> = Option<
    unsafe extern "C" fn(*mut H, *const c_char, *const u8, usize, *mut AdbcError) -> AdbcStatusCode,
>;
/// `Adbc*GetOption` and `Adbc*GetOptionBytes`, which share the in/out length protocol.
type GetOptionBuffer<H, T> = Option<
    unsafe extern "C" fn(
        *mut H,
        *const c_char,
        *mut T,
        *mut usize,
        *mut AdbcError,
    ) -> AdbcStatusCode,
>;
/// `Adbc*GetOptionDouble` and `Adbc*GetOptionInt`.
type GetOptionScalar<H, T> =
    Option<unsafe extern "C" fn(*mut H, *const c_char, *mut T, *mut AdbcError) -> AdbcStatusCode>;
/// A vtable slot writing one output before its error.
type Output<H, T> = Option<unsafe extern "C" fn(*mut H, *mut T, *mut AdbcError) -> AdbcStatusCode>;

/// The driver vtable. Fields before `ErrorGetDetailCount` are ADBC 1.0.0; the rest are 1.1.0. A
/// `None` slot means "not implemented".
#[repr(C)]
pub(crate) struct AdbcDriver {
    pub(crate) private_data: *mut c_void,
    pub(crate) private_manager: *const c_void,
    pub(crate) release: Option<unsafe extern "C" fn(*mut Self, *mut AdbcError) -> AdbcStatusCode>,

    // --- ADBC 1.0.0 ---
    pub(crate) DatabaseInit: Call<AdbcDatabase>,
    pub(crate) DatabaseNew: Call<AdbcDatabase>,
    pub(crate) DatabaseSetOption: SetOption<AdbcDatabase, *const c_char>,
    pub(crate) DatabaseRelease: Call<AdbcDatabase>,
    pub(crate) ConnectionCommit: Call<AdbcConnection>,
    pub(crate) ConnectionGetInfo: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const u32,
            usize,
            *mut FFI_ArrowArrayStream,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionGetObjects: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            c_int,
            *const c_char,
            *const c_char,
            *const c_char,
            *const *const c_char,
            *const c_char,
            *mut FFI_ArrowArrayStream,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionGetTableSchema: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const c_char,
            *const c_char,
            *const c_char,
            *mut FFI_ArrowSchema,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionGetTableTypes: Output<AdbcConnection, FFI_ArrowArrayStream>,
    pub(crate) ConnectionInit: Output<AdbcConnection, AdbcDatabase>,
    pub(crate) ConnectionNew: Call<AdbcConnection>,
    pub(crate) ConnectionSetOption: SetOption<AdbcConnection, *const c_char>,
    pub(crate) ConnectionReadPartition: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const u8,
            usize,
            *mut FFI_ArrowArrayStream,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionRelease: Call<AdbcConnection>,
    pub(crate) ConnectionRollback: Call<AdbcConnection>,
    pub(crate) StatementBind: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *mut FFI_ArrowArray,
            *mut FFI_ArrowSchema,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementBindStream: Output<AdbcStatement, FFI_ArrowArrayStream>,
    pub(crate) StatementExecuteQuery: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *mut FFI_ArrowArrayStream,
            *mut i64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementExecutePartitions: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *mut FFI_ArrowSchema,
            *mut AdbcPartitions,
            *mut i64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementGetParameterSchema: Output<AdbcStatement, FFI_ArrowSchema>,
    pub(crate) StatementNew: Output<AdbcConnection, AdbcStatement>,
    pub(crate) StatementPrepare: Call<AdbcStatement>,
    pub(crate) StatementRelease: Call<AdbcStatement>,
    pub(crate) StatementSetOption: SetOption<AdbcStatement, *const c_char>,
    pub(crate) StatementSetSqlQuery: Option<
        unsafe extern "C" fn(*mut AdbcStatement, *const c_char, *mut AdbcError) -> AdbcStatusCode,
    >,
    pub(crate) StatementSetSubstraitPlan: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *const u8,
            usize,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,

    // --- ADBC 1.1.0 ---
    pub(crate) ErrorGetDetailCount: Option<unsafe extern "C" fn(*const AdbcError) -> c_int>,
    pub(crate) ErrorGetDetail:
        Option<unsafe extern "C" fn(*const AdbcError, c_int) -> AdbcErrorDetail>,
    pub(crate) ErrorFromArrayStream: Option<
        unsafe extern "C" fn(*mut FFI_ArrowArrayStream, *mut AdbcStatusCode) -> *const AdbcError,
    >,
    pub(crate) DatabaseGetOption: GetOptionBuffer<AdbcDatabase, c_char>,
    pub(crate) DatabaseGetOptionBytes: GetOptionBuffer<AdbcDatabase, u8>,
    pub(crate) DatabaseGetOptionDouble: GetOptionScalar<AdbcDatabase, f64>,
    pub(crate) DatabaseGetOptionInt: GetOptionScalar<AdbcDatabase, i64>,
    pub(crate) DatabaseSetOptionBytes: SetOptionBytes<AdbcDatabase>,
    pub(crate) DatabaseSetOptionDouble: SetOption<AdbcDatabase, f64>,
    pub(crate) DatabaseSetOptionInt: SetOption<AdbcDatabase, i64>,
    pub(crate) ConnectionCancel: Call<AdbcConnection>,
    pub(crate) ConnectionGetOption: GetOptionBuffer<AdbcConnection, c_char>,
    pub(crate) ConnectionGetOptionBytes: GetOptionBuffer<AdbcConnection, u8>,
    pub(crate) ConnectionGetOptionDouble: GetOptionScalar<AdbcConnection, f64>,
    pub(crate) ConnectionGetOptionInt: GetOptionScalar<AdbcConnection, i64>,
    pub(crate) ConnectionGetStatistics: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const c_char,
            *const c_char,
            *const c_char,
            c_char,
            *mut FFI_ArrowArrayStream,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionGetStatisticNames: Output<AdbcConnection, FFI_ArrowArrayStream>,
    pub(crate) ConnectionSetOptionBytes: SetOptionBytes<AdbcConnection>,
    pub(crate) ConnectionSetOptionDouble: SetOption<AdbcConnection, f64>,
    pub(crate) ConnectionSetOptionInt: SetOption<AdbcConnection, i64>,
    pub(crate) StatementCancel: Call<AdbcStatement>,
    pub(crate) StatementExecuteSchema: Output<AdbcStatement, FFI_ArrowSchema>,
    pub(crate) StatementGetOption: GetOptionBuffer<AdbcStatement, c_char>,
    pub(crate) StatementGetOptionBytes: GetOptionBuffer<AdbcStatement, u8>,
    pub(crate) StatementGetOptionDouble: GetOptionScalar<AdbcStatement, f64>,
    pub(crate) StatementGetOptionInt: GetOptionScalar<AdbcStatement, i64>,
    pub(crate) StatementSetOptionBytes: SetOptionBytes<AdbcStatement>,
    pub(crate) StatementSetOptionDouble: SetOption<AdbcStatement, f64>,
    pub(crate) StatementSetOptionInt: SetOption<AdbcStatement, i64>,
}

/// `offsetof(struct AdbcDriver, ErrorGetDetailCount)`: the bytes a 1.0.0 caller allocated, and the
/// most `init` may write for it.
pub(crate) const ADBC_DRIVER_1_0_0_SIZE: usize =
    std::mem::offset_of!(AdbcDriver, ErrorGetDetailCount);

/// `offsetof(struct AdbcError, private_data)`: the size of a 1.0.0 error struct.
pub(crate) const ADBC_ERROR_1_0_0_SIZE: usize = std::mem::offset_of!(AdbcError, private_data);

/// `error.rs` writes the 1.0.0 prefix through [`AdbcErrorV100`], which is sound only while that
/// struct is exactly the prefix.
const _: () = assert!(ADBC_ERROR_1_0_0_SIZE == size_of::<AdbcErrorV100>());

/// `options::slot_of` reaches `private_data` by casting the handle.
const _: () = assert!(
    std::mem::offset_of!(AdbcDatabase, private_data) == 0
        && std::mem::offset_of!(AdbcConnection, private_data) == 0
        && std::mem::offset_of!(AdbcStatement, private_data) == 0
);

#[cfg(test)]
mod tests {
    use adbc_core::error::Status;

    use super::*;

    /// Check `adbc_core` status constants against the C header so a dependency change cannot
    /// silently alter the driver's ABI.
    #[test]
    fn status_codes_match_the_c_header() {
        assert_eq!(size_of::<AdbcStatusCode>(), 1);
        let header_codes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14];
        assert_eq!(
            [
                ADBC_STATUS_OK,
                ADBC_STATUS_UNKNOWN,
                ADBC_STATUS_NOT_IMPLEMENTED,
                ADBC_STATUS_NOT_FOUND,
                ADBC_STATUS_ALREADY_EXISTS,
                ADBC_STATUS_INVALID_ARGUMENT,
                ADBC_STATUS_INVALID_STATE,
                ADBC_STATUS_INVALID_DATA,
                ADBC_STATUS_INTEGRITY,
                ADBC_STATUS_INTERNAL,
                ADBC_STATUS_IO,
                ADBC_STATUS_CANCELLED,
                ADBC_STATUS_TIMEOUT,
                ADBC_STATUS_UNAUTHENTICATED,
                ADBC_STATUS_UNAUTHORIZED,
            ],
            header_codes
        );
        assert_eq!(
            [
                Status::Ok,
                Status::Unknown,
                Status::NotImplemented,
                Status::NotFound,
                Status::AlreadyExists,
                Status::InvalidArguments,
                Status::InvalidState,
                Status::InvalidData,
                Status::Integrity,
                Status::Internal,
                Status::IO,
                Status::Cancelled,
                Status::Timeout,
                Status::Unauthenticated,
                Status::Unauthorized,
            ]
            .map(AdbcStatusCode::from),
            header_codes
        );
    }

    /// Layout drift is silent memory corruption, so sizes are pinned to what `adbc.h` produces on
    /// a 64-bit target:
    ///
    /// ```text
    /// ADBC_DRIVER_1_0_0_SIZE=232   ADBC_DRIVER_1_1_0_SIZE=464
    /// ADBC_ERROR_1_0_0_SIZE=32     ADBC_ERROR_1_1_0_SIZE=48
    /// ```
    ///
    /// Interior offsets catch swapped same-shaped fields, which sizes alone miss.
    #[test]
    #[cfg(target_pointer_width = "64")]
    fn driver_vtable_matches_the_c_header() {
        assert_eq!(ADBC_DRIVER_1_0_0_SIZE, 232);
        assert_eq!(size_of::<AdbcDriver>(), 464);

        assert_eq!(std::mem::offset_of!(AdbcDriver, ConnectionGetInfo), 64);
        assert_eq!(
            std::mem::offset_of!(AdbcDriver, StatementSetSubstraitPlan),
            224
        );
        assert_eq!(std::mem::offset_of!(AdbcDriver, StatementSetOptionInt), 456);
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn the_remaining_structs_match_the_c_header() {
        assert_eq!(ADBC_ERROR_1_0_0_SIZE, 32);
        assert_eq!(size_of::<AdbcError>(), 48);
        assert_eq!(size_of::<AdbcPartitions>(), 40);
        assert_eq!(size_of::<AdbcErrorDetail>(), 24);
    }
}
