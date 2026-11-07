//! Synthetic credential material shared by token-signing tests.

use serde_json::{Value as JsonValue, json};

/// A service-account key document for `client_email` that can really sign a token.
pub(crate) fn service_account_json(client_email: &str) -> JsonValue {
    json!({
        "type": "service_account",
        "project_id": "data-project",
        "private_key_id": "0123456789abcdef",
        "private_key": TEST_PRIVATE_KEY,
        "client_email": client_email,
    })
}

/// A throwaway 2048-bit RSA key, belonging to no account.
///
/// It has to be a real key: service-account credentials mint their bearer token by signing a
/// JWT locally, so an unparsable key would fail the request before it reached the mock.
const TEST_PRIVATE_KEY: &str = concat!(
    "-----BEGIN PRIVATE KEY-----\n",
    "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCYtPWhLUVCR0ox\n",
    "dt35g42+qK+Lf+CbCdrANlWnZA2nos/XAGximbwkbj1lTknAGuD6NEl2TrCgDY0w\n",
    "YopV0wT5DYzaDIv/lwTCXLkTB4K74zFW1O7EOdfRidhVoZxbfAHlU4uECvNGlhhD\n",
    "SkQBUbd4tR+AFZh1jiMRNWDZSU+Q4j0FfUTriO/C+/hpsYD6HJSU6JVLeOP5fQ0A\n",
    "qTH0o8JU8WVSpSlUaKSyO/upxMOMktBvAoHteK/Cg5ORLwxxfv1UV5+1uzZTZqzv\n",
    "3/NXYxkhB/Qrq7gOd0y84wd0F7p8GI5SqyD4pqZjFhIIX8jV6pS8oOxjkw/3VHXg\n",
    "MTkUCTyxAgMBAAECggEAKkFEiJ1kwULFwmswYAcpdTzrGlA/v9bZu38tc1yptJ3G\n",
    "IimhVCVTHMeOYJZprOkjIMjQHRcII7msPFvACKCUCzBXLczz/jz1Ga0bnyrgX4ot\n",
    "0GeqTAzvbbFVddh60gu94I2eGOMIzxrgStpld/H7MUD5HvGa1siGZpAdJPL2u9GY\n",
    "z8y3M1MrHE4lqv55zrvtlVPXWF/PFoXf6OS59P9BX7tRHCIubVZVtCA6+FXOIcch\n",
    "qaMGxywpe2jG1bjq5kyBEY+ZZw7m2aBkhkOEn7i9izz2o3M76WOTOIO3AmOuzP4L\n",
    "/8MfZDI6Q6otOWolwA+z+sYxIgnmI+/tdIMOFm8U8wKBgQDR6f/FOYez1bVmqlcp\n",
    "L0C55PtQmjKv/VRm5jJw3pkCq+Z9brSkTFGWidlYaqcKmR2hhvQAyInYgEUab6hF\n",
    "q92+Bqp8RcOA/lFbUkBVfvHQBReet6nM976V/SLstuwP1yREwc0kTAGGJ1Cy1ntq\n",
    "01ZGx2u01MzrOmahmSvvqWgtawKBgQC6O7CsB0PEZpr7Egon1buAUoZFKY+PrPNG\n",
    "PzcDIMvC+IueD3/qqGHGTdogoWg1vBhhcMu4KVTIXyT3w+L2zuYcUG09UXUAs4OG\n",
    "q9hzwMcY9Q3cVv4JxQ5vq4PVK5FcYr+3fRLhWNFyKyY8RM9v1FrOdK3636fIblN4\n",
    "o7Q2g61JUwKBgFmOM4i5GqQmsNW0JqtzdXHZtz6UCrv6oF+mJi7xSOZS4GrzKC5Q\n",
    "QR6wgQGvvsROvs2C/ea3p9euIc09hdTzlg7UWTfbq6UE20DSvo+7MP9GrOUSBmdj\n",
    "6vOio5vKDrxTVS3gYNzzcFMDwvIJf2376/sWaWUkxKg2qkKXTp9Y7xExAoGABVEU\n",
    "7yRZ8+51+DsAnTiF3Nj+SlUwawql+1hsqqMyDgPogH+yyDE8cVvhoVLaVcZj78w3\n",
    "lK9hY0MMHCK0hRVNDnvhUoHT1ExLsfmP+WYEiRrd5W96Wy8MBXYhEmHnCXssPAHP\n",
    "Gy3c9OR0d/4C6g9m9EPyf5oJATjdGzGZQIEr3k8CgYBixCLvBoIbDdWsNVa7VGr/\n",
    "AtQ7FfpqpfB6m2Wj9Ifl+ozg2AHGGv6e0BhYXLOAPpjSv5WOvhSdCJLKu4q4YVR/\n",
    "wWwxvJqcC6ncvr05sll3r3UeJqt7/sAjo9UdnrF1vfLSbDIX10ByBTEqwtrAMTpC\n",
    "Ds2mzQh6O8IqdHZt3REjQg==\n",
    "-----END PRIVATE KEY-----\n",
);
