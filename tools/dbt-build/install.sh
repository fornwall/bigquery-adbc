#!/bin/sh
# Installs dbt-bigquery-adbc, the dbt that tools/dbt-build builds, from bigquery-adbc's GitHub
# releases. `dbt-bigquery-adbc system update` runs the latest release's copy of this script.
# Usage: curl -fsSL https://raw.githubusercontent.com/fornwall/bigquery-adbc/main/tools/dbt-build/install.sh | sh -s -- --update
#
# Adapted from dbt's installer, crates/dbt-common/assets/install.sh at dbt-labs/dbt commit
# 327f5269737bfec2440f10e07702f8280dfec1e9, Copyright 2026 dbt Labs, Inc., licensed under the
# Apache License, Version 2.0. Changes: it downloads from bigquery-adbc's releases and checks them
# against dbt-SHA256SUMS; it names the binary dbt-bigquery-adbc, so that it does not replace dbt;
# it reads the latest version from GitHub instead of with jq; it drops dbt-db-runner, dbt-lsp, the
# dbt logo and the dbtf alias; --to is not deprecated, because `system update` uses it; and a shell
# it cannot configure no longer fails an installation that succeeded.

# Exit immediately if a command exits with a non-zero status.
# Treat unset variables as an error when substituting.
set -eu

# Function to be called on script exit for cleanup purposes.
cleanup() {
    # Clean up any temporary directories
    if [ -n "${td:-}" ]; then
        rm -rf "$td"
    fi
    # Remove any in-progress staging binary left behind by a failed install
    if [ -n "${tmp_bin:-}" ]; then
        rm -f "$tmp_bin"
    fi
}

# Register the cleanup function to be called on EXIT.
trap cleanup EXIT

# Color codes
GRAY='\033[0;90m'
RED='\033[0;31m'
NC='\033[0m' # No Color

TARGET_VERSION=""
RELEASES_URL="${BIGQUERY_ADBC_RELEASES_URL:-https://github.com/fornwall/bigquery-adbc/releases}"
BINARY="dbt-bigquery-adbc"

log() {
    printf "install.sh: %s\n" "$1"
}

log_grey() {
    printf "${GRAY}install.sh: %s${NC}\n" "$1"
}

# debug messages only show where there's an error in the installation
# when the install/update is triggered from dbt-bigquery-adbc system update
log_debug() {
    printf "${GRAY}install.sh: %s${NC}\n" "$1" 2>&1
}

err_and_exit() {
    Message="${1:-}"
    AdditionalInfo="${2:-}"

    info_suffix=""
    if [ -n "$AdditionalInfo" ]; then
        info_suffix=" - $AdditionalInfo"
    fi

    printf "${RED}ERROR: %s%s ${NC}\n" "$Message" "$info_suffix"
    exit 1
}

need() {
    if ! command -v "$1" >/dev/null 2>&1; then
        err_and_exit "need $1 (command not found)"
    fi
}

help() {
    echo "Usage: install.sh [options]"
    echo ""
    echo "Options:"
    echo "  --update, -u       Update to the latest or specified version"
    echo "  --version VER      Install version VER (default: latest)"
    echo "  --target TARGET    Override platform detection (e.g. aarch64-apple-darwin or macos-aarch64)"
    echo "  --prefix PREFIX    Install under PREFIX: $BINARY in PREFIX/bin, its driver in PREFIX/lib/bigquery-adbc"
    echo "                     (default: \$HOME/.local)"
    echo "  --to DEST          Install $BINARY into DEST, without its drivers"
    echo "  --help, -h         Show this help text"
    echo "  --package PACKAGE  Select package set [dbt]"
}

update=false
while [ $# -gt 0 ]; do
    case $1 in
    --update | -u)
        update=true
        ;;
    --help | -h)
        help
        exit 0
        ;;
    --version | --package | -p | --target | --prefix | --to)
        [ "$#" -ge 2 ] && [ -n "$2" ] || err_and_exit "$1 requires a value"
        case "$2" in
            --*) err_and_exit "$1 requires a value" ;;
        esac
        case "$1" in
            --version) version=$2 ;;
            --package | -p) package=$2 ;;
            --target) target=$2 ;;
            --prefix) prefix=$2 ;;
            --to) dest=$2 ;;
        esac
        shift
        ;;
    *) err_and_exit "Unknown option: $1" "Run with --help for usage" ;;

    esac
    shift
done

check_dependencies() {
    # Dependencies
    need awk
    need basename
    need curl
    need dirname
    need grep
    need install
    need mkdir
    need mktemp
    need sed
    need tar
    if ! command -v sha256sum >/dev/null 2>&1; then
        need shasum
    fi

    if [ -n "${prefix:-}" ] && [ -n "${dest:-}" ]; then
        err_and_exit "--prefix and --to cannot be combined"
    fi
    if [ -n "${dest:-}" ]; then
        # Convert relative path to absolute
        case "$dest" in
            /*) ;; # Already absolute path
            *) dest="$PWD/$dest" ;; # Convert relative to absolute
        esac
        lib_dir=""
    else
        prefix="${prefix:-$HOME/.local}"
        case "$prefix" in
            /*) ;;
            *) prefix="$PWD/$prefix" ;;
        esac
        prefix="${prefix%/}"
        dest="$prefix/bin"
        lib_dir="$prefix/lib/bigquery-adbc"
    fi
}

# Check version of an installed binary
# Usage: check_binary_version binary_path
# Returns: the bigquery-adbc version it reports, or an empty string for none or another dbt
check_binary_version() {
    binary_path="$1"
    installed_version=""

    if [ -f "$binary_path" ] && [ -x "$binary_path" ]; then
        installed_version=$("$binary_path" --version 2>/dev/null |
            sed -n 's/^dbt (modified with bigquery-adbc \([^)]*\)).*/\1/p' || echo "")
    fi

    echo "$installed_version"
}

# Compare installed version with target version
# Usage: compare_versions current_version target_version requested_version package
# Returns: 0 if versions match, 1 if they don't match
compare_versions() {
    compare_current_version="$1"
    compare_target_version="$2"
    compare_requested_version="$3"
    compare_package="$4"

    if [ "$compare_current_version" = "$compare_target_version" ]; then
        if [ -z "$compare_requested_version" ]; then
            log "Latest $compare_package version $compare_target_version is already installed"
        else
            log "$compare_package version $compare_target_version is already installed"
        fi
        return 0
    fi

    return 1
}

archive_name() {
    echo "$BINARY-$1-$2.tar.gz"
}

# validate if a version has a build for the target
# Usage: check_version_exists version target
# Returns: 0 if version exists, error if it doesn't
check_version_exists() {
    url="$RELEASES_URL/download/v$1/$(archive_name "$1" "$2")"
    log_debug "Checking if version $1 exists: $url"
    if ! curl -sLI -f "$url" >/dev/null 2>&1; then
        err_and_exit "Version $1 has no $BINARY build for $2. Please check $RELEASES_URL and try again."
    fi
    return 0
}

# Detect and set target platform
# Usage: detect_target
# Returns: target platform string
detect_target_platform() {
    cpu_arch_target=$(uname -m)
    operating_system=$(uname -s | tr '[:upper:]' '[:lower:]')
    target=""

    if [ "$operating_system" = "linux" ]; then
        if [ "$cpu_arch_target" = "arm64" ] || [ "$cpu_arch_target" = "aarch64" ]; then
            target="linux-aarch64"
        elif [ "$cpu_arch_target" = "x86_64" ]; then
            target="linux-x86_64"
        else
            err_and_exit "Unsupported CPU Architecture: $cpu_arch_target"
        fi
    elif [ "$operating_system" = "darwin" ]; then
        if [ "$cpu_arch_target" = "arm64" ]; then
            target="macos-aarch64"
        elif [ "$cpu_arch_target" = "x86_64" ]; then
            target="macos-x86_64"
        else
            err_and_exit "Unsupported CPU Architecture: $cpu_arch_target"
        fi
    else
        err_and_exit "Unsupported OS: $operating_system"
    fi

    echo "$target"
}

show_path_instructions() {
    log_grey ""
    log_grey "NOTE: $dest may not be in your PATH."
    log_grey "To add it permanently, run one of these commands depending on your shell:"
    log_grey "  For bash/zsh: echo 'export PATH=\"\$PATH:$dest\"' >> ~/.bashrc  # or ~/.zshrc"
    log_grey ""
    log_grey "To use $BINARY in this session immediately, run:"
    log_grey "    export PATH=\"\$PATH:$dest\""
    log_grey ""
    log_grey "Then restart your terminal or run 'source ~/.bashrc' (or equivalent) for permanent changes"
}

# Setup shell config file and PATH
# Usage: setup_shell_config dest_path
setup_shell_config() {
    dest="$1"
    config_file=""
    shell_name=""

    # Detect shell and config file early
    if [ -n "${SHELL:-}" ]; then
        shell_name=$(basename "$SHELL")
    else
        if [ -f "$HOME/.bashrc" ]; then
            shell_name="bash"
        elif [ -f "$HOME/.profile" ]; then
            shell_name="sh"
        else
            shell_name=$(ps -p "$PPID" -o comm= | sed 's/.*\///')
        fi
    fi

    # Set config file based on shell
    if [ "$shell_name" = "zsh" ]; then
        config_file="$HOME/.zshrc"
    elif [ "$shell_name" = "bash" ]; then
        config_file="$HOME/.bashrc"
    elif [ "$shell_name" = "fish" ]; then
        config_file="$HOME/.config/fish/config.fish"
    fi

    if [ -z "$config_file" ]; then
        log_grey "NOTE: Failed to identify config file."
        show_path_instructions
        return 1
    fi

    # check if the config file exists or not and create it if it doesn't
    if [ ! -f "$config_file" ]; then
        if touch "$config_file"; then
            log_debug "Created config file $config_file"
        else
            log_grey "Note: Failed to create config file $config_file."
            show_path_instructions
            return 1
        fi
    fi

    # dbt's installer adds the same lines for the same directory.
    needs_config_path_update=false
    if [ "$shell_name" != "fish" ]; then
        if ! grep -F -q "export PATH=\"\$PATH:$dest\"" "$config_file" 2>/dev/null; then
            needs_config_path_update=true
        fi
    elif ! grep -F -q "fish_add_path $dest" "$config_file" 2>/dev/null; then
        needs_config_path_update=true
    fi

    needs_path_update=false
    case ":$PATH:" in
        *":$dest:"*) ;;
        *) needs_path_update=true ;;
    esac

    if [ "$shell_name" != "fish" ]; then
        if [ "$needs_config_path_update" = true ]; then
            {
                echo "" >> "$config_file" && \
                echo "# Added by the bigquery-adbc dbt installer" >> "$config_file" && \
                echo "export PATH=\"\$PATH:$dest\"" >> "$config_file" && \
                log_debug "Added $dest to PATH in $config_file"
            } || {
                log_grey "NOTE: Failed to modify $config_file."
                show_path_instructions
                return 1
            }
        fi
    else
        if [ "$needs_config_path_update" = true ]; then
            {
                echo "fish_add_path $dest" >> "$config_file"
                log_debug "Added $dest to PATH in $config_file"
            } || {
                log_grey "NOTE: Failed to modify $config_file."
                show_path_instructions
                return 1
            }
        fi
    fi

    if [ "$needs_path_update" = true ]; then
        log "To use $BINARY in this session, run:" && \
        log "    source $config_file" && \
        log "" && \
        log "The PATH change will be permanent for new terminal sessions."
    fi

    return 0
}

# Determine version to install
# Usage: determine_version [specific_version]
# Returns: target version string
determine_version() {
    specific_version="${1#v}"

    TARGET_VERSION=""

    if [ -z "$specific_version" ] || [ "$specific_version" = "latest" ]; then
        log_grey "Checking for latest version"
        # GitHub redirects the latest release to its tag, so this needs no API or JSON parser.
        latest_url=$(curl -sLI -f -o /dev/null -w '%{url_effective}' "$RELEASES_URL/latest") ||
            err_and_exit "Failed to fetch the latest release from $RELEASES_URL/latest"
        TARGET_VERSION="${latest_url##*/}"
        TARGET_VERSION="${TARGET_VERSION#v}"
        case "$TARGET_VERSION" in
            '' | *[!0-9.]*) err_and_exit "Failed to parse the latest version from $latest_url" ;;
        esac
        log_grey "Latest available version: $TARGET_VERSION"
    else
        TARGET_VERSION="$specific_version"
        log_grey "Requested version: $TARGET_VERSION"
    fi
}

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{ print $1 }'
    else
        shasum -a 256 "$1" | awk '{ print $1 }'
    fi
}

# Replace only the archive's libraries; other packages own the rest.
install_sidecar_packages() {
    source_dir="$1"
    destination_dir="$2"
    mkdir -p "$destination_dir" || err_and_exit "Cannot create $destination_dir. Make $(dirname "$(dirname "$destination_dir")") writable, or install with another --prefix."
    for library in "$source_dir"/*; do
        [ -e "$library" ] || [ -L "$library" ] || continue
        { [ -f "$library" ] && [ ! -L "$library" ]; } || err_and_exit "Invalid sidecar file"
        library_leaf=$(basename "$library")
        tmp_bin="$destination_dir/.$library_leaf.tmp.$$"
        install -m 644 "$library" "$tmp_bin" || err_and_exit "Cannot stage sidecar"
        mv -f "$tmp_bin" "$destination_dir/$library_leaf" || err_and_exit "Cannot publish sidecar"
        tmp_bin=""
    done
}

# Install a package
# Usage: install_package version target dest update
# Returns: 0 on success, 1 on failure
install_package() {
    version="$1"
    target="$2"
    dest="$3"
    update="$4"
    td=""
    url=""
    current_version=""

    # Check if already installed and get version
    current_version=$(check_binary_version "$dest/$BINARY")
    if [ -n "$current_version" ] && [ "$current_version" = "$version" ]; then
        log "$BINARY version $version is already installed"
        return 0
    elif [ -n "$current_version" ]; then
        log_debug "Current installed $BINARY version: $current_version"
    fi

    # If we get here, version is different, so check if we can proceed
    if [ -e "$dest/$BINARY" ] && [ "$update" = false ]; then
        err_and_exit "$BINARY already exists in $dest, use the --update flag to reinstall"
    fi

    log "Installing $BINARY to: $dest"
    # Create the directory if it doesn't exist
    mkdir -p "$dest"

    # Create temp directory
    td=$(mktemp -d || mktemp -d -t tmp)

    name="$BINARY-$version-$target"
    url="$RELEASES_URL/download/v$version/$name.tar.gz"
    log_debug "Downloading: $url"
    archive="$td/package.tar.gz"
    if ! curl -sL -f -o "$archive" "$url"; then
        err_and_exit "Failed to download $BINARY version $version for $target."
    fi
    if ! curl -sL -f -o "$td/dbt-SHA256SUMS" "$RELEASES_URL/download/v$version/dbt-SHA256SUMS"; then
        err_and_exit "Failed to download the checksums of $BINARY version $version."
    fi
    expected=$(awk -v name="$name.tar.gz" '$2 == name { print $1 }' "$td/dbt-SHA256SUMS")
    actual=$(sha256 "$archive")
    if [ -z "$expected" ] || [ "$actual" != "$expected" ]; then
        err_and_exit "The downloaded $name.tar.gz does not match dbt-SHA256SUMS."
    fi

    # Download and extract
    if ! tar -C "$td" -xzf "$archive"; then
        err_and_exit "Failed to extract package. The downloaded archive appears to be invalid."
    fi
    rm -f "$archive"

    # Check if any files were extracted
    if [ ! -d "$td/$name" ] || [ -z "$(ls -A "$td/$name")" ]; then
        err_and_exit "No files were extracted from the archive"
    fi

    # 0.4.0 named the binary dbt.
    binary="$td/$name/$BINARY"
    if [ ! -f "$binary" ]; then
        binary="$td/$name/dbt"
    fi
    [ -f "$binary" ] || err_and_exit "Binary $BINARY missing from archive"
    if [ -n "${lib_dir:-}" ] && [ -d "$td/$name/lib/bigquery-adbc" ]; then
        install_sidecar_packages "$td/$name/lib/bigquery-adbc" "$lib_dir"
    fi
    tmp_bin="$dest/.${BINARY}.tmp.$$"
    install -m 755 "$binary" "$tmp_bin" || err_and_exit "Failed to stage $BINARY"
    mv -f "$tmp_bin" "$dest/$BINARY" || err_and_exit "Failed to replace $BINARY"
    tmp_bin=""

    rm -rf "$td"
    td=""

    log "Successfully installed $("$dest/$BINARY" --version) to: $dest/$BINARY"

    if [ "$update" = true ] && [ -n "$current_version" ]; then
        log_grey "Successfully updated $BINARY from $current_version to $version"
    fi

    return 0
}

# Install packages based on selection
# Usage: install_packages package target_version target dest update
# Returns: 0 on success, 1 on failure
install_packages() {
    package="$1"
    target_version="$2"
    target="$3"
    dest="$4"
    update="$5"
    current_dbt_version=""

    if [ "$package" != "dbt" ]; then
        err_and_exit "Invalid package name: $package" "bigquery-adbc publishes only dbt"
    fi

    # Check if versions match
    current_dbt_version=$(check_binary_version "$dest/$BINARY")
    if compare_versions "$current_dbt_version" "$target_version" "$requested_version" "$BINARY"; then
        return 0
    fi

    if ! install_package "$target_version" "$target" "$dest" "$update"; then
        return 1
    fi

    # A shell it cannot configure prints instructions instead; the installation still succeeded.
    setup_shell_config "$dest" || true

    return 0
}

main() {
    # Initialize variables with defaults if not set by command-line parsing
    # If not provided on command line, they will default to empty or a specific value here.
    package="${package:-dbt}"
    requested_version="${version:-}"
    target="${target:-}"
    dest="${dest:-}"
    update="${update:-false}"

    if [ "$package" != "dbt" ]; then
        err_and_exit "Invalid package name: $package" "bigquery-adbc publishes only dbt"
    fi

    check_dependencies

    # Determine version to install
    determine_version "$requested_version"
    target_version="$TARGET_VERSION"

    target_platform="${target:-$(detect_target_platform)}"
    # Accept dbt's Rust target triples as well as the release archive's platform names.
    case "$target_platform" in
        x86_64-unknown-linux-gnu | linux-x86_64) target_platform=linux-x86_64 ;;
        aarch64-unknown-linux-gnu | linux-aarch64) target_platform=linux-aarch64 ;;
        x86_64-apple-darwin | macos-x86_64) target_platform=macos-x86_64 ;;
        aarch64-apple-darwin | macos-aarch64) target_platform=macos-aarch64 ;;
        *) err_and_exit "Unsupported target: $target_platform" ;;
    esac
    log_grey "Target: $target_platform"

    check_version_exists "$target_version" "$target_platform"

    install_packages "$package" "$target_version" "$target_platform" "$dest" "$update"

}


main
