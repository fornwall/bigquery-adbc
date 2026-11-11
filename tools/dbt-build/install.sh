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
# dbt logo and the dbtf alias; --to is not deprecated, because `system update` uses it; a shell it
# cannot configure no longer fails an installation that succeeded; it reads no option or temporary
# path from the environment; and its code is restructured and condensed.
set -eu

# sh imports the environment as variables, so an inherited td would be what cleanup removes, and an
# inherited prefix or version would choose the installation.
version="" package=dbt target="" prefix="" dest="" update=false td="" tmp_file=""

cleanup() {
    if [ -n "$td" ]; then
        rm -rf "$td"
    fi
    # A file a failed installation staged but did not rename into place.
    if [ -n "$tmp_file" ]; then
        rm -f "$tmp_file"
    fi
}
trap cleanup EXIT

GRAY='\033[0;90m'
RED='\033[0;31m'
NC='\033[0m'
RELEASES_URL="${BIGQUERY_ADBC_RELEASES_URL:-https://github.com/fornwall/bigquery-adbc/releases}"
BINARY=dbt-bigquery-adbc

log() {
    printf "install.sh: %s\n" "$1"
}

log_grey() {
    printf "${GRAY}install.sh: %s${NC}\n" "$1"
}

err_and_exit() {
    printf "${RED}ERROR: %s${NC}\n" "$1"
    exit 1
}

help() {
    cat <<EOF
Usage: install.sh [options]

Options:
  --update, -u       Update to the latest or specified version
  --version VER      Install version VER (default: latest)
  --target TARGET    Override platform detection (e.g. aarch64-apple-darwin or macos-aarch64)
  --prefix PREFIX    Install under PREFIX: $BINARY in PREFIX/bin, its driver in PREFIX/lib/bigquery-adbc
                     (default: \$HOME/.local)
  --to DEST          Install $BINARY into DEST, without its drivers
  --package PACKAGE  Select package set [dbt]
  --help, -h         Show this help text
EOF
}

while [ $# -gt 0 ]; do
    case $1 in
    --update | -u) update=true ;;
    --help | -h)
        help
        exit 0
        ;;
    --version | --package | -p | --target | --prefix | --to)
        case ${2:-} in
        '' | --*) err_and_exit "$1 requires a value" ;;
        esac
        case $1 in
        --version) version=$2 ;;
        --package | -p) package=$2 ;;
        --target) target=$2 ;;
        --prefix) prefix=$2 ;;
        --to) dest=$2 ;;
        esac
        shift
        ;;
    *) err_and_exit "Unknown option: $1 - Run with --help for usage" ;;
    esac
    shift
done

if [ "$package" != dbt ]; then
    err_and_exit "Invalid package name: $package - bigquery-adbc publishes only dbt"
fi
if [ -n "$prefix" ] && [ -n "$dest" ]; then
    err_and_exit "--prefix and --to cannot be combined"
fi
for tool in awk basename curl dirname grep install mkdir mktemp sed tar; do
    command -v "$tool" >/dev/null 2>&1 || err_and_exit "need $tool (command not found)"
done
command -v sha256sum >/dev/null 2>&1 || command -v shasum >/dev/null 2>&1 ||
    err_and_exit "need sha256sum or shasum (command not found)"

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1"
    else
        shasum -a 256 "$1"
    fi
}

if [ -n "$dest" ]; then
    case $dest in
    /*) ;;
    *) dest=$PWD/$dest ;;
    esac
    lib_dir=""
else
    prefix=${prefix:-$HOME/.local}
    case $prefix in
    /*) ;;
    *) prefix=$PWD/$prefix ;;
    esac
    prefix=${prefix%/}
    dest=$prefix/bin
    lib_dir=$prefix/lib/bigquery-adbc
fi

# Accept dbt's Rust target triples as well as the release archive's platform names.
case ${target:-$(uname -s)-$(uname -m)} in
x86_64-unknown-linux-gnu | linux-x86_64 | Linux-x86_64) target=linux-x86_64 ;;
aarch64-unknown-linux-gnu | linux-aarch64 | Linux-aarch64 | Linux-arm64) target=linux-aarch64 ;;
x86_64-apple-darwin | macos-x86_64 | Darwin-x86_64) target=macos-x86_64 ;;
aarch64-apple-darwin | macos-aarch64 | Darwin-arm64) target=macos-aarch64 ;;
*) err_and_exit "Unsupported target: ${target:-$(uname -s) $(uname -m)}" ;;
esac
log_grey "Target: $target"

version=${version#v}
if [ -z "$version" ] || [ "$version" = latest ]; then
    log_grey "Checking for latest version"
    # GitHub redirects the latest release to its tag, so this needs no API or JSON parser.
    latest_url=$(curl -sLI -f -o /dev/null -w '%{url_effective}' "$RELEASES_URL/latest") ||
        err_and_exit "Failed to fetch the latest release from $RELEASES_URL/latest"
    version=${latest_url##*/}
    version=${version#v}
    case $version in
    '' | *[!0-9.]*) err_and_exit "Failed to parse the latest version from $latest_url" ;;
    esac
    log_grey "Latest available version: $version"
else
    log_grey "Requested version: $version"
fi

current_version=""
if [ -f "$dest/$BINARY" ] && [ -x "$dest/$BINARY" ]; then
    # Empty for a binary that is not this build of dbt.
    current_version=$("$dest/$BINARY" --version 2>/dev/null |
        sed -n 's/^dbt (modified with bigquery-adbc \([^)]*\)).*/\1/p' || true)
fi
if [ "$current_version" = "$version" ]; then
    log "$BINARY version $version is already installed"
    exit 0
fi
if [ -e "$dest/$BINARY" ] && [ "$update" = false ]; then
    err_and_exit "$BINARY already exists in $dest, use the --update flag to reinstall"
fi

log "Installing $BINARY to: $dest"
td=$(mktemp -d)
name=$BINARY-$version-$target
download=$RELEASES_URL/download/v$version
log_grey "Downloading: $download/$name.tar.gz"
curl -sLf -o "$td/$name.tar.gz" "$download/$name.tar.gz" ||
    err_and_exit "Failed to download $BINARY version $version for $target. Check $RELEASES_URL and try again."
curl -sLf -o "$td/dbt-SHA256SUMS" "$download/dbt-SHA256SUMS" ||
    err_and_exit "Failed to download the checksums of $BINARY version $version."
expected=$(awk -v name="$name.tar.gz" '$2 == name { print $1 }' "$td/dbt-SHA256SUMS")
actual=$(sha256 "$td/$name.tar.gz" | awk '{ print $1 }')
if [ -z "$expected" ] || [ "$actual" != "$expected" ]; then
    err_and_exit "The downloaded $name.tar.gz does not match dbt-SHA256SUMS."
fi
tar -C "$td" -xzf "$td/$name.tar.gz" ||
    err_and_exit "Failed to extract package. The downloaded archive appears to be invalid."

# 0.4.0 named the binary dbt.
binary=$td/$name/$BINARY
if [ ! -f "$binary" ]; then
    binary=$td/$name/dbt
fi
[ -f "$binary" ] || err_and_exit "Binary $BINARY missing from archive"

# Copies a file beside its destination, then renames it into place, so that a failure leaves the
# previous file intact. Usage: publish SOURCE DESTINATION MODE
publish() {
    tmp_file=$(dirname "$2")/.$(basename "$2").tmp.$$
    install -m "$3" "$1" "$tmp_file" || err_and_exit "Cannot stage $2"
    mv -f "$tmp_file" "$2" || err_and_exit "Cannot replace $2"
    tmp_file=""
}

# Replace only the archive's libraries; other packages own the rest of the directory.
if [ -n "$lib_dir" ] && [ -d "$td/$name/lib/bigquery-adbc" ]; then
    mkdir -p "$lib_dir" || err_and_exit "Cannot create $lib_dir. Make $prefix writable, or install with another --prefix."
    for library in "$td/$name/lib/bigquery-adbc"/*; do
        [ -e "$library" ] || [ -L "$library" ] || continue
        if [ ! -f "$library" ] || [ -L "$library" ]; then
            err_and_exit "$(basename "$library") in the archive is not a regular file"
        fi
        publish "$library" "$lib_dir/$(basename "$library")" 644
    done
fi
mkdir -p "$dest" || err_and_exit "Cannot create $dest"
publish "$binary" "$dest/$BINARY" 755
rm -rf "$td"
td=""

log "Successfully installed $("$dest/$BINARY" --version) to: $dest/$BINARY"
if [ -n "$current_version" ]; then
    log_grey "Successfully updated $BINARY from $current_version to $version"
fi

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

# Adds dest to PATH in the shell's startup file, with the line dbt's installer adds, unless the file
# already has it.
setup_shell_config() {
    case $(basename "${SHELL:-sh}") in
    zsh) config_file=$HOME/.zshrc line="export PATH=\"\$PATH:$dest\"" ;;
    bash) config_file=$HOME/.bashrc line="export PATH=\"\$PATH:$dest\"" ;;
    fish) config_file=$HOME/.config/fish/config.fish line="fish_add_path $dest" ;;
    *)
        log_grey "NOTE: Failed to identify config file."
        show_path_instructions
        return
        ;;
    esac
    if ! grep -F -q "$line" "$config_file" 2>/dev/null; then
        if ! printf '\n# Added by the bigquery-adbc dbt installer\n%s\n' "$line" >>"$config_file"; then
            log_grey "NOTE: Failed to modify $config_file."
            show_path_instructions
            return
        fi
        log_grey "Added $dest to PATH in $config_file"
    fi
    case ":$PATH:" in
    *":$dest:"*) ;;
    *)
        log "To use $BINARY in this session, run:"
        log "    source $config_file"
        log ""
        log "The PATH change will be permanent for new terminal sessions."
        ;;
    esac
}

# The installation has succeeded, so a shell it cannot configure must not fail it.
setup_shell_config || true
