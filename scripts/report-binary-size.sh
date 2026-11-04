#!/usr/bin/env bash
# Report stable, machine-readable size data for the release cdylib.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/report-binary-size.sh [OPTIONS]

Build and inspect the native release cdylib, or inspect an existing artifact.

Options:
  --artifact PATH      Inspect PATH instead of building the default artifact.
  --symbols-from PATH  Read symbol attribution from a matching unstripped artifact.
  --profile NAME       Build profile, recorded as context (default: release).
  --top COUNT          Number of top crates and symbols to print (default: 20).
  -h, --help           Show this help.

The report is tab-separated. Context rows describe the checkout and tools running
this command, not the provenance of an artifact supplied with --artifact.
EOF
}

fail() {
    printf 'binary-size: %s\n' "$*" >&2
    exit 2
}

find_tool() {
    local candidate
    for candidate in "$@"; do
        if command -v "$candidate" >/dev/null 2>&1; then
            command -v "$candidate"
            return
        fi
    done
    return 1
}

absolute_path() {
    local path=$1
    local directory
    local filename
    directory=$(dirname -- "$path")
    filename=$(basename -- "$path")
    (cd -- "$directory" && printf '%s/%s\n' "$PWD" "$filename")
}

file_bytes() {
    wc -c < "$1" | tr -d '[:space:]'
}

file_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum -- "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 -- "$1" | awk '{print $1}'
    else
        printf 'unavailable\n'
    fi
}

display_path() {
    local path=$1
    case "$path" in
        "$repo_root"/*) printf '%s\n' "${path#"$repo_root"/}" ;;
        *) printf '%s\n' "$path" ;;
    esac
}

artifact=
symbol_artifact=
profile=release
top_count=20
build_mode=built

while (($# > 0)); do
    case "$1" in
        --artifact)
            (($# >= 2)) || fail '--artifact needs a path'
            artifact=$2
            build_mode=provided
            shift 2
            ;;
        --symbols-from)
            (($# >= 2)) || fail '--symbols-from needs a path'
            symbol_artifact=$2
            shift 2
            ;;
        --profile)
            (($# >= 2)) || fail '--profile needs a name'
            profile=$2
            shift 2
            ;;
        --top)
            (($# >= 2)) || fail '--top needs a count'
            top_count=$2
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *) fail "unknown argument: $1" ;;
    esac
done

[[ $top_count =~ ^[1-9][0-9]*$ ]] || fail '--top must be a positive integer'

caller_directory=$PWD
script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source-path=SCRIPTDIR source=lib/cargo-driver-library.sh
. "$script_directory/lib/cargo-driver-library.sh"
if ! repo_root=$(git -C "$script_directory/.." rev-parse --show-toplevel 2>/dev/null); then
    # An exported tree carries no repository, so fall back to the checkout layout
    # and report the revision as unavailable below.
    repo_root=$(cd -- "$script_directory/.." && pwd)
fi
cd -- "$repo_root"

rustc_details=$(rustc -Vv)
rustc_host=$(awk -F ': ' '$1 == "host" {print $2}' <<< "$rustc_details")
[[ -n $rustc_host ]] || fail 'rustc did not report its host target'

if [[ $build_mode == built ]]; then
    artifact=$(cargo_build_driver_library "$repo_root/Cargo.toml" --profile "$profile") ||
        fail 'could not build the driver library'
elif [[ $artifact != /* ]]; then
    artifact=$caller_directory/$artifact
fi

[[ -f $artifact ]] || fail "artifact does not exist: $artifact"
artifact=$(absolute_path "$artifact")

if [[ -n $symbol_artifact ]]; then
    if [[ $symbol_artifact != /* ]]; then
        symbol_artifact=$caller_directory/$symbol_artifact
    fi
    [[ -f $symbol_artifact ]] || fail "symbol artifact does not exist: $symbol_artifact"
    symbol_artifact=$(absolute_path "$symbol_artifact")
else
    symbol_artifact=$artifact
fi

size_tool=$(find_tool llvm-size size) || fail 'llvm-size or size is required'
strip_tool=$(find_tool llvm-strip strip) || fail 'llvm-strip or strip is required'
nm_tool=$(find_tool llvm-nm nm) || fail 'llvm-nm or nm is required'

temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/bigquery-adbc-size.XXXXXX")
stripped_artifact=$temporary_directory/stripped
sections_file=$temporary_directory/sections.tsv
nm_file=$temporary_directory/nm.txt
text_symbols_file=$temporary_directory/text-symbols.tsv
crates_file=$temporary_directory/crates.tsv

cleanup() {
    rm -rf -- "$temporary_directory"
}
trap cleanup EXIT

cp -- "$artifact" "$stripped_artifact"
chmod u+w "$stripped_artifact"
if ! "$strip_tool" --strip-unneeded "$stripped_artifact" 2>/dev/null; then
    "$strip_tool" -S "$stripped_artifact" || fail "could not strip a copy with $strip_tool"
fi

"$size_tool" --format=sysv --radix=10 "$artifact" |
    awk '$2 ~ /^[0-9]+$/ && $1 != "Total" {bytes[$1] += $2}
         END {for (section in bytes) print section "\t" bytes[section]}' |
    LC_ALL=C sort -t $'\t' -k1,1 > "$sections_file"
[[ -s $sections_file ]] || fail "$size_tool did not report any sections"

if git_commit=$(git rev-parse HEAD 2>/dev/null); then
    if [[ -n $(git status --porcelain --untracked-files=normal) ]]; then
        git_tree=dirty
    else
        git_tree=clean
    fi
else
    git_commit=unavailable
    git_tree=unavailable
fi
rustc_version=$(awk -F ': ' '$1 == "release" {print $2}' <<< "$rustc_details")
rustc_commit=$(awk -F ': ' '$1 == "commit-hash" {print $2}' <<< "$rustc_details")
cargo_version=$(cargo -V | awk '{print $2}')
binary_format=$(file -b -- "$artifact" 2>/dev/null || printf 'unavailable')

printf 'binary_size_report_version\t2\n'
printf 'context\tgit_commit\t%s\n' "$git_commit"
printf 'context\tgit_tree\t%s\n' "$git_tree"
printf 'context\trustc_version\t%s\n' "$rustc_version"
printf 'context\trustc_commit\t%s\n' "$rustc_commit"
printf 'context\trustc_host\t%s\n' "$rustc_host"
printf 'context\tcargo_version\t%s\n' "$cargo_version"
printf 'context\tcargo_build_target\t%s\n' "${CARGO_BUILD_TARGET:-unset}"
printf 'context\trequested_profile\t%s\n' "$profile"
printf 'identity\tbuild_mode\t%s\n' "$build_mode"
printf 'identity\tartifact\t%s\n' "$(display_path "$artifact")"
printf 'identity\tbinary_format\t%s\n' "$binary_format"
printf 'context\tsize_tool\t%s\n' "$(basename -- "$size_tool")"
printf 'context\tstrip_tool\t%s\n' "$(basename -- "$strip_tool")"
printf 'context\tnm_tool\t%s\n' "$(basename -- "$nm_tool")"
printf 'metric\tartifact_bytes\t%s\n' "$(file_bytes "$artifact")"
printf 'metric\tartifact_sha256\t%s\n' "$(file_sha256 "$artifact")"
printf 'metric\tstripped_bytes\t%s\n' "$(file_bytes "$stripped_artifact")"
printf 'metric\tstripped_sha256\t%s\n' "$(file_sha256 "$stripped_artifact")"
awk -v OFS='\t' '{print "section", $0}' "$sections_file"

if "$nm_tool" --print-size --size-sort --demangle --format=posix --radix=d \
    "$symbol_artifact" > "$nm_file" 2>/dev/null; then
    awk '
        NF >= 4 {
            type = $(NF - 2)
            if (type !~ /^[tTwW]$/ || $NF !~ /^[0-9]+$/ || $NF == 0) next
            name = $1
            for (field = 2; field <= NF - 3; field++) name = name " " $field
            gsub(/\t/, " ", name)
            print $NF "\t" type "\t" name
        }
    ' "$nm_file" | LC_ALL=C sort -t $'\t' -k1,1nr -k3,3 > "$text_symbols_file"
fi

printf 'attribution\tsymbol_source\t%s\n' "$(display_path "$symbol_artifact")"
printf 'attribution\tsymbol_source_bytes\t%s\n' "$(file_bytes "$symbol_artifact")"
printf 'attribution\tsymbol_source_sha256\t%s\n' "$(file_sha256 "$symbol_artifact")"
if [[ -s $text_symbols_file ]]; then
    named_text_bytes=$(awk -F '\t' '{total += $1} END {printf "%.0f", total}' "$text_symbols_file")
    printf 'attribution\tstatus\tavailable\n'
    printf 'attribution\tnamed_text_bytes\t%s\n' "$named_text_bytes"
    awk -F '\t' '
        function crate_of(name, candidate) {
            sub(/^<+/, "", name)
            if (match(name, /^[A-Za-z_][A-Za-z0-9_]*/)) {
                candidate = substr(name, RSTART, RLENGTH)
                if (substr(name, RLENGTH + 1, 2) == "::" && candidate !~ /^__/) {
                    return candidate
                }
            }
            return "[native-or-unattributed]"
        }
        {
            crate = crate_of($3)
            bytes[crate] += $1
            symbols[crate]++
        }
        END {
            for (crate in bytes) print bytes[crate] "\t" symbols[crate] "\t" crate
        }
    ' "$text_symbols_file" | LC_ALL=C sort -t $'\t' -k1,1nr -k3,3 > "$crates_file"

    head -n "$top_count" "$crates_file" | awk -v OFS='\t' '{print "crate", NR, $0}'
    head -n "$top_count" "$text_symbols_file" |
        awk -v OFS='\t' '{print "symbol", NR, $0}'
else
    printf 'attribution\tstatus\tunavailable-stripped-or-no-sized-text-symbols\n'
fi
