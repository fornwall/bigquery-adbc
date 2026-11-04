# shellcheck shell=bash
# Resolve the single wheel a build just produced. Sourced, not executed.

# Usage: built_wheel <directory>
# Prints the wheel's path, or explains on stderr and returns non-zero.
built_wheel() (
    local directory=$1
    local wheels
    shopt -s nullglob
    wheels=("$directory"/*.whl)

    if [[ ${#wheels[@]} -ne 1 ]]; then
        echo "expected exactly one built wheel in $directory, found ${#wheels[@]}" >&2
        if [[ ${#wheels[@]} -gt 1 ]]; then
            printf '   %s\n' "${wheels[@]}" >&2
        fi
        return 1
    fi
    printf '%s\n' "${wheels[0]}"
)
