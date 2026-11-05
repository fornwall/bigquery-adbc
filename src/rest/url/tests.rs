//! Resource-path and individual-component escaping.

use super::{escape_resource_path, escape_url_component};

#[test]
fn both_escapes_keep_a_value_from_reshaping_the_url_it_is_placed_in() {
    for (value, as_path, as_component) in [
        // A resource name is a path of segments; a component is one opaque value.
        (
            "projects/p/locations/eu",
            "projects/p/locations/eu",
            "projects%2Fp%2Flocations%2Feu",
        ),
        // Everything that could start a new query, fragment or parameter, whichever it is in.
        ("a?b", "a%3Fb", "a%3Fb"),
        ("a#b", "a%23b", "a%23b"),
        ("a&b", "a%26b", "a%26b"),
        ("a=b", "a=b", "a%3Db"),
        ("100%", "100%25", "100%25"),
        // A space and a `+`, which a form-encoded reader would otherwise read as each other.
        ("a b", "a%20b", "a%20b"),
        ("a+b", "a%2Bb", "a%2Bb"),
        // A BigQuery job or dataset name is written with these, and only a component has to
        // escape them: `project:location.job` survives a path unchanged.
        ("p:EU.job_1", "p:EU.job_1", "p%3AEU.job_1"),
        ("a,b;c@d", "a,b;c@d", "a%2Cb%3Bc%40d"),
        ("!$'()*", "!$'()*", "%21%24%27%28%29%2A"),
        // Delimiters no URL syntax gives a meaning, escaped by both so neither can be read as
        // one by a lenient parser.
        (
            "\"<>[\\]^`{|}",
            "%22%3C%3E%5B%5C%5D%5E%60%7B%7C%7D",
            "%22%3C%3E%5B%5C%5D%5E%60%7B%7C%7D",
        ),
        // Controls and everything non-ASCII, which neither set spells out and both encode.
        ("a\nb\tc", "a%0Ab%09c", "a%0Ab%09c"),
        ("dataset_é", "dataset_%C3%A9", "dataset_%C3%A9"),
        // The unreserved characters, which never need escaping and never get it.
        ("abcXYZ019-._~", "abcXYZ019-._~", "abcXYZ019-._~"),
        // Escaping leaves dots unchanged; resource validation rejects dot segments separately.
        ("../secret", "../secret", "..%2Fsecret"),
    ] {
        assert_eq!(
            escape_resource_path(value).to_string(),
            as_path,
            "path: {value}"
        );
        assert_eq!(
            escape_url_component(value).to_string(),
            as_component,
            "component: {value}"
        );
    }
}
