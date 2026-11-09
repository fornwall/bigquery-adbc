//! SQL `LIKE` filters for ADBC metadata calls.

use crate::identifiers::is_dot_segment;

/// `%` matches zero or more characters and `_` matches one. ADBC defines no escaping:
/// backslash matches itself. Matching is case-sensitive and follows UTF-8 character boundaries.
/// `bigquery.get_objects.disable_wildcards` selects literal matching instead.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SqlPattern<'a> {
    value: &'a str,
    literal: bool,
}

impl<'a> SqlPattern<'a> {
    pub(crate) fn new(pattern: &'a str) -> Self {
        Self {
            value: pattern,
            literal: false,
        }
    }

    pub(crate) fn literal(name: &'a str) -> Self {
        Self {
            value: name,
            literal: true,
        }
    }

    /// On mismatch, give the most recent `%` one more character and resume after it.
    /// Earlier wildcards can yield their extra characters to this one, so a single backtrack
    /// point suffices. Work is bounded by `pattern.len() * value.len()`, with no allocations.
    pub(crate) fn matches(self, value: &str) -> bool {
        if self.literal {
            return self.value == value;
        }
        let mut pattern = self.value.chars();
        let mut value = value.chars();
        let mut wildcard = None;

        while let Some(value_char) = value.clone().next() {
            match pattern.clone().next() {
                Some('%') => {
                    pattern.next();
                    wildcard = Some((pattern.clone(), value.clone()));
                }
                Some(pattern_char) if pattern_char == '_' || pattern_char == value_char => {
                    pattern.next();
                    value.next();
                }
                _ => {
                    let Some((after_wildcard, matched_through)) = &mut wildcard else {
                        return false;
                    };
                    matched_through.next();
                    pattern = after_wildcard.clone();
                    value = matched_through.clone();
                }
            }
        }

        pattern.all(|character| character == '%')
    }

    /// A name eligible for direct lookup. In pattern mode, even common dataset names such as
    /// `analytics_marts` need a listing because `_` is a wildcard. Literal mode allows them.
    /// URL dot segments are excluded because requesting them would address the parent resource.
    pub(crate) fn exact(self) -> Option<&'a str> {
        ((self.literal || !self.value.contains(['%', '_'])) && !is_dot_segment(self.value))
            .then_some(self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::SqlPattern;

    #[test]
    fn object_search_patterns_support_sql_wildcards() {
        for (pattern, value) in [
            ("%", "anything"),
            ("%", ""),
            ("%%%", "anything"),
            ("sample%", "samples"),
            ("%query", "big-fast-query"),
            ("big%query", "big-fast-query"),
            ("table_", "table1"),
            ("", ""),
            // The wildcard the greedy walk has to reconsider several times before the tail
            // lines up: every `%a` can absorb a different run of the value.
            ("%a%a%b", "aaaaab"),
            ("%_%", "x"),
            // Matching is by character, not by byte: `_` spans a whole multi-byte character,
            // and a wildcard's backtracking resumes on a character boundary.
            ("d_t_", "dåtå"),
            ("%å", "dåtå"),
            ("%å_", "dåtåx"),
            // Backslash is an ordinary character: adbc.h has no escape syntax, so `\%` is a
            // backslash followed by the wildcard, and a trailing backslash matches itself.
            (r"a\%", "a\\bc"),
            (r"back\slash", "back\\slash"),
            (r"trailing\", "trailing\\"),
            (r"%\", "trailing\\"),
        ] {
            assert!(
                SqlPattern::new(pattern).matches(value),
                "{pattern:?} vs {value:?}"
            );
        }
        for (pattern, value) in [
            ("", "table"),
            ("table", "tables"),
            ("table_", "table"),
            ("big%query", "big-fast-query-extra"),
            ("d_t_", "datax"),
            ("%a%a%b", "ab"),
            // One `_` is one character, not one byte.
            ("_", "då"),
            ("__", "å"),
            // Case-sensitive, like every BigQuery identifier the patterns filter.
            ("TABLE", "table"),
            ("%TABLE", "big_table"),
            // With no escape syntax, `\%` cannot ask for a literal percent sign.
            (r"a\%", "a%"),
            (r"trailing\", "trailing"),
        ] {
            assert!(
                !SqlPattern::new(pattern).matches(value),
                "{pattern:?} vs {value:?}"
            );
        }
    }

    /// Direct lookup must agree with [`SqlPattern::matches`] on what a pattern means: every
    /// character but `%` and `_`, backslash included, is part of the literal name.
    #[test]
    fn a_wildcard_free_pattern_is_the_literal_name() {
        for pattern in ["events", r"back\slash", r"trailing\", ""] {
            assert_eq!(SqlPattern::new(pattern).exact(), Some(pattern));
            assert!(
                SqlPattern::new(pattern).matches(pattern),
                "{pattern:?} must match itself"
            );
        }
        for pattern in ["event%", "table_", "%", "_", r"a\%"] {
            assert_eq!(SqlPattern::new(pattern).exact(), None, "{pattern:?}");
        }
    }

    /// A direct lookup of a URL dot segment would address the parent resource, so it goes
    /// through a listing, where it matches nothing BigQuery can hold.
    #[test]
    fn a_dot_segment_pattern_is_never_looked_up_directly() {
        for pattern in [".", ".."] {
            assert_eq!(SqlPattern::new(pattern).exact(), None, "{pattern:?}");
            assert_eq!(SqlPattern::literal(pattern).exact(), None, "{pattern:?}");
            assert!(SqlPattern::new(pattern).matches(pattern), "{pattern:?}");
        }
        // Only those two spellings: a name that merely contains a dot is an ordinary name.
        for pattern in ["...", "events.2026", ".events"] {
            assert_eq!(
                SqlPattern::new(pattern).exact(),
                Some(pattern),
                "{pattern:?}"
            );
        }
    }

    /// A matcher that reconsidered every `%` would take exponential time on these inputs.
    #[test]
    fn a_pathological_pattern_does_not_backtrack_exponentially() {
        let value = "a".repeat(50_000);
        assert!(!SqlPattern::new("%a%a%a%a%a%a%b").matches(&value));
        assert!(SqlPattern::new("%a%a%a%a%a%a%a").matches(&value));
        assert!(!SqlPattern::new(&"%a".repeat(500)).matches(&"a".repeat(499)));
    }

    #[test]
    fn greedy_matching_agrees_with_exhaustive_backtracking() {
        fn exhaustive(pattern: &[char], value: &[char]) -> bool {
            match (pattern, value) {
                ([], []) => true,
                (['%', rest @ ..], _) => {
                    (0..=value.len()).any(|count| exhaustive(rest, &value[count..]))
                }
                ([first, rest @ ..], [character, remaining @ ..])
                    if *first == '_' || first == character =>
                {
                    exhaustive(rest, remaining)
                }
                _ => false,
            }
        }

        let mut words = vec![String::new()];
        let mut start = 0;
        for _ in 0..4 {
            let end = words.len();
            for index in start..end {
                for character in ['a', 'å', '%', '_'] {
                    words.push(format!("{}{character}", words[index]));
                }
            }
            start = end;
        }
        let words = words
            .iter()
            .map(|word| (word, word.chars().collect::<Vec<_>>()))
            .collect::<Vec<_>>();
        for (pattern, pattern_chars) in &words {
            for (value, value_chars) in &words {
                assert_eq!(
                    SqlPattern::new(pattern).matches(value),
                    exhaustive(pattern_chars, value_chars),
                    "{pattern:?} vs {value:?}"
                );
            }
        }
    }
}
