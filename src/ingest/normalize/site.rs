//! Map normalization errors back to the caller's field and batch row.
//!
//! List validation sees flattened child indexes under a synthetic `"item"` field. [`ValueSite`]
//! retains the original field name and list offsets through nested lists and structs. It resolves
//! the row only when constructing an error.

/// The field name and row a rejection below a normalized column should report.
#[derive(Clone, Copy)]
pub(super) struct ValueSite<'a> {
    name: &'a str,
    /// The first row of this normalization window in the caller's batch.
    first_row: usize,
    elements_of: Option<ListEntries<'a>>,
}

/// Map a child index to its list entry, then through any enclosing lists to the batch row.
#[derive(Clone, Copy)]
struct ListEntries<'a> {
    offsets: &'a [i32],
    first_element: usize,
    list: &'a ValueSite<'a>,
}

impl<'a> ValueSite<'a> {
    /// A column of the bound batch whose first value is at batch row `first_row`.
    pub(super) fn column(name: &'a str, first_row: usize) -> Self {
        Self {
            name,
            first_row,
            elements_of: None,
        }
    }

    /// The elements of this list, addressed by `offsets`. Reported element indexes start at
    /// `first_element` in that child: zero for the full or gathered child, or the start of its
    /// validation window. Keeping that origin avoids rebasing offsets just to locate failures.
    ///
    /// The elements keep the list's own name, because `"item"` names nothing a caller bound.
    pub(super) fn list_elements(&'a self, offsets: &'a [i32], first_element: usize) -> Self {
        Self {
            name: self.name,
            first_row: self.first_row,
            elements_of: Some(ListEntries {
                offsets,
                first_element,
                list: self,
            }),
        }
    }

    /// A struct field of this value. Its values sit at the parent's rows, so the row mapping
    /// carries over unchanged.
    pub(super) fn field(&self, name: &'a str) -> Self {
        Self { name, ..*self }
    }

    pub(super) fn name(&self) -> &'a str {
        self.name
    }

    /// Resolve `index` through any enclosing lists to the caller's batch row.
    ///
    /// A binary search finds the last offset not past `index`. Empty entries repeat an offset, and
    /// the search steps over them to the entry that holds the element.
    pub(super) fn row(&self, index: usize) -> usize {
        let Some(ListEntries {
            offsets,
            first_element,
            list,
        }) = self.elements_of
        else {
            return self.first_row + index;
        };
        // Offsets are `i32` and an index is a `usize`; both fit `i64`, so the comparison needs no
        // fallible conversion. A `usize` past `i64::MAX` cannot index any array Arrow built.
        let index = i64::try_from(first_element.saturating_add(index)).unwrap_or(i64::MAX);
        let entry = offsets
            .partition_point(|&offset| i64::from(offset) <= index)
            .saturating_sub(1);
        list.row(entry)
    }
}
