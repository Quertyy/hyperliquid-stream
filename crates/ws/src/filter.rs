use std::collections::BTreeSet;

/// The kinds a subscription selects, for example action names. An empty
/// filter selects every record.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct KindFilter(Option<BTreeSet<String>>);

impl KindFilter {
    /// A filter selecting records of any of `kinds`; no kinds selects all.
    pub fn new(kinds: impl IntoIterator<Item = String>) -> Self {
        let kinds: BTreeSet<String> = kinds.into_iter().collect();
        Self((!kinds.is_empty()).then_some(kinds))
    }

    /// The filter selecting what any of `filters` selects; an empty filter
    /// among them selects everything.
    #[must_use]
    pub fn union<'a>(filters: impl IntoIterator<Item = &'a KindFilter>) -> Self {
        let mut kinds = std::collections::BTreeSet::new();
        for filter in filters {
            match &filter.0 {
                None => return Self::default(),
                Some(selected) => kinds.extend(selected.iter().cloned()),
            }
        }
        Self((!kinds.is_empty()).then_some(kinds))
    }

    /// Whether the filter selects every record.
    #[must_use]
    pub fn is_all(&self) -> bool {
        self.0.is_none()
    }

    /// The selected kinds, sorted; empty when every record is selected.
    pub fn kinds(&self) -> impl Iterator<Item = &str> {
        self.0.iter().flatten().map(String::as_str)
    }

    /// Whether a record carrying these kinds is selected. A record without
    /// kinds is selected only by an empty filter.
    #[must_use]
    pub fn accepts(&self, kinds: &[&str]) -> bool {
        self.0
            .as_ref()
            .is_none_or(|selected| kinds.iter().any(|kind| selected.contains(*kind)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter_accepts_everything() {
        let all = KindFilter::default();
        assert!(all.is_all() && all.accepts(&[]) && all.accepts(&["order"]));
        assert_eq!(KindFilter::new(Vec::new()), all);
        let orders = KindFilter::new(["order".to_owned()]);
        assert!(orders.accepts(&["cancel", "order"]));
        assert!(!orders.accepts(&["cancel"]));
        assert!(!orders.accepts(&[]));
        let cancels = KindFilter::new(["cancel".to_owned()]);
        let both = KindFilter::union([&orders, &cancels]);
        assert_eq!(both.kinds().collect::<Vec<_>>(), ["cancel", "order"]);
        assert!(KindFilter::union([&orders, &all]).is_all());
    }
}
