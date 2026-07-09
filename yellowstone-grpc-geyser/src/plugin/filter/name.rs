use std::{
    borrow::Borrow,
    collections::HashSet,
    ops::Deref,
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Debug, thiserror::Error)]
pub enum FilterNameError {
    #[error("oversized filter name (max allowed size {limit}), found {size}")]
    Oversized { limit: usize, size: usize },
}

pub type FilterNameResult<T> = Result<T, FilterNameError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FilterName(Arc<String>);

impl AsRef<str> for FilterName {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for FilterName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Borrow<str> for FilterName {
    #[inline]
    fn borrow(&self) -> &str {
        &self.0[..]
    }
}

impl FilterName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(Arc::new(name.into()))
    }

    pub fn is_uniq(&self) -> bool {
        Arc::strong_count(&self.0) == 1
    }
}

#[derive(Debug)]
pub struct FilterNames {
    name_size_limit: usize,
    names: HashSet<FilterName>,
    names_size_limit: usize,
    cleanup_ts: Instant,
    cleanup_interval: Duration,
}

impl FilterNames {
    pub fn new(
        name_size_limit: usize,
        names_size_limit: usize,
        cleanup_interval: Duration,
    ) -> Self {
        Self {
            name_size_limit,
            names: HashSet::with_capacity(names_size_limit),
            names_size_limit,
            cleanup_ts: Instant::now(),
            cleanup_interval,
        }
    }

    pub fn try_clean(&mut self) {
        if self.names.len() > self.names_size_limit
            && self.cleanup_ts.elapsed() > self.cleanup_interval
        {
            self.names.retain(|name| !name.is_uniq());
            self.cleanup_ts = Instant::now();
        }
    }

    pub fn get(&mut self, name: &str) -> FilterNameResult<FilterName> {
        match self.names.get(name) {
            Some(name) => Ok(name.clone()),
            None => {
                if name.len() > self.name_size_limit {
                    Err(FilterNameError::Oversized {
                        limit: self.name_size_limit,
                        size: name.len(),
                    })
                } else {
                    let name = FilterName::new(name);
                    self.names.insert(name.clone());
                    Ok(name)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Documents that per-connection `FilterNames` instances (as constructed
    // once per connection in `Geyser::subscribe`) are fully independent: each
    // enforces its own `name_size_limit` and has no visibility into names
    // registered by another instance.
    #[test]
    fn test_independent_filter_names_enforce_own_limits() {
        let mut small_limit = FilterNames::new(5, 1024, Duration::from_secs(1));
        let mut large_limit = FilterNames::new(100, 1024, Duration::from_secs(1));

        let long_name = "a".repeat(50);

        assert!(matches!(
            small_limit.get(&long_name),
            Err(FilterNameError::Oversized { limit: 5, size: 50 })
        ));
        assert!(large_limit.get(&long_name).is_ok());
    }

    // Documents the accepted trade-off from de-sharing `FilterNames`: two
    // connections subscribing with the same filter name each get their own
    // `FilterName` (backed by a distinct `Arc<String>`), instead of sharing
    // one interned instance as they would have with a shared `FilterNames`.
    #[test]
    fn test_filter_names_no_longer_intern_across_instances() {
        let mut connection_a = FilterNames::new(64, 1024, Duration::from_secs(1));
        let mut connection_b = FilterNames::new(64, 1024, Duration::from_secs(1));

        let name_a = connection_a.get("shared").unwrap();
        let name_b = connection_b.get("shared").unwrap();

        assert_eq!(name_a.as_ref(), name_b.as_ref());
        assert!(!Arc::ptr_eq(&name_a.0, &name_b.0));
    }
}
