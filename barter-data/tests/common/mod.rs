//! Common test utilities for barter-data integration tests.

use barter_data::subscription::Map;
use barter_integration::subscription::SubscriptionId;
use fnv::FnvHashMap;

/// Create a test instrument map from a vector of (SubscriptionId, Key) pairs.
///
/// This helper simplifies test setup by allowing concise map creation.
pub fn create_test_instrument_map<K: Clone>(
    entries: Vec<(SubscriptionId, K)>,
) -> Map<K> {
    Map(FnvHashMap::from_iter(entries))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_test_instrument_map() {
        let sub_id1 = SubscriptionId::from("test_1");
        let sub_id2 = SubscriptionId::from("test_2");

        let map = create_test_instrument_map(vec![
            (sub_id1.clone(), 1u32),
            (sub_id2.clone(), 2u32),
        ]);

        assert_eq!(map.0.len(), 2);
        assert_eq!(map.0.get(&sub_id1), Some(&1u32));
        assert_eq!(map.0.get(&sub_id2), Some(&2u32));
    }
}
