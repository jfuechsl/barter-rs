use crate::error::DataError;

/// Trait for Binance-family order book L2 sequence validation.
///
/// Binance Spot and Futures have similar validation flows but differ in:
/// - Outdated update detection (Spot: `<=`, Futures: `<`)
/// - Subsequent update validation
/// - Internal state tracking requirements
///
/// This trait extracts the common validation flow while delegating
/// exchange-specific logic to implementors.
pub trait BinanceSequenceValidator: Sized {
    /// The exchange-specific update type.
    type Update;

    /// Returns `true` if this is the first update being processed.
    fn is_first_update(&self) -> bool;

    /// Check if update should be dropped as outdated.
    ///
    /// - Spot: `update.last_update_id <= self.last_update_id`
    /// - Futures: `update.last_update_id < self.last_update_id`
    fn is_outdated(&self, update: &Self::Update) -> bool;

    /// Validate the first update after snapshot initialization.
    fn validate_first_update(&self, update: &Self::Update) -> Result<(), DataError>;

    /// Validate subsequent updates maintain sequence continuity.
    fn validate_next_update(&self, update: &Self::Update) -> Result<(), DataError>;

    /// Update internal state after accepting a valid update.
    fn apply_update(&mut self, update: &Self::Update);

    /// Validate an incoming update using the common flow.
    ///
    /// This default implementation handles the shared algorithm:
    /// 1. Drop outdated updates
    /// 2. Validate first or subsequent update
    /// 3. Apply state changes
    /// 4. Return the valid update
    fn validate_sequence(
        &mut self,
        update: Self::Update,
    ) -> Result<Option<Self::Update>, DataError> {
        // Step 1: Drop outdated updates
        if self.is_outdated(&update) {
            return Ok(None);
        }

        // Step 2: Validate based on update position
        if self.is_first_update() {
            self.validate_first_update(&update)?;
        } else {
            self.validate_next_update(&update)?;
        }

        // Step 3: Apply state changes
        self.apply_update(&update);

        // Step 4: Return valid update
        Ok(Some(update))
    }
}
