//! Shared query-param parsing for paginated list endpoints.

use serde::Deserialize;

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Deserialize)]
pub struct Pagination {
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default = "default_offset")]
    pub offset: i64,
}

fn default_limit() -> i64 {
    50
}
fn default_offset() -> i64 {
    0
}

impl Pagination {
    /// Validate bounds and return `(limit, offset)` for SQL.
    pub fn bounds(&self) -> AppResult<(i64, i64)> {
        if self.limit < 1 || self.limit > 500 {
            return Err(AppError::Validation(
                "limit must be between 1 and 500".into(),
            ));
        }
        if self.offset < 0 {
            return Err(AppError::Validation("offset must be >= 0".into()));
        }
        Ok((self.limit, self.offset))
    }
}
