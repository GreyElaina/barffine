use async_graphql::{InputObject, SimpleObject};

use crate::AppError;

#[derive(InputObject, Default, Clone)]
pub struct PaginationInput {
    after: Option<String>,
    first: Option<i32>,
    offset: Option<i32>,
}

impl PaginationInput {
    pub fn normalize(&self) -> Result<(i64, i64, Option<String>), AppError> {
        let limit = self.first.unwrap_or(10).clamp(1, 100) as i64;
        let after = self.after.clone();

        if after.is_some() && self.offset.unwrap_or(0) > 0 {
            return Err(AppError::bad_request(
                "offset pagination is not supported when using after cursors",
            ));
        }

        let offset = if after.is_some() {
            0
        } else {
            self.offset.unwrap_or(0).max(0) as i64
        };

        Ok((limit, offset, after))
    }
}

#[derive(SimpleObject, Clone)]
pub struct PageInfo {
    #[graphql(name = "endCursor")]
    pub end_cursor: Option<String>,
    #[graphql(name = "hasNextPage")]
    pub has_next_page: bool,
    #[graphql(name = "hasPreviousPage")]
    pub has_previous_page: bool,
    #[graphql(name = "startCursor")]
    pub start_cursor: Option<String>,
}

impl Default for PageInfo {
    fn default() -> Self {
        Self {
            end_cursor: None,
            has_next_page: false,
            has_previous_page: false,
            start_cursor: None,
        }
    }
}
