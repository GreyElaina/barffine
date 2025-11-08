use async_graphql::{EmptySubscription, Schema};

use crate::AppState;

use super::{MutationRoot, QueryRoot};

pub type BarffineSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

pub fn build_schema(state: AppState) -> BarffineSchema {
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(state)
        .finish()
}
