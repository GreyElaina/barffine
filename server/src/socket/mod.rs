mod ack;
mod auth;
mod events;
pub mod rooms;
pub mod types;

use std::sync::Arc;

use socketioxide::{SocketIo, layer::SocketIoLayer};

use crate::state::AppState;

pub(crate) fn build_socket_layer(state: AppState) -> (SocketIoLayer, Arc<SocketIo>) {
    let shared = Arc::new(state);
    let runtime = shared.runtime();
    let (layer, io) = auth::build_socket(shared.clone(), runtime.clone());
    events::register_namespace(&io, shared, runtime);
    (layer, Arc::new(io))
}
