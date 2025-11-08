mod ack;
mod auth;
mod events;
pub mod rooms;
mod types;

use socketioxide::{SocketIo, layer::SocketIoLayer};

use crate::state::AppState;

pub(crate) fn build_socket_layer(state: AppState) -> (SocketIoLayer, SocketIo) {
    let (layer, io) = auth::build_socket(state.clone());
    events::register_namespace(&io, state);
    (layer, io)
}
