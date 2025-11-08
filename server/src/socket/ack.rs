use serde::Serialize;
use socketioxide::extract::AckSender;
use tracing::warn;

use crate::error::AppError;

use super::types::SocketAck;

pub(crate) fn ack_ok<T>(ack: AckSender, data: T)
where
    T: Serialize,
{
    if let Err(err) = ack.send(&SocketAck::ok(data)) {
        warn!(?err, "failed to send socket ack response");
    }
}

pub(crate) fn ack_error<T>(ack: AckSender, error: AppError, request_id: Option<&str>)
where
    T: Serialize,
{
    if let Err(err) = ack.send(&SocketAck::<T>::from_error(error, request_id)) {
        warn!(?err, "failed to send socket ack error");
    }
}
