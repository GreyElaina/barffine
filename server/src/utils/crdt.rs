use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use yrs::StateVector;
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;

use crate::AppError;

pub fn decode_state_vector(bytes: &[u8]) -> Result<StateVector, AppError> {
    StateVector::decode_v1(bytes).map_err(|_| AppError::bad_request("invalid state vector"))
}

pub fn encode_state_vector(state: &StateVector) -> Result<Vec<u8>, AppError> {
    Ok(state.encode_v1())
}

pub fn decode_updates(encoded: &[String]) -> Result<Vec<Vec<u8>>, AppError> {
    let mut updates = Vec::with_capacity(encoded.len());
    for update in encoded {
        let bytes = BASE64
            .decode(update)
            .map_err(|_| AppError::bad_request("invalid base64 payload"))?;
        updates.push(bytes);
    }
    Ok(updates)
}
