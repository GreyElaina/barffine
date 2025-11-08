use anyhow::Error as AnyError;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use y_octo::{
    CrdtRead as YoctoCrdtRead, CrdtWrite as YoctoCrdtWrite, RawDecoder as YoctoRawDecoder,
    RawEncoder as YoctoRawEncoder, StateVector as YoctoStateVector,
};

use crate::AppError;

pub fn decode_state_vector(bytes: &[u8]) -> Result<YoctoStateVector, AppError> {
    let mut decoder = YoctoRawDecoder::new(bytes);
    YoctoStateVector::read(&mut decoder).map_err(|_| AppError::bad_request("invalid state vector"))
}

pub fn encode_state_vector(state: &YoctoStateVector) -> Result<Vec<u8>, AppError> {
    let mut encoder = YoctoRawEncoder::default();
    state
        .write(&mut encoder)
        .map_err(|err| AppError::internal(AnyError::new(err)))?;
    Ok(encoder.into_inner())
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
