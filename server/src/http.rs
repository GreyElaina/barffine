use anyhow::Error as AnyError;
use axum::{
    http::{HeaderValue, header::SET_COOKIE},
    response::Response,
};
use chrono::{DateTime, Utc};
use httpdate::fmt_http_date;
use std::time::{Duration as StdDuration, UNIX_EPOCH};

use crate::AppError;

pub fn http_date_from_datetime(datetime: &DateTime<Utc>) -> Option<String> {
    let seconds = datetime.timestamp();
    if seconds < 0 {
        return None;
    }

    let seconds_duration = StdDuration::from_secs(seconds as u64);
    let nanos_duration = StdDuration::from_nanos(datetime.timestamp_subsec_nanos() as u64);
    let duration = seconds_duration.checked_add(nanos_duration)?;
    let system_time = UNIX_EPOCH.checked_add(duration)?;
    Some(fmt_http_date(system_time))
}

pub fn append_set_cookie_headers(
    response: &mut Response,
    cookies: &[String],
) -> Result<(), AppError> {
    for cookie in cookies {
        let value =
            HeaderValue::from_str(cookie).map_err(|err| AppError::internal(AnyError::new(err)))?;
        response.headers_mut().append(SET_COOKIE, value);
    }

    Ok(())
}
