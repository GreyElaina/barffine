pub fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

pub fn option_bool_to_i64(value: Option<bool>) -> Option<i64> {
    value.map(bool_to_i64)
}
