use once_cell::sync::Lazy;
use std::{collections::HashMap, sync::RwLock};

static CACHE: Lazy<RwLock<HashMap<&'static str, &'static str>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub(crate) fn convert(sql: &'static str) -> &'static str {
    if let Some(existing) = CACHE.read().unwrap().get(sql).copied() {
        return existing;
    }

    let rewritten = rewrite(sql);
    let leaked: &'static str = Box::leak(rewritten.into_boxed_str());
    CACHE.write().unwrap().insert(sql, leaked);
    leaked
}

fn rewrite(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len() + 8);
    let mut param_index = 1;
    let mut chars = sql.chars().peekable();
    let mut in_string = false;

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                result.push(ch);
                if in_string {
                    if let Some('\'') = chars.peek() {
                        // Escaped quote inside string literal; keep consuming but remain in-string.
                        result.push(chars.next().unwrap());
                    } else {
                        in_string = false;
                    }
                } else {
                    in_string = true;
                }
            }
            '?' if !in_string => {
                result.push('$');
                result.push_str(&param_index.to_string());
                param_index += 1;
            }
            _ => result.push(ch),
        }
    }

    result
}
