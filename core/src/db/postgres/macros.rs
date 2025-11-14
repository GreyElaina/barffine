macro_rules! pg_query {
    ($sql:literal $(,)?) => {{
        static SQL: once_cell::sync::Lazy<&'static str> =
            once_cell::sync::Lazy::new(|| crate::db::postgres::placeholders::convert($sql));
        sqlx::query(*SQL)
    }};
}

macro_rules! pg_query_scalar {
    ($sql:literal $(,)?) => {{
        static SQL: once_cell::sync::Lazy<&'static str> =
            once_cell::sync::Lazy::new(|| crate::db::postgres::placeholders::convert($sql));
        sqlx::query_scalar(*SQL)
    }};
}
