pub(crate) const ONE_KIB: i64 = 1024;
pub(crate) const ONE_MIB: i64 = ONE_KIB * 1024;
pub(crate) const ONE_GIB: i64 = ONE_MIB * 1024;
pub(crate) const ONE_MINUTE_MS: i64 = 60_000;
pub(crate) const ONE_DAY_MS: i64 = ONE_MINUTE_MS * 60 * 24;

pub(crate) const FREE_PLAN_NAME: &str = "Free";
pub(crate) const FREE_BLOB_LIMIT: i64 = 10 * ONE_MIB;
pub(crate) const FREE_STORAGE_QUOTA: i64 = 10 * ONE_GIB;
pub(crate) const FREE_HISTORY_PERIOD_MS: i64 = 7 * ONE_DAY_MS;
pub(crate) const FREE_MEMBER_LIMIT: i32 = 3;
