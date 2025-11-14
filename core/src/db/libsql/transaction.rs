use std::ops::Deref;

use anyhow::Result;
use deadpool_libsql::Connection as PoolConnection;

use crate::db::libsql::pool::LibsqlPool;

pub struct LibsqlTransaction {
    conn: Option<PoolConnection>,
    tx: Option<libsql::Transaction>,
}

impl LibsqlTransaction {
    pub(crate) async fn begin_from_pool(pool: &LibsqlPool) -> Result<Self> {
        let conn = pool.pool().get().await?;
        let tx = conn.transaction().await?;
        Ok(Self {
            conn: Some(conn),
            tx: Some(tx),
        })
    }

    pub fn transaction(&self) -> &libsql::Transaction {
        self.tx
            .as_ref()
            .expect("libsql transaction should be active")
    }

    pub async fn commit(mut self) -> Result<()> {
        if let Some(tx) = self.tx.take() {
            tx.commit().await?;
        }
        self.conn.take();
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<()> {
        if let Some(tx) = self.tx.take() {
            tx.rollback().await?;
        }
        self.conn.take();
        Ok(())
    }
}

impl Deref for LibsqlTransaction {
    type Target = libsql::Transaction;

    fn deref(&self) -> &Self::Target {
        self.transaction()
    }
}
