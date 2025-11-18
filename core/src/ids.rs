use serde::{Deserialize, Serialize};
use sqlx::{Database, Decode, Encode, Type};
use std::{borrow::Borrow, fmt, ops::Deref, str::FromStr};

macro_rules! define_id_type {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl From<&$name> for String {
            fn from(value: &$name) -> Self {
                value.0.clone()
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }

        impl Deref for $name {
            type Target = str;

            fn deref(&self) -> &Self::Target {
                self.as_str()
            }
        }

        impl FromStr for $name {
            type Err = std::convert::Infallible;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self::from(s))
            }
        }

        impl<DB> Type<DB> for $name
        where
            DB: Database,
            String: Type<DB>,
        {
            fn type_info() -> DB::TypeInfo {
                <String as Type<DB>>::type_info()
            }

            fn compatible(ty: &DB::TypeInfo) -> bool {
                <String as Type<DB>>::compatible(ty)
            }
        }

        impl<'q, DB> Encode<'q, DB> for $name
        where
            DB: Database,
            String: Encode<'q, DB>,
        {
            fn encode_by_ref(
                &self,
                buf: &mut <DB as Database>::ArgumentBuffer<'q>,
            ) -> std::result::Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                let owned: String = self.as_str().to_owned();
                owned.encode_by_ref(buf)
            }
        }

        impl<'r, DB> Decode<'r, DB> for $name
        where
            DB: Database,
            String: Decode<'r, DB>,
        {
            fn decode(
                value: <DB as Database>::ValueRef<'r>,
            ) -> std::result::Result<Self, sqlx::error::BoxDynError> {
                let inner = <String as Decode<DB>>::decode(value)?;
                Ok(Self::from(inner))
            }
        }
    };
}

define_id_type!(WorkspaceId);
define_id_type!(DocId);
define_id_type!(UserId);
