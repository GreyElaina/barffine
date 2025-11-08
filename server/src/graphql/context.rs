#[derive(Clone)]
pub struct RequestUser {
    pub(crate) user_id: String,
    pub(crate) session_id: Option<String>,
}

impl RequestUser {
    pub(crate) fn new(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            session_id: None,
        }
    }

    pub(crate) fn with_session(user_id: impl Into<String>, session_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            session_id: Some(session_id.into()),
        }
    }

    pub(crate) fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }
}
