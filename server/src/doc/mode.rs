#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DocPublishMode {
    Page,
    Edgeless,
}

impl Default for DocPublishMode {
    fn default() -> Self {
        Self::Page
    }
}

impl DocPublishMode {
    pub fn from_str(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("page") {
            Some(Self::Page)
        } else if value.eq_ignore_ascii_case("edgeless") {
            Some(Self::Edgeless)
        } else {
            None
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Page => "page",
            Self::Edgeless => "edgeless",
        }
    }
}
