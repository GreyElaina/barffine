use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpaceType {
    Workspace,
    Userspace,
}

impl SpaceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SpaceType::Workspace => "workspace",
            SpaceType::Userspace => "userspace",
        }
    }
}

impl std::fmt::Display for SpaceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomKind {
    Sync,
    LegacySync,
    Awareness { doc_id: String },
}

impl RoomKind {
    fn suffix(&self) -> String {
        match self {
            RoomKind::Sync => "sync".to_string(),
            RoomKind::LegacySync => "sync-019".to_string(),
            RoomKind::Awareness { doc_id } => format!("{}:awareness", doc_id),
        }
    }
}

pub fn space_room_name(space_type: SpaceType, space_id: &str, kind: RoomKind) -> String {
    format!("{}:{}:{}", space_type.as_str(), space_id, kind.suffix())
}
