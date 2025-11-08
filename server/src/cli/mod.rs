#![allow(dead_code)]

use std::fmt;

/// Declarative descriptor for a future CLI subcommand.
#[derive(Debug, Clone)]
pub struct CommandDescriptor {
    pub name: &'static str,
    pub summary: &'static str,
    pub help: &'static str,
}

impl CommandDescriptor {
    pub const fn new(name: &'static str, summary: &'static str, help: &'static str) -> Self {
        Self {
            name,
            summary,
            help,
        }
    }
}

/// Planned entry points for CLI parity with the Node service.
pub const PLANNED_COMMANDS: &[CommandDescriptor] = &[
    CommandDescriptor::new(
        "data-migrate",
        "Run data layer migrations and compatibility checks",
        "Placeholder for future command that mirrors `packages/backend/server/src/data/commands`.",
    ),
    CommandDescriptor::new(
        "admin",
        "Manage administrative users",
        "Will unify create/update workflows and include password reset support.",
    ),
    CommandDescriptor::new(
        "workspace",
        "Inspect and operate on workspaces",
        "Intended to expose listing, seeding, and permission diagnostics.",
    ),
];

/// Interface that forthcoming CLI implementations will satisfy.
pub trait CliCommand {
    fn descriptor(&self) -> &CommandDescriptor;
    fn run(&self) -> Result<(), CliError>;
}

#[derive(Debug)]
pub struct CliError {
    message: String,
}

impl CliError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
