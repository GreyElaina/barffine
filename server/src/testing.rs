#![allow(dead_code)]

use anyhow::Result;

/// Captures metadata for a recorded contract test fixture.
#[derive(Debug, Clone)]
pub struct ContractCase<'a> {
    pub name: &'a str,
    pub request_path: &'a str,
    pub method: &'a str,
}

/// Planned interface for replaying captured Node backend traffic.
pub trait ContractTestHarness {
    fn register_case(&mut self, case: ContractCase<'static>);
    fn execute(&self) -> Result<()>;
}

/// Placeholder type for Vitest integration once the harness is wired up.
pub struct VitestBridge;

impl VitestBridge {
    pub fn new() -> Self {
        Self
    }
}
