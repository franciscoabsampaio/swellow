#[derive(Clone, PartialEq)]
pub enum MigrationDirection {
    Up,
    Down,
}

impl MigrationDirection {
    pub fn verb(&self) -> &'static str {
        match self {
            Self::Up => "Migrating",
            Self::Down => "Rolling back",
        }
    }
    pub fn noun(&self) -> &'static str {
        match self {
            Self::Up => "Migration",
            Self::Down => "Rollback",
        }
    }
    pub fn filename(&self) -> &'static str {
        match self {
            Self::Up => "up.sql",
            Self::Down => "down.sql",
        }
    }
}