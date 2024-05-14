#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub master: bool,
}

impl NodeInfo {
    pub fn new(master: bool) -> Self {
        Self { master }
    }
}
