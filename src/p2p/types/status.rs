use crate::models::*;

#[derive(Debug, Clone, Copy, Default)]
pub struct Status {
    pub height: BlockNumber,
    pub hash: H256,
    pub total_difficulty: H256,
}

impl Status {
    pub fn new(height: BlockNumber, hash: H256, td: u128) -> Self {
        Self {
            height,
            hash,
            total_difficulty: H256::from(td.as_u256().to_be_bytes()),
        }
    }
}

impl<'a> From<&'a ChainConfig> for Status {
    fn from(config: &'a ChainConfig) -> Self {
        let height = config.chain_spec.genesis.number;
        let hash = config.genesis_hash;
        let total_difficulty = H256::from(
            config
                .chain_spec
                .genesis
                .seal
                .difficulty()
                .as_u256()
                .to_be_bytes(),
        );
        Self {
            height,
            hash,
            total_difficulty,
        }
    }
}

impl PartialEq for Status {
    #[inline(always)]
    fn eq(&self, other: &Status) -> bool {
        self.height == other.height
            && self.hash == other.hash
            && self.total_difficulty == other.total_difficulty
    }
}
