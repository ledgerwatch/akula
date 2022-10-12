// Gas fee schedule—see Appendix G of the Yellow Paper
// https://ethereum.github.io/yellowpaper/paper.pdf
pub mod fee {
    pub const ACCESS_LIST_STORAGE_KEY_COST: u64 = 1900;
    pub const ACCESS_LIST_ADDRESS_COST: u64 = 2400;

    pub const G_CODE_DEPOSIT: u64 = 200;

    pub const G_TX_CREATE: u64 = 32_000;
    pub const G_TX_DATA_ZERO: u64 = 4;
    pub const G_TX_DATA_NON_ZERO_FRONTIER: u64 = 68;
    pub const G_TX_DATA_NON_ZERO_ISTANBUL: u64 = 16;
    pub const G_TRANSACTION: u64 = 21_000;
} // namespace fee

pub mod param {
    // https://eips.ethereum.org/EIPS/eip-170
    pub const MAX_CODE_SIZE: usize = 0x6000;

    pub const G_QUAD_DIVISOR_BYZANTIUM: u64 = 20; // EIP-198
    pub const G_QUAD_DIVISOR_BERLIN: u64 = 3; // EIP-2565

    // https://eips.ethereum.org/EIPS/eip-3529
    pub const MAX_REFUND_QUOTIENT_FRONTIER: u64 = 2;
    pub const MAX_REFUND_QUOTIENT_LONDON: u64 = 5;

    // https://eips.ethereum.org/EIPS/eip-1559
    pub const INITIAL_BASE_FEE: u64 = 1_000_000_000;
    pub const BASE_FEE_MAX_CHANGE_DENOMINATOR: u64 = 8;
    pub const ELASTICITY_MULTIPLIER: u64 = 2;
}
