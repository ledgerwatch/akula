use crate::execution::tracer::{NoopTracer, Tracer};

use super::{
    common::{InterpreterMessage, Output},
    CreateMessage, StatusCode,
};
use bytes::Bytes;
use ethereum_types::Address;
use ethnum::U256;

/// State access status (EIP-2929).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessStatus {
    Cold,
    Warm,
}

impl Default for AccessStatus {
    fn default() -> Self {
        Self::Cold
    }
}

#[derive(Clone, Copy, Debug)]
pub enum StorageStatus {
    /// The new/same value is assigned to the storage item without affecting the cost structure.
    ///
    /// The storage value item is either:
    /// - left unchanged (c == v) or
    /// - the dirty value (o != c) is modified again (c != v).
    /// This is the group of cases related to minimal gas cost of only accessing warm storage.
    /// 0|X   -> 0 -> 0 (current value unchanged)
    /// 0|X|Y -> Y -> Y (current value unchanged)
    /// 0|X   -> Y -> Z (modified previously added/modified value)
    ///
    /// This is "catch all remaining" status. I.e. if all other statuses are correctly matched
    /// this status should be assigned to all remaining cases.
    Assigned,

    /// A new storage item is added by changing
    /// the current clean zero to a nonzero value.
    /// 0 -> 0 -> Z
    Added,

    /// A storage item is deleted by changing
    /// the current clean nonzero to the zero value.
    /// X -> X -> 0
    Deleted,

    /// A storage item is modified by changing
    /// the current clean nonzero to other nonzero value.
    /// X -> X -> Z
    Modified,

    /// A storage item is added by changing
    /// the current dirty zero to a nonzero value other than the original value.
    /// X -> 0 -> Z
    DeletedAdded,

    /// A storage item is deleted by changing
    /// the current dirty nonzero to the zero value and the original value is not zero.
    /// X -> Y -> 0
    ModifiedDeleted,

    /// A storage item is added by changing
    /// the current dirty zero to the original value.
    /// X -> 0 -> X
    DeletedRestored,

    /// A storage item is deleted by changing
    /// the current dirty nonzero to the original zero value.
    /// 0 -> Y -> 0
    AddedDeleted,

    /// A storage item is modified by changing
    /// the current dirty nonzero to the original nonzero value other than the current value.
    /// X -> Y -> X
    ModifiedRestored,
}

/// The transaction and block data for execution.
#[derive(Clone, Debug)]
pub struct TxContext {
    /// The transaction gas price.
    pub tx_gas_price: U256,
    /// The transaction origin account.
    pub tx_origin: Address,
    /// The miner of the block.
    pub block_coinbase: Address,
    /// The block number.
    pub block_number: u64,
    /// The block timestamp.
    pub block_timestamp: u64,
    /// The block gas limit.
    pub block_gas_limit: u64,
    /// The block difficulty.
    pub block_difficulty: U256,
    /// The blockchain's ChainID.
    pub chain_id: U256,
    /// The block base fee per gas (EIP-1559, EIP-3198).
    pub block_base_fee: U256,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Call<'a> {
    Call(&'a InterpreterMessage),
    Create(&'a CreateMessage),
}

/// Abstraction that exposes host context to EVM.
pub trait Host {
    fn trace_instructions(&self) -> bool {
        false
    }
    fn tracer(&mut self, mut f: impl FnMut(&mut dyn Tracer)) {
        (f)(&mut NoopTracer)
    }
    /// Check if an account exists.
    fn account_exists(&mut self, address: Address) -> bool;
    /// Get value of a storage key.
    ///
    /// Returns `Ok(U256::zero())` if does not exist.
    fn get_storage(&mut self, address: Address, key: U256) -> U256;
    /// Set value of a storage key.
    fn set_storage(&mut self, address: Address, key: U256, value: U256) -> StorageStatus;
    /// Get balance of an account.
    ///
    /// Returns `Ok(0)` if account does not exist.
    fn get_balance(&mut self, address: Address) -> U256;
    /// Get code size of an account.
    ///
    /// Returns `Ok(0)` if account does not exist.
    fn get_code_size(&mut self, address: Address) -> U256;
    /// Get code hash of an account.
    ///
    /// Returns `Ok(0)` if account does not exist.
    fn get_code_hash(&mut self, address: Address) -> U256;
    /// Copy code of an account.
    ///
    /// Returns `Ok(0)` if offset is invalid.
    fn copy_code(&mut self, address: Address, offset: usize, buffer: &mut [u8]) -> usize;
    /// Self-destruct account.
    fn selfdestruct(&mut self, address: Address, beneficiary: Address) -> bool;
    /// Call to another account.
    fn call(&mut self, msg: Call) -> Output;
    /// Retrieve transaction context.
    fn get_tx_context(&mut self) -> Result<TxContext, StatusCode>;
    /// Get block hash.
    ///
    /// Returns `Ok(U256::zero())` if block does not exist.
    fn get_block_hash(&mut self, block_number: u64) -> U256;
    /// Emit a log.
    fn emit_log(&mut self, address: Address, data: Bytes, topics: &[U256]);
    /// Mark account as warm, return previous access status.
    ///
    /// Returns `Ok(AccessStatus::Cold)` if account does not exist.
    fn access_account(&mut self, address: Address) -> AccessStatus;
    /// Mark storage key as warm, return previous access status.
    ///
    /// Returns `Ok(AccessStatus::Cold)` if account does not exist.
    fn access_storage(&mut self, address: Address, key: U256) -> AccessStatus;
}
