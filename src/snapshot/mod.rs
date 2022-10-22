use crate::{
    kv::{
        mdbx::*,
        traits::{Table, TableDecode, TableObject},
    },
    models::*,
};
use anyhow::{bail, format_err};
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, ErrorKind, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

pub const DEFAULT_STRIDE: NonZeroUsize = NonZeroUsize::new(100_000).unwrap();

pub const INDEX_FILE_NAME: &str = "index";
pub const SEGMENT_FILE_NAME: &str = "segment";

pub trait SnapshotVersion {
    const ID: &'static str;
    const STRIDE: NonZeroUsize;
}

pub trait SnapshotObject: TableObject {
    const ID: &'static str;
    type Table: Table<Key = u64, Value = H160, SeekKey = u64>;

    fn db_table() -> Self::Table;
}

#[derive(Debug)]
pub struct V1;

impl SnapshotVersion for V1 {
    const ID: &'static str = "v1";
    const STRIDE: NonZeroUsize = NonZeroUsize::new(100_000).unwrap();
}

#[derive(Debug)]
struct Snapshot {
    total_items: usize,
    segment_len: usize,
    segment: BufReader<File>,
    index: BufReader<File>,
}

impl Snapshot {
    fn read(&mut self, idx: usize) -> anyhow::Result<Vec<u8>> {
        {
            let idx_seek_pos = (idx * 8) as u64;
            let idx_seeked_to = self.index.seek(SeekFrom::Start(idx_seek_pos))?;
            if idx_seeked_to != idx_seek_pos {
                bail!("idx seek invalid: {idx_seeked_to} != {idx_seek_pos}");
            }
        }

        let mut seg_seek_pos_buf = [0_u8; 8];
        self.index.read_exact(&mut seg_seek_pos_buf)?;
        let seg_seek_pos = u64::from_be_bytes(seg_seek_pos_buf);

        let entry_size = if idx + 1 < self.total_items {
            let mut seg_seek_end_buf = [0_u8; 8];
            self.index.read_exact(&mut seg_seek_end_buf)?;
            let seg_seek_end = u64::from_be_bytes(seg_seek_end_buf);

            let entry_size = seg_seek_end
                .checked_sub(seg_seek_pos)
                .ok_or_else(|| format_err!("size negative"))? as usize;

            let seg_seeked_to = self.segment.seek(SeekFrom::Start(seg_seek_pos))?;
            if seg_seeked_to != seg_seek_pos {
                bail!("seg seek invalid: {seg_seeked_to} != {seg_seek_pos}");
            }

            entry_size
        } else {
            self.segment_len - seg_seek_pos as usize
        };

        let mut entry = vec![0; entry_size];

        self.segment.read_exact(&mut entry)?;

        Ok(entry)
    }
}

#[derive(Debug)]
pub struct Snapshotter<Version, T>
where
    Version: SnapshotVersion,
    T: SnapshotObject,
{
    base_path: PathBuf,
    snapshots: Vec<Snapshot>,
    _marker: PhantomData<(Version, T)>,
}

impl<Version, T> Snapshotter<Version, T>
where
    Version: SnapshotVersion,
    T: SnapshotObject,
{
    pub fn new<K: TransactionKind, E: EnvironmentKind>(
        path: impl AsRef<Path>,
        tx: &MdbxTransaction<K, E>,
    ) -> anyhow::Result<Self> {
        Self::new_with_predicate(path, |snapshot_idx| {
            tx.get(T::db_table(), snapshot_idx as u64)
                .unwrap()
                .is_some()
        })
    }

    fn new_with_predicate(
        path: impl AsRef<Path>,
        predicate: impl Fn(usize) -> bool,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();

        std::fs::create_dir_all(&path)?;

        let mut snapshots = vec![];

        let mut snapshot_idx = 0;
        loop {
            let snapshot_directory = Self::snapshot_directory(snapshot_idx);
            let snapshot_path = path.join(&snapshot_directory);

            if !(predicate)(snapshot_idx) {
                break;
            }
            let index = OpenOptions::new()
                .read(true)
                .open(snapshot_path.join(INDEX_FILE_NAME))?;

            let segment = OpenOptions::new()
                .read(true)
                .open(snapshot_path.join(SEGMENT_FILE_NAME))?;

            let segment_len = segment.metadata()?.len() as usize;

            snapshots.push(Snapshot {
                total_items: Version::STRIDE.get(),
                segment_len,
                segment: BufReader::new(segment),
                index: BufReader::new(index),
            });
            snapshot_idx += 1;
        }

        Ok(Self {
            base_path: path,
            snapshots,
            _marker: PhantomData,
        })
    }

    fn snapshot_directory(snapshot_idx: usize) -> String {
        format!("{}-{}-{snapshot_idx:08}", Version::ID, T::ID)
    }

    pub fn get(&mut self, block_number: BlockNumber) -> anyhow::Result<Option<T>> {
        let snapshot_idx = block_number.0 as usize / Version::STRIDE.get();
        if let Some(snapshot) = self.snapshots.get_mut(snapshot_idx) {
            let entry_idx = block_number.0 as usize % Version::STRIDE.get();
            return Ok(Some(TableDecode::decode(&snapshot.read(entry_idx)?)?));
        }

        Ok(None)
    }

    pub fn max_block(&self) -> Option<BlockNumber> {
        (self.snapshots.len() * Version::STRIDE.get())
            .checked_sub(1)
            .map(|v| BlockNumber(v as u64))
    }

    pub fn next_max_block(&self) -> BlockNumber {
        BlockNumber((((self.snapshots.len() + 1) * Version::STRIDE.get()) - 1) as u64)
    }

    fn snapshot_path(&self, snapshot_idx: usize) -> PathBuf {
        self.base_path.join(Self::snapshot_directory(snapshot_idx))
    }

    pub fn snapshot_dirs(&self) -> Vec<PathBuf> {
        (0..self.snapshots.len())
            .map(|snapshot_idx| self.snapshot_path(snapshot_idx))
            .collect()
    }

    pub fn snapshot(
        &mut self,
        mut items: impl Iterator<Item = anyhow::Result<(BlockNumber, T)>>,
    ) -> anyhow::Result<(usize, PathBuf)> {
        let mut last_block = self.max_block();

        let next_snapshot_idx = self.snapshots.len();

        let snapshot_directory_name = Self::snapshot_directory(next_snapshot_idx);
        let snapshot_path = self.base_path.join(&snapshot_directory_name);

        let segment_file_path = snapshot_path.join(SEGMENT_FILE_NAME);
        let idx_file_path = snapshot_path.join(INDEX_FILE_NAME);

        if let Err(e) = std::fs::remove_dir_all(&snapshot_path) {
            if !matches!(e.kind(), ErrorKind::NotFound) {
                return Err(e.into());
            }
        }
        std::fs::create_dir_all(&snapshot_path)?;

        let mut segment = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(segment_file_path)?;

        let mut i = 0;
        let mut total_len = 0_usize;

        let mut index_data = Vec::with_capacity(Version::STRIDE.get() * 8);

        while let Some((block, item)) = items.next().transpose()? {
            if let Some(last_block) = last_block {
                if block != last_block + 1 {
                    return Err(format_err!("block gap between {last_block} and {block}"));
                }
            } else if block != 0 {
                return Err(format_err!("Empty snapshotter, but block (#{block}) != 0"));
            }

            last_block = Some(block);

            let encoded = item.encode();
            index_data.extend_from_slice(&total_len.to_be_bytes());
            segment.write_all(encoded.as_ref())?;

            i += 1;
            total_len += encoded.as_ref().len();

            if i == Version::STRIDE.get() {
                break;
            }
        }

        if i != Version::STRIDE.get() {
            return Err(format_err!("end too early"));
        }

        segment.flush()?;

        // Do this at the very end both for performance and to ensure index only gets created when all is said and done
        let mut index = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(idx_file_path)?;
        index.write_all(&index_data)?;

        index.flush()?;

        let snapshot_idx = self.snapshots.len();

        self.snapshots.push(Snapshot {
            total_items: i,
            segment_len: total_len,
            segment: BufReader::new(segment),
            index: BufReader::new(index),
        });

        Ok((next_snapshot_idx, self.snapshot_path(snapshot_idx)))
    }
}

trait SnapshotParams {
    const BASE_STRIDE: NonZeroUsize;
    const STRIDE_FACTOR: NonZeroUsize;
    const SNAPSHOTS_PER_LEVEL: NonZeroUsize;
}

const fn snapshot_stride<T: SnapshotParams>(level: u8) -> NonZeroUsize {
    unsafe {
        NonZeroUsize::new_unchecked(
            T::BASE_STRIDE.get() * (T::STRIDE_FACTOR.get().pow(level as u32)),
        )
    }
}

const fn stride(
    base_stride: NonZeroUsize,
    max_snapshots_per_level: NonZeroUsize,
    level: u8,
) -> NonZeroUsize {
    unsafe {
        NonZeroUsize::new_unchecked(
            base_stride.get() * (max_snapshots_per_level.get().pow(level as u32)),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SnapshotPos {
    level: usize,
    level_pos: usize,
}

fn snapshot_pos(
    base_stride: NonZeroUsize,
    max_snapshots_per_level: NonZeroUsize,
    snapshot_level_lens: &[usize],
    mut needle: usize,
) -> Option<SnapshotPos> {
    if snapshot_level_lens.is_empty() {
        return None;
    }

    for (current_level, current_level_len) in snapshot_level_lens.iter().copied().enumerate().rev()
    {
        let current_level_stride =
            stride(base_stride, max_snapshots_per_level, current_level as u8);
        for idx in 0..current_level_len {
            // Subtract until we overtake the needle
            if let Some(new_needle) = needle.checked_sub(current_level_stride.get()) {
                needle = new_needle;
            } else {
                // Hit
                return Some(SnapshotPos {
                    level: current_level,
                    level_pos: idx,
                });
            }
        }
    }

    None
}

fn snapshot_lens<const MIN_SNAPSHOT_POWER: usize>(
    num_snapshots: usize,
) -> impl Iterator<Item = usize> {
    (MIN_SNAPSHOT_POWER..MIN_SNAPSHOT_POWER + num_snapshots)
        .rev()
        .map(|power| 2_usize.pow(power as u32))
}

fn snapshot_pos2<const MIN_SNAPSHOT: usize>(
    num_snapshots: usize,
    mut needle: usize,
) -> Option<usize> {
    for (i, len) in snapshot_lens::<MIN_SNAPSHOT>(num_snapshots).enumerate() {
        if let Some(new_needle) = needle.checked_sub(len) {
            needle = new_needle
        } else {
            return Some(i);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::traits::TableEncode;
    use derive_more::From;
    use hex_literal::hex;
    use tempfile::tempdir;

    #[test]
    fn snapshot() {
        let tmp_dir = tempdir().unwrap();

        let items = [0x42_u64, 0x0, 0xDEADBEEF, 0xAACC, 0xBAADCAFE];

        struct TestSnapshot;

        impl SnapshotVersion for TestSnapshot {
            const ID: &'static str = "test";
            const STRIDE: NonZeroUsize = NonZeroUsize::new(4).unwrap();
        }

        #[derive(Clone, Copy, Debug, From)]
        pub struct TestNumber(pub U256);

        impl TableEncode for TestNumber {
            type Encoded = <U256 as TableEncode>::Encoded;

            fn encode(self) -> Self::Encoded {
                self.0.encode()
            }
        }

        impl TableDecode for TestNumber {
            fn decode(b: &[u8]) -> anyhow::Result<Self> {
                U256::decode(b).map(From::from)
            }
        }

        impl SnapshotObject for U256 {
            const ID: &'static str = "testobj";
            type Table = crate::kv::tables::HeaderSnapshot;

            fn db_table() -> Self::Table {
                crate::kv::tables::HeaderSnapshot
            }
        }

        for new in [true, false] {
            let mut snapshotter =
                Snapshotter::<TestSnapshot, U256>::new_with_predicate(&tmp_dir, |idx| {
                    idx < if new { 0 } else { 1 }
                })
                .unwrap();

            if new {
                assert_eq!(snapshotter.max_block(), None);
                assert_eq!(snapshotter.next_max_block(), 3);
                assert_eq!(snapshotter.get(0.into()).unwrap(), None);
                snapshotter
                    .snapshot(
                        items
                            .iter()
                            .enumerate()
                            .map(|(block, item)| Ok((BlockNumber(block as u64), item.as_u256()))),
                    )
                    .unwrap();
            }

            assert_eq!(snapshotter.max_block(), Some(BlockNumber(3)));
            assert_eq!(snapshotter.next_max_block(), 7);

            {
                let snapshot = snapshotter.snapshots.get_mut(0).unwrap();
                let mut segment_buffer = vec![];
                snapshot.segment.seek(SeekFrom::Start(0)).unwrap();
                snapshot.segment.read_to_end(&mut segment_buffer).unwrap();

                assert_eq!(&segment_buffer, &hex!("42DEADBEEFAACC"));

                let mut index_buffer = vec![];
                snapshot.index.seek(SeekFrom::Start(0)).unwrap();
                snapshot.index.read_to_end(&mut index_buffer).unwrap();

                assert_eq!(
                    &index_buffer,
                    &hex!("0000000000000000000000000000000100000000000000010000000000000005")
                );
            }

            for (i, item) in items.iter().enumerate().take(TestSnapshot::STRIDE.get()) {
                assert_eq!(
                    snapshotter.get((i as u64).into()).unwrap(),
                    Some(item.as_u256())
                );
            }

            for i in TestSnapshot::STRIDE.get()..items.len() {
                assert_eq!(snapshotter.get((i as u64).into()).unwrap(), None);
            }
        }
    }

    #[test]
    fn compute_stride() {
        for (base_stride, max_snapshots_per_level, level, expected_stride) in std::iter::empty()
            .chain(
                [(0, 1000), (1, 10000), (2, 100000), (3, 1000000)]
                    .iter()
                    .map(|&(level, expected_stride)| (1000, 10, level, expected_stride)),
            )
        {
            assert_eq!(
                stride(
                    NonZeroUsize::new(base_stride).unwrap(),
                    NonZeroUsize::new(max_snapshots_per_level).unwrap(),
                    level
                ),
                NonZeroUsize::new(expected_stride).unwrap()
            );
        }
    }

    #[test]
    fn compute_snapshot_idx() {
        for (base_stride, max_snapshots_per_level, snapshot_level_lens, needle, level, level_pos) in
            std::iter::empty().chain(
                [
                    (0, 3, 0),
                    (1, 3, 0),
                    (999_999, 3, 0),
                    (1_000_000, 3, 1),
                    (1_000_001, 3, 1),
                    (1_999_999, 3, 1),
                    (4_101_042, 3, 4),
                    (4_999_999, 3, 4),
                    (5_000_000, 1, 0),
                    (5_100_000, 0, 0),
                ]
                .iter()
                .map(|&(needle, level, level_pos)| {
                    //               0           |           1           |           2           |           3           |           4
                    // 0 | 5_100_000 - 5_100_999 | 5_101_000 - 5_101_999 | 5_102_000 - 5_102_999 | 5_103_000 - 5_103_999
                    // 1 | 5_000_000 - 5_000_999
                    // 2 |
                    // 3 | 0         - 999_999   | 1_000_000 - 1_999_999 | 2_000_000 - 2_999_999 | 3_000_000 - 3_999_999 | 4_000_000 - 4_999_999
                    (1000, 10, vec![4, 1, 0, 5], needle, level, level_pos)
                }),
            )
        {
            assert_eq!(
                snapshot_pos(
                    NonZeroUsize::new(base_stride).unwrap(),
                    NonZeroUsize::new(max_snapshots_per_level).unwrap(),
                    &snapshot_level_lens,
                    needle
                ),
                Some(SnapshotPos { level, level_pos })
            );
        }
    }

    #[test]
    fn compute_snapshot_lens() {
        assert_eq!(
            snapshot_lens::<10>(4).collect::<Vec<_>>(),
            vec![
                2_usize.pow(13),
                2_usize.pow(12),
                2_usize.pow(11),
                2_usize.pow(10),
            ]
        );
    }
}
