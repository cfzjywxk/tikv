// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use collections::HashMap;
use txn_types::{Key, TimeStamp};

use crate::storage::{
    metrics::CommandKind,
    mvcc::MvccReader,
    txn::{
        commands::{
            Command, CommandExt, PessimisticRollback, ReadCommand, ResolveLock, TypedCommand,
        },
        sched_pool::tls_collect_keyread_histogram_vec,
        ProcessResult, Result, RESOLVE_LOCK_BATCH_SIZE,
    },
    ScanMode, Snapshot, Statistics,
};

command! {
    /// Scan locks for resolving according to `txn_status`.
    ///
    /// During the GC operation, this should be called to find out stale locks whose timestamp is
    /// before safe point.
    /// This should followed by a `ResolveLock`.
    ResolveLockReadPhase:
        cmd_ty => (),
        display => "kv::resolve_lock_readphase", (),
        content => {
            /// Maps lock_ts to commit_ts. See ./resolve_lock.rs for details.
            txn_status: HashMap<TimeStamp, TimeStamp>,
            scan_key: Option<Key>,
            next_cmd: CommandKind,
            /// The pessimistic locks that would be rolled back belonging to. Check pessimistic
            /// rollback for more details.
            start_ts: TimeStamp,
            for_update_ts: TimeStamp,
        }
}

impl ResolveLockReadPhase {
    pub fn new_for_pessimistic_rollback(
        start_ts: TimeStamp,
        for_update_ts: TimeStamp,
        scan_key: Option<Key>,
        ctx: crate::storage::Context,
    ) -> TypedCommand<Vec<crate::storage::Result<()>>> {
        let execution_duration_limit = if ctx.max_execution_duration_ms == 0 {
            crate::storage::txn::scheduler::DEFAULT_EXECUTION_DURATION_LIMIT
        } else {
            ::std::time::Duration::from_millis(ctx.max_execution_duration_ms)
        };
        let deadline = ::tikv_util::deadline::Deadline::from_now(execution_duration_limit);
        let resolve_lock_read_phase = ResolveLockReadPhase {
            ctx,
            deadline,
            txn_status: HashMap::default(),
            scan_key,
            next_cmd: CommandKind::pessimistic_rollback,
            start_ts,
            for_update_ts,
        };

        Command::ResolveLockReadPhase(resolve_lock_read_phase).into()
    }
}

impl CommandExt for ResolveLockReadPhase {
    ctx!();
    tag!(resolve_lock);
    request_type!(KvResolveLock);
    property!(readonly);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

impl<S: Snapshot> ReadCommand<S> for ResolveLockReadPhase {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let tag = self.tag();
        let (ctx, txn_status) = (self.ctx, self.txn_status);
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &ctx);
        let result = reader.scan_locks(
            self.scan_key.as_ref(),
            None,
            |lock| match self.next_cmd {
                CommandKind::resolve_lock => txn_status.contains_key(&lock.ts),
                CommandKind::pessimistic_rollback => lock.ts == self.start_ts,
                _ => unreachable!(),
            },
            RESOLVE_LOCK_BATCH_SIZE,
        );
        statistics.add(&reader.statistics);
        let (kv_pairs, has_remain) = result?;
        tls_collect_keyread_histogram_vec(tag.get_str(), kv_pairs.len() as f64);

        if kv_pairs.is_empty() {
            Ok(ProcessResult::Res)
        } else {
            let next_scan_key = if has_remain {
                // There might be more locks.
                kv_pairs.last().map(|(k, _lock)| k.clone())
            } else {
                // All locks are scanned
                None
            };
            let cmd = match self.next_cmd {
                CommandKind::resolve_lock => {
                    let next_cmd = ResolveLock {
                        ctx,
                        deadline: self.deadline,
                        txn_status,
                        scan_key: next_scan_key,
                        key_locks: kv_pairs,
                    };
                    Command::ResolveLock(next_cmd)
                }
                CommandKind::pessimistic_rollback => {
                    assert!(txn_status.is_empty());
                    assert_gt!(self.start_ts, TimeStamp::default());
                    assert_ge!(self.for_update_ts, self.start_ts);
                    let next_cmd = PessimisticRollback {
                        deadline: self.deadline,
                        keys: kv_pairs.into_iter().map(|(key, _)| key).collect(),
                        start_ts: self.start_ts,
                        for_update_ts: self.for_update_ts,
                        scan_key: next_scan_key,
                        ctx,
                    };
                    Command::PessimisticRollback(next_cmd.into())
                }
                _ => unreachable!(),
            };
            Ok(ProcessResult::NextCommand { cmd })
        }
    }
}
