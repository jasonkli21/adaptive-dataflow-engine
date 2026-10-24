use ade_distributed::{
    coordinator::LocalCluster,
    ids::JobId,
    join::{self, Config, Source, Spec},
    join_policy::{Distribution, Policy},
};
use ade_exec::join::{Algorithm, Side};
use arrow::{
    array::{Int64Array, UInt64Array},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use std::{fs::File, path::Path, sync::Arc, time::Duration};
fn source(root: &Path, side: Side, rows: &[(u64, i64)], files: usize) -> Source {
    let mut paths = Vec::new();
    for f in 0..files {
        let path = root.join(format!("{side:?}-{f}.parquet"));
        let assigned: Vec<_> = rows
            .iter()
            .enumerate()
            .filter(|(i, _)| i % files == f)
            .map(|(_, r)| *r)
            .collect();
        let batch = RecordBatch::try_new(
            ade_exec::join::input_schema(),
            vec![
                Arc::new(UInt64Array::from(
                    assigned.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    assigned.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut w =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        paths.push((path, assigned.len() as u64));
    }
    Source::capture(side, paths).unwrap()
}
fn expected(left: &[(u64, i64)], right: &[(u64, i64)]) -> Vec<(u64, i64, i64)> {
    let mut rows = Vec::new();
    for &(k, l) in left {
        for &(r, v) in right {
            if k == r {
                rows.push((k, l, v));
            }
        }
    }
    rows.sort_unstable();
    rows
}
fn actual(batches: &[RecordBatch]) -> Vec<(u64, i64, i64)> {
    let mut rows = Vec::new();
    for b in batches {
        let k = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        let l = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let r = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            rows.push((k.value(i), l.value(i), r.value(i)));
        }
    }
    rows.sort_unstable();
    rows
}
fn spec(cluster: &LocalCluster, sources: [Source; 2], id: u64, config: Config) -> Spec {
    Spec {
        job: JobId(id),
        execution: uuid::Uuid::new_v4().to_string(),
        workers: cluster.workers.clone(),
        sources,
        config,
    }
}
#[test]
fn join_bags_all_physical_controls_two_three_four_workers() {
    let root = tempfile::tempdir().unwrap();
    let left = [(0, 1), (0, 1), (0, -1), (u64::MAX, i64::MIN), (5, 4)];
    let right = [(0, 2), (0, 2), (0, -4), (u64::MAX, i64::MAX), (9, 7)];
    let sources = [
        source(root.path(), Side::Left, &left, 4),
        source(root.path(), Side::Right, &right, 3),
    ];
    let expected = expected(&left, &right);
    for n in 2..=4 {
        let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), n).unwrap();
        let mut id = 0;
        for policy in [
            Policy::ForcedRepartition,
            Policy::ForcedBroadcast,
            Policy::ObservedV1,
        ] {
            for build in [Side::Left, Side::Right] {
                for algorithm in [Algorithm::HashOnly, Algorithm::Grace, Algorithm::Auto] {
                    let mut config = Config {
                        policy,
                        build: Some(build),
                        ..Default::default()
                    };
                    config.local.algorithm = algorithm;
                    let output = join::execute(
                        &spec(&cluster, sources.clone(), id, config),
                        Duration::from_secs(30),
                    )
                    .unwrap();
                    id += 1;
                    assert_eq!(actual(&output.batches), expected);
                    let broadcast = policy != Policy::ForcedRepartition;
                    assert_eq!(
                        output.metrics.decision.distribution,
                        if broadcast {
                            Distribution::BroadcastBuild
                        } else {
                            Distribution::HashRepartition
                        }
                    );
                    assert_eq!(output.metrics.finals.len(), n);
                    for side in 0..2 {
                        let sent: u64 = output
                            .metrics
                            .producers
                            .values()
                            .map(|m| m.sent[side].rows)
                            .sum();
                        assert_eq!(
                            sent,
                            5 * if broadcast && side == build.index() {
                                n as u64
                            } else {
                                1
                            }
                        );
                    }
                }
            }
        }
        let empty_root = tempfile::tempdir().unwrap();
        let empty = [
            source(empty_root.path(), Side::Left, &[], 4),
            source(empty_root.path(), Side::Right, &right, 3),
        ];
        let out = join::execute(
            &spec(&cluster, empty, id, Config::default()),
            Duration::from_secs(30),
        )
        .unwrap();
        assert_eq!(actual(&out.batches), []);
        let temp = cluster.temp_root().to_path_buf();
        drop(cluster);
        assert!(!temp.exists());
    }
}
#[test]
fn join_pressure_prefix_safety_refusals_and_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let rows: Vec<_> = (0..1024)
        .map(|i| (i, i64::try_from(i % 7).unwrap() - 3))
        .collect();
    let sources = [
        source(root.path(), Side::Left, &rows, 4),
        source(root.path(), Side::Right, &rows, 4),
    ];
    let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    let mut config = Config {
        probe_rows: 8,
        ..Default::default()
    };
    config.local.hash_rows = 32;
    config.local.memory_limit_bytes = 256 * 1024;
    let out = join::execute(
        &spec(&cluster, sources.clone(), 0, config.clone()),
        Duration::from_secs(30),
    )
    .unwrap();
    assert_eq!(actual(&out.batches), expected(&rows, &rows));
    assert_eq!(
        out.metrics.decision.distribution,
        Distribution::HashRepartition
    );
    assert!(out.metrics.decision.complete.iter().all(|v| !*v));
    assert!(out.metrics.finals.values().all(|m| {
        let l = m.local.as_ref().unwrap();
        l.transitioned
            && l.used_grace
            && l.cleaned
            && l.managed_current == 0
            && l.spill_fd_peak == 1
    }));
    config.policy = Policy::ForcedBroadcast;
    assert!(
        join::execute(
            &spec(&cluster, sources.clone(), 1, config.clone()),
            Duration::from_secs(30)
        )
        .is_err()
    );
    config.policy = Policy::ForcedRepartition;
    config.local.spill_enabled = false;
    assert!(
        join::execute(
            &spec(&cluster, sources.clone(), 2, config.clone()),
            Duration::from_secs(30)
        )
        .is_err()
    );
    config.local.spill_enabled = true;
    config.local.algorithm = Algorithm::Grace;
    let out = join::execute(
        &spec(&cluster, sources.clone(), 3, config.clone()),
        Duration::from_secs(30),
    )
    .unwrap();
    assert_eq!(actual(&out.batches), expected(&rows, &rows));
    config.local.algorithm = Algorithm::Auto;
    config.local.max_output_rows = 2;
    assert!(
        join::execute(
            &spec(&cluster, sources.clone(), 4, config.clone()),
            Duration::from_secs(30)
        )
        .is_err()
    );
    config.local.max_output_rows = 10000;
    let drift = sources;
    let changed: Vec<_> = rows.iter().map(|&(k, v)| (k, v + 1)).collect();
    source(root.path(), Side::Left, &changed, 4);
    let error = join::execute(&spec(&cluster, drift, 5, config), Duration::from_secs(30))
        .err()
        .expect("source drift");
    assert!(error.to_string().contains("source drift"));
    let temp = cluster.temp_root().to_path_buf();
    drop(cluster);
    assert!(!temp.exists());
}
#[cfg(feature = "research-hooks")]
#[test]
fn real_join_final_loss_after_gather_frame_never_returns_partial_output() {
    let root = tempfile::tempdir().unwrap();
    let rows: Vec<_> = (0..2048).map(|i| (i, 1)).collect();
    let sources = [
        source(root.path(), Side::Left, &rows, 4),
        source(root.path(), Side::Right, &rows, 4),
    ];
    let cluster = LocalCluster::spawn(Path::new(env!("CARGO_BIN_EXE_ade-worker")), 2).unwrap();
    let config = Config {
        policy: Policy::ForcedRepartition,
        research_exit_final: Some(0),
        ..Default::default()
    };
    let error = join::execute(&spec(&cluster, sources, 0, config), Duration::from_secs(30))
        .err()
        .expect("loss must fail");
    assert!(error.to_string().contains("unrecoverable"));
    let survivor = ade_distributed::protocol::rpc(
        &cluster.workers[1],
        ade_distributed::protocol::Request::Ping,
        Duration::from_secs(2),
    )
    .unwrap();
    assert!(matches!(
        survivor,
        ade_distributed::protocol::Reply::Pong(_)
    ));
    let temp = cluster.temp_root().to_path_buf();
    drop(cluster);
    assert!(!temp.exists());
}
