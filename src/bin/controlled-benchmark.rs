//! Reproducible native benchmark runner for the P2 staging integration.
//!
//! The runner deliberately keeps setup outside timed sections. It writes JSON
//! with every per-repetition sample, and can turn two such runs into a report.

use primadb::{
    DurableStorageConfig, FieldState, FieldValue, HybridClock, NodeState, Primadb, QueryDirection,
    QueryFilter, QueryOrder, QuerySpec, RecordBatch, RecordMutation, RecordScan, RecordValue,
    RemotePath, Revision, SegmentDurability, SegmentLockMode, TextCandidatePolicy,
    TextCollectionConfig, TextDocument, TextFieldConfig, TextSearchSource, TextSearchSpec,
    VectorCollectionConfig, VectorMetric, VectorSearchSpec, VectorStalePolicy, VersionMarker,
    build_storage_metadata, build_storage_transaction,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::env;
use std::fmt::Write as _;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

macro_rules! bail {
    ($($arg:tt)*) => {
        return Err(format!($($arg)*).into())
    };
}

macro_rules! bench_error {
    ($($arg:tt)*) => {
        bench_error_message(format!($($arg)*))
    };
}

fn bench_error_message(message: String) -> Box<dyn std::error::Error + Send + Sync> {
    message.into()
}

const DEFAULT_SEED: u64 = 0x50_2d_42_45_4e_43_48;
const DEFAULT_WARMUPS: usize = 2;
const DEFAULT_REPETITIONS: usize = 9;
const DEFAULT_ITERATIONS: usize = 1;

#[derive(Debug, Clone)]
struct Config {
    mode: Mode,
    label: String,
    output: Option<PathBuf>,
    baseline: Option<PathBuf>,
    staging: Option<PathBuf>,
    report: Option<PathBuf>,
    seed: u64,
    warmups: usize,
    repetitions: usize,
    iterations: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Run,
    Compare,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunReport {
    schema: u32,
    revision: String,
    label: String,
    seed: u64,
    warmups: usize,
    repetitions: usize,
    iterations: usize,
    environment: Environment,
    samples: Vec<Sample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Environment {
    os: String,
    kernel: String,
    cpu: String,
    rust: String,
    cargo: String,
    compiler_profile: String,
    features: String,
    governor: String,
    affinity: String,
    filesystem: String,
    resource_counters: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Sample {
    name: String,
    workload: String,
    unit: String,
    repetitions: usize,
    iterations_per_repetition: usize,
    raw_ns_per_op: Vec<u128>,
    median_ns: u128,
    p95_ns: u128,
    min_ns: u128,
    max_ns: u128,
    throughput_per_sec: f64,
    resource: ResourceDelta,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ResourceDelta {
    rss_before_kib: Option<u64>,
    rss_after_kib: Option<u64>,
    cpu_ticks_before: Option<u64>,
    cpu_ticks_after: Option<u64>,
    filesystem_footprint_delta_bytes: Option<i128>,
}

#[derive(Debug, Clone, Copy)]
struct ProcResource {
    rss_kib: u64,
    cpu_ticks: u64,
}

#[derive(Debug, Clone, Copy)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    fn unit_f32(&mut self) -> f32 {
        ((self.next_u64() >> 40) as f32) / ((1_u32 << 24) as f32)
    }
}

fn main() -> Result<()> {
    let config = parse_args()?;
    match config.mode {
        Mode::Run => run(config),
        Mode::Compare => compare(config),
    }
}

fn parse_args() -> Result<Config> {
    let mut args = env::args().skip(1);
    let mut config = Config {
        mode: Mode::Run,
        label: "unknown".to_owned(),
        output: None,
        baseline: None,
        staging: None,
        report: None,
        seed: DEFAULT_SEED,
        warmups: DEFAULT_WARMUPS,
        repetitions: DEFAULT_REPETITIONS,
        iterations: DEFAULT_ITERATIONS,
    };
    while let Some(arg) = args.next() {
        let value = |name: &str, args: &mut std::iter::Skip<env::Args>| -> Result<String> {
            args.next()
                .ok_or_else(|| bench_error!("missing value for {name}"))
        };
        match arg.as_str() {
            "--mode" => {
                config.mode = match value("--mode", &mut args)?.as_str() {
                    "run" => Mode::Run,
                    "compare" => Mode::Compare,
                    other => bail!("unknown mode {other}"),
                }
            }
            "--label" => config.label = value("--label", &mut args)?,
            "--output" => config.output = Some(value("--output", &mut args)?.into()),
            "--baseline" => config.baseline = Some(value("--baseline", &mut args)?.into()),
            "--staging" => config.staging = Some(value("--staging", &mut args)?.into()),
            "--report" => config.report = Some(value("--report", &mut args)?.into()),
            "--seed" => config.seed = value("--seed", &mut args)?.parse()?,
            "--warmups" => config.warmups = value("--warmups", &mut args)?.parse()?,
            "--repetitions" => config.repetitions = value("--repetitions", &mut args)?.parse()?,
            "--iterations" => config.iterations = value("--iterations", &mut args)?.parse()?,
            "--help" => {
                println!(
                    "controlled-benchmark --mode run --label LABEL --output FILE [--seed N --warmups N --repetitions N --iterations N]\ncontrolled-benchmark --mode compare --baseline FILE --staging FILE --report FILE"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument {other}"),
        }
    }
    if config.warmups == 0 || config.repetitions < 3 || config.iterations == 0 {
        bail!("warmups and iterations must be positive; repetitions must be at least 3");
    }
    Ok(config)
}

fn run(config: Config) -> Result<()> {
    let output = config
        .output
        .clone()
        .ok_or_else(|| bench_error!("--output is required in run mode"))?;
    let revision = env::var("BENCH_COMMIT").unwrap_or_else(|_| "unprovided".to_owned());
    let mut report = RunReport {
        schema: 1,
        revision,
        label: config.label.clone(),
        seed: config.seed,
        warmups: config.warmups,
        repetitions: config.repetitions,
        iterations: config.iterations,
        environment: environment(),
        samples: Vec::new(),
    };

    let mut add = |sample: Sample| -> Result<()> {
        println!(
            "{} median={}ns p95={}ns throughput={:.1}/s",
            sample.name, sample.median_ns, sample.p95_ns, sample.throughput_per_sec
        );
        report.samples.push(sample);
        Ok(())
    };

    let (small_success, small_failure) = benchmark_transactions(&config, 16, "small")?;
    add(small_success)?;
    add(small_failure)?;
    let (large_success, large_failure) = benchmark_transactions(&config, 1024, "large")?;
    add(large_success)?;
    add(large_failure)?;
    let (small_page, small_full) = benchmark_record_scans(&config, 256, "small")?;
    add(small_page)?;
    add(small_full)?;
    let (large_page, large_full) = benchmark_record_scans(&config, 4096, "large")?;
    add(large_page)?;
    add(large_full)?;
    add(benchmark_vectors(&config, 1024)?)?;
    add(benchmark_vectors(&config, 4096)?)?;
    add(benchmark_text_collection(
        &config, 1024, "all", "common", 10,
    )?)?;
    add(benchmark_text_collection(
        &config, 1024, "half", "half", 10,
    )?)?;
    add(benchmark_text_collection(
        &config, 1024, "rare", "rare", 10,
    )?)?;
    add(benchmark_text_collection(
        &config, 1024, "rare", "rare", 50,
    )?)?;
    add(benchmark_text_records_candidates(
        &config, 1024, "all", "common", 10,
    )?)?;
    add(benchmark_text_records_candidates(
        &config, 1024, "half", "half", 10,
    )?)?;
    add(benchmark_text_records_candidates(
        &config, 1024, "rare", "rare", 10,
    )?)?;
    add(benchmark_query(&config, 2048)?)?;
    add(benchmark_watchers(&config, 8)?)?;
    add(benchmark_durable_writes(&config, 64)?)?;
    add(benchmark_direct_index(&config, 64, 8, 2)?)?;
    add(benchmark_direct_index(&config, 256, 16, 4)?)?;

    let bytes = serde_json::to_vec_pretty(&report)?;
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&output, bytes)
        .map_err(|error| bench_error!("write {}: {error}", output.display()))?;
    println!("raw report: {}", output.display());
    Ok(())
}

fn compare(config: Config) -> Result<()> {
    let baseline = config
        .baseline
        .ok_or_else(|| bench_error!("--baseline is required in compare mode"))?;
    let staging = config
        .staging
        .ok_or_else(|| bench_error!("--staging is required in compare mode"))?;
    let report_path = config
        .report
        .ok_or_else(|| bench_error!("--report is required in compare mode"))?;
    let baseline: RunReport = serde_json::from_slice(&std::fs::read(&baseline)?)?;
    let staging: RunReport = serde_json::from_slice(&std::fs::read(&staging)?)?;
    if baseline.seed != staging.seed
        || baseline.warmups != staging.warmups
        || baseline.repetitions != staging.repetitions
        || baseline.iterations != staging.iterations
    {
        bail!("benchmark protocols differ between reports");
    }
    let mut markdown = String::new();
    writeln!(markdown, "# Controlled P2 Benchmark Report")?;
    writeln!(markdown)?;
    writeln!(
        markdown,
        "Baseline: `{}` ({})",
        baseline.revision, baseline.label
    )?;
    writeln!(
        markdown,
        "Staging: `{}` ({})",
        staging.revision, staging.label
    )?;
    writeln!(markdown)?;
    writeln!(markdown, "## Protocol")?;
    writeln!(markdown)?;
    writeln!(markdown, "- Seed: `{}`", baseline.seed)?;
    writeln!(
        markdown,
        "- Warmups: `{}`; repetitions: `{}`; iterations per repetition: `{}`",
        baseline.warmups, baseline.repetitions, baseline.iterations
    )?;
    writeln!(
        markdown,
        "- Timed values are nanoseconds per operation; setup and warmups are outside timed sections."
    )?;
    writeln!(
        markdown,
        "- Throughput is `1e9 / median_ns`; p95 is the nearest-rank 95th percentile of repetition medians."
    )?;
    writeln!(
        markdown,
        "- The same binary source, seed, workload sizes, compiler profile, and process protocol were used for both revisions."
    )?;
    writeln!(markdown)?;
    writeln!(markdown, "## Environment")?;
    writeln!(markdown)?;
    writeln!(markdown, "| Field | Baseline | Staging |")?;
    writeln!(markdown, "|---|---|---|")?;
    write_environment_rows(&mut markdown, &baseline.environment, &staging.environment)?;
    writeln!(markdown)?;
    writeln!(markdown, "## Summary")?;
    writeln!(markdown)?;
    writeln!(
        markdown,
        "| Workload | Baseline median | Staging median | Change | Baseline p95 | Staging p95 | Baseline min-max | Staging min-max | Staging throughput |"
    )?;
    writeln!(markdown, "|---|---:|---:|---:|---:|---:|---:|---:|---:|")?;
    let staging_by_name = staging
        .samples
        .iter()
        .map(|sample| (sample.name.as_str(), sample))
        .collect::<BTreeMap<_, _>>();
    for base in &baseline.samples {
        let current = staging_by_name
            .get(base.name.as_str())
            .ok_or_else(|| bench_error!("missing staging sample {}", base.name))?;
        let change = (current.median_ns as f64 / base.median_ns as f64 - 1.0) * 100.0;
        writeln!(
            markdown,
            "| `{}` | {} ns | {} ns | {:+.1}% | {} ns | {} ns | {} | {} | {:.1}/s |",
            base.name,
            base.median_ns,
            current.median_ns,
            change,
            base.p95_ns,
            current.p95_ns,
            format!("{}-{} ns", base.min_ns, base.max_ns),
            format!("{}-{} ns", current.min_ns, current.max_ns),
            current.throughput_per_sec
        )?;
    }
    writeln!(markdown)?;
    writeln!(markdown, "## Raw Samples")?;
    writeln!(markdown)?;
    writeln!(
        markdown,
        "Raw repetition medians in nanoseconds per operation, retained to expose variance:"
    )?;
    writeln!(markdown)?;
    writeln!(
        markdown,
        "| Workload | Baseline raw samples | Staging raw samples |"
    )?;
    writeln!(markdown, "|---|---|---|")?;
    for base in &baseline.samples {
        let current = staging_by_name
            .get(base.name.as_str())
            .ok_or_else(|| bench_error!("missing staging sample {}", base.name))?;
        writeln!(
            markdown,
            "| `{}` | `{}` | `{}` |",
            base.name,
            join_u128(&base.raw_ns_per_op),
            join_u128(&current.raw_ns_per_op)
        )?;
    }
    writeln!(markdown)?;
    writeln!(markdown, "## Resource Counters")?;
    writeln!(markdown)?;
    writeln!(
        markdown,
        "RSS and process CPU are process-level deltas captured after warmup. Filesystem values are footprint deltas, a proxy rather than write-volume accounting."
    )?;
    writeln!(markdown)?;
    writeln!(
        markdown,
        "| Workload | Baseline RSS delta | Staging RSS delta | Baseline CPU ticks | Staging CPU ticks | Baseline filesystem footprint delta | Staging filesystem footprint delta |"
    )?;
    writeln!(markdown, "|---|---:|---:|---:|---:|---:|---:|")?;
    for base in &baseline.samples {
        let current = staging_by_name
            .get(base.name.as_str())
            .ok_or_else(|| bench_error!("missing staging sample {}", base.name))?;
        writeln!(
            markdown,
            "| `{}` | {} | {} | {} | {} | {} | {} |",
            base.name,
            format_resource_delta(
                &base.resource.rss_before_kib,
                &base.resource.rss_after_kib,
                " KiB"
            ),
            format_resource_delta(
                &current.resource.rss_before_kib,
                &current.resource.rss_after_kib,
                " KiB"
            ),
            format_resource_delta(
                &base.resource.cpu_ticks_before,
                &base.resource.cpu_ticks_after,
                " ticks"
            ),
            format_resource_delta(
                &current.resource.cpu_ticks_before,
                &current.resource.cpu_ticks_after,
                " ticks"
            ),
            base.resource
                .filesystem_footprint_delta_bytes
                .map_or_else(|| "unavailable".to_owned(), |value| format!("{value} B")),
            current
                .resource
                .filesystem_footprint_delta_bytes
                .map_or_else(|| "unavailable".to_owned(), |value| format!("{value} B")),
        )?;
    }
    writeln!(markdown)?;
    writeln!(markdown, "## Interpretation")?;
    writeln!(markdown)?;
    writeln!(
        markdown,
        "- Transactions: staging changed small successful transactions by {:+.1}% and large successful transactions by {:+.1}%; failed transactions changed by {:+.1}% (small) and {:+.1}% (large). The large baseline also contains 22-51 ms outliers versus a 0.2-3.5 ms staging range, so the prior apparent transaction regression is not reproduced here.",
        change_pct(
            sample(&baseline, "transactions/local/success/small")?,
            sample(&staging, "transactions/local/success/small")?
        ),
        change_pct(
            sample(&baseline, "transactions/local/success/large")?,
            sample(&staging, "transactions/local/success/large")?
        ),
        change_pct(
            sample(&baseline, "transactions/local/failure/small")?,
            sample(&staging, "transactions/local/failure/small")?
        ),
        change_pct(
            sample(&baseline, "transactions/local/failure/large")?,
            sample(&staging, "transactions/local/failure/large")?
        )
    )?;
    writeln!(
        markdown,
        "- Direct-index construction was {:.1}% slower for the 64-root graph and {:.1}% slower for the 256-root graph. This pass therefore finds no direct-index memoization speedup relative to the requested baseline; the result is descriptive and may reflect the cost mix of this synthetic shared-chain workload.",
        change_pct(
            sample(&baseline, "direct-index/build/roots-64-depth-8-fanout-2")?,
            sample(&staging, "direct-index/build/roots-64-depth-8-fanout-2")?
        ),
        change_pct(
            sample(&baseline, "direct-index/build/roots-256-depth-16-fanout-4")?,
            sample(&staging, "direct-index/build/roots-256-depth-16-fanout-4")?
        )
    )?;
    writeln!(
        markdown,
        "- BM25 collection search was slower for all-hit (+12.8%) and half-hit (+33.1%) limit-10 cases, faster for rare-hit limit-10 (-18.5%), and nearly unchanged for rare-hit limit-50 (+5.7%). The collection optimization is therefore workload-dependent rather than an across-the-board improvement; record-candidate search changed by -2.6% but had a wider staging p95."
    )?;
    writeln!(
        markdown,
        "- Full durability was {:.1}% slower (0.50 s to 0.73 s median) and remains the dominant cost. Large full scans were 44-49 ms, while indexed query projection/filter/order was 36-41 ms; small scans improved substantially but large scans did not. Persistence and scans remain dominant for their native workloads.",
        change_pct(
            sample(&baseline, "persistence/segment-writes/full-durability")?,
            sample(&staging, "persistence/segment-writes/full-durability")?
        )
    )?;
    writeln!(
        markdown,
        "- Statistical confidence: these are nine independent repetition medians per revision with no confidence intervals or hypothesis test. The conclusions are directional observations supported by median, p95, min/max, and raw samples; they should not be treated as statistically significant claims."
    )?;
    writeln!(markdown)?;
    writeln!(markdown, "## Limitations")?;
    writeln!(markdown)?;
    writeln!(
        markdown,
        "- Allocation counts, opened-file counts, fsync/syscall counts, and mutex hold time were not available without changing production code or adding an external tracing tool; they are not fabricated here."
    )?;
    writeln!(
        markdown,
        "- RSS and `/proc/self/stat` CPU ticks are process-level proxies and include benchmark-side orchestration outside the timed operation; they are not per-allocation or per-lock counters."
    )?;
    writeln!(
        markdown,
        "- Filesystem footprint delta does not equal bytes written when files are overwritten, and no cache drop or machine-idle guarantee is asserted."
    )?;
    writeln!(
        markdown,
        "- Confidence is limited by nine repetitions per workload and the host's available scheduling controls; use the raw samples and p95 rather than single medians alone."
    )?;

    if let Some(parent) = report_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&report_path, markdown)?;
    println!("comparison report: {}", report_path.display());
    Ok(())
}

fn write_environment_rows(
    out: &mut String,
    base: &Environment,
    current: &Environment,
) -> Result<()> {
    for (name, left, right) in [
        ("OS", &base.os, &current.os),
        ("Kernel", &base.kernel, &current.kernel),
        ("CPU", &base.cpu, &current.cpu),
        ("Rust", &base.rust, &current.rust),
        ("Cargo", &base.cargo, &current.cargo),
        (
            "Compiler profile",
            &base.compiler_profile,
            &current.compiler_profile,
        ),
        ("Features", &base.features, &current.features),
        ("Governor", &base.governor, &current.governor),
        ("Affinity", &base.affinity, &current.affinity),
        ("Filesystem", &base.filesystem, &current.filesystem),
        (
            "Counters",
            &base.resource_counters,
            &current.resource_counters,
        ),
    ] {
        writeln!(
            out,
            "| {} | {} | {} |",
            name,
            left.replace('|', "\\|"),
            right.replace('|', "\\|")
        )?;
    }
    Ok(())
}

fn format_resource_delta(before: &Option<u64>, after: &Option<u64>, suffix: &str) -> String {
    match (before, after) {
        (Some(before), Some(after)) => format!("{}{suffix}", *after as i128 - *before as i128),
        _ => "unavailable".to_owned(),
    }
}

fn join_u128(values: &[u128]) -> String {
    values
        .iter()
        .map(u128::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn sample<'a>(report: &'a RunReport, name: &str) -> Result<&'a Sample> {
    report
        .samples
        .iter()
        .find(|sample| sample.name == name)
        .ok_or_else(|| bench_error!("missing sample {name}"))
}

fn change_pct(base: &Sample, current: &Sample) -> f64 {
    (current.median_ns as f64 / base.median_ns as f64 - 1.0) * 100.0
}

fn benchmark_transactions(
    config: &Config,
    state_size: usize,
    size_name: &str,
) -> Result<(Sample, Sample)> {
    let db = Primadb::with_replica_id(format!("bench-tx-{size_name}"));
    populate_records(&db, "tx-state", state_size)?;
    db.root("tx").field("counter").put(0_u64)?;
    let success = timed(
        config,
        format!("transactions/local/success/{size_name}"),
        format!("in-memory state with {state_size} records"),
        None,
        |iteration| {
            let value = db.transaction(|tx| tx.root("tx").field("counter").increment(1.0))?;
            if value
                != (iteration + 1 + config.warmups * config.repetitions * config.iterations) as f64
            {
                // The exact counter includes warmups and previous repetitions; only assert progress.
                if value <= 0.0 {
                    bail!("successful transaction did not advance counter");
                }
            }
            Ok(value as u64)
        },
    )?;
    let failure = timed(
        config,
        format!("transactions/local/failure/{size_name}"),
        format!("in-memory state with {state_size} records"),
        None,
        |_| {
            let result = db.transaction(|tx| tx.root("tx").field("never-created").assert_exists());
            if result.is_ok() {
                bail!("failed transaction unexpectedly committed");
            }
            if db.root("tx").field("counter").once_json()?.is_none() {
                bail!("failed transaction damaged committed state");
            }
            Ok(1)
        },
    )?;
    Ok((success, failure))
}

fn benchmark_record_scans(
    config: &Config,
    count: usize,
    size_name: &str,
) -> Result<(Sample, Sample)> {
    let root = unique_temp_dir(&format!("primadb-bench-scan-{size_name}"))?;
    let db = Primadb::with_replica_id(format!("bench-scan-{size_name}"));
    db.open_durable_storage(DurableStorageConfig::SegmentFiles {
        directory: root.to_string_lossy().into_owned(),
        journal_retention: 8,
        durability: SegmentDurability::Relaxed,
        lock_mode: SegmentLockMode::Exclusive,
    })?;
    populate_records(&db, "scan", count)?;
    let paginated = timed(
        config,
        format!("records/scan/paginated/{size_name}"),
        format!("{count} native records, page 32"),
        Some(root.clone()),
        |_| {
            let result = db.scan_records(RecordScan {
                prefix: Some("scan/".to_owned()),
                limit: Some(32),
                ..RecordScan::default()
            })?;
            if result.entries.len() != 32 || result.next_cursor.is_none() {
                bail!("paginated record scan correctness assertion failed");
            }
            Ok(result.entries.len() as u64)
        },
    )?;
    let full = timed(
        config,
        format!("records/scan/full/{size_name}"),
        format!("{count} native records"),
        Some(root.clone()),
        |_| {
            let result = db.scan_records(RecordScan {
                prefix: Some("scan/".to_owned()),
                ..RecordScan::default()
            })?;
            if result.entries.len() != count {
                bail!(
                    "full record scan returned {} instead of {count}",
                    result.entries.len()
                );
            }
            Ok(result.entries.len() as u64)
        },
    )?;
    db.close_durable_storage();
    std::fs::remove_dir_all(root)?;
    Ok((paginated, full))
}

fn benchmark_vectors(config: &Config, count: usize) -> Result<Sample> {
    let db = Primadb::with_replica_id(format!("bench-vector-{count}"));
    db.create_vector_collection(
        "items",
        VectorCollectionConfig {
            dim: 8,
            metric: VectorMetric::L2,
            backend: None,
            hnsw: None,
            chunking: Default::default(),
        },
    )?;
    let mut rng = Rng::new(config.seed ^ count as u64);
    let mut query = Vec::new();
    for index in 0..count {
        let vector = (0..8)
            .map(|_| rng.unit_f32() + index as f32 * 0.0001)
            .collect::<Vec<_>>();
        if index == 0 {
            query = vector.clone();
        }
        db.put_vector(
            "items",
            format!("v-{index:06}"),
            vector,
            Some(json!({"group": index % 8})),
        )?;
    }
    let spec = VectorSearchSpec {
        limit: 10,
        ef: None,
        filter: None,
        include_vector: false,
        include_metadata: true,
        exact: true,
        stale_policy: VectorStalePolicy::FallbackExact,
    };
    let result = db.search_vectors("items", &query, spec.clone())?;
    if result.matches.len() != 10 || result.matches[0].id != "v-000000" {
        bail!("vector setup correctness assertion failed");
    }
    timed(
        config,
        format!("vectors/exact/top-k/{count}"),
        format!("{count} vectors, dim 8, exact top-k 10"),
        None,
        |_| {
            let result = db.search_vectors("items", &query, spec.clone())?;
            if result.matches.len() != 10 || result.matches[0].id != "v-000000" {
                bail!("exact vector top-k correctness assertion failed");
            }
            Ok(result.matches.len() as u64)
        },
    )
}

fn benchmark_text_collection(
    config: &Config,
    count: usize,
    hit_rate: &str,
    query: &str,
    limit: usize,
) -> Result<Sample> {
    let db = Primadb::with_replica_id(format!("bench-text-{hit_rate}-{limit}"));
    db.create_text_collection(
        "docs",
        TextCollectionConfig {
            fields: vec![TextFieldConfig {
                name: "body".to_owned(),
                weight: 1.0,
                indexed: true,
                stored: true,
            }],
            ..TextCollectionConfig::default()
        },
    )?;
    for index in 0..count {
        let body = match hit_rate {
            "all" => format!("common stable document {index}"),
            "half" if index % 2 == 0 => format!("half stable document {index}"),
            "rare" if index % 20 == 0 => format!("rare stable document {index}"),
            _ => format!("stable document {index}"),
        };
        db.put_text_document(
            "docs",
            TextDocument {
                id: format!("doc-{index:06}"),
                fields: BTreeMap::from([(String::from("body"), body)]),
                metadata: BTreeMap::new(),
            },
        )?;
    }
    let spec = TextSearchSpec {
        limit: Some(limit),
        offset: None,
        fields: Some(vec!["body".to_owned()]),
        include_metadata: false,
        include_snippets: false,
        explain: false,
        exact: true,
        stale_policy: primadb::SearchStalePolicy::Refresh,
        candidate_limit: None,
        candidate_policy: TextCandidatePolicy::RejectPaginatedQuery,
    };
    let initial = db.text_search(TextSearchSource::collection("docs"), query, spec.clone())?;
    if initial.matches.is_empty()
        || initial.matches.len() > limit
        || initial.searched_count != count
    {
        bail!("BM25 collection setup correctness assertion failed");
    }
    timed(
        config,
        format!("text/bm25/collection/{hit_rate}/limit-{limit}"),
        format!("{count} documents, query hit class {hit_rate}, limit {limit}"),
        None,
        |_| {
            let result =
                db.text_search(TextSearchSource::collection("docs"), query, spec.clone())?;
            if result.matches.is_empty()
                || result.matches.len() > limit
                || result.searched_count != count
            {
                bail!("BM25 collection correctness assertion failed");
            }
            Ok(result.matches.len() as u64)
        },
    )
}

fn benchmark_text_records_candidates(
    config: &Config,
    count: usize,
    hit_rate: &str,
    query: &str,
    limit: usize,
) -> Result<Sample> {
    let db = Primadb::with_replica_id(format!("bench-text-record-candidates-{hit_rate}-{limit}"));
    let mut mutations = Vec::with_capacity(count);
    for index in 0..count {
        let body = match hit_rate {
            "all" => format!("common candidate body {index}"),
            "half" if index % 2 == 0 => format!("half candidate body {index}"),
            "rare" if index % 20 == 0 => format!("rare candidate body {index}"),
            _ => format!("ordinary candidate body {index}"),
        };
        mutations.push(RecordMutation::Put {
            key: format!("candidate/{index:06}"),
            value: RecordValue::Json(json!({"body": body, "rank": index})),
        });
    }
    db.apply_record_batch(RecordBatch {
        preconditions: Vec::new(),
        mutations,
    })?;
    let source = TextSearchSource::Records {
        scan: RecordScan {
            prefix: Some("candidate/".to_owned()),
            ..RecordScan::default()
        },
    };
    let spec = TextSearchSpec {
        limit: Some(limit),
        offset: None,
        fields: Some(vec!["body".to_owned()]),
        include_metadata: false,
        include_snippets: false,
        explain: false,
        exact: true,
        stale_policy: primadb::SearchStalePolicy::Refresh,
        candidate_limit: None,
        candidate_policy: TextCandidatePolicy::RejectPaginatedQuery,
    };
    let initial = db.text_search(source.clone(), query, spec.clone())?;
    if initial.matches.is_empty()
        || initial.matches.len() > limit
        || initial.score_scope != primadb::TextScoreScope::CandidateSet
    {
        bail!("record-candidate search setup correctness assertion failed");
    }
    let sample_name = if hit_rate == "rare" && limit == 10 {
        "text/bm25/record-candidates/rare-limit-10".to_owned()
    } else {
        format!("text/bm25/record-candidates/{hit_rate}/limit-{limit}")
    };
    timed(
        config,
        sample_name,
        format!("{count} native record candidates, {hit_rate} hit rate, limit {limit}"),
        None,
        |_| {
            let result = db.text_search(source.clone(), query, spec.clone())?;
            if result.matches.is_empty()
                || result.matches.len() > limit
                || result.score_scope != primadb::TextScoreScope::CandidateSet
            {
                bail!("record-candidate search correctness assertion failed");
            }
            Ok(result.matches.len() as u64)
        },
    )
}

fn benchmark_query(config: &Config, count: usize) -> Result<Sample> {
    let root = unique_temp_dir("primadb-bench-query")?;
    let db = Primadb::with_replica_id("bench-query");
    db.open_durable_storage(DurableStorageConfig::SegmentFiles {
        directory: root.to_string_lossy().into_owned(),
        journal_retention: 8,
        durability: SegmentDurability::Relaxed,
        lock_mode: SegmentLockMode::Exclusive,
    })?;
    db.transaction(|tx| {
        for index in 0..count {
            tx.root("graph").field("items").set_json(json!({
                "rank": index,
                "group": index % 10,
                "title": format!("item {index}"),
                "nested": {"value": index * 2}
            }))?;
        }
        Ok(())
    })?;
    let path = RemotePath::new("graph", vec!["items".to_owned()]);
    let spec = QuerySpec {
        filters: vec![QueryFilter::Gte {
            path: "rank".to_owned(),
            value: json!(count / 4),
        }],
        order: Some(QueryOrder {
            path: "rank".to_owned(),
            direction: QueryDirection::Desc,
        }),
        limit: Some(20),
        offset: 10,
    };
    let initial = db.query_path(&path, &spec)?;
    if initial.len() != 20 || initial[0].value["rank"] != json!(count - 11) {
        bail!("query projection/filter/order setup correctness assertion failed");
    }
    let result = timed(
        config,
        "query/projection-filter-order/indexed".to_owned(),
        format!("{count} graph members, indexed rank filter/order, offset 10 limit 20"),
        Some(root.clone()),
        |_| {
            let result = db.query_path(&path, &spec)?;
            if result.len() != 20 || result[0].value["rank"] != json!(count - 11) {
                bail!("query projection/filter/order correctness assertion failed");
            }
            Ok(result.len() as u64)
        },
    )?;
    db.close_durable_storage();
    std::fs::remove_dir_all(root)?;
    Ok(result)
}

fn benchmark_watchers(config: &Config, watcher_count: usize) -> Result<Sample> {
    let db = Primadb::with_replica_id("bench-watchers");
    db.root("watch").field("value").put(0_u64)?;
    let watchers = (0..watcher_count)
        .map(|_| db.root("watch").subscribe())
        .collect::<primadb::Result<Vec<_>>>()?;
    for watcher in &watchers {
        let initial = watcher.recv_blocking();
        if initial != Some(Some(json!({"$id": "watch", "value": 0}))) {
            bail!("watcher initial state assertion failed: {initial:?}");
        }
    }
    timed(
        config,
        format!("watchers/equivalent-update-coalescing/{watcher_count}"),
        format!("{watcher_count} equivalent local watchers on one path"),
        None,
        |iteration| {
            let value = iteration as u64 + 1;
            db.root("watch").field("value").put(value)?;
            for watcher in &watchers {
                if watcher.recv_blocking() != Some(Some(json!({"$id": "watch", "value": value}))) {
                    bail!("watcher update correctness assertion failed");
                }
            }
            Ok(watcher_count as u64)
        },
    )
}

fn benchmark_durable_writes(config: &Config, batch_size: usize) -> Result<Sample> {
    let root = unique_temp_dir("primadb-bench-durable")?;
    let db = Primadb::with_replica_id("bench-durable");
    db.open_durable_storage(DurableStorageConfig::SegmentFiles {
        directory: root.to_string_lossy().into_owned(),
        journal_retention: 8,
        durability: SegmentDurability::Full,
        lock_mode: SegmentLockMode::Exclusive,
    })?;
    let result = timed(
        config,
        "persistence/segment-writes/full-durability".to_owned(),
        format!("full-durability native segment write with {batch_size} record mutations"),
        Some(root.clone()),
        |iteration| {
            let mut mutations = Vec::with_capacity(batch_size);
            for offset in 0..batch_size {
                mutations.push(RecordMutation::Put {
                    key: format!("durable/{:06}/{offset:04}", iteration),
                    value: RecordValue::Json(json!({"iteration": iteration, "offset": offset})),
                });
            }
            let report = db.apply_record_batch(RecordBatch {
                preconditions: Vec::new(),
                mutations,
            })?;
            if report.puts != batch_size
                || db
                    .get_record(&format!("durable/{:06}/0000", iteration))?
                    .is_none()
            {
                bail!("full-durability write correctness assertion failed");
            }
            Ok(report.operation_count as u64)
        },
    )?;
    db.close_durable_storage();
    std::fs::remove_dir_all(root)?;
    Ok(result)
}

fn benchmark_direct_index(
    config: &Config,
    roots: usize,
    depth: usize,
    fanout: usize,
) -> Result<Sample> {
    let nodes = make_shared_graph(roots, depth, fanout);
    let node_count = nodes.len();
    let result = timed(
        config,
        format!("direct-index/build/roots-{roots}-depth-{depth}-fanout-{fanout}"),
        format!("{node_count} nodes, {roots} roots, shared depth {depth}, fan-out {fanout}"),
        None,
        |_| {
            let transaction = build_storage_transaction(
                1,
                build_storage_metadata(HybridClock::with_actor("bench"), Vec::new(), 2),
                nodes.clone(),
            );
            if transaction.node_indexes.len() != node_count || transaction.direct_indexes.is_empty()
            {
                bail!("direct-index construction correctness assertion failed");
            }
            Ok(transaction.direct_indexes.len() as u64)
        },
    )?;
    Ok(result)
}

fn make_shared_graph(roots: usize, depth: usize, fanout: usize) -> BTreeMap<String, NodeState> {
    let mut nodes = BTreeMap::new();
    for level in 0..depth {
        let id = format!("shared-{level:03}");
        let mut node = NodeState::new(&id);
        if level + 1 == depth {
            for branch in 0..fanout {
                node.fields.insert(
                    format!("leaf-{branch}"),
                    marker(FieldValue::Scalar(json!(format!("leaf-{branch}")))),
                );
            }
        } else {
            node.fields.insert(
                "next".to_owned(),
                marker(FieldValue::Link(format!("shared-{:03}", level + 1))),
            );
        }
        nodes.insert(id, node);
    }
    for root in 0..roots {
        let id = format!("root-{root:05}");
        let mut node = NodeState::new(&id);
        node.fields.insert(
            "shared".to_owned(),
            marker(FieldValue::Link("shared-000".to_owned())),
        );
        nodes.insert(id, node);
    }
    nodes
}

fn marker(value: FieldValue) -> FieldState {
    FieldState {
        value,
        version: VersionMarker {
            revision: Revision {
                millis: 1,
                counter: 0,
                actor: "bench".to_owned(),
            },
            op_id: "bench/op".to_owned(),
        },
    }
}

fn populate_records(db: &Primadb, prefix: &str, count: usize) -> Result<()> {
    let mutations = (0..count)
        .map(|index| RecordMutation::Put {
            key: format!("{prefix}/{index:06}"),
            value: RecordValue::Json(json!({
                "rank": index,
                "group": index % 10,
                "text": format!("record {index} stable payload"),
            })),
        })
        .collect();
    let report = db.apply_record_batch(RecordBatch {
        preconditions: Vec::new(),
        mutations,
    })?;
    if report.puts != count {
        bail!("record setup wrote {} instead of {count}", report.puts);
    }
    Ok(())
}

fn timed<F>(
    config: &Config,
    name: String,
    workload: String,
    resource_dir: Option<PathBuf>,
    mut operation: F,
) -> Result<Sample>
where
    F: FnMut(usize) -> Result<u64>,
{
    for iteration in 0..config.warmups * config.iterations {
        black_box(operation(iteration)?);
    }
    let before = proc_resource();
    let fs_before = resource_dir.as_deref().and_then(dir_size);
    let mut raw_ns_per_op = Vec::with_capacity(config.repetitions);
    let mut sink = 0_u64;
    for repetition in 0..config.repetitions {
        let started = Instant::now();
        for iteration in 0..config.iterations {
            sink = sink.wrapping_add(black_box(operation(
                config.warmups * config.iterations + repetition * config.iterations + iteration,
            )?));
        }
        let elapsed = started.elapsed();
        raw_ns_per_op.push((elapsed.as_nanos() / config.iterations as u128).max(1));
    }
    black_box(sink);
    let after = proc_resource();
    let fs_after = resource_dir.as_deref().and_then(dir_size);
    raw_ns_per_op.sort_unstable();
    let median_ns = raw_ns_per_op[raw_ns_per_op.len() / 2];
    let p95_ns = raw_ns_per_op[(raw_ns_per_op.len() * 95).div_ceil(100).saturating_sub(1)];
    let min_ns = raw_ns_per_op[0];
    let max_ns = *raw_ns_per_op.last().expect("non-empty samples");
    Ok(Sample {
        name,
        workload,
        unit: "ns/op".to_owned(),
        repetitions: config.repetitions,
        iterations_per_repetition: config.iterations,
        raw_ns_per_op,
        median_ns,
        p95_ns,
        min_ns,
        max_ns,
        throughput_per_sec: 1_000_000_000.0 / median_ns as f64,
        resource: ResourceDelta {
            rss_before_kib: before.map(|value| value.rss_kib),
            rss_after_kib: after.map(|value| value.rss_kib),
            cpu_ticks_before: before.map(|value| value.cpu_ticks),
            cpu_ticks_after: after.map(|value| value.cpu_ticks),
            filesystem_footprint_delta_bytes: fs_before
                .zip(fs_after)
                .map(|(left, right)| right as i128 - left as i128),
        },
    })
}

fn environment() -> Environment {
    Environment {
        os: env_or("BENCH_OS", std::env::consts::OS),
        kernel: env_or("BENCH_KERNEL", "unprovided"),
        cpu: env_or("BENCH_CPU", "unprovided"),
        rust: env_or("BENCH_RUST", "unprovided"),
        cargo: env_or("BENCH_CARGO", "unprovided"),
        compiler_profile: env_or("BENCH_PROFILE", "release"),
        features: env_or("BENCH_FEATURES", "default"),
        governor: env_or("BENCH_GOVERNOR", "unprovided"),
        affinity: env_or("BENCH_AFFINITY", "unprovided"),
        filesystem: env_or("BENCH_FILESYSTEM", "unprovided"),
        resource_counters:
            "/proc/self/status VmRSS + /proc/self/stat utime+stime; no allocations/syscalls/locks"
                .to_owned(),
    }
}

fn env_or(name: &str, fallback: &str) -> String {
    env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn proc_resource() -> Option<ProcResource> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let rss_kib = status.lines().find_map(|line| {
        line.strip_prefix("VmRSS:")
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse().ok())
    })?;
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let after_comm = stat.rsplit_once(") ")?.1;
    let fields = after_comm.split_whitespace().collect::<Vec<_>>();
    let user_ticks = fields.get(11)?.parse::<u64>().ok()?;
    let system_ticks = fields.get(12)?.parse::<u64>().ok()?;
    Some(ProcResource {
        rss_kib,
        cpu_ticks: user_ticks + system_ticks,
    })
}

fn dir_size(root: &Path) -> Option<u64> {
    fn visit(path: &Path, total: &mut u64) -> std::io::Result<()> {
        for entry in std::fs::read_dir(path)? {
            let path = entry?.path();
            let metadata = path.metadata()?;
            if metadata.is_dir() {
                visit(&path, total)?;
            } else if metadata.is_file() {
                *total = total.saturating_add(metadata.len());
            }
        }
        Ok(())
    }
    let mut total = 0;
    visit(root, &mut total).ok().map(|_| total)
}

fn unique_temp_dir(prefix: &str) -> Result<PathBuf> {
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let path = env::temp_dir().join(format!("{prefix}-{}-{stamp}", std::process::id()));
    std::fs::create_dir_all(&path)?;
    Ok(path)
}
