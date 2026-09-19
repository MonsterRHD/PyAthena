# Cursor benchmarks

This project measures result retrieval, memory use, and concurrent query behavior for PyAthena and AWS SDK for pandas (AWS Wrangler).
It includes a disposable CloudFormation environment and does not run benchmarks in CI.
Measurements and cursor recommendations for this harness are pending.

## Input data

The supplied input is `pyathena_benchmark.pypi_file_downloads`, with the Hive partition `download_date='2026-09-17'` (UTC).
The source contains 4,773,620,317 rows, approximately 30.4 GiB of Parquet compressed with Snappy.
The default region and existing Athena workgroup are `us-west-2` and `pyathena`.

`prepare` creates one fixed Parquet/Snappy snapshot per selected scale in the stack's scratch database and bucket.
Defaults are 10,000, 100,000, 1,000,000, and 10,000,000 rows.
Every adapter reads the same snapshot without another `LIMIT`.
Preparation verifies the actual row count and records SQL, schema, table names, query IDs, and locations in a manifest.
Selection from the source uses `LIMIT` without ordering: separate preparations can select different rows.
Preserve a snapshot for comparisons across revisions, and use a separate output directory for each run.
The Athena timings describe queries against these smaller snapshots, not a scan of the complete source partition.

The flat workload projects scalar fields from `file`, `details`, and `http` alongside the top-level scalar columns.
The nested workload preserves those structures.
Neither workload converts every library's output into a common Python object representation.

## Dependencies and local checks

The benchmark is an independent, non-packaged uv project for Python 3.12.
Its lockfile and virtual environment are separate from the parent project; PyAthena is an editable path dependency on the parent checkout.
The root `uv build -v` still builds only PyAthena, including a wheel built from its sdist.
Benchmark dependencies do not become PyAthena runtime dependencies.

From the repository root:

```bash
uv sync --project benchmarks --locked
just benchmark lint
just benchmark test
```

The tests use local data, fake AWS clients, and child processes.
They do not execute Athena queries or create AWS resources.
`just benchmark format` formats the benchmark separately from the parent project.

Commands below run from `benchmarks/`.
`plan` and `report` do not contact AWS.
For local AWS access, insert `--profile pyathena` before the subcommand, or set `AWS_PROFILE=pyathena`.
On EC2, omit the profile and use the instance role.

```bash
cd benchmarks
uv run --locked python -m pyathena_bench plan --suite single --scale small
```

Review the case count before running a suite.
`run` requires explicit suite and scale selections and never overwrites an existing output directory.
Use `--family`, `--api`, `--transport`, and `--output-kind` to narrow both `plan` and `run`.
Copy `config.toml` to an ignored location to customize scales, repetitions, timeouts, memory limits, or executor sizes, and pass it with `--config` before the subcommand.
Keep the same configuration for preparation, measurement, and cleanup.

## Disposable EC2 environment

Deploy `cloudformation/benchmark.yaml` in `us-west-2` with the stack tag `Purpose=pyathena-benchmark`.
The template creates its own VPC, public subnet, route, Internet Gateway, EC2 instance, EBS volume, IAM role, scratch Glue database, and scratch S3 bucket.
There is no inbound security-group rule or SSH key; use Session Manager.
Internet access permits package installation and calls to AWS APIs without a NAT gateway.

The defaults are Amazon Linux 2023 x86_64, `r7i.2xlarge` (8 vCPUs, 64 GiB), and an encrypted 100 GiB gp3 root volume.
Instance size and disk size are parameters, so a later run can deliberately test another memory budget.
The AMI parameter resolves the current AL2023 image at deployment; record the resulting AMI when comparing environments.
Source bucket access is read-only and restricted to the configured prefix.
The instance can write only to its scratch bucket and database, and can query the specified existing workgroup.
The supplied template assumes ordinary IAM access and SSE-S3 source objects; Lake Formation restrictions or a customer-managed KMS key require corresponding grants before execution.

The deployment identity needs permission to create these resources and pass the instance role.
Use an immutable, remotely accessible commit that contains this directory and its lockfile.
Bootstrap checks out that commit, installs pinned uv and Python versions, and runs `uv sync --locked --no-dev`.
It signals setup completion to CloudFormation but does not prepare data or start measurements.

From the local checkout, replace the commit below with the implementation commit:

```bash
export AWS_PROFILE=pyathena
export AWS_DEFAULT_REGION=us-west-2
BENCHMARK_STACK=pyathena-benchmark-run
BENCHMARK_COMMIT=<full-40-character-commit-sha>
aws cloudformation deploy \
  --stack-name "$BENCHMARK_STACK" \
  --template-file cloudformation/benchmark.yaml \
  --capabilities CAPABILITY_IAM \
  --tags Purpose=pyathena-benchmark \
  --parameter-overrides GitCommit="$BENCHMARK_COMMIT"
aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK"
BENCHMARK_INSTANCE=$(aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK" \
  --query 'Stacks[0].Outputs[?OutputKey==`InstanceId`].OutputValue | [0]' --output text)
aws ssm start-session --target "$BENCHMARK_INSTANCE"
```

Session Manager requires the AWS CLI Session Manager plugin on the local machine.
Inside the session, switch to `ec2-user` and enter the benchmark directory:

```bash
sudo -iu ec2-user
cd /opt/pyathena/benchmarks
export AWS_DEFAULT_REGION=us-west-2
BENCHMARK_STACK_ID=$(cat stack-id.txt)
uv run --locked --no-sync python -m pyathena_bench preflight --stack "$BENCHMARK_STACK_ID"
uv run --locked --no-sync python -m pyathena_bench prepare \
  --stack "$BENCHMARK_STACK_ID" --manifest results/input.json --scale small
```

`preflight` only reads metadata.
It checks the live stack identity, source location and partition, and whether the workgroup permits a dedicated S3 output location.
`prepare` submits CTAS and count queries and incurs Athena and S3 usage.
Select additional scales in the initial preparation command when needed.
Each preparation needs a new manifest filename; failed preparation leaves a manifest for cleanup.

## Running the suites

Start with a small, selected single-query comparison:

```bash
uv run --locked --no-sync python -m pyathena_bench run \
  --manifest results/input.json --out results/single-small \
  --suite single --scale small --family cursor --transport csv
```

Remove the family filter to include all families, or compare native DataFrame output:

```bash
uv run --locked --no-sync python -m pyathena_bench run \
  --manifest results/input.json --out results/native-small \
  --suite single --scale small --family pandas arrow polars wrangler --output-kind native
uv run --locked --no-sync python -m pyathena_bench run \
  --manifest results/input.json --out results/nested-small \
  --suite single --scale small --shape nested --output-kind native
```

The concurrency suite compares ThreadPool and native asyncio APIs at 1, 10, 50, and 100 simultaneous queries by default.
It uses a bounded matrix without the chunk-size sweep.
Use the small snapshot first; requested concurrency does not override the account's Athena quotas.
Throttling, queuing, and failed queries remain visible in the output.
Both asynchronous integrations offload blocking operations when required: ThreadPool `execute()` and result consumption use an asyncio executor, and native DataFrame accessors on Aio cursors also need offloading.
The configured asyncio executor has 32 workers by default; the shared AsyncCursor owns a separate pool of the same size.
Native libraries and S3 readers can create additional threads.

```bash
uv run --locked --no-sync python -m pyathena_bench run \
  --manifest results/input.json --out results/concurrent-small \
  --suite concurrent --scale small
uv run --locked --no-sync python -m pyathena_bench run \
  --manifest results/input.json --out results/init-small \
  --suite init --scale small
```

The initialization suite prepares completed CSV and UNLOAD query results once, outside the timed trials.
It compares direct synchronous ResultSet construction with `asyncio.to_thread()` construction using the same stored output.
Construction can include S3 I/O and conversion; the difference is not a measurement of pure scheduler overhead.
It validates the full row count after construction, outside `init_seconds`.

## Measurement semantics

Each warmup and measured trial runs in a fresh child process, with imports outside the measurement baseline.
All DataFrame backends are imported before the baseline, including for row cursors.
Absolute RSS therefore includes the harness's common imports; inspect the baseline and increase when interpreting memory overhead.
A concurrent batch shares one child process, so its RSS belongs to the batch rather than an individual query.
Defaults are one discarded warmup and five measured trials per case, executed serially by the parent.
Warmups can affect service-side caches, but do not warm the next child's in-process caches.
No attempt is made to flush operating-system or Athena caches.
Athena result reuse and library query caches are disabled.

| Measurement | Meaning |
| --- | --- |
| `setup_seconds` | Connection and cursor preparation |
| `execute_seconds` | Public execute call through completion, including waiting for a Future or coroutine |
| `consume_seconds` | Consumption after execute completes |
| `total_seconds` | Execute plus full result consumption; excludes connection preparation and cleanup |
| `post_completion_setup_seconds` | First observed successful Athena status through result readiness, when available |
| `init_seconds` | ResultSet construction in the initialization suite |
| Athena statistics | Server execution, queue time, scanned bytes, and engine version from SDK responses |
| RSS samples | Externally sampled resident memory, including setup and validation; 50 ms by default |
| Process high-water RSS | OS high-water value, including imports and process startup |
| Event-loop lag | Delay beyond the heartbeat interval; 10 ms interval by default |
| Thread metrics | Observed process threads and CPU time since the baseline, including native library threads |

Some cursors eagerly read data during `execute()`, while others fetch lazily.
Consequently, `consume_seconds` alone is not a fair retrieval comparison.
Compare end-to-end times for the same output representation, then inspect server-side statistics separately.
API row conversion and native DataFrame/Table access appear as separate cases.
Sampled RSS can miss short peaks, and the constructor suite's RSS includes its subsequent validation read.
If the operating system denies access to per-thread CPU times, `thread_cpu_available` is false; thread counts and RSS are still recorded.

| Capability | Treatment |
| --- | --- |
| Cursor, DictCursor, S3FS | Row batches with arraysize 100 and 1000 |
| Pandas CSV | Full DataFrame or streamed chunks |
| Pandas UNLOAD | Full Parquet read; requested chunked cases are marked unsupported as bounded-memory measurements |
| Arrow CSV and UNLOAD | Eager table loading; arraysize changes row extraction, not S3 streaming |
| Polars CSV and UNLOAD | Full result or chunk iterator |
| Wrangler CSV | Scalar projections; nested cases are marked unsupported |
| Wrangler CTAS and UNLOAD | Full DataFrame or chunks, including nested workloads |

The [Wrangler API documentation](https://aws-sdk-pandas.readthedocs.io/en/stable/stubs/awswrangler.athena.read_sql_query.html) describes its CSV nested-type restriction.
CTAS and UNLOAD have separate limitations, including timestamp-with-time-zone columns; the supplied timestamp is a bigint.
Unsupported combinations are included in the plan and report with reasons.
An attempted operation that fails is recorded as a failure, not silently reclassified as unsupported.
Row-count validation detects incomplete consumption; it is not a proof that different libraries produce identical dtypes or nested Python objects.

The parent stops a trial at the configured timeout or RSS fraction of physical RAM and attempts to cancel observed active queries.
The suite stops at the first failed trial, including a failed warmup, so outstanding queries cannot affect later measurements.
Inspect the recorded failure, run cleanup, and prepare a new snapshot before retrying in a new output directory.
An external kill is reported as a worker exit, not automatically as an out-of-memory error.
Use `cleanup` after interrupted runs to discover outstanding queries whose IDs were not returned before a worker died.
Do not run two orchestrators concurrently on the same dedicated host.

## Reports and recovery

Every output directory contains `environment.json`, a copy of the input manifest, `events.jsonl`, and `trials.jsonl`.
Environment metadata includes Git SHA, dirty state, Python and dependency versions, lockfile hash, CPU count, RAM, and settings.
SDK events preserve query IDs, SQL, output locations, Athena statistics, and failed operations without storing result rows.
Successful measurements generate `summary.csv` and `summary.md`; failed trials and unsupported cases remain visible.
Regenerate reports after interruption with:

```bash
uv run --locked --no-sync python -m pyathena_bench report results/single-small
```

Report recovery ignores an incomplete final JSONL record with a warning and marks started trials without a final record as incomplete.
Malformed records elsewhere remain errors.

Small sample sizes make tail percentiles descriptive rather than reliable population estimates.
Retain the raw files alongside the summary.
Results are gitignored and are not uploaded by CI.

## Recovering reports and deleting the environment

Stop the benchmark process before cleanup or stack deletion.
Recover all local manifests and reports first, including failed-run evidence.
Use the stack's generated bucket as a temporary transfer location; it is separate from `pyathena-benchmark`, which holds the input data.

On EC2:

```bash
BENCHMARK_BUCKET=$(aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK_ID" \
  --query 'Stacks[0].Outputs[?OutputKey==`Bucket`].OutputValue | [0]' --output text)
aws s3 sync results/ "s3://$BENCHMARK_BUCKET/reports/"
```

On the local machine:

```bash
BENCHMARK_BUCKET=$(aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK" \
  --query 'Stacks[0].Outputs[?OutputKey==`Bucket`].OutputValue | [0]' --output text)
mkdir -p results/recovered
aws s3 sync "s3://$BENCHMARK_BUCKET/reports/" results/recovered/
aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK" > results/recovered/stack.json
aws ec2 describe-instances --instance-ids "$BENCHMARK_INSTANCE" > results/recovered/instance.json
```

Verify the downloaded files before proceeding.
For every preparation manifest, preview and then clean that run using its original configuration:

```bash
uv run --locked python -m pyathena_bench --profile pyathena cleanup \
  --manifest results/recovered/input.json
uv run --locked python -m pyathena_bench --profile pyathena cleanup \
  --manifest results/recovered/input.json --execute
```

Cleanup checks the live stack identity, cancels and waits for matching active queries, removes run-prefixed scratch tables, aborts matching multipart uploads, and removes the run's S3 prefix.
The transfer copy under `reports/` remains until the final stack teardown.
For multiple manifests, repeat cleanup for each; never clean while another process is still submitting queries.

Confirm the scratch database is empty and the only remaining S3 objects are the recovered reports.
An unexpected table or object is a reason to inspect the corresponding run before deleting anything further.

```bash
BENCHMARK_DATABASE=$(aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK" \
  --query 'Stacks[0].Outputs[?OutputKey==`ScratchDatabase`].OutputValue | [0]' --output text)
aws glue get-tables --database-name "$BENCHMARK_DATABASE" --query 'TableList[].Name'
aws s3 ls "s3://$BENCHMARK_BUCKET/" --recursive
aws s3 rm "s3://$BENCHMARK_BUCKET/reports/" --recursive
aws s3api list-multipart-uploads --bucket "$BENCHMARK_BUCKET"
aws s3 ls "s3://$BENCHMARK_BUCKET/" --recursive
aws cloudformation delete-stack --stack-name "$BENCHMARK_STACK"
aws cloudformation wait stack-delete-complete --stack-name "$BENCHMARK_STACK"
```

CloudFormation can delete an S3 bucket only when it is empty.
The template does not retain EBS, S3, or other benchmark resources, and it does not include an automatic bucket-emptying Lambda.
The source table, source data, and existing workgroup are not owned by this stack and remain intact.
The local recovered reports are the retained benchmark evidence.
If bootstrap fails, inspect the stack events and `/var/log/cloud-init-output.log`; no benchmark data is created during bootstrap.
