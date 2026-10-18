# Task breakdown: chunks 10-50

Every chunk from `docs/ROADMAP.md` is split into small tasks. Pick the next unticked task for your name. Task IDs are `chunk.number` (for example `16.3`). Chunks 1-9 are finished.

## Summary

- **217 tasks** across 41 chunks (10-50).
- Sizes: S=146, M=70, L=1.
- Tasks per person: Jaswanth 47, Kushal 48, Tanuj 34, Vaibhav 52, Yogya 36.

## How to use this file

- **Sizes:** S is about 1-2 hours, M is about half a day, L is about a day. Nothing is bigger than L.
- **Needs:** tasks or chunks that must be finished first. `-` means you can start now.
- **One task = one small pull request** (branch name `chunk-NN-task-M`). Open it as soon as the task is done, do not wait to finish the whole chunk.
- **Done when:** the check that the reviewer will make. If your task meets it and has a test, it is done.
- **Owner `Yogya (rev. Vaibhav)`** means a junior does the task and the named senior reviews it.
- **Stuck for 30 minutes?** Ask in the group chat. Juniors: ask your mentor first, then anyone.

## Chunk 10: npm tarball fetcher plus JS feature extractor

Phase: Data | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 10.1 | Fetch npm package metadata from registry.npmjs.org (host allowlist like the PyPI fetcher) | Vaibhav | S | 9 | Returns versions, `dist.tarball`, integrity hash for a real package |
| 10.2 | Download the tarball with a size cap and verify the integrity hash | Vaibhav | S | 10.1 | Wrong hash raises, test proves it |
| 10.3 | Unpack .tgz safely with the existing `unpack_archive` (strip the `package/` root) | Vaibhav | S | 10.2 | Traversal and symlink fixtures rejected; files come back as `{path: text}` |
| 10.4 | Map npm metadata to the `meta` dict (maintainer count, dependency count, repo mismatch, age) | Vaibhav | M | 10.1 | Function returns all keys the extractor expects, unit-tested with a fake registry response |
| 10.5 | JS feature: detect `preinstall`/`install`/`postinstall` scripts in package.json | Vaibhav | S | 10.3 | `has_install_hook` is 1 for a package.json with a postinstall script, 0 otherwise |
| 10.6 | JS features: count `eval`, `new Function`, `child_process`, `exec`, `https.get` in .js files | Vaibhav | M | 10.3 | Counts match hand-checked fixtures; obfuscated variant still counted |
| 10.7 | Create 5 harmless JS test packages (2 benign, 3 look-malicious but only `console.log`) | Tanuj (rev. Vaibhav) | S | - | Fixtures in `tests/fixtures/npm/`, each with a README line saying what it simulates |
| 10.8 | Live check: fetch 3 real npm packages and print their features | Vaibhav | S | 10.1-10.6 | Output pasted into the pull request; no crashes |

## Chunk 11: Metadata enrichment: email-domain change, dependency depth

Phase: Data | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 11.1 | Fetch per-release PyPI JSON (`/pypi/{name}/{version}/json`) and read the author email | Jaswanth | S | 9 | Email returned for 3 real releases |
| 11.2 | Compute `email_domain_changed` by comparing with the previous release's email domain | Jaswanth | M | 11.1 | True only when the domain differs; tested with fake release history |
| 11.3 | Dependency depth: follow `requires_dist` up to depth 3, with a visited-set to stop cycles | Jaswanth | M | 9 | Depth for `requests` printed; cyclic fixture does not loop forever |
| 11.4 | On-disk cache for registry responses (JSON files keyed by name+version) | Jaswanth | S | - | Second call makes zero network requests (tested) |
| 11.5 | Write tests for 11.1-11.3 using saved JSON fixtures | Yogya (rev. Jaswanth) | S | 11.1-11.3 | Tests run offline in CI |

## Chunk 12: Real-feature pipeline to Parquet offline store

Phase: Data | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 12.1 | Define the offline feature table schema (columns, dtypes, `package, version, ts, label, source`) | Vaibhav | S | - | Schema in code + a validation function with a test |
| 12.2 | Fix `base64_blob_ratio`: weight by file size and ignore data/test paths (numpy scored 0.98) | Vaibhav | M | - | numpy sdist ratio drops below 0.1; evil fixture stays above 0.3; tests for both |
| 12.3 | Pipeline script: read `packages.parquet`, fetch, extract, append rows; resumable (skips done rows) | Vaibhav | L | 7, 9, 12.1 | Kill and rerun: no duplicate work |
| 12.4 | Parallel fetching (8 threads) with polite rate limit and retry on network errors | Vaibhav | M | 12.3 | 100 packages fetched in parallel without errors |
| 12.5 | Write `features.parquet` partitioned by date and `failures.parquet` with the reason per failure | Vaibhav | S | 12.3 | Both files written; failure reasons readable |
| 12.6 | Run on the first 500 PyPI rows of MalwareBench; report fetch success rate (removed packages) | Vaibhav | M | 12.3-12.5 | Rate and failure reasons in the PR description |
| 12.7 | Research: where else to get malicious package source (e.g. Datadog malicious-software-packages-dataset) | Vaibhav | S | 12.6 | One-page note in `docs/` with link, licence, access method |

## Chunk 13: Dataset QA: dedup, leakage checks, temporal split

Phase: Data | Owner: **Yogya** | Mentor: **Vaibhav**

> **How to start:** Work in a new file `src/sentinalops/data/qa.py`. Each task is one small function that takes a pandas DataFrame. Write a tiny 5-row DataFrame in the test first, then make the function pass it.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 13.1 | `find_duplicates(df)`: exact duplicate feature rows, return them | Yogya | S | - | Test with a 5-row DataFrame containing 1 duplicate |
| 13.2 | `class_balance(df)`: table of malicious/benign counts per ecosystem | Yogya | S | - | Returns a DataFrame; test with 6 rows |
| 13.3 | `find_leakage(train, test)`: same package name in both splits | Yogya | M | 13.4 | Returns overlapping names; test with 1 overlap |
| 13.4 | `temporal_split(df, date_col, test_fraction)`: older rows train, newer rows test | Yogya | M | - | No test row is older than any train row (asserted) |
| 13.5 | CLI `python -m sentinalops.data.qa <parquet>` printing a one-page report | Yogya | S | 13.1-13.4 | Runs on the MalwareBench output and prints 4 sections |

## Chunk 14: Install-time behavior features

Phase: Data | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 14.1 | Decide which of the 36 QUT-DV25 features exist at inference time; write the mapping table | Vaibhav | M | 8 | Table in `docs/behavior-features.md` |
| 14.2 | Decision note: use install-time traces offline only (secondary model) vs live sandbox | Vaibhav | S | 14.1 | One-paragraph decision with reasons |
| 14.3 | Loader that joins QUT-DV25 features to package names | Vaibhav | M | 8, 14.1 | Returns one row per package with behavior columns |
| 14.4 | Static proxy features from `setup.py`: file-write calls, socket connects, `open(..., 'w')` | Vaibhav | M | - | 3 new features in the extractor with tests |
| 14.5 | Tests for 14.3 and 14.4 | Vaibhav | S | 14.3, 14.4 | CI green |

## Chunk 15: Redpanda topics and event schemas

Phase: Streaming | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 15.1 | Define topics: `raw_packages`, `features`, `predictions`, `labels`, `dlq` (partitions, retention) | Jaswanth | S | 6 | Table in `docs/streaming.md` |
| 15.2 | Idempotent `scripts/create_topics.py` | Jaswanth | S | 15.1 | Running it twice does not fail |
| 15.3 | JSON (de)serialiser helpers for each event schema | Jaswanth | M | 15.1 | Round-trip test for every schema |
| 15.4 | Producer/consumer wrappers (aiokafka) plus an in-memory fake for tests | Jaswanth | M | 15.3 | Fake used in tests, real one used in compose |
| 15.5 | Compose `topic-init` service; check with `rpk topic list` | Jaswanth | S | 15.2 | `docker compose up` creates the five topics |

## Chunk 16: PyPI live poller to raw topic

Phase: Streaming | Owner: **Yogya** | Mentor: **Jaswanth**

> **How to start:** New file `src/sentinalops/stream/pypi_poller.py`. Open https://pypi.org/rss/packages.xml in your browser first and look at the XML. Do tasks in order; each one prints something so you can see it working.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 16.1 | Fetch `https://pypi.org/rss/packages.xml` and print the list of (name, version) | Yogya | S | - | Prints 20+ items |
| 16.2 | Do the same for `https://pypi.org/rss/updates.xml` | Yogya | S | 16.1 | Prints 20+ items |
| 16.3 | Remember seen items in a JSON file so nothing repeats | Yogya | S | 16.1 | Second run prints nothing new |
| 16.4 | Turn each new item into a `PackageEvent` and print it | Yogya | S | 16.3 | Valid `PackageEvent` objects |
| 16.5 | Publish events to the `raw_packages` topic | Yogya | S | 15.4, 16.4 | Events visible with `rpk topic consume raw_packages` |
| 16.6 | Poll every 60 s in a loop; Ctrl-C exits cleanly | Yogya | S | 16.5 | Runs 5 minutes without errors |
| 16.7 | Test with a saved RSS sample file | Yogya | S | 16.1 | Test passes offline |

## Chunk 17: npm registry feed poller

Phase: Streaming | Owner: **Tanuj** | Mentor: **Jaswanth**

> **How to start:** Copy the structure of `pypi_poller.py` (Yogya's chunk 16) into `npm_poller.py`. Do 17.1 by hand with your browser or curl before writing any code.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 17.1 | By hand: fetch 20 changes from the npm changes feed, save the JSON in `tests/fixtures/npm_changes.json` | Tanuj | S | - | Fixture file committed |
| 17.2 | Parse the changes into a list of package names | Tanuj | S | 17.1 | Function returns names; test uses the fixture |
| 17.3 | For each name get the latest version from `registry.npmjs.org` | Tanuj | S | 17.2 | Returns (name, version) pairs |
| 17.4 | Remember the last sequence number in a file | Tanuj | S | 17.2 | Restart continues where it stopped |
| 17.5 | Create `PackageEvent(registry='npm')` and publish to `raw_packages` | Tanuj | S | 15.4, 17.3 | Events visible on the topic |
| 17.6 | Test with the saved sample | Tanuj | S | 17.1 | Passes offline |

## Chunk 18: GitHub webhook receiver

Phase: Streaming | Owner: **Tanuj** | Mentor: **Jaswanth**

> **How to start:** New file `src/sentinalops/stream/github_webhook.py`. Look at `src/sentinalops/serving/app.py` for how a FastAPI endpoint is written. Each task adds ONE small thing and one test.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 18.1 | FastAPI endpoint `POST /webhook/github` that returns 200 and logs the body | Tanuj | S | - | `curl` against it returns 200 |
| 18.2 | Verify the `X-Hub-Signature-256` HMAC using a secret from `.env` | Tanuj | M | 18.1 | Bad signature returns 401, good returns 200 |
| 18.3 | Handle `push` events: list the changed file paths | Tanuj | S | 18.1 | Test with a saved sample payload |
| 18.4 | Download changed files via raw.githubusercontent.com (host allowlist) into a `PackageEvent` | Tanuj | M | 18.3 | Event has file contents; foreign hosts refused |
| 18.5 | Publish to `raw_packages` | Tanuj | S | 15.4, 18.4 | Event visible on the topic |
| 18.6 | Tests: valid signature, bad signature, `ping` event | Tanuj | S | 18.2 | 3 tests pass |
| 18.7 | Write `docs/github-webhook.md`: how to add the webhook in repo Settings (plus a free tunnel option) | Tanuj | S | 18.1 | A teammate can follow it unaided |

## Chunk 19: Faust stream processor

Phase: Streaming | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 19.1 | Spike: confirm faust-streaming runs on our Python; if not, use a plain aiokafka consumer | Jaswanth | S | 15.4 | Decision recorded in `docs/streaming.md` |
| 19.2 | Consumer: read `raw_packages` and call the PyPI/npm/GitHub fetcher by `registry` | Jaswanth | M | 19.1, 9, 10 | A test event produces fetched files |
| 19.3 | Extract features and publish a `FeatureRecord` to `features` | Jaswanth | M | 19.2 | Record on topic matches `extract_package_features` |
| 19.4 | Score with the model and publish a `Prediction` to `predictions` | Jaswanth | M | 19.3 | Evil fixture produces a prediction record |
| 19.5 | Prometheus counters: processed, errors, consumer lag | Jaswanth | S | 19.2 | Visible on `/metrics` |
| 19.6 | Integration test with in-memory fake topics | Jaswanth | M | 19.4 | Runs in CI without Redpanda |

## Chunk 20: Dead-letter queue, retries, idempotency, backpressure

Phase: Streaming | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 20.1 | Classify errors: retryable (network, 5xx) vs permanent (`UnsafeArchive`, bad input) | Jaswanth | S | - | Function with unit tests |
| 20.2 | Retry helper: exponential backoff with jitter | Jaswanth | S | - | Tested with a fake clock |
| 20.3 | Send permanently failed events to `dlq` with the reason | Jaswanth | S | 20.1, 15.4 | DLQ message contains the original event + error |
| 20.4 | Idempotency: skip events already processed (key = registry+name+version, stored in Redis) | Jaswanth | M | 20.3 | Same event twice produces one prediction |
| 20.5 | Backpressure: bounded concurrency (semaphore) in the processor | Jaswanth | S | 19.2 | Never more than N fetches in flight (tested) |
| 20.6 | DLQ replay script | Jaswanth | S | 20.3 | Replays DLQ events back to `raw_packages` |
| 20.7 | Tests for 20.1-20.6 | Jaswanth | M | 20.1-20.6 | CI green |

## Chunk 21: Replay simulator with speed control

Phase: Streaming | Owner: **Tanuj** | Mentor: **Jaswanth**

> **How to start:** New file `src/sentinalops/simulator/replay.py`. Start with 21.1 in a Python shell: `pd.read_parquet(...)`, look at the columns, then build up one flag at a time.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 21.1 | Read a Parquet file and print the first 5 rows | Tanuj | S | - | Prints rows from the QUT or features file |
| 21.2 | Yield rows in timestamp order | Tanuj | S | 21.1 | Order asserted in a test |
| 21.3 | `--speed N`: sleep (gap between timestamps / N) | Tanuj | M | 21.2 | `--speed 1000` replays 1 hour of data in seconds |
| 21.4 | Publish each row to a topic as an event | Tanuj | S | 15.4, 21.2 | Rows visible on the topic |
| 21.5 | `--limit` and `--loop` flags | Tanuj | S | 21.2 | Flags documented in `--help` |
| 21.6 | Test: high speed finishes fast and preserves order | Tanuj | S | 21.3 | Test passes in under 2 s |

## Chunk 22: Feast repo: entities, feature views, offline store

Phase: Feature store | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 22.1 | Create `feature_repo/` with `feature_store.yaml` (local offline Parquet) | Jaswanth | S | 12 | `feast apply` succeeds |
| 22.2 | Entity `package` and a Parquet file source | Jaswanth | S | 22.1 | Registered in Feast |
| 22.3 | Feature view with the 13 features from `FEATURE_ORDER` | Jaswanth | M | 22.2 | `feast apply` lists the view |
| 22.4 | Run `feast apply` in CI/compose | Jaswanth | S | 22.3 | Step green in CI |
| 22.5 | Test: `get_historical_features` for 10 packages returns the right columns | Jaswanth | M | 22.3 | Test passes |
| 22.6 | Write a short 'how features flow' page in `docs/` with a Mermaid diagram | Yogya (rev. Jaswanth) | S | 22.3 | Page reviewed by Jaswanth |

## Chunk 23: Redis online materialization

Phase: Feature store | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 23.1 | Point the online store at Redis in `feature_store.yaml` | Jaswanth | S | 22.1 | `feast apply` still succeeds |
| 23.2 | Script for `feast materialize-incremental` | Jaswanth | S | 23.1 | Keys visible in Redis |
| 23.3 | Helper `get_online_features(package)` returning the feature dict | Jaswanth | M | 23.2 | Matches the offline value for 1 package |
| 23.4 | Measure p50/p99 lookup latency over 1000 calls | Jaswanth | S | 23.3 | Numbers in the PR |
| 23.5 | CI test with a Redis service container | Jaswanth | M | 23.3 | Test passes in GitHub Actions |

## Chunk 24: Point-in-time-correct training sets

Phase: Feature store | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 24.1 | Write 10 lines explaining point-in-time correctness with an example | Vaibhav | S | - | In `docs/pit.md` |
| 24.2 | `build_training_set(entity_df, as_of)` using Feast historical features | Vaibhav | M | 22.5 | Returns features as of each row's time |
| 24.3 | Leakage test: a feature created after the label time never appears | Vaibhav | M | 24.2 | Test fails if you remove the time filter |
| 24.4 | Combine with `temporal_split` from chunk 13 | Vaibhav | S | 13.4, 24.2 | One function returns train and test sets |

## Chunk 25: Online/offline consistency test

Phase: Feature store | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 25.1 | Compute features for 100 packages via the live path and the offline path | Vaibhav | M | 12, 23 | Two DataFrames |
| 25.2 | Assert equality within tolerance and print any differences | Vaibhav | S | 25.1 | Zero diffs, or each diff explained |
| 25.3 | Run it nightly in CI | Vaibhav | S | 25.2 | Workflow file added |

## Chunk 26: Real-data champion plus metrics suite

Phase: Models | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 26.1 | Load `features.parquet` and apply the temporal split | Vaibhav | S | 12, 13.4 | Train/test shapes printed |
| 26.2 | Metrics module: PR-AUC, precision, recall, recall at 1% false-positive rate | Vaibhav | M | - | Unit-tested on a toy example |
| 26.3 | Train GBM on the 'classic attacks only' subset (typosquats, simple setup.py) | Vaibhav | M | 26.1, 26.2 | PR-AUC printed |
| 26.4 | Log params, metrics and model to MLflow | Vaibhav | S | 26.3 | Run visible in the MLflow UI |
| 26.5 | Save the model as the API's champion file | Vaibhav | S | 26.3 | API loads it and `make smoke` passes |
| 26.6 | Write a one-page model card (data, metrics, known limits) | Yogya (rev. Vaibhav) | S | 26.3 | In `docs/model-card.md`, reviewed by Vaibhav |

## Chunk 27: Classic detectors: IsolationForest, PyOD

Phase: Models | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 27.1 | Define a common `Detector` interface (`fit`, `score`) | Vaibhav | S | - | Protocol + a dummy implementation |
| 27.2 | IsolationForest detector | Vaibhav | S | 27.1 | Scores in [0,1]; unit test |
| 27.3 | PyOD ECOD detector | Vaibhav | S | 27.1 | Scores in [0,1]; unit test |
| 27.4 | PyOD COPOD and LOF detectors | Vaibhav | S | 27.1 | Scores in [0,1]; unit tests |
| 27.5 | Run all on the real features and record PR-AUC | Vaibhav | M | 26.1, 27.2-27.4 | Table in the PR |

## Chunk 28: PyTorch autoencoder

Phase: Models | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 28.1 | Dataset/DataLoader for benign-only features (scaled) | Kushal | S | 26.1 | Batches have the right shape |
| 28.2 | MLP autoencoder 13 -> 8 -> 4 -> 8 -> 13 | Kushal | M | - | Forward pass test |
| 28.3 | Training loop with early stopping, CPU only | Kushal | M | 28.1, 28.2 | Loss decreases on synthetic data |
| 28.4 | Anomaly score = reconstruction error; threshold at the 99th percentile of benign | Kushal | S | 28.3 | Implements the `Detector` interface |
| 28.5 | CPU smoke test | Kushal | S | 28.4 | Runs in under 10 s in CI |

## Chunk 29: Streaming learners (River/PySAD)

Phase: Models | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 29.1 | HalfSpaceTrees streaming detector (River) | Jaswanth | M | 27.1 | Learns one event at a time; test |
| 29.2 | ADWIN drift detector on the score stream | Jaswanth | S | 29.1 | Fires on synthetic drift |
| 29.3 | Wrap both in the `Detector` interface | Jaswanth | S | 29.1 | Same interface as the others |
| 29.4 | Test on the classic-to-zero-day synthetic shift | Jaswanth | S | 29.2 | Drift detected within N events |

## Chunk 30: Comparison harness and MLflow experiments

Phase: Models | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 30.1 | Evaluation runner over all detectors | Kushal | M | 27, 28, 29 | One command prints a table |
| 30.2 | MLflow experiment naming and param logging convention | Yogya (rev. Kushal) | S | 26.4 | Documented in `docs/mlflow.md` |
| 30.3 | Markdown results-table generator | Yogya (rev. Kushal) | S | 30.1 | Writes `docs/results.md` |
| 30.4 | Detection-delay metric (events until first alert after an attack starts) | Kushal | M | 30.1 | Unit-tested on a synthetic stream |

## Chunk 31: Calibration and cost-sensitive thresholds

Phase: Models | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 31.1 | Probability calibration (isotonic or Platt) on a validation split | Kushal | S | 26.3 | Reliability numbers improve |
| 31.2 | Cost matrix in the domain config (missed malware costs far more than a false alarm) | Tanuj (rev. Kushal) | S | - | Config field validated |
| 31.3 | Pick the threshold that minimises expected cost | Kushal | M | 31.1, 31.2 | Chosen threshold printed with its cost |
| 31.4 | Write the threshold where the API reads it (`block_threshold`) | Kushal | S | 31.3 | API uses it after restart |

## Chunk 32: MLflow registry: champion/challenger aliases

Phase: Models | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 32.1 | Register the model in the MLflow registry | Jaswanth | S | 26.4 | Version 1 exists |
| 32.2 | Set `champion` and `challenger` aliases | Jaswanth | S | 32.1 | Aliases visible in the UI |
| 32.3 | `promote()` and `rollback()` helpers | Jaswanth | M | 32.2 | Tested against a local SQLite MLflow |
| 32.4 | API loads `models:/<name>@champion` and reloads when the alias moves | Jaswanth | M | 32.2 | Promoting changes served `model_version` without restart |
| 32.5 | Tests for 32.1-32.4 | Jaswanth | M | 32.1-32.4 | CI green |

## Chunk 33: /score with feature-store lookup and alias hot-reload

Phase: Serving | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 33.1 | Load the model by MLflow alias in the API (uses 32.4) | Kushal | M | 32.4 | `/health` shows the alias version |
| 33.2 | Online feature lookup for metadata features when the package is already known | Kushal | M | 23.3 | Uses cached features, skips refetching |
| 33.3 | Timeout and fallback: if the store is slow, extract on the fly | Kushal | S | 33.2 | Test with a slow fake store |
| 33.4 | Tests for 33.1-33.3 | Yogya (rev. Kushal) | S | 33.1-33.3 | CI green |

## Chunk 34: /labels endpoint, label store, prediction-label join

Phase: Serving | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 34.1 | Append-only label store (SQLite or Parquet) | Kushal | S | - | Labels survive restarts |
| 34.2 | Prediction log store with timestamps and model version | Kushal | S | - | Every `/score` call logged |
| 34.3 | Join predictions with labels by (package, version) | Kushal | M | 34.1, 34.2 | Returns the confusion table for a window |
| 34.4 | Batch `/labels` endpoint | Kushal | S | 34.1 | Accepts a list; validated |
| 34.5 | Delayed-label simulator script (writes a label file N minutes after publish) | Tanuj (rev. Kushal) | S | 34.1 | Used by the demo |

## Chunk 35: Block policy, allowlist, SHAP explanations

Phase: Serving | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 35.1 | Allowlist of popular packages (config file) | Vaibhav | S | - | Loaded from config; tested |
| 35.2 | Three-tier decision: allow / warn / block, with thresholds | Vaibhav | S | 31.4 | Pure function with tests |
| 35.3 | SHAP top-3 reasons for the tree model | Vaibhav | M | 26.3 | Reasons for the evil fixture mention entropy or exec |
| 35.4 | Return `reasons` in the `/score` response | Vaibhav | S | 35.2, 35.3 | Schema updated; API test |
| 35.5 | Tests for 35.1-35.4 | Vaibhav | S | 35.1-35.4 | CI green |

## Chunk 36: Load testing and p99 latency targets

Phase: Serving | Owner: **Yogya** | Mentor: **Kushal**

> **How to start:** Install locust (`pip install locust`) and follow its 'Quick start' page. Our API is already running with `make api`; the request body is in `scripts/smoke.py`.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 36.1 | Install locust and write `locustfile.py` that posts the benign fixture to `/score` | Yogya | S | - | Locust UI shows requests succeeding |
| 36.2 | Run 1 minute with 20 users and save the results | Yogya | S | 36.1 | Screenshot or CSV in `docs/perf/` |
| 36.3 | Mix in 10% evil-fixture requests | Yogya | S | 36.1 | Both fixtures appear in the stats |
| 36.4 | Write the p50/p95/p99 table | Yogya | S | 36.2 | In `docs/perf/README.md` |
| 36.5 | Open a GitHub issue for anything slower than 100 ms at p99 | Yogya | S | 36.4 | Issue linked in the PR |

## Chunk 37: Prometheus metrics: scores, latency, throughput

Phase: Monitoring | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 37.1 | Request latency histogram and request counter | Jaswanth | S | - | Visible on `/metrics` |
| 37.2 | Histograms for watched feature values (base64_entropy, maintainer_age_days) | Jaswanth | M | 37.1 | Drift jobs can read them |
| 37.3 | Verify Prometheus targets are UP | Jaswanth | S | 37.1 | Screenshot in the PR |
| 37.4 | Metric naming guide | Jaswanth | S | 37.1-37.2 | Short page in `docs/` |

## Chunk 38: Evidently drift jobs

Phase: Monitoring | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 38.1 | Build reference and current datasets from the features log | Kushal | M | 34.2 | Two DataFrames with the same columns |
| 38.2 | Evidently data-drift report on the watched features | Kushal | M | 38.1 | HTML report generated |
| 38.3 | Export the drift score as a Prometheus gauge | Kushal | S | 38.2 | Gauge visible in Prometheus |
| 38.4 | Run the job on a schedule (every N minutes) | Jaswanth (rev. Kushal) | S | 38.3 | Gauge updates by itself |
| 38.5 | Test: classic -> zero-day data triggers high drift | Kushal | M | 38.2 | Test passes |

## Chunk 39: Delayed-label rolling recall/precision

Phase: Monitoring | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 39.1 | Rolling-window precision/recall from joined predictions and labels | Kushal | M | 34.3 | Unit-tested |
| 39.2 | Expose them as Prometheus gauges | Kushal | S | 39.1 | Visible in Prometheus |
| 39.3 | Handle 'no labels yet' without errors | Kushal | S | 39.1 | Gauges report NaN or 0 plus a flag |
| 39.4 | Tests for 39.1-39.3 | Yogya (rev. Kushal) | S | 39.1-39.3 | CI green |

## Chunk 40: Alert rules (model_drift_high)

Phase: Monitoring | Owner: **Tanuj** | Mentor: **Kushal**

> **How to start:** Alert rules are one small YAML file. Read the Prometheus 'Alerting rules' docs page (10 min), then do 40.1. Files go in `infra/`; Prometheus UI is on :9090 after `docker compose up`.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 40.1 | Write rule `ModelDriftHigh`: drift gauge above 0.3 for 1 minute | Tanuj | S | 38.3 | File in `infra/` |
| 40.2 | Mount the rules file in compose and load it in Prometheus | Tanuj | S | 40.1 | Rule listed under Alerts in the UI |
| 40.3 | Write rule `RecallDropped` for the recall gauge | Tanuj | S | 39.2 | Rule listed in the UI |
| 40.4 | Add `promtool check rules` to CI | Tanuj | S | 40.1 | CI step green |
| 40.5 | Trigger the alert on purpose and take a screenshot | Tanuj | S | 40.2, 38.5 | Screenshot in the PR |

## Chunk 41: Grafana dashboards and 'Model Healed' annotations

Phase: Monitoring | Owner: **Yogya** | Mentor: **Kushal**

> **How to start:** Open Grafana on :3000 (admin/admin) after `docker compose up`. Build each panel with the UI first, then export its JSON. One panel per task.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 41.1 | Panel: score histogram | Yogya | S | 37.1 | Shows live data |
| 41.2 | Panel: blocked packages over time | Yogya | S | 37.1 | Shows live data |
| 41.3 | Panel: drift gauge with a threshold line at 0.3 | Yogya | S | 38.3 | Line visible |
| 41.4 | Panel: rolling precision and recall | Yogya | S | 39.2 | Shows live data |
| 41.5 | 'Model Healed' annotation driven by a `model_promotions_total` metric | Yogya | M | 45.4 | Marker appears after a promotion |
| 41.6 | Export JSON into `infra/grafana/dashboards/` and provision it | Yogya | S | 41.1-41.4 | Dashboard appears after a fresh `docker compose up` |

## Chunk 42: Prefect flow with drift plus label trigger

Phase: Self-healing | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 42.1 | Prefect flow skeleton `heal_flow` | Kushal | S | - | Runs locally and logs a line |
| 42.2 | Task: read the latest drift score | Kushal | S | 38.3 | Returns a float; mocked in tests |
| 42.3 | Task: count new labels since the last retrain | Kushal | S | 34.1 | Returns an int; mocked in tests |
| 42.4 | Trigger rule: drift above threshold AND labels at least N | Kushal | S | 42.2, 42.3 | Truth-table test |
| 42.5 | Prefect server and schedule in compose | Jaswanth (rev. Kushal) | M | 42.1 | Flow visible in the Prefect UI |
| 42.6 | Tests for the flow with mocks | Kushal | S | 42.4 | CI green |

## Chunk 43: Retrain DAG: 48h window plus replay buffer

Phase: Self-healing | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 43.1 | Assemble the last 48 h of features plus labels | Kushal | M | 34.3 | Returns a labelled DataFrame |
| 43.2 | Replay buffer: sample older data so the model does not forget old attacks | Kushal | M | 43.1 | Mix ratio configurable; tested |
| 43.3 | Train the challenger | Kushal | S | 26.3 | Model object returned |
| 43.4 | Log the challenger to MLflow with tags (window, data counts) | Kushal | S | 43.3 | Run visible |
| 43.5 | Glue + idempotency: never retrain twice for the same drift event | Kushal | S | 43.1-43.4 | Second trigger is skipped (tested) |
| 43.6 | End-to-end test on synthetic classic -> zero-day data | Kushal | M | 43.5 | Challenger catches the zero-day, passes in CI |

## Chunk 44: Challenger vs champion gates

Phase: Self-healing | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 44.1 | Evaluate champion and challenger on the same holdout | Kushal | S | 43.3 | Metrics for both returned |
| 44.2 | Gate: challenger improves recall on the new-attack slice by at least X | Kushal | S | 44.1 | Configurable X; tested |
| 44.3 | Gate: no regression on the old-attack slice | Kushal | S | 44.1 | Configurable tolerance; tested |
| 44.4 | Gate report (JSON + Markdown) saved with the run | Kushal | S | 44.2, 44.3 | Report attached to the MLflow run |
| 44.5 | Tests for 44.1-44.4 | Yogya (rev. Kushal) | S | 44.1-44.4 | CI green |

## Chunk 45: Auto-promotion and rollback safety

Phase: Self-healing | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 45.1 | Promote the challenger to champion through the registry | Kushal | S | 32.3, 44.4 | Alias moves only if the gates pass |
| 45.2 | Post-promotion shadow check: score a sample with both models and compare | Kushal | M | 45.1 | Large disagreement flagged |
| 45.3 | Automatic rollback when the shadow check fails | Kushal | M | 45.2 | Old champion restored (tested) |
| 45.4 | Emit `model_promotions_total` and a promotion log line | Kushal | S | 45.1 | Counter visible |
| 45.5 | Tests for 45.1-45.4 | Yogya (rev. Kushal) | S | 45.1-45.4 | CI green |

## Chunk 46: Harmless adversarial variant library

Phase: Self-healing | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 46.1 | Attack taxonomy doc: typosquat, dependency confusion, compromised maintainer, staged payload, obfuscation levels | Vaibhav | S | - | `docs/attacks.md` |
| 46.2 | Generator: base64/hex obfuscation variants (print-only payload) | Vaibhav | S | 46.1 | 5 variants generated |
| 46.3 | Generator: staged payload (downloader stub that only prints) | Vaibhav | M | 46.1 | Variant parses and has install hook |
| 46.4 | Generator: dependency-confusion metadata profile | Vaibhav | S | 46.1 | Metadata matches the profile |
| 46.5 | Generator: string-built call evasions like `getattr(__builtins__, 'ex'+'ec')` | Vaibhav | M | 46.1 | Documents what the extractor misses (feeds new features) |
| 46.6 | Safety checklist: every variant only prints or logs, never touches network/files | Vaibhav | S | 46.2-46.5 | Checklist signed off by Kushal |
| 46.7 | Tests: each variant parses and yields the expected features | Vaibhav | S | 46.2-46.5 | CI green |

## Chunk 47: End-to-end 5-step zero-day demo

Phase: Demo and polish | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 47.1 | Create the GitHub test repo and webhook (uses chunk 18) | Kushal | M | 18 | A push produces a `PackageEvent` |
| 47.2 | Step 1 script: push the benign package, show score ~0.01 | Tanuj (rev. Kushal) | S | 47.1 | Script prints the score |
| 47.3 | Step 2 script: push the attack, show the champion misses it | Tanuj (rev. Kushal) | S | 47.1 | Score below threshold |
| 47.4 | Step 3 script: drift alert fires, then delayed label arrives | Kushal | S | 40, 34.5 | Alert visible in Grafana |
| 47.5 | Step 4: watch the heal flow retrain and promote | Kushal | S | 45 | Promotion visible in MLflow and Grafana |
| 47.6 | Step 5 script: push the same attack again, show it is blocked | Kushal | S | 47.5 | Score above threshold, blocked |
| 47.7 | `make demo` runs all steps; three full dry runs | Kushal | M | 47.2-47.6 | Three clean runs logged |

## Chunk 48: Docs: architecture diagram, runbook, ADRs

Phase: Demo and polish | Owner: **Yogya** | Mentor: **Kushal**

> **How to start:** Write in plain Markdown in `docs/`. For the diagram use Mermaid (GitHub renders it) so nobody needs a drawing tool. Ask the owner of each component to review your paragraph about it.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 48.1 | Architecture diagram in Mermaid (stream, store, model, serving, monitoring, healing) | Yogya | M | - | Renders on GitHub in `docs/architecture.md` |
| 48.2 | Update the README 'How to run' and fix broken `docs/` links | Yogya | S | - | Every link in the README opens a real page |
| 48.3 | Runbook: what to do when the API, stream, or heal flow fails | Yogya | M | - | `docs/runbook.md` reviewed by each component owner |
| 48.4 | Five short ADRs (why Redpanda, Feast, MLflow, Prefect, Evidently) | Yogya | M | - | 5 files in `docs/adr/`, half a page each |

## Chunk 49: Benchmarks and results writeup

Phase: Demo and polish | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 49.1 | Before/after table: champion vs healed model on the attack slice | Vaibhav | M | 47 | Numbers in `docs/results.md` |
| 49.2 | Measure heal time: drift alert to promotion | Vaibhav | M | 47 | Median of 3 runs |
| 49.3 | Add latency numbers from chunk 36 | Vaibhav | S | 36.4 | One table |
| 49.4 | Two-page results writeup | Vaibhav | M | 49.1-49.3 | Reviewed by Kushal |

## Chunk 50: Demo video, resume bullets, v1.0 release

Phase: Demo and polish | Owner: **Tanuj** | Mentor: **Kushal**

> **How to start:** Do 50.1 (the script) first and get Kushal's OK before recording anything.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 50.1 | Video script: 5 scenes matching the demo steps | Tanuj | S | 47 | Approved by Kushal |
| 50.2 | Record the screen and voiceover | Tanuj | M | 50.1 | Raw recording saved |
| 50.3 | Edit and export (under 5 minutes) | Tanuj | M | 50.2 | Video file ready |
| 50.4 | Draft resume bullets for each team member (3 versions each) | Tanuj | S | 49 | In `docs/resume-bullets.md` |
| 50.5 | Tag v1.0 and write the release notes | Tanuj | S | all | GitHub release created (Kushal presses the button) |

