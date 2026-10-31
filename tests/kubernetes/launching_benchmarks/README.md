# Kubernetes launch latency A/B benchmark

`bench_launch_latency.py` measures whether a change makes `sky launch` on
Kubernetes faster. It launches the same task many times from two SkyPilot
builds, a baseline and a candidate, and compares timings parsed from each
launch's `provision.log`.

It was written to validate the `ej-prj-1` change (commit `532cced15`, "Wait
for Ray on the head in one call and start skylet meanwhile"). It works for any
pair of git refs.

## What it does

1. **Isolates each build.** Each ref gets:
   - its own `git worktree`;
   - its own virtualenv (`uv pip install -e <worktree>[kubernetes,server]`);
   - its own local API server, with its own port, `SKY_RUNTIME_DIR`, `HOME`
     and config.

   The config allows only Kubernetes and only the chosen context. Your
   working tree, `~/.sky` and main API server are not touched. Commands run
   with the build's runtime directory as cwd, so `python -m` can't pick up
   the `sky` package from the repo you started in.
2. **Checks before launching.**
   - Each build must import `sky` from its own worktree.
   - The context must have no SkyPilot pods (`--force` skips this).
3. **Interleaves the launches.** It runs `--warmup` untimed launches per build
   first (image pulls), then `--runs` timed launches. The order alternates
   A B B A … so any cluster drift (image cache, node load) hits both builds
   equally. Each launch is `sky launch` then `sky logs --provision` then
   `sky down`.
4. **Parses `provision.log`.** The metrics are the times between these
   markers:

   | Metric | From → to | Role |
   |---|---|---|
   | `post_runtime_s` | `End: setup_runtime_on_cluster` → `Cluster launched` | **Primary.** The segment `ej-prj-1` changes: Ray wait and skylet start |
   | `provision_total_s` | first `provisioner.py` line → `Cluster launched` | Whole provision |
   | `pods_ready_s` | first `provisioner.py` line → `all pods are scheduled and running` | Control. Should not move |
   | `runtime_setup_s` | `all pods ... running` → `End: setup_runtime_on_cluster` | Control. Should not move |

   The parser also records:
   - the commit the server ran;
   - whether Ray was restarted by the fallback;
   - how many rounds of the old polling loop ran;
   - how many times the new blocking wait was retried.
5. **Gives a verdict.** VALID only if all of these hold:
   - each build ran its own, distinct commit;
   - every launch succeeded and ran on every rank;
   - no candidate launch fell back to restarting Ray;
   - the candidate never ran the old polling loop;
   - the candidate's primary metric is lower, with a one-sided permutation
     test p < `--alpha`.

   The permutation test is pure Python, so no scipy is needed.
6. **Cleans up.** Clusters are taken down, both servers stopped and the
   worktrees removed, also on Ctrl-C or SIGTERM. Pass `--keep-worktrees` to
   keep the worktrees.

## Prerequisites

- An idle Kubernetes context. A local kind cluster from `sky local up` works
  (context `kind-skypilot`, the default).
- `git`, `uv` and `kubectl` on `PATH`. The repo needs an `origin/master` ref
  so the default baseline can be computed.
- A task YAML. A multi-node task shows the Ray wait best. This is the task
  used for the results below:

  ```yaml
  name: multinode-bench
  resources:
    infra: k8s/kind-skypilot
    cpus: 1+
    memory: 1+
  num_nodes: 4
  run: |
    echo "[rank ${SKYPILOT_NODE_RANK}] started at $(date +%s.%N) on $(hostname)"
  ```

## How it was tested

1. **Unit tests for the change itself:**

   ```bash
   pytest tests/unit_tests/test_sky/provision/test_provisioner_ray_wait.py
   ```

2. **Kubernetes smoke tests that touch the changed path**, run on
   `kind-skypilot`. All passed:
   - `test_node_names_multi_node`
   - `test_provision_logs_streaming`
   - `test_worker_provision_logs_streaming`
   - `test_kubernetes_non_debian_image` (2 cases)
   - `test_status_refresh_keeps_interrupted_launch_init`
   - `test_refresh_during_launch`
   - `test_kubernetes_host_network_relaunch_after_head_ray_stopped`
   - `test_kubernetes_host_network_multi_node_same_node`

3. **The A/B benchmark.** Run it from the repository root, with the task
   above saved as `job.yaml`:

   ```bash
   source .venv/bin/activate
   # Make sure nothing else is running on the context.
   kubectl --context kind-skypilot get pods -A -l parent=skypilot

   python tests/kubernetes/launching_benchmarks/bench_launch_latency.py run \
       --candidate-ref ej-prj-1 \
       --context kind-skypilot \
       --task job.yaml \
       --runs 5 --warmup 1
   ```

   The baseline defaults to the merge-base of the candidate and
   `origin/master`, here `1f9136510`. One launch pair takes about 1 minute on
   kind. Results go to `./bench-<timestamp>/`:
   - `summary.md`: the table, checks and verdict;
   - `results.csv`: one row per launch;
   - `meta.json`: the refs, context, task and run counts;
   - `logs/`: each launch's CLI output and `provision.log`.

4. **Re-analyzing saved logs.** The `analyze` subcommand runs the same parser
   and checks on `provision.log` files you already have. The table below was
   regenerated this way and matched the original run exactly:

   ```bash
   python tests/kubernetes/launching_benchmarks/bench_launch_latency.py analyze \
       --baseline bench-<ts>/logs/bench-base-[1-5].provision.log \
       --candidate bench-<ts>/logs/bench-cand-[1-5].provision.log
   ```

   Run 0 is the warm-up, so it is excluded.

5. **Confirming cleanup** after the run:

   ```bash
   kubectl --context kind-skypilot get pods -A -l parent=skypilot   # none
   ss -ltn | grep -E ':4660[12]'                                     # none
   git worktree list                                                 # no bench worktrees
   ```

## Results (2026-10-30, kind, 4 nodes, 5 + 5 runs)

| Metric | Baseline `1f9136510` median (mean ± sd) | Candidate `532cced15` median (mean ± sd) | Δ median | p (one-sided) |
|---|---|---|---|---|
| **`post_runtime_s` (primary)** | 12.47 s (13.02 ± 1.35) | 9.89 s (9.83 ± 0.24) | **−2.57 s (−21%)** | **0.0041** |
| `provision_total_s` | 20.38 s (20.76 ± 1.17) | 17.38 s (17.40 ± 0.23) | −3.00 s (−15%) | 0.0041 |
| `pods_ready_s` (control) | 4.73 s (4.70 ± 0.08) | 4.52 s (4.25 ± 0.54) | −0.20 s (−4%) | 0.054 |
| `runtime_setup_s` (control) | 3.17 s (3.04 ± 0.27) | 3.24 s (3.32 ± 0.43) | +0.07 s (+2%) | 0.82 |

All checks passed and the verdict was **VALID**.
- Every baseline launch ran 2–3 rounds of the old polling loop. No candidate
  launch ran any.
- Neither build restarted Ray or retried the wait.
- The slowest candidate launch (10.0 s) was faster than the fastest baseline
  launch (12.3 s). With 5 runs per side and no overlap, p = 0.0041 is the
  smallest value the test can give.
- The candidate was also much more consistent: ±0.24 s against ±1.35 s.

The two control metrics did not change significantly, so the gain comes from
the segment the change targets.

## Pitfalls

- **Compare builds on the same cluster at the same time.** Comparing logs
  taken at different times, or on a recreated cluster, mixes in cluster
  drift. That gave an inflated 25–31% estimate before this A/B was run.
- **Check that each build ran its own commit.** An early version ran both
  "builds" from the candidate's code, because `python -m` puts the cwd on
  `sys.path`. The commit check and the import pre-flight catch this now.
  Don't trust a verdict where the check "Each build ran its own, distinct
  commit" failed.
- **The context must be idle.** Other SkyPilot pods skew `pods_ready_s` and
  everything after it.
