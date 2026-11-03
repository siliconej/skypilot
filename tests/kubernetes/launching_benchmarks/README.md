# Kubernetes launch latency A/B benchmark

`bench_launch_latency.py` launches the same task many times from two SkyPilot
builds, a baseline and a candidate, and compares the provisioning timings from
each launch's `provision.log`.

## Prerequisites

- An idle Kubernetes context, for example a kind cluster from `sky local up`
  (context `kind-skypilot`).
- `git`, `uv` and `kubectl` on `PATH`, and an `origin/master` ref in the repo.
- A task YAML. A multi-node task works best, for example:

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

## Run the benchmark

1. From the repository root, activate the virtualenv:

   ```bash
   source .venv/bin/activate
   ```

2. Make sure no SkyPilot pods are running on the context. The script won't
   start if there are any, unless you pass `--force`.

   ```bash
   kubectl --context kind-skypilot get pods -A -l parent=skypilot
   ```

3. Run the A/B benchmark:

   ```bash
   python tests/kubernetes/launching_benchmarks/bench_launch_latency.py run \
       --candidate-ref <branch-or-commit> \
       --context kind-skypilot \
       --task job.yaml \
       --runs 5 --warmup 1
   ```

   | Option | Default | Meaning |
   |---|---|---|
   | `--candidate-ref` | `ej-prj-1` | Build under test |
   | `--baseline-ref` | merge-base of candidate and `origin/master` | Build to compare against |
   | `--context` | `kind-skypilot` | Kubernetes context to launch on |
   | `--task` | (required) | Task YAML to launch |
   | `--candidate-task` | `--task` | Task YAML for the candidate only, for example to use an image built from it. Needs the same `num_nodes` |
   | `--runs` / `--warmup` | `5` / `1` | Timed and untimed launches per build |
   | `--port` | `46601` | Baseline API server port; the candidate uses port + 1 |
   | `--workdir` | `/tmp/sky-bench-ab` | Where worktrees, venvs and server state go |
   | `--out` | `./bench-<timestamp>` | Results directory |
   | `--alpha` | `0.05` | Significance level for the verdict |
   | `--keep-worktrees` | off | Keep the worktrees after the run |

   Each build gets its own worktree, virtualenv and isolated local API server,
   so your working tree, `~/.sky` and main API server are not touched. Launches
   alternate between the builds. Each launch pair takes about 1 minute on kind.

4. Read the results in the output directory:
   - `summary.md`: the metrics table, the checks and the verdict;
   - `results.csv`: one row per launch;
   - `meta.json`: the refs, context, tasks and run counts;
   - `logs/`: each launch's CLI output and `provision.log`.

5. Confirm the cleanup. These commands should show no SkyPilot pods, no
   listeners on the bench ports and no bench worktrees:

   ```bash
   kubectl --context kind-skypilot get pods -A -l parent=skypilot
   ss -ltn | grep -E ':4660[12]'
   git worktree list
   ```

## Re-analyze saved logs

To recompute the summary from `provision.log` files you already have, run the
`analyze` subcommand. Skip run 0, which is the warm-up.

```bash
python tests/kubernetes/launching_benchmarks/bench_launch_latency.py analyze \
    --baseline bench-<ts>/logs/bench-base-[1-5].provision.log \
    --candidate bench-<ts>/logs/bench-cand-[1-5].provision.log
```
