<!-- SPDX-License-Identifier: MIT -->
# Gate runners (ADR-0046)

The heavy gate set — the battery's lanes, `./internal/coordinator/` without `-short` split into four
shards, the pre-push hook's race run and the two PostgreSQL oracle arms — runs for ONE commit on an
ephemeral spot `c7a.8xlarge` in us-east-2. One sha in; `summary.json` and one log per lane out, in
`s3://wadjet-gates-use2/gates/<sha>/<run>/`, mirrored to the host. The decision, the caps and the
standing approval are in [ADR-0046](../../docs/adr/0046-gate-runners-off-the-host.md).

There are exactly two commands. Neither takes a variable, a flag file or an environment variable.

| When | Who | Command |
|---|---|---|
| once (and again after `runner.sh` changes) | Derek | `tofu -chdir=deploy/gates/terraform init && tofu -chdir=deploy/gates/terraform apply` |
| every gate request | anyone | `~/.claude/projects/-home-dwright-Projects-caelum/tooling/gaterun.sh <sha>` |

`gaterun.sh` lives in the tooling repository, not here. Below it is written `gaterun.sh`.

## Before the first request: `gaterun.sh --check`

Prints one line per prerequisite and launches nothing. Before the one-time setup it prints FAIL
for the launch template, the bucket and the gate reaper (the template-tag line appears only once
the template exists). A `FAIL` line names the command that fixes it;
`WARN` lines do not block. Exit 0 = ready.

```
$ gaterun.sh --check
PASS profile citc resolves
PASS launch template wadjet-gate-runner exists
PASS template tags Project=wadjet-bench, Name
PASS bucket wadjet-gates-use2 exists
WARN SSM /wadjet/gates/github-token present                  absent: runners clone anonymously (fine while the repo is public); ...
PASS reaper wadjet-gate-reaper keyed on Name=wadjet-gate-runner
PASS reaper wadjet-bench-reaper keyed on Project=wadjet-bench
PASS runners running: 0 (< 4)
PASS git can push refs/gates/* to origin
check: ready
```

## One-time setup (Derek)

1. Optional while the repository is public — a read-only token for the clone. Create a fine-grained
   GitHub token with `Contents: read` on `derekmwright/wadjet`, then:
   `aws --profile citc --region us-east-2 ssm put-parameter --name /wadjet/gates/github-token --type SecureString --value <token>`.
   Without it the runner clones anonymously. Tofu never creates or reads the token's value.
2. From the repository root: `tofu -chdir=deploy/gates/terraform init && tofu -chdir=deploy/gates/terraform apply`.
   Every input has a default in `terraform/variables.tf` (profile `citc`, region `us-east-2`, the
   bucket, the instance type, the 40-minute reaper bound). It creates:
   - the bucket `wadjet-gates-use2` (every object expires after 14 days; no public access);
   - the IAM role `wadjet-gate-runner`: read/write `gates/*` in that bucket, read the SSM token, SSM Session Manager;
   - the security group `wadjet-gate-runner`: no ingress;
   - the launch template `wadjet-gate-runner`: spot one-time, shutdown = terminate, IMDSv2 with
     instance tags, 200 GB gp3, tags `Project=wadjet-bench` + `Name=wadjet-gate-runner`, and
     `runner.sh` as its user_data;
   - the gate reaper `wadjet-gate-reaper` (Lambda, every 5 minutes): terminates any
     `Name=wadjet-gate-runner` instance older than 40 minutes. The benchmark reaper
     (`deploy/benchmark/reaper`, `Project=wadjet-bench`, 2 hours) remains the outer backstop.

   The apply is idempotent. Re-run it after `runner.sh` changes: the runner is the template's
   user_data, so a new default template version is what makes the change live. Its state is local
   (`deploy/gates/terraform/terraform.tfstate`, gitignored) in the checkout it was run from.

## A request

```
gaterun.sh [--evidence DIR] [--rerun-flakes[=T1,T2]] <sha> [lanes]
```

- `<sha>`: any commit in the repository (a branch tip in any worktree). It is pushed as the plain ref
  `refs/gates/<sha>` (no branch; `--no-verify` because the box runs the hook's race command itself;
  the hook's AI-trailer refusal is repeated first).
- `lanes`: `all` (default) or a comma list of `gofmt A B C D E F F1 F2 F3 F4 G HOOK PG PG1 PG2`
  (`F` = the four coordinator shards, `PG` = both oracle arms). Lanes A–G and gofmt are
  `tooling/battery_run.sh`'s commands verbatim; `F1..F4` are lane F split round-robin over
  `go test -list . ./internal/coordinator/`; `HOOK` is `go test -race -short ./internal/... -timeout 10m`;
  `PG1`/`PG2` are `task pg-oracle:test` and `task pg-oracle:test-decimal`, run after the other lanes
  (an arm that SKIPs for want of PostgreSQL is recorded as a failure).
- `--rerun-flakes`: a listed test that fails is rerun ALONE on the same box after all lanes; a lane
  whose every failed top-level test passed alone reports `effective_exit 0` (both runs stay in the
  summary). With no list it uses the timing pins of #789 and #1391.
- `--evidence DIR`: where `summary.json` and the logs are mirrored (default
  `tooling/gate-runs/<sha>/<run>/`).

It refuses (exit 2) when four runners are already up or the launch template does not tag instances
`Project=wadjet-bench`; launches with `aws ec2 run-instances --launch-template` (retrying another
default-VPC subnet on a spot capacity refusal); polls every 30 s with a 45-minute cap; prints the
lane table; exits with the worst lane's effective exit (3 = the instance vanished without a summary,
for example a spot interruption; 124 = the wait cap, after terminating the instance).

`--dry-run` shows what a request would do and does nothing (no push, no AWS call, so it works
before the one-time setup and whatever `--check` says):

```
$ gaterun.sh --dry-run e9abe974 gofmt,A
sha:      e9abe974b2a0e19432d5f85c4b588931efaaa673
run:      20261002T211533Z   (results: s3://wadjet-gates-use2/gates/e9abe974b2a0e19432d5f85c4b588931efaaa673/20261002T211533Z/summary.json)
push:     git -C /home/dwright/Projects/caelum push --no-verify origin e9abe974b2a0e19432d5f85c4b588931efaaa673:refs/gates/e9abe974b2a0e19432d5f85c4b588931efaaa673
tags:     Project=wadjet-bench Name=wadjet-gate-runner sha=e9abe974b2a0e19432d5f85c4b588931efaaa673 run=20261002T211533Z launched=20261002T211533Z bucket=wadjet-gates-use2 lanes=gofmt+A
launch:   aws --profile citc --region us-east-2 ec2 run-instances --count 1 --launch-template 'LaunchTemplateName=wadjet-gate-runner,Version=$Default' --tag-specifications 'ResourceType=instance,Tags=[{Key=Project,Value=wadjet-bench},{Key=Name,Value=wadjet-gate-runner},{Key=sha,Value=e9abe974b2a0e19432d5f85c4b588931efaaa673},{Key=run,Value=20261002T211533Z},{Key=launched,Value=20261002T211533Z},{Key=bucket,Value=wadjet-gates-use2},{Key=lanes,Value=gofmt+A}]' 'ResourceType=volume,Tags=[...same...]' --query 'Instances[0].InstanceId' --output text
dry run: nothing pushed, nothing launched
```

### On the box (`runner.sh`)

The instance reads its inputs only from its own tags through IMDS (`sha`, `run`, `bucket`, `lanes`,
`rerun`), so the launch template never changes between requests. It installs git and Docker, pulls
`postgres:17-alpine`, clones `refs/gates/<sha>`, installs the Go version `go.mod` names and go-task
(both cached in `gates/cache/`, as is the module cache keyed by `go.sum`), compiles every test
binary once, starts the lanes as separate processes, writes `status.txt` as lanes finish, and uploads
`logs/<lane>.log` and finally `summary.json` (sha, run, instance, started/finished, phase walls, and
per lane: exit, effective_exit, FAIL count, failed test names, wall, log key). Then `shutdown -h now`,
which terminates the instance. `shutdown -h +38` at boot is the hard watchdog; at 35 minutes a soft
watchdog kills the lanes still running (recorded as exit 124) so the summary still lands.

### Citing a run as the SHA-MATCH log

`summary.json` names the sha it ran and is written by the box, not by the requester. A gate row
cites it as `s3://wadjet-gates-use2/gates/<sha>/<run>/summary.json` (lane log:
`.../logs/<lane>.log`), or the mirrored copy under the evidence directory. The reviewer checks that
`sha` in the summary equals the tip under review and that the lane's `effective_exit` is 0; a lane
cleared by `--rerun-flakes` names the rerun log in its `note`.

## Caps and cost

Spot `c7a.8xlarge` only; self-terminating; 40-minute hard bound (gate reaper); at most four at once
(checked before every launch); the gate bucket only. Anything outside these is a deploy and needs
its own approval (ADR-0046). The `c7a.8xlarge` spot price in us-east-2 was $0.37–0.39/hour on
2026-10-02 (on-demand $1.64), so a 25-minute request costs about $0.16, plus cents of EBS and S3.

## Housekeeping

- Running runners: `aws --profile citc --region us-east-2 ec2 describe-instances --filters Name=tag:Name,Values=wadjet-gate-runner Name=instance-state-name,Values=pending,running --query 'Reservations[].Instances[].[InstanceId,Tags[?Key==`sha`]|[0].Value,LaunchTime]' --output text`
- `refs/gates/*` accumulate on origin; prune with `git push origin --delete refs/gates/<sha>` (gaterun never deletes one, since a second request may be cloning it).

> A change to `runner.sh` is a change to the launch template's user_data: re-run the one-time `tofu apply` after it lands, or the next runner still boots the old script.
