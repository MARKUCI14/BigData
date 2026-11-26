# Development Process

This guide describes how we take an issue from the project board to a reviewed change in `develop`, then release selected milestones from `main`. Use one feature branch per issue, keep the pull request small enough to review, and record how the change was checked.

## Branch roles

| Branch | Purpose |
| --- | --- |
| `main` | Release and deployment history. |
| `develop` | Integration branch and starting point for new feature work. |
| `test` | Shared validation branch used before a feature is integrated into `develop`. |
| `feature/<issue>-<short-name>` | Isolated implementation for one issue, created from current `develop`. |

At the time this guide was written, `test` points to the initial commit while `develop` contains later work. A maintainer must first bring `test` up to date with `develop` (preferably through a reviewed PR from `develop` into `test`). Until then, a feature PR against `test` will include unrelated history and the test gate is not ready to use. Do not force-push or reset the shared `test` branch to solve this locally.

## Start an issue

1. Choose an open issue and read its goal, acceptance criteria, dependencies, and definition of done. Ask for clarification in the issue if a requirement is ambiguous.
2. Confirm that the issue is not already being implemented. Assign it to yourself or comment that you are starting, so teammates do not duplicate the work.
3. Update local `develop` and create a feature branch from it:

   ```bash
   git fetch origin
   git switch develop
   git pull --ff-only origin develop
   git switch -c feature/18-dashboard-skeleton
   ```

   Replace `18-dashboard-skeleton` with the actual issue number and a short kebab-case description. If you do not have a local `develop` branch yet, use `git switch --track -c develop origin/develop` first.

4. Check that you are on the new branch before editing:

   ```bash
   git status --short --branch
   git log -1 --oneline
   ```

Do not start by checking out a commit hash. A commit is a snapshot, not a working branch; start from the latest `develop` branch and create a named branch for your issue.

## Implement and commit

1. Implement only work that belongs to the issue. If you discover a separate task, create or update a separate issue and link it in the PR.
2. Keep generated data, credentials, `.env`, checkpoints, and local build outputs out of commits. Use `.env.example` for variable names and safe placeholders.
3. Run the relevant checks for the files you changed. For pipeline work, record whether checks ran locally, in Docker, or against test data. Do not claim an end-to-end check if the infrastructure did not start.
4. Review the change before committing:

   ```bash
   git diff --check
   git diff --stat
   git status --short
   ```

5. Use the repository commit style:

   ```text
   Refs #18: Add dashboard navigation skeleton
   ```

   Configure the optional commit message template once per clone:

   ```bash
   git config --local commit.template docs/templates/commit-message.txt
   ```

6. Push the feature branch:

   ```bash
   git push -u origin feature/18-dashboard-skeleton
   ```

If `develop` changes while you work, fetch and integrate those changes into your feature branch before the PR is merged. Coordinate with the reviewer if this causes conflicts; avoid rewriting a branch after others have based work on it.

## Validate through `test`

When a maintainer confirms that `test` is synchronized with `develop`:

1. Open a PR from your feature branch into `test` and use the validation section of the PR template.
2. Teammates review and run the acceptance checks on the shared test environment. Report failures on the PR and fix them on the same feature branch.
3. Update the PR and repeat validation until accepted.
4. Open a second PR from the feature branch into `develop`. Mention the successful test PR and summarize any fixes made during validation.
5. Merge into `develop` only after review and acceptance. Delete the feature branch after merge if GitHub offers that option and no open PR depends on it.

If the `test` branch has not been synchronized, ask a maintainer to prepare it first. Do not target the stale branch or merge the entire feature stack directly into `develop` as a workaround.

## Open a pull request

1. In GitHub, select **New pull request** and compare your feature branch against the intended base branch (`test` for validation, then `develop` for integration).
2. Check the **base** and **compare** branches before creating the PR. A PR into `main` is for a release, not ordinary feature development.
3. Use a title such as `feat(#18): add dashboard navigation skeleton` or `fix(#24): handle empty monthly input`.
4. Fill in the repository PR template. Include the issue, change summary, checks actually run, and any known limitation.
5. Review the GitHub diff yourself, request a teammate review, respond to comments, and update the same branch until accepted.
6. Merge using the repository's agreed merge method after review and required checks pass.

Use `Refs #18` in feature commits and PRs targeting `test` or `develop`. Since `main` is currently the GitHub default branch, a PR targeting `develop` does not automatically close the issue even if its description says `Closes #18`; GitHub only interprets closing keywords for PRs targeting the default branch. Link the issue from the PR sidebar or use `Refs #18`, then close the issue when its agreed completion point is reached (for example, after release to `main`). See [GitHub's issue linking rules](https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/linking-a-pull-request-to-an-issue).

## Release

When a milestone is ready, open a PR from `develop` into `main`. Review the milestone scope, deployment configuration, and verification evidence. Merge only the changes intended for that release. Use closing keywords in this PR only for issues that are complete and included in the release.

## Current project checks and limits

- `docker compose up --build` currently stops because `bitnami/spark:3.5` cannot be pulled. Do not report Docker-based acceptance checks as passed until the Spark image reference is corrected and the services start.
- Compose configures infrastructure but does not submit the batch or streaming Spark jobs automatically. Consult [batch pipeline](../pipelines/batch-pipeline.md) and [streaming pipeline](../pipelines/streaming-pipeline.md) for execution steps.
- The proposal's live source is weather data, while the current Kafka code streams butterfly observations. PRs touching streaming should state which contract they implement and whether they close this architecture gap.

