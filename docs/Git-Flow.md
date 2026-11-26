# Git flow

## Branches

- `develop` is the integration branch for completed and reviewed features.
- `feature/<issue-number>` is created from `develop` for one issue.
- `test` is the shared branch for testing a feature before it is integrated into `develop`.
- `main` is the main branch with all done features in reached milestone release.

For step-by-step commands, validation guidance, and PR templates, see [Development Process](dev-guide/development-process.md).

## Feature workflow

1. Create `feature/<issue-number>` from the latest `develop`.
2. Implement only the assigned issue and commit the changes on that feature branch.
3. When the feature is ready, merge `feature/<issue-number>` into `test` for team review and validation.
4. After the team accepts the feature on `test`, merge it into `develop`.
5. Fix any issues found during review on the feature branch, then merge the updates into `test` and repeat validation before integration into `develop`.

## Example feature

Issue #n is being implemented on `feature/<n>`. The intended merge path is:

`feature/<n>` -> `test` -> `develop`

Do not merge directly into `develop` before the feature has been accepted on `test`.

## Commit

Please use this commit structure: `Refs #<commitNumber>: Commit message`.

Configure `docs/templates/commit-message.txt` with `git config --local commit.template docs/templates/commit-message.txt` to prefill a commit message. GitHub PRs use [.github/PULL_REQUEST_TEMPLATE.md](../.github/PULL_REQUEST_TEMPLATE.md).
