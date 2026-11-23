# Team contribution guide

This guide establishes a shared workflow for the five-person course project. Adjust it as the team agrees on repository tooling and implementation conventions. The goal is to make work reviewable, reproducible, and understandable by every team member.

## Team responsibilities

The accepted proposal assigns dataset acquisition, sorting, and cleaning primarily to the Bioinformatics Applied in Lifesciences members, and architecture and pipelines primarily to the Software Engineering members. These are starting responsibilities, not silos: every member should understand the full architecture and be ready to explain their contribution during reviews and the final demonstration.

For each issue or substantial task, name one directly responsible owner and any collaborators. Record handoffs, assumptions, and decisions in the issue or relevant documentation. Share knowledge when tasks cross data acquisition, processing, and visualization boundaries.

## Before starting work

1. Check the issue, project requirements, architecture overview, and relevant ADRs.
2. Confirm the expected input/output contract, acceptance criteria, owner, and collaborators.
3. Check whether source data may be committed or shared. Do not add credentials, private data, or restricted datasets to the repository.
4. Break work into reviewable changes. Coordinate early when another task depends on a schema, interface, or service decision.
5. If requirements or architecture need to change, update the relevant documentation with the implementation.

## Branches and commits

- Work on a short-lived branch per issue or cohesive change; do not commit directly to the default branch.
- Use a descriptive branch name such as `docs/add-contribution-guide` or `feat/batch-ingestion`.
- Keep commits focused and use clear imperative summaries, optionally prefixed by area (for example, `docs: clarify county mapping assumptions`).
- Avoid mixing formatting-only changes, unrelated fixes, and feature work in one pull request.
- Do not commit generated outputs, local environment files, credentials, or large data files unless the team has explicitly agreed they belong in version control.

## Pull requests and review

- Open a pull request for each cohesive change and link the related issue.
- Explain purpose, approach, important tradeoffs, data/schema/interface changes, and how the change was checked.
- Update documentation, setup instructions, and examples when behavior or commands change.
- Request review from at least one teammate. For changes to shared schemas, architecture, or research methodology, include the relevant data/architecture teammate(s).
- Review for correctness, clarity, reproducibility, data provenance, error handling, and consistency with the research question.
- Resolve review conversations or document why a suggestion was not adopted. Do not merge your own pull request without the agreed team review.
- Keep the pull request current with the target branch and confirm required checks before merging.

## Data and research contributions

- Record source, extraction date/query, license or terms, coverage, units, and known limitations for every dataset used.
- Preserve raw-source provenance and make cleaning, deduplication, geographic matching, and aggregation rules explicit.
- Distinguish missing climate data from zero butterfly observations.
- Treat iNaturalist counts as reported observations, not direct population estimates. Discuss observation effort and other limitations when interpreting trends.
- Keep batch historical results and live-stream summaries distinguishable when their time windows or measurement definitions differ.
- Do not publish precise or sensitive location details unless appropriate for the source terms and project purpose.

## Quality and reproducibility

- Follow the established Python style, module layout, dependency, and configuration conventions once those are added to the repository.
- Include input validation and clear errors at data boundaries; make transformations deterministic where feasible.
- Describe manual or automated checks performed in the pull request. Add or update automated checks when appropriate to the implementation and team plan.
- Provide exact commands and expected outputs for new workflows in `docs/dev-guide/setup.md` or a focused runbook.
- Never claim a check passed unless it was run and its result is known.

## Group coordination and course readiness

- Keep issues and project-board status current enough that teammates can see ownership, blockers, and next steps.
- Agree on interfaces before parallel work depends on them; capture decisions in `docs/technical-decisions.md` or an ADR.
- Share short progress updates and raise blockers early. Reassign or pair on work when a task risks becoming a single-person knowledge bottleneck.
- Before each course review, ensure each member can explain the problem, sources, their contribution, and how the complete pipeline works.
- Maintain a team contribution record in the course materials if requested by the instructor; ensure it accurately reflects work completed.
