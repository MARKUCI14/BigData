# How to add a feature

The codebase is not implemented yet. Use this workflow when adding pipeline, data, or dashboard features:

1. Describe the user/research need and the input/output data contract.
2. Inspect actual source schemas and existing code before choosing module boundaries or adding dependencies.
3. Implement a small vertical path through the appropriate layer, preserving source lineage and making transformations repeatable.
4. Handle invalid, missing, duplicate, and late-arriving data explicitly where relevant.
5. Document configuration, execution, outputs, and failure/restart behavior.
6. Update `docs/architecture/overview.md`, `docs/data-model.md`, requirements, or an ADR when the feature changes those contracts or decisions.
Once code and project conventions exist, replace this initial workflow with concrete module paths and commands.
