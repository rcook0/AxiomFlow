# Release Notes — v4.2 → v4.6

This release advances the V4 optimizer loop toward a research-grade, self-improving execution engine:

- **v4.2**: Thompson sampling enabled via variance tracking in `plan_variant_stats`.
- **v4.3**: Online wall-time regression model (`runtime_models`, `wall_ms_linear_v1`) to provide cold-start estimates.
- **v4.4**: Plan-family cache (`plan_family_cache`) to skip repeated enumeration and reuse candidate sets.
- **v4.5**: Offline evaluation loop (`scripts/policy-lab.js`) using recorded decision traces.
- **v4.6**: Streaming execution fast-path for linear pipelines (cursor-based processing to avoid full materialization).

See the per-feature docs for details:
- `V4_2_THOMPSON.md`
- `V4_3_FEATURE_MODEL.md`
- `V4_4_CACHE.md`
- `V4_5_POLICY_LAB.md`
- `V4_6_STREAMING.md`
