# V4 policy knobs

Environment variables:

- `AXIOMFLOW_POLICY`: `ucb | thompson | epsilon | best`
- `AXIOMFLOW_UCB_C`: exploration coefficient for UCB (default 0.5)
- `AXIOMFLOW_EXPLORE_EPS`: probability of epsilon exploration (default 0.05)
- `AXIOMFLOW_EXPLORE_MAX_FACTOR`: admissibility window for exploration (default 3.0)

Feature model:
- `AXIOMFLOW_MODEL_LR` (default 0.001)
- `AXIOMFLOW_MODEL_L2` (default 0.0001)

Streaming:
- `AXIOMFLOW_STREAMING`: set `0` to disable
- `AXIOMFLOW_STREAMING_BATCH`: sink insert batch size
