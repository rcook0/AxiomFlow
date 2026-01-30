# FullStack Workspace (Backend Artifacts + Middleware Core)

This workspace contains the backend-core modules for:
- DQ Engine v1
- Stats Pack v1.1 (frequency inference + correlation)
- Operator-level lineage summary
- IR normalization + optimizer v1
- v2 building blocks: projectMap + exprRewrite

## Rebuild
This repository includes a rebuild script that can regenerate the workspace into a new folder.

```bash
chmod +x rebuild.sh
./rebuild.sh ./out_workspace
```

## Mongo indexes
```bash
mongosh "mongodb://localhost:27017/fullstack_app" scripts/db-indexes.js
```
