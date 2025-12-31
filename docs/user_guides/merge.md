# Merge

## Overview
Merge is the compaction pipeline in NutsDB. It rewrites valid entries into new merge files, removes stale data, and optionally produces hint files for faster startup. There is a single merge implementation with no algorithm toggle.

## When Merge Runs
- **Manual**: Call `db.Merge()` to trigger a merge.
- **Automatic**: Set `Options.MergeInterval` to a positive duration. Set it to 0 to disable auto merge.

## How It Works
1. **Preparation**: Enumerates data files and validates merge state.
2. **Rewrite**: Scans and rewrites valid entries into new merge files.
3. **Commit**: Updates in-memory indexes and writes hint files (if enabled).
4. **Finalize**: Persists outputs and removes old data/hint files.

## File ID Strategy
Merge outputs use negative file IDs (starting from `math.MinInt64`). During index rebuild, merge files are processed before normal data files, so newer writes always win.

## Configuration
```go
options := nutsdb.DefaultOptions
options.MergeInterval = 2 * time.Hour // set to 0 to disable auto merge
options.EnableHintFile = true
options.SegmentSize = 256 * 1024 * 1024 // 256MB

db, err := nutsdb.Open(options, nutsdb.WithDir("/tmp/nutsdb"))
```

## Manual Merge Example
```go
if err := db.Merge(); err != nil {
    log.Fatal(err)
}
```

## Operational Notes
- Merge is I/O intensive; schedule it during low-traffic periods.
- If hint files are disabled, Merge will skip hint file creation.
- Merge preserves correctness with concurrent writes and uses atomic commit/rollback.
