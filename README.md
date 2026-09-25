# CrispDB
A Constant Database (CDB) library that implements the public domain spec

### Requirements
This project requires `Java 17` and `Scala 3.3.7`

## Create a JAR
1. use gradle to create a build of the jar:
```
gradle shadowjar
```

## File formats

### Legacy (32-bit) CDB
- **Header**: 2048 bytes = 256 entries of `(pos:u32_le,len:u32_le)`
- **Record**: `(klen:u32_le,dlen:u32_le,key,data)`
- **Hash slot**: `(hash:u32_le,recordPos:u32_le)`
- **Limit**: offsets effectively cap out below 4GiB

### 64-bit extension (CDB64)
- **Header**: 2048 bytes = 256 entries of `tablePos:u64_le`
- **Hash slot**: `(hash:u64_le,recordPos:u64_le)` (16 bytes per slot)
- **Record**: unchanged from legacy
- **Table size**: derived from `tablePos[i+1] - tablePos[i]` (last table uses `fileSize - tablePos[255]`)
- **Compatibility**: reads both formats; writing legacy remains default

## Writing CDB64

Use `CdbMake.make64(...)` to create a 64-bit database (legacy writer remains `CdbMake.make(...)`).

## Tests (Docker)

Run unit tests without installing Gradle locally:

```
./bin/docker-build test
```

The suite includes a sparse-file test that validates offsets beyond 4GiB. An optional full-write test is available:

```
CRISPDB_RUN_LARGE_WRITE=1 ./bin/docker-build test
```
