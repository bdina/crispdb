# CrispDB
A Constant Database (CDB) library that implements the public domain spec

### Requirements
This project requires `Java 17` and `Scala 3.9.0`

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

## Data Compression (Values)

CrispDB supports opt-in compression for record values using standard JVM compression libraries (`Deflate` / `Gzip`). 

- **Standard Compliance**: Default database creation remains 100% compliant with standard uncompressed CDB public domain specification.
- **Adaptive Framing**: Compressible values are framed with a safe magic envelope (`\0CDZ`) and decompressed transparently on read. Small (< 32 bytes) or incompressible data automatically remain uncompressed.
- **Raw Access**: `cdb.findRaw(key)` and `cdb.rawIterator` provide access to exact on-disk payload bytes.

### Scala API Example

```scala
import cdb._
import cdb.compression._

// Create CDB with Deflate compression
val config = CompressionConfig(codec = CompressionCodec.Deflate)
val maker = CdbMake(config)
maker.start(tempPath)
maker.add("key".getBytes, "large compressible value".getBytes)
maker.finish()

// Reading automatically decompresses
val cdb = Cdb(cdbPath)
val value = cdb.find("key".getBytes) // Returns Some(decompressed bytes)
val raw = cdb.findRaw("key".getBytes) // Returns Some(raw on-disk bytes)
```

### CLI Usage

```bash
# Create compressed CDB
java -cp crispdb.jar cdb.make output.cdb temp.cdb --compress=deflate < input.txt

# Read value (decompressed by default)
java -cp crispdb.jar cdb.get output.cdb "key"

# Read raw on-disk bytes
java -cp crispdb.jar cdb.get output.cdb "key" --raw

# Dump database (decompressed or raw)
java -cp crispdb.jar cdb.dump output.cdb
java -cp crispdb.jar cdb.dump output.cdb --raw
```

## Tests (Docker)

Run unit tests without installing Gradle locally:

```
./bin/docker-build test
```

The suite includes a sparse-file test that validates offsets beyond 4GiB. An optional full-write test is available:

```
CRISPDB_RUN_LARGE_WRITE=1 ./bin/docker-build test
```
