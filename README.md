# CrispDB
A Constant Database (CDB) library that implements the public domain spec

### Requirements
This project requires `Java 17` and `Scala 3.9.0`

## Building CLI Applications

CrispDB produces three dedicated CLI tools (`cdbmake`, `cdbdump`, and `cdbget`) with first-class support for both native executables and JVM execution.

### 1. GraalVM Native Executables
To compile standalone native binaries (outputs to `build/native/`):
```bash
# Build all three native binaries
gradle nativeImageAll

# Or build individual binaries
gradle nativeImageCdbmake
gradle nativeImageCdbdump
gradle nativeImageCdbget
```

### 2. Standalone Fat JARs
To build standalone executable JARs (outputs to `build/libs/`):
```bash
# Build all three fat JARs
gradle shadowJarAll

# Or build individually
gradle shadowJarCdbmake
gradle shadowJarCdbdump
gradle shadowJarCdbget
```

### 3. JVM Start Scripts
To generate runnable JVM shell and batch scripts (outputs to `build/install/crispdb/bin/`):
```bash
gradle installDist
```

## CLI Usage

The CLI applications match the reference Dan J. Bernstein `cdb` conventions (reading from stdin, exit codes `0` for match, `100` for key not found, `111` on error), while supporting CrispDB features (compression, 64-bit offsets, and file arguments).

### `cdbmake`
Creates a CDB database from `+klen,dlen:key->data\n` records read from standard input:
```bash
# Using native binary
./build/native/cdbmake output.cdb temp.cdb < input.txt

# With compression
./build/native/cdbmake output.cdb temp.cdb --compress=deflate < input.txt

# Using fat JAR
java -jar build/libs/cdbmake-all.jar output.cdb temp.cdb < input.txt
```

### `cdbdump`
Dumps records from a CDB database in `cdbmake` format:
```bash
# Standard input redirection
./build/native/cdbdump < output.cdb

# Direct file argument
./build/native/cdbdump output.cdb

# Dump raw on-disk bytes without decompression
./build/native/cdbdump output.cdb --raw
```

### `cdbget`
Queries a key from a CDB database (exits `0` if found, `100` if not found, `111` on error):
```bash
# Standard input redirection (DJB style)
./build/native/cdbget "mykey" < output.cdb

# Direct file argument
./build/native/cdbget output.cdb "mykey"

# Read raw on-disk bytes
./build/native/cdbget output.cdb "mykey" --raw
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
