# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a performance optimization project for the "One Billion Row Challenge" - processing a large CSV file (`measurements.txt`, ~13GB) containing weather station data in the format `station_name;temperature`. Each implementation progressively improves performance through different optimization techniques.

## Build and Run Commands

**Build the project:**
```bash
mvn clean compile
```

**Run a specific solution:**
```bash
# Replace ClassName with: NaiveSolution, NaiveParallelSolution, FileChannelSolution, etc.
mvn exec:java -Dexec.mainClass="org.example.ClassName"
```

**Compile and run directly with Java:**
```bash
javac -d target/classes src/main/java/org/example/ClassName.java
java -cp target/classes org.example.ClassName
```

## Architecture and Solutions Evolution

The codebase contains 6 solutions demonstrating progressive performance optimization (fastest to slowest):

1. **SwarSolution** (~5.6s) - SWAR (SIMD Within A Register) byte scanning, 8MB buffers, custom temperature parsing
2. **SwarAndThreadLocalMaps** (~6.2s) - Adds thread-local HashMaps to reduce contention during merging
3. **FileChannelSolution** (~16.6s) - Memory-mapped file chunks with parallel processing via ExecutorService
4. **ByteArrayKeySolution** (~19.8s) - Uses byte array keys instead of Strings to reduce allocations
5. **NaiveParallelSolution** (~56s) - Parallel streams over BufferedReader
6. **NaiveSolution** (~205s) - Sequential stream processing (baseline)

### Key Optimization Techniques

**File Processing:**
- All parallel solutions split the file into chunks (one per CPU core)
- `findNextLineStart()` ensures chunks align on line boundaries to avoid splitting records
- Advanced solutions use memory-mapped files or direct byte buffers instead of BufferedReader

**SWAR Technique (SwarSolution, SwarAndThreadLocalMaps):**
- `findByteSWAR()` searches for delimiter bytes (`;` and `\n`) by processing 8 bytes at a time packed into a long
- Uses bitwise operations to detect matching bytes in parallel: `(xor - 0x0101010101010101L) & ~xor & 0x8080808080808080L`
- Falls back to scalar search for short segments or unaligned positions

**Temperature Parsing:**
- Custom `parseTemperatureFast()` parses format `[-]XX.X` directly from bytes without String conversion
- Uses integer arithmetic: `intPart + fracPart * 0.1`

**Data Structures:**
- MutableResult class accumulates statistics (min/max/sum/count) in place to avoid object allocations
- HashMap with pre-sized capacity (512-1024) reduces resizing overhead
- Final results sorted into TreeMap for alphabetical output

**Threading Strategy:**
- FileChannelSolution: ExecutorService with fixed thread pool
- SwarAndThreadLocalMaps: Manual Thread creation with CountDownLatch for fine-grained control
- Thread-local maps reduce lock contention; final merge combines results from all threads

## Input File

The solutions expect `measurements.txt` in the project root containing lines like:
```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
```

## Performance Characteristics

Performance metrics in source file comments reflect processing time on the test system. Key factors:
- Java version: Requires Java 25 (see pom.xml)
- File size: ~13GB (13,795,254,856 bytes)
- Expected station count: ~400-500 unique stations
- Thread count: Determined by `Runtime.getRuntime().availableProcessors()`