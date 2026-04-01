<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="narrow.svg">
    <source media="(prefers-color-scheme: dark)" srcset="narrow.svg">
    <img alt="narrow logo" src="narrow.svg" height="130">
  </picture>
  <br>
  <img src="https://github.com/BontaVlad/narrow/actions/workflows/tests.yml/badge.svg" alt="MainBranch">
</div>
<br>

# narrow

Nim bindings for Apache Arrow, providing access to Arrow's columnar memory format and compute capabilities.

## Overview

Narrow wraps the Apache Arrow GLib C API to provide Nim with:

- **Columnar data structures** - Arrays, chunked arrays, tables, and record batches
- **I/O operations** - Reading and writing Parquet, CSV, Feather, and IPC formats
- **Compute operations** - Expression-based filtering, aggregations, and the Acero execution engine
- **Memory safety** - Integration with Nim's ARC/ORC memory management via GObject reference counting

## Requirements

- Nim 2.2.6 or later
- Apache Arrow GLib (`arrow-glib`)
- Apache Parquet GLib (`parquet-glib`)
- Apache Arrow Dataset GLib (`arrow-dataset-glib`)
- GLib 2.0 (`glib-2.0`)
- GObject 2.0 (`gobject-2.0`)
- pkg-config

### Installing Dependencies

**Ubuntu/Debian:**
```bash
sudo apt install libarrow-glib-dev libparquet-glib-dev libarrow-dataset-glib-dev pkg-config
```

**macOS:**
```bash
#brew install apache-arrow-glib
TODO
```

## Installation

Add to your `nimble` file:

```nim
#requires "narrow >= 0.1.0"
TODO
```

Or install directly:

```bash
#nimble install narrow
TODO
```

## Usage

### Creating Arrays

```nim
import narrow

# Create arrays from sequences
let intArr = newArray(@[1'i32, 2'i32, 3'i32])
let strArr = newArray(@["hello", "world"])

# Or use builders for more control
var builder = newArrayBuilder[int64]()
builder.append(1'i64)
builder.append(2'i64)
builder.appendNull()
let arr = builder.finish()
```

### Creating Tables

```nim
import narrow
import narrow/column/metadata

# Define schema
let schema = newSchema([
  newField[int32]("id"),
  newField[string]("name"),
  newField[float64]("score")
])

# Create arrays
let ids = newArray(@[1'i32, 2'i32, 3'i32])
let names = newArray(@["Alice", "Bob", "Charlie"])
let scores = newArray(@[95.5'f64, 87.2'f64, 92.1'f64])

# Build table
let table = newArrowTable(schema, ids, names, scores)
```

### Reading and Writing Parquet

```nim
import narrow/io/parquet

# Write table to Parquet
writeTable(table, "data.parquet")

# Read table from Parquet
let table = readTable("data.parquet")

# Read with filtering
import narrow/compute/expressions

let age = col("age")
let filtered = readTable("data.parquet", filter = age > 18)
```

### Reading and Writing CSV

```nim
import narrow/io/csv

# Read CSV
let table = readCSV("data.csv")

# Read with custom delimiter
let table = readCSV("data.csv", newCsvReadOptions(delimiter = some(';')))

# Write table to CSV
writeCsv("output.csv", table)
```

### Filtering Data

```nim
import narrow/compute/expressions

let name = col("name")
let age = col("age")
let active = col("active")

# Simple filters
let filter1 = age >= 18
let filter2 = name.contains("admin")
let filter3 = startsWith(name, "A")

# Combined filters
let complexFilter = (age >= 18) and (name.toLower().contains("admin")) and active.isValid()

let filtered = table.filter(complexFilter)
```

## Project Status

This library is under active development. APIs may change until a stable release.

Current capabilities:
- Primitive array types (int, float, bool, string)
- Nested types (lists, structs, maps)
- Temporal types (date, timestamp, duration)
- CSV and Parquet I/O
- Expression-based filtering
- Acero execution engine bindings

## Development

### Building from Source

```bash
git clone https://github.com/BontaVlad/narrow.git
cd narrow
nimble install -y
```

### Running Tests

```bash
just test

# or run tests in parallel
just test-debug-par
```

Or with nimble:

```bash
LSAN_OPTIONS="suppressions=lsan.supp:print_suppressions=0" nimble test -d:useSanitizers
```

### Generating Bindings

The Arrow C API bindings are auto-generated using Futhark:

```bash
nimble generate
```

## Acknowledgments

This project relies on several open source libraries:

- [Apache Arrow](https://arrow.apache.org/) - Columnar data format and compute libraries
- [Arrow GLib](https://arrow.apache.org/docs/c_glib/) - GObject-based C bindings for Arrow
- [Parquet GLib](https://arrow.apache.org/docs/c_glib/parquet-glib/) - Parquet format support for Arrow GLib
- [Arrow Dataset GLib](https://arrow.apache.org/docs/c_glib/arrow-dataset-glib/) - Dataset API for Arrow GLib
- [GLib](https://docs.gtk.org/glib/) - Core application building blocks
- [GObject](https://docs.gtk.org/gobject/) - Object system for C
- [Futhark](https://github.com/PMunch/futhark) - Nim bindings generator for C libraries

## License

MIT License - see LICENSE file for details.
