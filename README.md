<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="narrow.svg">
    <source media="(prefers-color-scheme: dark)" srcset="narrow.svg">
    <img alt="narrow logo" src="narrow.svg" height="130">
  </picture>
  <br>
  <img src="https://github.com/BontaVlad/narrow/actions/workflows/tests.yml/badge.svg" alt="MainBranch">
  <img src="https://img.shields.io/badge/unstable-pre_alpha-blue" alt="Status">
</div>
<br>

# narrow

The library exposes Arrow's language-independent, columnar memory format to the Nim ecosystem. It is designed to map Nim's memory management and syntax to the underlying GObject/Arrow C++ primitives.

## Core Objectives

**Interoperability**  
Facilitate zero-copy data exchange with other Arrow-enabled languages (Python/PyArrow, R, C++) via shared memory and IPC.

**Storage I/O**  
Provide interfaces for reading and writing standard columnar formats (CSV, Parquet, ORC) and filesystem operations.

**Memory Management**  
implementation of safe wrappers around GLib reference counting to ensure correct resource handling within Nim's scope.

## Status

The library is currently in a pre-alpha state, APIs may change significantly until an initial alpha release. Functionality encompasses:

### Data Types
Construction and manipulation of Arrays, ChunkedArrays, Structs, and RecordBatches.

### IO
CSV reading/writing and FileSystem abstractions.

### Compute
Basic Arrow compute kernel bindings (in progress).
