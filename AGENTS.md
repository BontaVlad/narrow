# Agent Guidelines for narrow

## Build/Test Commands

- **Run all tests**: `just test`
- **Run single test**: Compile and run manually:
  ```
  nim c --verbosity:0 --hints:off --mm:orc -o:nimcache/tests/test_file tests/test_file.nim
  nimcache/tests/test_file
  ```
- **Debug tests**: `just test-debug` or `just test-debug-par` (parallel)
- **Release tests**: `just test-release`
- **With coverage**: `just test-coverage`
- **Generate bindings**: `nimble generate`
- **Format code**: `nimble format`
- **Clean cache**: `just clean`
- **Install deps**: `nimble install -y`

## Code Style

### Imports
```nim
import std/[options, strformat]  # stdlib first
import ./[ffi, gtypes, error]     # local modules after
```
NEVER import generated.nim directly, this will be included by ffi

### Naming Conventions
- Types: `PascalCase` (e.g., `Array[T]`, `ArrayBuilder[T]`)
- Procs/vars: `camelCase` (e.g., `newArrayBuilder`, `appendNull`)
- Generic params: `T` for single, descriptive for multiple
- C FFI types: prefix with `GArrow` (e.g., `GArrowArray`)
- Test files: `test_*.nim`

### Memory Management (ARC/ORC)
Always implement custom hooks for C pointer wrappers:
```nim
type
  Array*[T] = object
    handle: ptr GArrowArray

proc `=destroy`*[T](ar: Array[T]) =
  if not isNil(ar.handle):
    g_object_unref(ar.handle)

proc `=sink`*[T](dest: var Array[T], src: Array[T]) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*[T](dest: var Array[T], src: Array[T]) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)
```

### `src/narrow/core/error.nim` — Error Handling Pattern

#### Type Definitions

```nim
type
  OperationError* = object of CatchableError
  GErrorWrapper = object        # Private
    error: ptr GError
```

`GErrorWrapper` has `=destroy` that calls `gErrorFree` if non-nil.

#### The `check` Macro

```nim
macro check*(callable: untyped, message: static string = ""): untyped
```

**How it works:**

1. Takes a C FFI **call expression** (must be `nnkCall`).
2. Automatically appends an `addr error.error` parameter to the call — the GError output parameter.
3. Calls the function.
4. If the GError is set (non-nil), raises `OperationError` with the error message.
5. If the return type is `gboolean`: checks the result is `1`, raises `OperationError` if not, does **not** return a value.
6. If the return type is anything else: returns the actual result (the raw C pointer, etc.).

This means C functions using `check` must have their **last parameter** be `ptr ptr GError` (which `check` fills in automatically). The caller does NOT pass the error parameter — `check` injects it.

**Important:** Some procs in `table.nim` (`addColumn`, `removeColumn`, `combineChunks`, `validate`, `validateFull`) do **manual** error handling instead of using `check`, because they need finer control (e.g., `validate` returns `bool` while also having a GError).

```nim
var err = newError
let myResult = my_function(newError.toPtr)
if err:
  raise newException(OperationError, "Failed to do something: " & $err)
```

```nim

var err = newError
let myResult = my_function(newError.toPtr)
if err:
  raise newException(OperationError, "Failed to do something: " & $err)
```

### Types & Generics
- Use `sink T` for move semantics in builders
- Provide `toPtr` helpers for FFI interop
- Support: bool, int8-64, uint8-64, float32/64, string

### Testing (unittest2)
```nim
import unittest2
import ../src/[garray, ffi]

suite "Feature Name":
  test "description of test":
    check condition
    expect(OperationError):
      badOperation()
```
For quick running the test use `just test-debug-par`. This will run tests in parallel

### Formatting
Use `nph` for formatting: `nph file.nim` or better yet `nimble format`

## Project Structure

```
src/
  narrow.nim                    # Main exports
  narrow/
    core/
      ffi.nim                   # Foreign function interface
      concepts.nim              # Type concepts/interfaces
      error.nim                 # Error handling macros
      generated.nim             # Auto-generated bindings
    types/
      gtypes.nim                # Type mappings
      gtemporal.nim             # Temporal/date types
      glist.nim                 # List type implementation
      glisttype.nim             # List type definitions
      gfixedsizelisttype.nim    # Fixed-size list type
      glargelisttype.nim        # Large list type
      gmap.nim                  # Map type
      gfixedshapetensortype.nim # Fixed-shape tensor type
      guuid.nim                 # UUID type
    column/
      primitive.nim             # Primitive array types/builders
      nested.nim                # Nested array types
      metadata.nim              # Column metadata
    tabular/
      table.nim                 # Table types
      batch.nim                 # Record batch types
    compute/
      expressions.nim           # Expression API
      filters.nim               # Filter operations
      acero.nim                 # Acero compute engine
    io/
      csv.nim                   # CSV I/O
      parquet.nim               # Parquet I/O
      filesystem.nim            # FileSystem abstractions
tests/
  test_*.nim                    # Test files
```

## Dependencies

- Apache Arrow C++ and GLib bindings (arrow-glib)
- Apache Parquet GLib bindings (parquet-glib)
- GObject/GLib
- `futhark` for binding generation
- `unittest2` for testing

### Available Arrow Features

**Acero Execution Engine** (Already bound in `generated.nim`):
- `garrow_execute_plan_*` - Query execution plans
- `garrow_execute_plan_build_filter_node` - Filter operations with `GArrowExpression`
- `garrow_source_node_options_new_table` - Table sources
- `garrow_sink_node_options_*` - Result collection

**Arrow Dataset GLib** (Not yet bound, requires `arrow-dataset-glib`):
- `gadataset_scanner_builder_set_filter` - Push-down filtering with statistics pruning
- `gadataset_file_system_dataset_factory_*` - Multi-file datasets

**Key Functions Available**:
```
garrow_table_filter(table, booleanArray, options, error)
garrow_execute_plan_build_filter_node(plan, input, filterOptions, error)
gparquet_arrow_file_reader_read_row_group(reader, rowGroupIdx, columnIndices, nColumns, error)
```

## CI Requirements

Tests run on Ubuntu with clang, using AddressSanitizer in debug mode.

## Architecture Principles

### Use Battle-Tested Arrow Code
**Never reimplement functionality that Arrow already provides.** Arrow C++ has spent years hardening:
- Statistics evaluation and row group pruning
- Type coercion between filter literal types and column types
- Three-valued logic (true/false/unknown)
- Edge cases with null statistics, NaN in float comparisons, etc.

Instead of custom filtering logic, use:
- **Acero execution engine** (`garrow_execute_plan_*`) for in-memory filtering
- **Arrow Dataset GLib** (`gadataset_scanner_builder_set_filter`) for push-down filtering with automatic statistics-based pruning

### Layered Implementation Strategy
When adding complex features like filtering:
1. **Layer 1 (Immediate)**: Implement conservative, simple optimizations using existing bindings
2. **Layer 2 (Next)**: Add Arrow Dataset integration for optimal performance
3. Keep the same public API across layers - implementation swaps behind the scenes

### API Design Philosophy
- **Keep it simple**: Named parameters over builder patterns unless state accumulation is truly needed
- **Fail fast**: Raise `KeyError` immediately for missing columns, don't silently drop them
- **Avoid premature optimization**: Don't add parallel reading, batch sizing, or threading knobs until benchmarks prove they help
- **YAGNI**: Don't add `ReadOptions` objects until multi-file support actually needs them

## Pragmatic Principles
- **Safety First**: Every `ptr GArrow...` must be wrapped in a Nim `object` with ARC hooks (`=destroy`).
- **Zero-Copy**: Favor `sink` parameters and move semantics to avoid unnecessary `g_object_ref` calls.
- **Nim-ified API**: 
  - Use `len` instead of `get_length`.
  - Use `[]` instead of `get_value(i)`.
  - Use `$` for string representation (wrapping `garrow_..._to_string`).

## Implementation Checklist
1. [ ] Wrap FFI pointer in an `object`.
2. [ ] Implement `=destroy`, `=copy`, `=sink`.
3. [ ] Wrap C-call in `check()` macro for `GError` handling.
4. [ ] Export a `handle` or `toPtr` for low-level access.
5. [ ] Add `unittest2` test case in `tests/`.
