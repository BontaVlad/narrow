## Narrow Arrow Concepts
##
## This module defines Nim concepts that formalize the standardized API
## patterns for Arrow data structures. These concepts enable compile-time
## checking of API compliance and generic programming.
##
## Usage:
##   proc processData[T: ArrowIndexable](data: T) =
##     # Works with Array[T], ChunkedArray[T], etc.
##     for item in data:
##       echo item
##
## Note: These concepts are DECLARATIVE specifications. They define what
## methods must exist for a type to satisfy the concept, but don't require
## those methods to be defined in this module.

type
  ## ArrowIndexable represents any 1D Arrow data structure that supports
  ## indexed access to typed elements. This includes Array[T], ChunkedArray[T],
  ## and similar structures.
  ##
  ## Required methods (must be defined by implementing types):
  ## - `len(ds): int`: Returns the number of elements
  ## - `ds[int]: T`: Index operator with bounds checking
  ## - `ds.tryGet(int): Option[T]`: Safe indexed access
  ## - `items` iterator over elements
  ## - `$` (string representation)
  ## - `==` (equality comparison)
  ## - `isNull(ds, int): bool`: Check if element is null
  ## - `isValid(ds, int): bool`: Check if element is valid
  ArrowIndexable* =
    concept ds
        ds.len is int
        ds[int] is typed
        ds.tryGet(int) is typed # Option[T]
        # Note: iterator items is not checked in concept due to Nim limitations
        `$`(ds) is string
        `==`(ds, ds) is bool
        isNull(ds, int) is bool
        isValid(ds, int) is bool

  ## ArrowChunked represents chunked 1D data structures like ChunkedArray[T].
  ## Extends ArrowIndexable with chunk-specific operations.
  ##
  ## Additional required methods:
  ## - `nChunks(ds): uint`: Number of chunks
  ## - `chunks` iterator over chunks
  ## - `combine(ds)`: Merge chunks into single Array
  ArrowChunked* =
    concept ds
        ds is ArrowIndexable
        ds.nChunks is uint
        # Note: iterator chunks is not checked in concept due to Nim limitations
        combine(ds) is typed

  ## Schema is defined in gschema module but referenced here
  ## for concept definitions.
  Schema* =
    concept s
        s.nFields is int

  ## Field is defined in gschema module
  Field* =
    concept f
        f.name is string

  ## ArrowTabular represents tabular data structures like RecordBatch and Table.
  ##
  ## Required methods:
  ## - `nRows(tbl): int64`: Number of rows
  ## - `nColumns(tbl): int`: Number of columns
  ## - `schema(tbl)`: Table schema
  ## - `columns` iterator over column fields
  ## - `validate(tbl): bool`: Validate structure
  ## - `validateFull(tbl): bool`: Full validation
  ArrowTabular* =
    concept tbl
        tbl.nRows is int64
        tbl.nColumns is int
        tbl.schema is Schema
        # Note: iterator columns is not checked in concept due to Nim limitations
        validate(tbl) is bool
        validateFull(tbl) is bool
        `$`(tbl) is string
        `==`(tbl, tbl) is bool

  ## ArrowRecordBatch represents a fixed-schema tabular structure.
  ## Record batches have columns as Arrays (not chunked).
  ArrowRecordBatchConcept* =
    concept rb
        rb is ArrowTabular

  ## ArrowTableConcept represents a chunked tabular structure.
  ## Tables have columns as ChunkedArrays.
  ArrowTableConcept* =
    concept tbl
        tbl is ArrowTabular
        combineChunks(tbl) is typed

  ## ArrowBuilder represents any builder for Arrow data structures.
  ##
  ## Required methods:
  ## - `b.append(sink T)`: Add single value
  ## - `b.appendNull()`: Add null value
  ## - `b.finish()`: Complete building and return data structure
  ArrowBuilder* =
    concept b
        b.append(sink typed)
        b.appendNull()
        b.finish() is typed

  ## ArrowNullable represents structures that can contain null values.
  ArrowNullable* =
    concept ds
        isNull(ds, int) is bool
        isValid(ds, int) is bool
        ds.nNulls is int64

  ## ArrowSlicable represents structures that support zero-copy slicing.
  ArrowSlicable* =
    concept ds
        ds is ArrowIndexable
        slice(ds, int64, int64) is typed
        ds[int, int] is typed # Slice operator

  ## ArrowStruct represents struct array structures like StructArray.
  ##
  ## Required methods:
  ## - `len(sa): int`: Number of struct rows
  ## - `isNull(sa, int): bool`: Check if struct row is null
  ## - `isValid(sa, int): bool`: Check if struct row is valid
  ## - `nNulls(sa): int64`: Count of null rows
  ## - `items` iterator over struct rows
  ## - `toSeq(sa)`: Convert to sequence
  ArrowStruct* =
    concept sa
        sa.len is int
        isNull(sa, int) is bool
        isValid(sa, int) is bool
        sa.nNulls is int64
        # Note: iterator items is not checked in concept due to Nim limitations
        `$`(sa) is string
        `==`(sa, sa) is bool

  ## ArrowMap represents map array structures like MapArray.
  ##
  ## Required methods:
  ## - `len(ma): int`: Number of map entries
  ## - `isNull(ma, int): bool`: Check if map entry is null
  ## - `isValid(ma, int): bool`: Check if map entry is valid
  ## - `nNulls(ma): int64`: Count of null entries
  ## - `items` iterator over map entries
  ## - `toSeq(ma)`: Convert to sequence
  ArrowMap* =
    concept ma
        ma.len is int
        isNull(ma, int) is bool
        isValid(ma, int) is bool
        ma.nNulls is int64
        # Note: iterator items is not checked in concept due to Nim limitations
        `$`(ma) is string
        `==`(ma, ma) is bool

## Type Checking Helpers

template isArrowIndexable*(T: typedesc): bool =
  ## Check if a type satisfies the ArrowIndexable concept at compile time
  T is ArrowIndexable

template isArrowChunked*(T: typedesc): bool =
  ## Check if a type satisfies the ArrowChunked concept at compile time
  T is ArrowChunked

template isArrowTabular*(T: typedesc): bool =
  ## Check if a type satisfies the ArrowTabular concept at compile time
  T is ArrowTabular

template isArrowBuilder*(T: typedesc): bool =
  ## Check if a type satisfies the ArrowBuilder concept at compile time
  T is ArrowBuilder

template isArrowNullable*(T: typedesc): bool =
  ## Check if a type satisfies the ArrowNullable concept at compile time
  T is ArrowNullable

template isArrowSlicable*(T: typedesc): bool =
  ## Check if a type satisfies the ArrowSlicable concept at compile time
  T is ArrowSlicable

template isArrowStruct*(T: typedesc): bool =
  ## Check if a type satisfies the ArrowStruct concept at compile time
  T is ArrowStruct

template isArrowMap*(T: typedesc): bool =
  ## Check if a type satisfies the ArrowMap concept at compile time
  T is ArrowMap

## Compile-time Compliance Checks
##
## These templates can be used in test suites or at module level to verify
## that types comply with the standardized interface.

template checkArrowIndexable*(T: typedesc) =
  ## Compile-time check that a type satisfies ArrowIndexable
  static:
    when not (T is ArrowIndexable):
      {.
        error:
          $T & " does not satisfy ArrowIndexable concept. " &
          "Required: len, [], tryGet, items, $, ==, isNull, isValid"
      .}
    else:
      {.hint: $T & " satisfies ArrowIndexable concept".}

template checkArrowChunked*(T: typedesc) =
  ## Compile-time check that a type satisfies ArrowChunked
  static:
    when not (T is ArrowChunked):
      {.
        error:
          $T & " does not satisfy ArrowChunked concept. " &
          "Required: nChunks, chunks iterator, combine"
      .}

template checkArrowTabular*(T: typedesc) =
  ## Compile-time check that a type satisfies ArrowTabular
  static:
    when not (T is ArrowTabular):
      {.
        error:
          $T & " does not satisfy ArrowTabular concept. " &
          "Required: nRows, nColumns, schema, columns iterator, validate, validateFull"
      .}

template checkArrowBuilder*(T: typedesc) =
  ## Compile-time check that a type satisfies ArrowBuilder
  static:
    when not (T is ArrowBuilder):
      {.
        error:
          $T & " does not satisfy ArrowBuilder concept. " &
          "Required: append, appendNull, finish"
      .}

template checkArrowStruct*(T: typedesc) =
  ## Compile-time check that a type satisfies ArrowStruct
  static:
    when not (T is ArrowStruct):
      {.
        error:
          $T & " does not satisfy ArrowStruct concept. " &
          "Required: len, isNull, isValid, nNulls, items, $, =="
      .}

template checkArrowMap*(T: typedesc) =
  ## Compile-time check that a type satisfies ArrowMap
  static:
    when not (T is ArrowMap):
      {.
        error:
          $T & " does not satisfy ArrowMap concept. " &
          "Required: len, isNull, isValid, nNulls, items, $, =="
      .}

## Documentation References
##
## This module provides no runtime functionality - it is purely for:
## 1. Documenting the standardized API contract
## 2. Enabling compile-time type checking via concepts
## 3. Supporting generic programming
##
## To use these concepts:
##
##   import narrow
##   
##   # In your generic function
##   proc process[T: ArrowIndexable](data: T) =
##     for item in data:
##       echo item
##   
##   # Verify compliance at compile time
##   checkArrowIndexable(Array[int])
##   checkArrowIndexable(ChunkedArray[float64])
