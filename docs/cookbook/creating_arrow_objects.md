# Creating Arrow Objects

Recipes for creating the core Arrow data structures — arrays, tables, and
record batches — with [narrow](https://github.com/BontaVlad/narrow), a Nim
wrapper over Apache Arrow.

.. contents::
----

## Creating Arrays

An Arrow `Array[T]` is built from a Nim `seq` with `newArray`. The element
type `T` is inferred from the seq's element type, so the resulting array is
typed and indexable:

```nim test
import narrow

let arr = @[1, 2, 3, 4, 5]
let garr = newArray[int](arr)
echo garr.len
echo garr[0]
echo garr[4]
```

```
5
1
5
```

## Creating Arrays with a mask to specify nulls

Pass a boolean `mask` as the second argument to `newArray`. Where the mask
is `true` the value is treated as null (Arrow's validity bitmap is the
complement of a "valid" set — `true` in the mask marks a null):

```nim test
import narrow

let arr  = @[1, 2, 3, 4, 5]
let mask = @[true, false, true, false, true] # true => null
let garr = newArray[int](arr, mask)

for i in 0 ..< arr.len:
  if mask[i]:
    echo "null"
  else:
    echo garr[i]
```

```
null
2
null
4
null
```

## Creating Tables

A table is a schema plus one `ChunkedArray` per field. Build the schema with
`newField[T]` declarations and pass matching arrays to `newArrowTable`:

```nim test
import narrow

let schema = newSchema([
  newField[int]("id"),
  newField[string]("name"),
  newField[float64]("value"),
])

let idArr    = newArray(@[1, 2, 3, 4, 5])
let nameArr  = newArray(@["a", "b", "c", "d", "e"])
let valueArr = newArray(@[1.0, 2.0, 3.0, 4.0, 5.0])

let table = newArrowTable(schema, idArr, nameArr, valueArr)
echo table.nRows
echo table.nColumns
echo table.schema
```

```
5
3
id: int64
name: string
value: double
```

## Create Table from Plain Types

For quick prototypes `newArrowTable` accepts a `seq` of tuples (or objects)
directly — the schema is inferred from the tuple fields:

```nim test
import narrow

let data = @[
  (col1: 1, col2: "a"),
  (col1: 2, col2: "b"),
  (col1: 3, col2: "c"),
  (col1: 4, col2: "d"),
  (col1: 5, col2: "e"),
]
let table = newArrowTable(data)
echo table.nRows
echo table.nColumns
echo table.schema
```

```
5
2
col1: int64
col2: string
```

## Creating Record Batches

A `RecordBatch` is a collection of equal-length arrays sharing a schema —
the unit of streaming data in Arrow. Build one with `newRecordBatch`:

```nim test
import narrow

let schema = newSchema([
  newField[int32]("x"),
  newField[float64]("y"),
])

let batch = newRecordBatch(schema,
  newArray(@[1'i32, 2, 3]),
  newArray(@[1.5'f64, 2.5, 3.5]))

echo batch.nRows
echo batch.nColumns
echo batch.schema
```

```
3
2
x: int32
y: double
```

## Store Categorical Data

> **TODO:** A high-level categorical / dictionary-encoded helper (à la
> `pyarrow.array(..., type=pa.dictionary(...))`) is not yet exposed as a
> one-liner in narrow. The building blocks exist — `DictionaryDataType` and
> `DictionaryArray` in `narrow/column/dictionary` — but a ergonomic wrapper
> that infers the dictionary from a `seq` is planned.
>
> Track progress in the issue tracker. Once landed, this recipe will show
> the idiomatic construction of a dictionary-encoded array and how to write
> it to Parquet as a categorical column.
