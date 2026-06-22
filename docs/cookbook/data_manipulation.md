# Data Manipulation

Recipes for transforming, combining, and analysing Arrow data with
[narrow](https://github.com/BontaVlad/narrow), a Nim wrapper over Apache
Arrow.

.. contents::
----

## Computing Mean / Min / Max values of an array

`mean` returns the arithmetic mean of any numeric array. For min and max,
sort the array by index and take the first / last element — Arrow's compute
kernels handle nulls and NaNs correctly:

```nim test
import narrow

let arr = newArray(@[5'i32, 2, 8, 1, 9, 3])
echo mean(arr)

let idx = sortIndices(arr)
let sorted = take(arr, idx)
echo sorted[0]              # min
echo sorted[sorted.len - 1] # max
```

```
4.666666666666667
1
9
```

## Counting Occurrences of Elements

`countValues` returns a `StructArray` with two child arrays — `values` and
`counts` — one row per distinct value:

```nim test
import narrow

var nums: seq[int32]
for i in 0 ..< 100:
  nums.add((i mod 10).int32)
let arr = newArray(nums)
let cv = countValues(arr)
echo cv.len
```

```
10
```

## Applying arithmetic functions to arrays

Element-wise arithmetic is available via `multiply`, `subtract`, `divide`,
`add`, etc. They accept either two arrays or an array and a scalar, and
return a `Datum`:

```nim test
import std/sequtils
import narrow

let arr = newArray(sequtils.toSeq(0 .. 99))
let doubled = multiply(arr, 2)
let asArr = castTo[int](doubled.toArray())
echo asArr[0]
echo asArr[99]
```

```
0
198
```

## Appending tables to an existing table

`concatenate` vertically stacks tables that share a schema, returning a new
table — the original is left untouched:

```nim test
import narrow

let schema = newSchema([newField[int32]("id")])

let t1 = newArrowTable(schema, newArray(@[1'i32, 2, 3]))
let t2 = newArrowTable(schema, newArray(@[4'i32, 5]))
let t3 = newArrowTable(schema, newArray(@[6'i32, 7, 8, 9]))

let combined = t1.concatenate([t2, t3])
echo combined.nRows
```

```
9
```

## Adding a column to an existing Table

`addColumn` returns a new table with a column inserted at the given index —
the original table is not mutated:

```nim test
import narrow

let schema = newSchema([newField[int32]("id")])
let table  = newArrowTable(schema, newArray(@[1'i32, 2, 3]))

let extra = newChunkedArray(@[newArray(@[10'i32, 20, 30])])
let withCol = table.addColumn(table.nColumns,
  newField[int32]("value"), extra)
echo withCol.nColumns
echo withCol.schema
```

```
2
id: int32
value: int32
```

## Replacing a column in an existing Table

`replaceColumn` swaps the column at a given index for a new field + data,
returning a new table:

```nim test
import narrow

let schema = newSchema([newField[int32]("id"), newField[string]("name")])
let table  = newArrowTable(schema,
  newArray(@[1'i32, 2, 3]),
  newArray(@["a", "b", "c"]))

let newNames = newChunkedArray(@[newArray(@["x", "y", "z"])])
let replaced = table.replaceColumn(1, newField[string]("name"), newNames)
echo replaced[1, string][0]
echo replaced[1, string][2]
```

```
x
z
```

## Group a Table

`aggregateTable` runs aggregations grouped by one or more key columns,
returning one row per group with the aggregated values. Use the
`(field, fn, output)` triple form:

```nim test
import narrow

let schema = newSchema([
  newField[string]("category"),
  newField[int32]("amount"),
])
let table = newArrowTable(schema,
  newArray(@["a", "b", "a", "b", "a"]),
  newArray(@[1'i32, 2, 3, 4, 5]))

let result = aggregateTable(table, @["category"], @[
  (field: "amount", fn: "sum", output: "total"),
  (field: "amount", fn: "count", output: "n"),
])
echo result.nRows
echo result.nColumns
```

```
2
3
```

## Sort a Table

`sortBy` returns a new table sorted by one or more columns. Pass column
name / `SortOrder` pairs:

```nim test
import narrow

let schema = newSchema([
  newField[string]("name"),
  newField[int32]("age"),
])
let table = newArrowTable(schema,
  newArray(@["carol", "alice", "bob"]),
  newArray(@[30'i32, 25, 35]))

let byAge = table.sortBy([("age", Ascending)])
echo byAge[0, string][0]
echo byAge[0, string][1]
echo byAge[0, string][2]
```

```
alice
carol
bob
```

## Searching for values matching a predicate in Arrays

Build a comparison with `greater` / `less` / `equal`, materialise the result
as a `BooleanArray`, and `filter` the original array with it:

```nim test
import std/sequtils
import narrow

let arr = newArray(sequtils.toSeq(0 ..< 10))
let mask = toTyped[bool](greater(arr, 5).toArray())
let filtered = arr.filter(newBooleanArray(mask))
echo filtered.len
echo filtered[0]
echo filtered[filtered.len - 1]
```

```
4
6
9
```

## Filtering Arrays using a mask

Any `BooleanArray` of the right length can be used as a filter mask — only
elements where the mask is `true` are kept:

```nim test
import narrow

let arr = newArray(@[1'i32, 2, 3, 4])
let mask = toTyped[bool](equal(arr, 2).toArray())
let filtered = arr.filter(newBooleanArray(mask))
echo filtered.len
echo filtered[0]
```

```
1
2
```
