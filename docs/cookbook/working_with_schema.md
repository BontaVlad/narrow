# Working with Schema

Recipes for inspecting and changing the schema of Arrow arrays and tables
with [narrow](https://github.com/BontaVlad/narrow), a Nim wrapper over
Apache Arrow.

.. contents::
----

## Setting the data type of an Arrow Array

Use `castTo` to produce a new array with a different data type. The typed
overload `castTo[T]` returns an `Array[T]`:

```nim test
import narrow

let ints = newArray(@[1'i32, 2, 3, 4, 5])
let doubles = castTo[float64](ints)
echo doubles[0]
echo doubles[4]
```

```
1.0
5.0
```

## Setting the schema of a Table

A table's schema is set at construction time via `newSchema` + `newArrowTable`.
To change the schema of an existing table without touching the data (for
example to rename a field), build a new schema with `replaceField` and
recreate the table from the existing columns:

```nim test
import narrow

let schema = newSchema([
  newField[int32]("a"),
  newField[string]("b"),
])
let table = newArrowTable(schema,
  newArray(@[1'i32, 2, 3]),
  newArray(@["x", "y", "z"]))

let renamed = schema.replaceField(1, newField[string]("label"))
let rebuilt = newArrowTable(renamed, table[0], table[1])
echo rebuilt.schema
```

```
a: int32
label: string
```

## Merging multiple schemas

> **TODO:** narrow does not yet expose a `mergeSchemas` helper that unifies
> several schemas into one (resolving overlaps, preserving field order, and
> concatenating metadata). The underlying `garrow_schema_*` FFI surface is
> available but the ergonomic Nim wrapper is planned.
>
> Track progress in the issue tracker. Once landed, this recipe will show
> how to combine schemas from multiple tables — for example to build a
> single unified schema before concatenating batches with disjoint columns.
