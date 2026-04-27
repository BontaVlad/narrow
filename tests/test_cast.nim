import std/[tables, strutils]
import unittest2
import ../src/narrow

suite "Array cast":
  test "cast int32 to int64":
    let arr = newArray(@[1'i32, 2, 3])
    let casted = castTo[int64](arr)
    # let result = castTo[int64](arr)
    check casted.len == 3
    check casted[0] == 1'i64
    check casted[1] == 2'i64
    check casted[2] == 3'i64

  test "cast int32 to float64":
    let arr = newArray(@[1'i32, 2, 3])
    let casted = castTo[float64](arr)
    check casted.len == 3
    check casted[0] == 1.0'f64
    check casted[1] == 2.0'f64
    check casted[2] == 3.0'f64

  test "cast float64 to int32 truncates":
    let arr = newArray(@[1.7'f64, 2.3, 3.9])
    var opts = newCastOptions()
    opts.allowFloatTruncate = true
    let casted = castTo[int32](arr, opts)
    check casted.len == 3
    check casted[0] == 1'i32
    check casted[1] == 2'i32
    check casted[2] == 3'i32

  test "cast int32 to string":
    let arr = newArray(@[1'i32, 2, 3])
    let casted = castTo[string](arr)
    check casted.len == 3
    check casted[0] == "1"
    check casted[1] == "2"
    check casted[2] == "3"

  test "cast bool to int32":
    let arr = newArray(@[true, false, true])
    let casted = castTo[int32](arr)
    check casted.len == 3
    check casted[0] == 1'i32
    check casted[1] == 0'i32
    check casted[2] == 1'i32

  test "cast empty array returns empty":
    let arr = newArray[int32](@[])
    let casted = castTo[int64](arr)
    check casted.len == 0

  test "cast single element":
    let arr = newArray(@[42'i32])
    let casted = castTo[float64](arr)
    check casted.len == 1
    check casted[0] == 42.0'f64

  test "cast bool to int32":
    let arr = newArray(@[true, false, true])
    let casted = toTyped[int32](castTo(arr, newGType(int32)))
    check casted.len == 3
    check casted[0] == 1'i32
    check casted[1] == 0'i32
    check casted[2] == 1'i32

  test "cast chunks of int32 to string":

    let chunks = [
      newArray(@[true, false]),
      newArray(@[true]),
      newArray(@[false, true, false]),
    ]
    let cArray = newChunkedArray(chunks)
    let casted = toTyped[string](castChunks(cArray, newGType(string)))
    check casted.nChunks == 3
    check casted.getChunk(0)[1] == "false"
    check casted.getChunk(1)[0] == "true"
    check casted.getChunk(2)[2] == "false"


suite "Table cast (hashmap)":
  test "cast single column":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let ids = newArray(@[1'i32, 2, 3])
    let names = newArray(@["a", "b", "c"])
    let table = newArrowTable(schema, ids, names)

    let result = castTable(table, [("id", newGType(int64))])
    check result.nColumns == 2
    check result.nRows == 3

    let idCol = result["id", int64]
    check idCol[0] == 1'i64
    check idCol[1] == 2'i64
    check idCol[2] == 3'i64

    let nameCol = result["name", string]
    check nameCol[0] == "a"
    check nameCol[1] == "b"
    check nameCol[2] == "c"

  test "cast multiple columns":
    let schema = newSchema([
      newField[int32]("id"),
      newField[float64]("score"),
      newField[string]("name"),
    ])
    let ids = newArray(@[1'i32, 2])
    let scores = newArray(@[95.5'f64, 87.2])
    let names = newArray(@["a", "b"])
    let table = newArrowTable(schema, ids, scores, names)

    let result = castTable(table, [
      ("id", newGType(int64)),
      ("score", newGType(float32)),
    ])

    check result.nColumns == 3
    check result.nRows == 2

    let idCol = result["id", int64]
    check idCol[0] == 1'i64

    let scoreCol = result["score", float32]
    check scoreCol[0] == 95.5'f32

    let nameCol = result["name", string]
    check nameCol[0] == "a"

  test "pass-through columns unchanged":
    let schema = newSchema([
      newField[string]("name"),
      newField[int32]("age"),
    ])
    let names = newArray(@["alice", "bob"])
    let ages = newArray(@[25'i32, 30])
    let table = newArrowTable(schema, names, ages)

    let result = castTable(table, [("age", newGType(int64))])

    let nameCol = result["name", string]
    check nameCol[0] == "alice"
    check nameCol[1] == "bob"

    let ageCol = result["age", int64]
    check ageCol[0] == 25'i64
    check ageCol[1] == 30'i64

  test "cast empty table":
    let schema = newSchema([newField[int32]("x")])
    let data = newArray[int32](@[])
    let table = newArrowTable(schema, data)

    let result = castTable(table, [("x", newGType(int64))])
    check result.nRows == 0
    check result.nColumns == 1

  test "cast with options":
    let schema = newSchema([newField[float64]("score")])
    let scores = newArray(@[1.7'f64, 2.3])
    let table = newArrowTable(schema, scores)

    var opts = newCastOptions()
    opts.allowFloatTruncate = true
    let result = castTable(table, [("score", newGType(int32))], opts)
    let col = result["score", int32]
    check col[0] == 1'i32
    check col[1] == 2'i32

  test "schema fields updated correctly":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
      newField[float64]("score"),
    ])
    let ids = newArray(@[1'i32, 2])
    let names = newArray(@["a", "b"])
    let scores = newArray(@[95.5'f64, 87.2])
    let table = newArrowTable(schema, ids, names, scores)

    let result = castTable(table, [
      ("id", newGType(int64)),
      ("score", newGType(float32)),
    ])

    # Casted columns should have new types in schema
    check result.schema.getField(0).dataType == newGType(int64)
    check result.schema.getField(2).dataType == newGType(float32)

    # Pass-through column should keep original type
    check result.schema.getField(1).dataType == newGType(string)

    # Verify data still accessible via updated schema
    let idCol = result["id", int64]
    check idCol[0] == 1'i64

    let scoreCol = result["score", float32]
    check scoreCol[0] == 95.5'f32

suite "Cast with nulls":
  test "cast preserves nulls":
    let arr = newArray(@[1'i32, 2, 3], mask = [false, true, false])
    let result = castTo[int64](arr)
    check result.len == 3
    check result.isValid(0) == true
    check result.isNull(1) == true
    check result.isValid(2) == true
    check result[0] == 1'i64
    check result[2] == 3'i64

suite "CastOptions":
  test "default CastOptions construction":
    let opts = newCastOptions()
    check not opts.handle.isNil

  test "allowIntOverflow getter/setter":
    var opts = newCastOptions()
    check opts.allowIntOverflow == false
    opts.allowIntOverflow = true
    check opts.allowIntOverflow == true

  test "toFunctionOptions conversion":
    var opts = newCastOptions()
    opts.allowIntOverflow = true
    let fnOpts = opts.toFunctionOptions()
    check not fnOpts.handle.isNil

suite "toFunctionOptions with compute kernel":
  test "call cast kernel via function registry":
    let arr = newDatum(newArray(@[1'i32, 2, 3]))
    var opts = newCastOptions()
    opts.toDataType = newGType(int64)
    opts.allowIntOverflow = true
    let result = call("cast", arr, options = opts.toFunctionOptions())
    check result.isArray
