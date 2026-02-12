import std/[strutils]
import unittest2
import ../src/narrow/[tabular/table, types/gtypes, column/primitive, column/primitive, column/metadata, tabular/batch]

suite "ArrowTable Construction Tests":

  test "newArrowTable from RecordBatches":
    # Create schema
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
      newField[float64]("value")
    ])
    
    # Create arrays for first batch
    let ids1 = @[1'i32, 2'i32, 3'i32]
    let names1 = @["Alice", "Bob", "Charlie"]
    let values1 = @[1.5, 2.5, 3.5]
    
    # Create arrays for second batch
    let ids2 = @[4'i32, 5'i32]
    let names2 = @["David", "Eve"]
    let values2 = @[4.5, 5.5]
    
    # Create record batches
    let rb1 = newRecordBatch(schema, ids1, names1, values1)
    let rb2 = newRecordBatch(schema, ids2, names2, values2)
    
    # Create table
    let table = newArrowTable(schema, rb1, rb2)
    
    check table.nColumns == 3
    check table.nRows == 5
    check table.schema == schema

  test "newArrowTable from ChunkedArrays":
    let schema = newSchema([
      newField[bool]("col1"),
      newField[string]("col2"),
    ])
    
    let chunks1 = [
      newArray(@[true, true, false]),
      newArray(@[true, false, true]),
    ]

    let chunks2 = [
      newArray(@["a", "b", "c"]),
      newArray(@["x", "y", "z"]),
    ]

    let chunkedArray1 = newChunkedArray(chunks1)
    let chunkedArray2 = newChunkedArray(chunks2)
    let table = newArrowTable(schema, chunkedArray1, chunkedArray2)
    
    check table.nColumns == 2
    check table.nRows == 6
    
    let col1 = table["col1", bool]
    check col1.nChunks == 2
    check col1.len == 6

  test "newArrowTable from Arrays":
    let schema = newSchema([
      newField[int32]("x"),
      newField[string]("y")
    ])
    
    let arr1 = newArray(@[1'i32, 2'i32, 3'i32])
    let arr2 = newArray(@["a", "b", "c"])
    
    let table = newArrowTable(schema, arr1, arr2)
    
    check table.nColumns == 2
    check table.nRows == 3

suite "ArrowTable Column Operations":

  test "add column":
    let schema = newSchema([newField[int32]("a")])
    let arr = newArray[int32](@[1'i32, 2'i32, 3'i32])
    let table = newArrowTable(schema, arr)
    
    let newField = newField[float64]("b")
    let newArr = newArray[float64](@[1.1, 2.2, 3.3])
    let newChunked = newChunkedArray([newArr])
    
    let table2 = table.addColumn(1, newField, newChunked)
    
    check table2.nColumns == 2
    check table2.schema.getField(1).name == "b"

  test "remove column by index":
    let schema = newSchema([
      newField[int32]("a"),
      newField[int32]("b"),
      newField[int32]("c")
    ])
    
    let arr1 = newArray[int32](@[1'i32, 2'i32])
    let arr2 = newArray[int32](@[3'i32, 4'i32])
    let arr3 = newArray[int32](@[5'i32, 6'i32])
    
    let table = newArrowTable(schema, arr1, arr2, arr3)
    let table2 = table.removeColumn(1)
    
    check table2.nColumns == 2
    check table2.schema.getField(0).name == "a"
    check table2.schema.getField(1).name == "c"

  test "remove column by name":
    let schema = newSchema([
      newField[int32]("x"),
      newField[int32]("y")
    ])
    
    let arr1 = newArray[int32](@[1'i32, 2'i32])
    let arr2 = newArray[int32](@[3'i32, 4'i32])
    
    let table = newArrowTable(schema, arr1, arr2)
    let table2 = table.removeColumn("y")
    
    check table2.nColumns == 1
    check table2.schema.getField(0).name == "x"

  test "remove non-existent column should fail":
    let schema = newSchema([newField[int32]("a")])
    let arr = newArray[int32](@[1'i32])
    let table = newArrowTable(schema, arr)
    
    expect(KeyError):
      discard table.removeColumn("nonexistent")

  test "replace column":
    let schema = newSchema([
      newField[int32]("a"),
      newField[int32]("b")
    ])
    
    let arr1 = newArray[int32](@[1'i32, 2'i32])
    let arr2 = newArray[int32](@[3'i32, 4'i32])
    let table = newArrowTable(schema, arr1, arr2)
    
    let newField = newField[float64]("b_new")
    let newArr = newArray[float64](@[3.3, 4.4])
    let newChunked = newChunkedArray([newArr])
    
    let table2 = table.replaceColumn(1, newField, newChunked)
    
    check table2.nColumns == 2
    check table2.schema.getField(1).name == "b_new"

suite "ArrowTable Equality Tests":
  test "equal tables":
    let schema = newSchema([newField[int32]("a")])
    let arr = newArray[int32](@[1'i32, 2'i32])
    
    let table1 = newArrowTable(schema, arr)
    let table2 = newArrowTable(schema, arr)
    
    check table1.equal(table2)

  test "unequal tables - different data":
    let schema = newSchema([newField[int32]("a")])
    let arr1 = newArray[int32](@[1'i32, 2'i32])
    let arr2 = newArray[int32](@[3'i32, 4'i32])
    
    let table1 = newArrowTable(schema, arr1)
    let table2 = newArrowTable(schema, arr2)
    
    check not table1.equal(table2)

  test "equal metadata":
    let schema = newSchema([newField[int32]("a")])
    let arr = newArray[int32](@[1'i32, 2'i32])
    
    let table1 = newArrowTable(schema, arr)
    let table2 = newArrowTable(schema, arr)
    
    check table1.equalMetadata(table2, true)
    check table1.equalMetadata(table2, false)

suite "ArrowTable Slicing Tests":
  test "slice middle portion":
    let schema = newSchema([newField[int32]("a")])
    let arr = newArray[int32](@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32])
    let table = newArrowTable(schema, arr)
    
    let sliced = table.slice(1, 3)
    
    check sliced.nRows == 3
    check sliced.nColumns == 1

  test "slice from start":
    let schema = newSchema([newField[int32]("a")])
    let arr = newArray[int32](@[1'i32, 2'i32, 3'i32])
    let table = newArrowTable(schema, arr)
    
    let sliced = table.slice(0, 2)
    
    check sliced.nRows == 2

  test "slice to end":
    let schema = newSchema([newField[int32]("a")])
    let arr = newArray[int32](@[1'i32, 2'i32, 3'i32, 4'i32])
    let table = newArrowTable(schema, arr)
    
    let sliced = table.slice(2, 2)
    
    check sliced.nRows == 2

suite "ArrowTable Combination Tests":
  test "combine chunks":
    let schema = newSchema([newField[int32]("a")])
    
    let arr1 = newArray[int32](@[1'i32, 2'i32])
    let arr2 = newArray[int32](@[3'i32, 4'i32])
    let chunked = newChunkedArray([arr1, arr2])
    
    let table = newArrowTable(schema, chunked)
    check table["a", int32].nChunks == 2
    
    let combined = table.combineChunks()
    check combined["a", int32].nChunks == 1
    check combined.nRows == 4

  test "concatenate tables":
    let schema = newSchema([newField[int32]("a")])
    
    let arr1 = newArray[int32](@[1'i32, 2'i32])
    let arr2 = newArray[int32](@[3'i32, 4'i32])
    let arr3 = newArray[int32](@[5'i32, 6'i32])
    
    let table1 = newArrowTable(schema, arr1)
    let table2 = newArrowTable(schema, arr2)
    let table3 = newArrowTable(schema, arr3)
    
    let concatenated = table1.concatenate([table2, table3])
    
    check concatenated.nRows == 6
    check concatenated.nColumns == 1

suite "ArrowTable Validation Tests":
  test "validate valid table":
    let schema = newSchema([newField[int32]("a")])
    let arr = newArray[int32](@[1'i32, 2'i32])
    let table = newArrowTable(schema, arr)
    
    check table.validate()
    check table.validateFull()

  test "validate after operations":
    let schema = newSchema([newField[int32]("a")])
    let arr = newArray[int32](@[1'i32, 2'i32, 3'i32])
    let table = newArrowTable(schema, arr)
    
    let sliced = table.slice(0, 2)
    check sliced.validate()
    
    let combined = table.combineChunks()
    check combined.validate()

suite "ArrowTable Access Tests":

  test "access column by index":
    let schema = newSchema([
      newField[int32]("a"),
      newField[int32]("b")
    ])
    
    let arr1 = newArray[int32](@[1'i32, 2'i32])
    let arr2 = newArray[int32](@[3'i32, 4'i32])
    let table = newArrowTable(schema, arr1, arr2)
    
    let col0 = table.getColumnData[:int32](0)
    let col1 = table[1, int32]
    let col00 = table["a", int32]
    
    check len(col0) == 2
    check len(col1) == 2
    check col0 == col00

  test "access non-existent column should fail":
    let schema = newSchema([newField[int32]("a")])
    let arr = newArray[int32](@[1'i32])
    let table = newArrowTable(schema, arr)
    
    expect(KeyError):
      discard table["nonexistent", int32]

  test "iterate keys":
    let schema = newSchema([
      newField[int32]("col1"),
      newField[int32]("col2"),
      newField[int32]("col3")
    ])
    
    let arr1 = newArray[int32](@[1'i32])
    let arr2 = newArray[int32](@[2'i32])
    let arr3 = newArray[int32](@[3'i32])
    let table = newArrowTable(schema, arr1, arr2, arr3)
    
    var keys: seq[string]
    for key in table.keys:
      keys.add(key)
    
    check keys == @["col1", "col2", "col3"]

suite "ArrowTable String Representation":
  test "toString":
    let schema = newSchema([newField[int32]("id")])
    let arr = newArray[int32](@[1'i32, 2'i32, 3'i32])
    let table = newArrowTable(schema, arr)
    
    let str = $table
    check str.len > 0
    check "id" in str
