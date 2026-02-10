## Simple API Compliance Test
import std/options
import unittest2
import ../src/[ffi, garray, gchunkedarray, gschema, grecordbatch, gtables, gstruct, gmaparray]

suite "API Standardization Verification":
  
  test "Array has consistent API":
    let arr = newArray(@[1, 2, 3])
    check:
      arr.len == 3
      arr.nNulls == 0
      arr.isValid(0) == true
      arr.isNull(0) == false
      arr.tryGet(0).isSome == true
      arr.tryGet(0).get() == 1
  
  test "ChunkedArray has consistent API":
    let chunked = newChunkedArray([newArray(@[1, 2]), newArray(@[3, 4, 5])])
    check:
      chunked.len == 5
      chunked.nRows == 5
      chunked.nNulls == 0
      chunked.nChunks == 2
      chunked.isValid(0) == true
      chunked.tryGet(0).isSome == true
  
  test "Schema has consistent API":
    let schema = newSchema([newField[int]("a"), newField[string]("b")])
    check:
      schema.len == 2
      schema.nFields == 2
      schema[0].name == "a"
      schema["b"].name == "b"
      schema.tryGetField(0).isSome == true
      schema.tryGetField(10).isNone == true
      schema.tryGetField("a").isSome == true
      schema.tryGetField("z").isNone == true
  
  test "RecordBatch has row-level access":
    let schema = newSchema([newField[int]("col1")])
    let rb = newRecordBatch(schema, newArray(@[1, 2, 3]))
    check:
      rb.nRows == 3
      rb.nColumns == 1
      rb.isValid(0, 0) == true
      rb.isNull(0, 0) == false
      rb.nNulls == 0
    
    var count = 0
    for row in rb:
      count += 1
    check count == 3
  
  test "ArrowTable has row-level access":
    let schema = newSchema([newField[int]("col1")])
    let table = newArrowTable(schema, newArray(@[1, 2, 3]))
    check:
      table.nRows == 3
      table.nColumns == 1
      table.isValid(0, 0) == true
      table.isNull(0, 0) == false
      table.nNulls == 0
    
    var count = 0
    for row in table:
      count += 1
    check count == 3
  
  test "StructArray has full API":
    let structType = newStruct([newField[int]("x"), newField[string]("y")])
    let builder = newStructBuilder(structType)
    builder.append()
    builder.append()
    let sa = builder.finish()
    
    check:
      sa.len == 2
      sa.nNulls == 0
      sa.isValid(0) == true
      sa.isNull(0) == false
  
  test "MapArray has full API":
    let offsets = newArray(@[0'i32, 2])
    let keys = newArray(@["a", "b"])
    let values = newArray(@[1, 2])
    let mapArr = newMapArray(offsets, keys, values)
    
    check:
      mapArr.len == 1
      mapArr.nNulls == 0
      mapArr.isValid(0) == true
      mapArr.isNull(0) == false
