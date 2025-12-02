import std/[strutils]
import unittest2
import ../src/[gtables, gtypes, garray, gchunkedarray, gschema, grecordbatch]

suite "RecordBatchBuilder":
  test "Create RecordBatchBuilder default":
    let field1 = newField[bool]("visible")
    let field2 = newField[int32]("point")
    let schema = newSchema(@[field1, field2])
    let builder = newRecordBatchBuilder(schema)
    check builder.capacity == 32

  test "Create RecordBatchBuilder default":
    let field1 = newField[bool]("visible")
    let field2 = newField[int32]("point")
    let schema = newSchema(@[field1, field2])
    let builder = newRecordBatchBuilder(schema, 128)
    check builder.capacity == 128

  test "Add values to builder":

    let field1 = newField[bool]("visible")
    let field2 = newField[int32]("point")
    let schema = newSchema(@[field1, field2])
    let builder = newRecordBatchBuilder(schema)
    builder.columnBuilder(bool, 0).appendValues(newSeq[bool]())
    builder.columnBuilder(int32, 1).appendValues(newSeq[int32]())
    let record = builder.flush()
    check record.nColumns == 2
    check record.nRows == 0


suite "Field - Basic Operations":
  
  test "Create and destroy field":
    let field = newField[int32]("test_field")
    check field.name == "test_field"
  
  test "Field equality":
    let field1 = newField[int32]("col1")
    let field2 = newField[int32]("col1")
    let field3 = newField[float64]("col1")
    
    check field1 == field2
    check field1 != field3
  
  test "Field to string":
    let field = newField[int32]("age")
    let str = $field
    check str.len > 0
    check "age" in str
  
  test "Field copying":
    let original = newField[int32]("original")
    let copy = original
    check copy.name == "original"
    check copy == original

suite "Schema - Basic Operations":
  
  test "Create schema from fields":
    let ffields = [
      newField[int32]("id"),
      newField[string]("name"),
      newField[float64]("value")
    ]
    
    let schema = newSchema(ffields)
    check schema.nFields == 3
  
  test "Schema field access":
    let schema = newSchema([
      newField[int32]("a"),
      newField[int32]("b"),
      newField[int32]("c")
    ])
    
    check schema.getField(0).name == "a"
    check schema.getField(1).name == "b"
    check schema.getField(2).name == "c"
  
  test "Schema field by name":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name")
    ])
    
    let field = schema.getFieldByName("name")
    check field.name == "name"
  
  test "Schema field index":
    let schema = newSchema([
      newField[int32]("x"),
      newField[int32]("y"),
      newField[int32]("z")
    ])
    
    check schema.getFieldIndex("x") == 0
    check schema.getFieldIndex("y") == 1
    check schema.getFieldIndex("z") == 2
    check schema.getFieldIndex("missing") < 0
  
  test "Schema iteration":
    let schema = newSchema([
      newField[int32]("a"),
      newField[int32]("b"),
      newField[int32]("c")
    ])
    
    var names: seq[string]
    for field in schema:
      names.add(field.name)
    
    check names == @["a", "b", "c"]
  
  test "Schema to string":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name")
    ])
    
    let str = $schema
    check str.len > 0
  
  test "Schema equality":
    let schema1 = newSchema([newField[int32]("a"), newField[int32]("b")])
    let schema2 = newSchema([newField[int32]("a"), newField[int32]("b")])
    let schema3 = newSchema([newField[int32]("a"), newField[float64]("b")])
    
    check schema1 == schema2
    check schema1 != schema3
  
  test "Empty schema":
    let schema = newSchema([])
    check schema.nFields == 0

suite "Field - Memory Tests":
  
  test "Create many fields":
    for i in 0..1000:
      let field = newField[int32]("field_" & $i)
  
  test "Field copy chains":
    let original = newField[int32]("original")
    for i in 0..100:
      let copy1 = original
      let copy2 = copy1
      let copy3 = copy2
      check copy3.name == "original"
  
  test "Field string conversion stress":
    let field = newField[int32]("test")
    for i in 0..1000:
      let str = $field
      check str.len > 0

suite "Schema - Memory Tests":
  
  test "Create many schemas":
    for i in 0..1000:
      let schema = newSchema([
        newField[int32]("a"),
        newField[int32]("b")
      ])
      check schema.nFields == 2
  
  test "Schema with many fields":
    var fields: seq[Field]
    for i in 0..99:
      fields.add(newField[int32]("col_" & $i))
    
    let schema = newSchema(fields)
    check schema.nFields == 100
  
  test "Schema field iteration stress":
    let schema = newSchema([
      newField[int32]("a"),
      newField[int32]("b"),
      newField[int32]("c")
    ])
    
    for cycle in 0..1000:
      var count = 0
      for field in schema:
        count.inc
      check count == 3
  
  test "Schema copying":
    let original = newSchema([
      newField[int32]("x"),
      newField[int32]("y")
    ])
    
    for i in 0..1000:
      let copy = original
      check copy.nFields == 2
  
  test "Schema field access stress":
    let schema = newSchema([
      newField[int32]("a"),
      newField[int32]("b"),
      newField[int32]("c"),
      newField[int32]("d"),
      newField[int32]("e")
    ])
    
    for i in 0..1000:
      for j in 0..4:
        let field = schema.getField(j)

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
    let table = newArrowTable(schema, [rb1, rb2])
    
    check table.nColumns == 3
    check table.nRows == 5
    check table.schema == schema

  # test "newArrowTable from ChunkedArrays":
  #   let schema = newSchema([
  #     newField[int32]("col1"),
  #     newField[int32]("col2")
  #   ])
    
  #   # Create chunked arrays
  #   let arr1_1 = newArray[int32](@[1'i32, 2'i32])
  #   let arr1_2 = newArray[int32](@[3'i32, 4'i32])
  #   let chunked1 = newChunkedArray([arr1_1, arr1_2])
    
  #   let arr2_1 = newArray[int32](@[10'i32, 20'i32])
  #   let arr2_2 = newArray[int32](@[30'i32, 40'i32])
  #   let chunked2 = newChunkedArray([arr2_1, arr2_2])
    
  #   let table = newArrowTable(schema, [chunked1, chunked2])
    
    # check table.isValid
    # check table.nColumns == 2
    # check table.nRows == 4
    
    # Verify data
    # let col1 = table["col1"]
    # check col1.nChunks == 2
    # check col1.len == 4

  # test "newArrowTable from Arrays":
  #   let schema = newSchema([
  #     newField[int32]("x"),
  #     newField[int32]("y")
  #   ])
    
  #   let arr1 = newArray[int32](@[1'i32, 2'i32, 3'i32])
  #   let arr2 = newArray[int32](@[4'i32, 5'i32, 6'i32])
    
  #   let table = newArrowTable(schema, [arr1, arr2])
    
  #   check table.isValid
  #   check table.nColumns == 2
  #   check table.nRows == 3

  # test "empty array construction should fail":
  #   let schema = newSchema([newField[int32]("col")])
    
  #   expect(ValueError):
  #     discard newArrowTable(schema, newSeq[RecordBatch]())
    
  #   expect(ValueError):
  #     discard newArrowTable(schema, newSeq[ChunkedArray]())
    
  #   expect(ValueError):
  #     discard newArrowTable(schema, newSeq[ArrowArray]())

# suite "ArrowTable Column Operations":
#   test "add column":
#     let schema = newSchema([newField[int32]("a")])
#     let arr = newArray[int32](@[1'i32, 2'i32, 3'i32])
#     let table = newArrowTable(schema, [arr])
    
#     let newField = newField[float64]("b")
#     let newArr = newArray[float64](@[1.1, 2.2, 3.3])
#     let newChunked = newChunkedArray([newArr])
    
#     let table2 = table.addColumn(1, newField, newChunked)
    
#     check table2.nColumns == 2
#     check table2.schema.getField(1).name == "b"

#   test "remove column by index":
#     let schema = newSchema([
#       newField[int32]("a"),
#       newField[int32]("b"),
#       newField[int32]("c")
#     ])
    
#     let arr1 = newArray[int32](@[1'i32, 2'i32])
#     let arr2 = newArray[int32](@[3'i32, 4'i32])
#     let arr3 = newArray[int32](@[5'i32, 6'i32])
    
#     let table = newArrowTable(schema, [arr1, arr2, arr3])
#     let table2 = table.removeColumn(1)
    
#     check table2.nColumns == 2
#     check table2.schema.getField(0).name == "a"
#     check table2.schema.getField(1).name == "c"

#   test "remove column by name":
#     let schema = newSchema([
#       newField[int32]("x"),
#       newField[int32]("y")
#     ])
    
#     let arr1 = newArray[int32](@[1'i32, 2'i32])
#     let arr2 = newArray[int32](@[3'i32, 4'i32])
    
#     let table = newArrowTable(schema, [arr1, arr2])
#     let table2 = table.removeColumn("y")
    
#     check table2.nColumns == 1
#     check table2.schema.getField(0).name == "x"

#   test "remove non-existent column should fail":
#     let schema = newSchema([newField[int32]("a")])
#     let arr = newArray[int32](@[1'i32])
#     let table = newArrowTable(schema, [arr])
    
#     expect(KeyError):
#       discard table.removeColumn("nonexistent")

#   test "replace column":
#     let schema = newSchema([
#       newField[int32]("a"),
#       newField[int32]("b")
#     ])
    
#     let arr1 = newArray[int32](@[1'i32, 2'i32])
#     let arr2 = newArray[int32](@[3'i32, 4'i32])
#     let table = newArrowTable(schema, [arr1, arr2])
    
#     let newField = newField[float64]("b_new")
#     let newArr = newArray[float64](@[3.3, 4.4])
#     let newChunked = newChunkedArray([newArr])
    
#     let table2 = table.replaceColumn(1, newField, newChunked)
    
#     check table2.nColumns == 2
#     check table2.schema.getField(1).name == "b_new"

# suite "ArrowTable Equality Tests":
#   test "equal tables":
#     let schema = newSchema([newField[int32]("a")])
#     let arr = newArray[int32](@[1'i32, 2'i32])
    
#     let table1 = newArrowTable(schema, [arr])
#     let table2 = newArrowTable(schema, [arr])
    
#     check table1.equal(table2)

#   test "unequal tables - different data":
#     let schema = newSchema([newField[int32]("a")])
#     let arr1 = newArray[int32](@[1'i32, 2'i32])
#     let arr2 = newArray[int32](@[3'i32, 4'i32])
    
#     let table1 = newArrowTable(schema, [arr1])
#     let table2 = newArrowTable(schema, [arr2])
    
#     check not table1.equal(table2)

#   test "equal metadata":
#     let schema = newSchema([newField[int32]("a")])
#     let arr = newArray[int32](@[1'i32, 2'i32])
    
#     let table1 = newArrowTable(schema, [arr])
#     let table2 = newArrowTable(schema, [arr])
    
#     check table1.equalMetadata(table2, true)
#     check table1.equalMetadata(table2, false)

# suite "ArrowTable Slicing Tests":
#   test "slice middle portion":
#     let schema = newSchema([newField[int32]("a")])
#     let arr = newArray[int32](@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32])
#     let table = newArrowTable(schema, [arr])
    
#     let sliced = table.slice(1, 3)
    
#     check sliced.nRows == 3
#     check sliced.nColumns == 1

#   test "slice from start":
#     let schema = newSchema([newField[int32]("a")])
#     let arr = newArray[int32](@[1'i32, 2'i32, 3'i32])
#     let table = newArrowTable(schema, [arr])
    
#     let sliced = table.slice(0, 2)
    
#     check sliced.nRows == 2

#   test "slice to end":
#     let schema = newSchema([newField[int32]("a")])
#     let arr = newArray[int32](@[1'i32, 2'i32, 3'i32, 4'i32])
#     let table = newArrowTable(schema, [arr])
    
#     let sliced = table.slice(2, 2)
    
#     check sliced.nRows == 2

# suite "ArrowTable Combination Tests":
#   test "combine chunks":
#     let schema = newSchema([newField[int32]("a")])
    
#     let arr1 = newArray[int32](@[1'i32, 2'i32])
#     let arr2 = newArray[int32](@[3'i32, 4'i32])
#     let chunked = newChunkedArray([arr1, arr2])
    
#     let table = newArrowTable(schema, [chunked])
#     check table["a"].nChunks == 2
    
#     let combined = table.combineChunks()
#     check combined["a"].nChunks == 1
#     check combined.nRows == 4

#   test "concatenate tables":
#     let schema = newSchema([newField[int32]("a")])
    
#     let arr1 = newArray[int32](@[1'i32, 2'i32])
#     let arr2 = newArray[int32](@[3'i32, 4'i32])
#     let arr3 = newArray[int32](@[5'i32, 6'i32])
    
#     let table1 = newArrowTable(schema, [arr1])
#     let table2 = newArrowTable(schema, [arr2])
#     let table3 = newArrowTable(schema, [arr3])
    
#     let concatenated = table1.concatenate([table2, table3])
    
#     check concatenated.nRows == 6
#     check concatenated.nColumns == 1

# suite "ArrowTable Validation Tests":
#   test "validate valid table":
#     let schema = newSchema([newField[int32]("a")])
#     let arr = newArray[int32](@[1'i32, 2'i32])
#     let table = newArrowTable(schema, [arr])
    
#     check table.validate()
#     check table.validateFull()

#   test "validate after operations":
#     let schema = newSchema([newField[int32]("a")])
#     let arr = newArray[int32](@[1'i32, 2'i32, 3'i32])
#     let table = newArrowTable(schema, [arr])
    
#     let sliced = table.slice(0, 2)
#     check sliced.validate()
    
#     let combined = table.combineChunks()
#     check combined.validate()

# suite "ArrowTable Access Tests":
#   test "access column by index":
#     let schema = newSchema([
#       newField[int32]("a"),
#       newField[int32]("b")
#     ])
    
#     let arr1 = newArray[int32](@[1'i32, 2'i32])
#     let arr2 = newArray[int32](@[3'i32, 4'i32])
#     let table = newArrowTable(schema, [arr1, arr2])
    
#     let col0 = table[0]
#     let col1 = table[1]
    
#     check col0.length == 2
#     check col1.length == 2

#   test "access column by name":
#     let schema = newSchema([
#       newField[int32]("x"),
#       newField[int32]("y")
#     ])
    
#     let arr1 = newArray[int32](@[1'i32, 2'i32])
#     let arr2 = newArray[int32](@[3'i32, 4'i32])
#     let table = newArrowTable(schema, [arr1, arr2])
    
#     let colX = table["x"]
#     let colY = table["y"]
    
#     check colX.length == 2
#     check colY.length == 2

#   test "access non-existent column should fail":
#     let schema = newSchema([newField[int32]("a")])
#     let arr = newArray[int32](@[1'i32])
#     let table = newArrowTable(schema, [arr])
    
#     expect(KeyError):
#       discard table["nonexistent"]

#   test "iterate keys":
#     let schema = newSchema([
#       newField[int32]("col1"),
#       newField[int32]("col2"),
#       newField[int32]("col3")
#     ])
    
#     let arr1 = newArray[int32](@[1'i32])
#     let arr2 = newArray[int32](@[2'i32])
#     let arr3 = newArray[int32](@[3'i32])
#     let table = newArrowTable(schema, [arr1, arr2, arr3])
    
#     var keys: seq[string]
#     for key in table.keys:
#       keys.add(key)
    
#     check keys == @["col1", "col2", "col3"]

#   test "iterate columns":
#     let schema = newSchema([
#       newField[int32]("a"),
#       newField[int32]("b")
#     ])
    
#     let arr1 = newArray[int32](@[1'i32, 2'i32])
#     let arr2 = newArray[int32](@[3'i32, 4'i32])
#     let table = newArrowTable(schema, [arr1, arr2])
    
#     var count = 0
#     for (name, column) in table.columns:
#       check name in @["a", "b"]
#       check column.length == 2
#       inc count
    
#     check count == 2

# suite "ArrowTable String Representation":
#   test "toString":
#     let schema = newSchema([newField[int32]("id")])
#     let arr = newArray[int32](@[1'i32, 2'i32, 3'i32])
#     let table = newArrowTable(schema, [arr])
    
#     let str = $table
#     check str.len > 0
#     check "id" in str

# suite "Memory Management Tests":
#   test "table lifecycle":
#     for i in 0..<100:
#       let schema = newSchema([newField[int32]("a")])
#       let arr = newArray[int32](@[1'i32, 2'i32, 3'i32])
#       let table = newArrowTable(schema, [arr])
#       check table.isValid
    
#     # If we reach here without crashes, memory management is working

#   test "multiple references":
#     let schema = newSchema([newField[int32]("a")])
#     let arr = newArray[int32](@[1'i32, 2'i32])
#     let table1 = newArrowTable(schema, [arr])
    
#     var table2 = table1  # Copy
#     check table2.isValid
#     check table1.equal(table2)

#   test "column access doesn't leak":
#     let schema = newSchema([newField[int32]("a")])
#     let arr = newArray[int32](@[1'i32, 2'i32, 3'i32])
#     let table = newArrowTable(schema, [arr])
    
#     for i in 0..<100:
#       let col = table["a"]
#       check col.length == 3

# suite "Complex Type Tests":
#   test "table with multiple types":
#     let schema = newSchema([
#       newField[int32]("int_col"),
#       newField[float64]("float_col"),
#       newField[string]("string_col"),
#       newField[bool]("bool_col")
#     ])
    
#     let intArr = newArray[int32](@[1'i32, 2'i32])
#     let floatArr = newArray[float64](@[1.5, 2.5])
#     let stringArr = newArray[string](@["a", "b"])
#     let boolArr = newArray[bool](@[true, false])
    
#     let table = newArrowTable(schema, [intArr, floatArr, stringArr, boolArr])
    
#     check table.nColumns == 4
#     check table.nRows == 2
    
#     check table["int_col"].length == 2
#     check table["float_col"].length == 2
#     check table["string_col"].length == 2
#     check table["bool_col"].length == 2
