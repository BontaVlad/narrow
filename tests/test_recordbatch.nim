import std/options
import unittest2
import ../src/narrow/[core/ffi, column/primitive, column/metadata, tabular/batch,
    types/gtypes, compute/sorting, compute/filters]

suite "RecordBatch - Construction":
  
  test "Create from schema and arrays":
    let schema = newSchema([
      newField[int32]("col1"),
      newField["string"]("col2")
    ])

    let seq1 = @[1'i32, 2'i32, 3'i32]
    let seq2 = @["a", "b", "c"]
    
    let arr1 = newArray(@[1'i32, 2'i32, 3'i32])
    let arr2 = newArray(@["a", "b", "c"])
    
    let rb1 = newRecordBatch(schema, seq1, seq2)
    let rb2 = newRecordBatch(schema, arr1, arr2)
    
    check rb1.nRows() == 3
    check rb1.nColumns() == 2

    check rb2.nRows() == 3
    check rb2.nColumns() == 2

    check rb1 == rb2
  
  test "Create empty record batch":
    let schema = newSchema([
      newField[int16]("col1")
    ])
    
    let builder = newRecordBatchBuilder(schema)
    let rb = builder.flush()
    
    check rb.nRows() == 0
    check rb.nColumns() == 1
  
  test "Create with initial capacity":
    let schema = newSchema([
      newField[bool]("col1")
    ])
    
    let builder = newRecordBatchBuilder(schema, 100)
    check builder.capacity() == 100

suite "RecordBatch - Schema and Metadata":
  
  test "Get schema":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name")
    ])
    
    let arr1 = newArray(@[1'i32, 2'i32])
    let arr2 = newArray(@["Alice", "Bob"])
    
    let rb = newRecordBatch(schema, arr1, arr2)
    let retrievedSchema = rb.schema()
    
    check retrievedSchema.nFields() == 2
    check retrievedSchema.getField(0).name() == "id"
    check retrievedSchema.getField(1).name() == "name"

    check rb.getColumnName(0) == "id"
    check rb.getColumnName(1) == "name"
    check rb.nColumns() == 2
    check rb.nRows() == 2
  
suite "RecordBatch - Column Access":
  
  test "Get column data by index":
    let schema = newSchema([
      newField[int64]("values")
    ])
    
    let arr = @[10'i64, 20'i64, 30'i64]
    let rb = newRecordBatch(schema, arr)
    
    let col = rb.getColumnData[:int64](0)
    check col.len() == 3
    check col[0] == 10'i64
    check col[1] == 20'i64
    check col[2] == 30'i64
  
  test "Get column data by name":
    let schema = newSchema([
      newField[string]("first"),
      newField[int32]("second")
    ])
    
    let arr1 = @["hello", "world"]
    let arr2 = @[100'i32, 200'i32]
    
    let rb = newRecordBatch(schema, arr1, arr2)
    
    let col = rb["second", int32]
    let col2 = rb[1, int32]
    check col.len() == 2
    check col[0] == 100'i32
    check col[1] == 200'i32
    check col == col2
  
  test "Access non-existent column raises error":
    let schema = newSchema([
      newField[int32]("exists"),
      newField[int32]("a"),
      newField[int32]("b"),
      newField[int32]("c"),
    ])
    
    let arr = newArray[int32](@[1'i32])
    let arr1 = newArray[int32](@[1'i32])
    let arr2 = newArray[int32](@[1'i32])
    let arr3 = newArray[int32](@[1'i32])
    let rb = newRecordBatch(schema, arr, arr1, arr2, arr3)
    
    expect KeyError:
      discard rb["nonexistant", int32]

suite "RecordBatch - Equality":
  
  test "Equal record batches":
    let schema = newSchema([
      newField[int32]("col")
    ])
    
    let arr1 = newArray[int32](@[1'i32, 2'i32, 3'i32])
    let arr2 = newArray[int32](@[1'i32, 2'i32, 3'i32])
    
    let rb1 = newRecordBatch(schema, arr1)
    let rb2 = newRecordBatch(schema, arr2)
    
    check rb1 == rb2
  
  test "Not equal - different values":
    let schema = newSchema([
      newField[int32]("col")
    ])
    
    let arr1 = newArray[int32](@[1'i32, 2'i32, 3'i32])
    let arr2 = newArray[int32](@[1'i32, 2'i32, 4'i32])
    
    let rb1 = newRecordBatch(schema, arr1)
    let rb2 = newRecordBatch(schema, arr2)
    
    check rb1 != rb2
  
  test "Not equal - different schemas":
    let schema1 = newSchema([
      newField[int32]("col")
    ])
    let schema2 = newSchema([
      newField[int64]("col")
    ])
    
    let arr1 = newArray[int32](@[1'i32])
    let arr2 = newArray[int64](@[1'i64])
    
    let rb1 = newRecordBatch(schema1, arr1)
    let rb2 = newRecordBatch(schema2, arr2)
    
    check rb1 != rb2

suite "RecordBatch - Slicing":
  
  test "Slice record batch":
    let schema = newSchema([
      newField[int32]("values")
    ])
    
    let arr = newArray[int32](@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32])
    let rb = newRecordBatch(schema, arr)
    
    let sliced = rb.slice(1, 3)
    
    check sliced.nRows() == 3
    let col = sliced.getColumnData[:int32](0)
    check col[0] == 2'i32
    check col[1] == 3'i32
    check col[2] == 4'i32
  
  test "Slice from offset to end":
    let schema = newSchema([
      newField[int32]("values")
    ])
    
    let arr = newArray[int32](@[10'i32, 20'i32, 30'i32, 40'i32])
    let rb = newRecordBatch(schema, arr)
    
    let sliced = rb.slice(2, 2)
    check sliced.nRows() == 2

suite "RecordBatch - Validation":
  
  test "Validate valid record batch":
    let schema = newSchema([
      newField[bool]("col")
    ])
    
    let arr = newArray[bool](@[true, false, true])
    let rb = newRecordBatch(schema, arr)
    
    check rb.validate()
    check rb.validateFull()

suite "RecordBatch - Column Manipulation":
  
  test "Add column":
    let schema = newSchema([
      newField[int32]("col1")
    ])
    
    let arr1 = newArray[int32](@[1'i32, 2'i32])
    let rb = newRecordBatch(schema, arr1)
    
    let newField = newField[string]("col2")
    let newArr = newArray[string](@["a", "b"])
    
    let rbWithCol = rb.addColumn(1, newField, newArr)
    
    check rbWithCol.nColumns() == 2
    check rbWithCol.getColumnName(0) == "col1"
    check rbWithCol.getColumnName(1) == "col2"
  
  test "Remove column":
    let schema = newSchema([
      newField[int32]("col1"),
      newField[string]("col2")
    ])
    
    let arr1 = newArray[int32](@[1'i32])
    let arr2 = newArray[string](@["a"])
    
    let rb = newRecordBatch(schema, arr1, arr2)
    let rbRemoved = rb.removeColumn(1)
    
    check rbRemoved.nColumns() == 1
    check rbRemoved.getColumnName(0) == "col1"

suite "RecordBatchIterator - Construction and Iteration":
  
  test "Create iterator from sequence":
    let schema = newSchema([
      newField[int32]("val")
    ])
    
    let rb1 = newRecordBatch(schema, newArray[int32](@[1'i32, 2'i32]))
    let rb2 = newRecordBatch(schema, newArray[int32](@[3'i32, 4'i32]))
    
    let it = newRecordBatchIterator(@[rb1, rb2])
    var count = 0
    
    for batch in it:
      count += 1
    
    check count == 2
  
  test "Iterator next returns none when exhausted":
    let schema = newSchema([
      newField[int32]("val")
    ])
    
    let rb = newRecordBatch(schema, newArray[int32](@[1'i32]))
    let it = newRecordBatchIterator(@[rb])
    
    let first = it.next()
    check first.isSome
    
    let second = it.next()
    check second.isNone
  
  test "Convert iterator to list":
    let schema = newSchema([
      newField[int32]("val")
    ])
    
    let rb1 = newRecordBatch(schema, newArray[int32](@[1'i32]))
    let rb2 = newRecordBatch(schema, newArray[int32](@[2'i32]))
    let rb3 = newRecordBatch(schema, newArray[int32](@[3'i32]))
    
    let it = newRecordBatchIterator(@[rb1, rb2, rb3])
    let list = it.toList()
    
    check list.len == 3

suite "RecordBatch - Sort / Take":
  test "sortIndices returns correct order":
    let schema = newSchema([newField[int32]("v")])
    let rb = newRecordBatch(schema, newArray(@[3'i32, 1, 2]))
    let idx = sortIndices(rb, @[newSortKey("v", Ascending)])
    check idx.len == 3
    check idx.toSeq == @[1'u64, 2, 0]

  test "take reorders rows":
    let schema = newSchema([newField[int32]("v"),
                            newField[string]("name")])
    let rb = newRecordBatch(schema,
      newArray(@[3'i32, 1, 2]),
      newArray(@["c", "a", "b"]))
    let idx = newArray(@[1'u64, 2, 0])
    let taken = take(rb, idx)
    check taken.nRows == 3
    check taken[0, int32].toSeq == @[1'i32, 2, 3]
    check taken[1, string].toSeq == @["a", "b", "c"]

  test "sortBy sorts record batch":
    let schema = newSchema([newField[int32]("age"),
                            newField[string]("name")])
    let rb = newRecordBatch(schema,
      newArray(@[30'i32, 10, 20]),
      newArray(@["c", "a", "b"]))
    let sorted = sortBy(rb, @[("age", Ascending)])
    check sorted[0, int32].toSeq == @[10'i32, 20, 30]
    check sorted[1, string].toSeq == @["a", "b", "c"]

  test "sortBy descending":
    let schema = newSchema([newField[int32]("v")])
    let rb = newRecordBatch(schema, newArray(@[1'i32, 3, 2]))
    let sorted = sortBy(rb, @[("v", Descending)])
    check sorted[0, int32].toSeq == @[3'i32, 2, 1]

  test "multi-key sort":
    let schema = newSchema([newField[string]("region"),
                            newField[int32]("score")])
    let rb = newRecordBatch(schema,
      newArray(@["US", "UK", "US", "UK"]),
      newArray(@[100'i32, 200, 50, 150]))
    let sorted = sortBy(rb, @[("region", Ascending), ("score", Ascending)])
    check sorted[0, string].toSeq == @["UK", "UK", "US", "US"]
    check sorted[1, int32].toSeq == @[150'i32, 200, 50, 100]

  test "empty record batch sort":
    let schema = newSchema([newField[int32]("v")])
    let empty: seq[int32] = @[]
    let rb = newRecordBatch(schema, newArray(empty))
    let sorted = sortBy(rb, @[("v", Ascending)])
    check sorted.nRows == 0

suite "RecordBatch - Filter":
  test "filter record batch with boolean array":
    let schema = newSchema([newField[int32]("id"),
                            newField[string]("name")])
    let rb = newRecordBatch(schema,
      newArray(@[1'i32, 2, 3, 4, 5]),
      newArray(@["a", "b", "c", "d", "e"]))
    let mask = newBooleanArray(@[true, false, true, false, true])
    let filtered = filter(rb, mask)
    check filtered.nRows == 3
    check filtered[0, int32].toSeq == @[1'i32, 3, 5]

  test "filter returns empty when no match":
    let schema = newSchema([newField[int32]("v")])
    let rb = newRecordBatch(schema, newArray(@[1'i32, 2, 3]))
    let mask = newBooleanArray(@[false, false, false])
    let filtered = filter(rb, mask)
    check filtered.nRows == 0
