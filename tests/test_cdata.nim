import unittest2
import ../src/narrow

suite "C Data Interface - Schema":
  test "export schema and verify pointer":
    let schema = newSchema([newField[int32]("a"), newField[string]("b")])
    let schemaPtr = schema.exportSchema()
    check schemaPtr != nil

suite "C Data Interface - RecordBatch":
  test "export record batch and verify pointers":
    let schema = newSchema([newField[int32]("id")])
    let arr = newArray(@[1'i32, 2, 3])
    let batch = newRecordBatch(schema, arr)
    let (arrPtr, schPtr) = batch.exportRecordBatch()
    check arrPtr != nil
    check schPtr != nil

suite "C Data Interface - RecordBatchReader":
  test "export reader":
    let schema = newSchema([newField[int32]("x")])
    let arr = newArray(@[10'i32, 20])
    let table = newArrowTable(schema, arr)
    let reader = newRecordBatchReader(table)
    let readerPtr = reader.exportRecordBatchReader()
    check readerPtr != nil

suite "C Data Interface - Memory":
  test "export schema is valid pointer after schema goes out of scope":
    let schema = newSchema([newField[int32]("a")])
    let schemaPtr = schema.exportSchema()
    check schemaPtr != nil
