import unittest2
import testfixture
import ../src/narrow except check

suite "IPC File Format":
  var fixture: TestFixture

  setup:
    fixture = newTestFixture("test_io/ipc")

  teardown:
    fixture.cleanup()

  test "write and read round-trip preserves data":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
      newField[bool]("active"),
      newField[float64]("score")
    ])
    let ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32])
    let names = newArray(@["alpha", "beta", "gamma", "delta", "epsilon"])
    let actives = newArray(@[true, false, true, false, true])
    let scores = newArray(@[95.5'f64, 87.2'f64, 92.1'f64, 78.5'f64, 88.0'f64])

    let original = newArrowTable(schema, ids, names, actives, scores)
    let uri = fixture / "roundtrip.arrow"
    
    writeIpcFile(uri, original)
    let restored = readIpcFile(uri)
    
    check restored == original

  test "handles multiple batches":
    let schema = newSchema([
      newField[int32]("batch_id"),
      newField[string]("data")
    ])
    
    let fs = newFileSystem("file://" & fixture.basePath)
    let uri = fixture / "multi_batch.arrow"
    let writer = newIpcFileWriter(fs, uri, schema)
    
    var batches: seq[RecordBatch] = @[]
    for i in 0 ..< 5:
      let ids = newArray(@[i.int32])
      let data = newArray(@["data_" & $i])
      let batch = newRecordBatch(schema, ids, data)
      batches.add(batch)
      writer.writeRecordBatch(batch)
    
    writer.close()
    
    let reader = newIpcFileReader(fs, uri)
    check reader.nRecordBatches == 5
    
    let restored = reader.readAll()
    check restored.nRows == 5

  test "low-level reader API":
    let schema = newSchema([
      newField[int64]("value"),
      newField[float64]("score")
    ])
    let values = newArray(@[100'i64, 200'i64, 300'i64])
    let scores = newArray(@[1.5'f64, 2.5'f64, 3.5'f64])
    let original = newArrowTable(schema, values, scores)
    
    let uri = fixture / "lowlevel.arrow"
    writeIpcFile(uri, original)
    
    let fs = newFileSystem("file://" & fixture.basePath)
    let stream = fs.openInputFile(uri)
    let reader = newIpcFileReader(stream)
    
    check reader.nRecordBatches > 0
    check reader.schema == schema
    
    let batch = reader.readRecordBatch(0)
    check batch.nRows == 3

suite "IPC Stream Format":
  var fixture: TestFixture

  setup:
    fixture = newTestFixture("test_io/ipc_stream")

  teardown:
    fixture.cleanup()

  test "stream format round-trip":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
      newField[bool]("active")
    ])
    let ids = newArray(@[1'i32, 2'i32, 3'i32])
    let names = newArray(@["alice", "bob", "charlie"])
    let actives = newArray(@[true, false, true])

    let original = newArrowTable(schema, ids, names, actives)
    let uri = fixture / "stream.arrow"
    
    writeIpcStream(uri, original)
    let restored = readIpcStream(uri)
    
    check restored == original

  test "record batch round-trip":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name")
    ])
    let ids = newArray(@[1'i32, 2'i32, 3'i32])
    let names = newArray(@["x", "y", "z"])
    let original = newRecordBatch(schema, ids, names)
    
    let uri = fixture / "batch.arrow"
    
    writeIpcFile(uri, original)
    let restored = readIpcFile(uri)
    
    check restored.nRows == original.nRows
    check restored.nColumns == original.nColumns

  test "stream reader from input stream":
    let schema = newSchema([
      newField[string]("data")
    ])
    let data = newArray(@["item1", "item2", "item3"])
    let original = newArrowTable(schema, data)
    
    let uri = fixture / "from_stream.arrow"
    writeIpcStream(uri, original)
    
    let fs = newFileSystem("file://" & fixture.basePath)
    let stream = fs.openInputStream(uri)
    let reader = newIpcStreamReader(stream)
    let restored = reader.readAll()
    
    check restored == original
