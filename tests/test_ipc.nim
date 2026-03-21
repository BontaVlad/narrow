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

    let table = newArrowTable(schema, batches)
    
    let reader = newIpcFileReader(fs, uri)
    let restored = reader.readAll()
    check restored == table

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

  test "read Ipc stream":
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
    
    writeIpcStream(uri, original)


    let fs = newFileSystem(uri)

    with fs.openInputStream(uri), stream:
      let reader = newIpcStreamReader(stream)
      for batch in reader.batches:
        check batch.nRows == 5
        check batch.nColumns == 4

    let restored = readIpcStream(uri)
    check restored == original
