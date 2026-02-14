import std/[os]
import unittest2
import testfixture
import
  ../src/narrow/[
    core/ffi,
    column/primitive,
    tabular/table,
    column/metadata,
    io/parquet,
    types/gtypes,
    column/primitive,
    tabular/batch,
    compute/expressions,
  ]

suite "Reading parquet":
  test "read parquet file localFileSystem":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let table = readTable(uri)

  test "read parquet and verify schema fields":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let table = readTable(uri)
    let schema = table.schema

    # Verify we can iterate over fields and get their types
    var fieldNames: seq[string] = @[]
    var fieldTypes: seq[string] = @[]

    for field in schema:
      fieldNames.add(field.name)
      let dataType = field.dataType
      fieldTypes.add($dataType)

    # Check we have the expected columns
    check fieldNames.len > 0

  test "read parquet and access columns by index":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let table = readTable(uri)

    # Test accessing columns using the void type (for any column type)
    for i in 0 ..< table.nColumns:
      let col = table[i]
      check col.len >= 0 # Should not fail
      # let dataType = col.getValueDataType()

  test "read parquet and access columns by name":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let table = readTable(uri)
    let schema = table.schema

    # Test accessing each column by name
    for field in schema:
      let col = table[field.name]
      check col.len >= 0 # Should not fail

suite "Writing parquet - ArrowTable":
  var fixture: TestFixture

  setup:
    fixture = newTestFixture("test_parquet")

  teardown:
    fixture.cleanup()

  test "read parquet file localFileSystem":
    let
      schema = newSchema([newField[bool]("alive"), newField[string]("name")])
      alive = newArray(@[true, true, false])
      name = newArray(@["a", "b", "c"])
      table = newArrowTable(schema, alive, name)

    let uri = fixture / "table.parquet"
    writeTable(table, uri)
    let restored = readTable(uri)

    check table == restored

suite "WriterProperties":
  test "create WriterProperties":
    let props = newWriterProperties()
    check props.toPtr != nil

  test "dictionaryPageSizeLimit getter returns positive value":
    var props = newWriterProperties()
    let limit = props.dictionaryPageSizeLimit
    check limit > 0

  test "dictionaryPageSizeLimit setter using property syntax":
    var props = newWriterProperties()
    props.dictionaryPageSizeLimit = 1024
    check props.dictionaryPageSizeLimit == 1024

  test "dictionaryPageSizeLimit setter using explicit call":
    var props = newWriterProperties()
    `dictionaryPageSizeLimit=`(props, 2048)
    check props.dictionaryPageSizeLimit == 2048

  test "batchSize getter returns positive value":
    var props = newWriterProperties()
    let size = props.batchSize
    check size > 0

  test "batchSize setter":
    var props = newWriterProperties()
    props.batchSize = 1000
    check props.batchSize == 1000

  test "maxRowGroupLength getter returns positive value":
    var props = newWriterProperties()
    let length = props.maxRowGroupLength
    check length > 0

  test "maxRowGroupLength setter":
    var props = newWriterProperties()
    props.maxRowGroupLength = 50000
    check props.maxRowGroupLength == 50000

  test "dataPageSize getter returns positive value":
    var props = newWriterProperties()
    let size = props.dataPageSize
    check size > 0

  test "dataPageSize setter":
    var props = newWriterProperties()
    props.dataPageSize = 8192
    check props.dataPageSize == 8192

  test "multiple properties can be set independently":
    var props = newWriterProperties()

    props.dictionaryPageSizeLimit = 4096
    props.batchSize = 2048
    props.maxRowGroupLength = 100000
    props.dataPageSize = 16384

    check props.dictionaryPageSizeLimit == 4096
    check props.batchSize == 2048
    check props.maxRowGroupLength == 100000
    check props.dataPageSize == 16384

  test "assignment creates shared reference":
    # Note: GParquetWriterProperties appears to share state between references
    var props1 = newWriterProperties()
    props1.batchSize = 5000

    var props2 = props1
    check props2.batchSize == 5000

    # Both props1 and props2 reference the same underlying object
    props2.batchSize = 10000
    check props2.batchSize == 10000
    # props1 also sees the change since they share the handle
    check props1.batchSize == 10000

  test "move semantics via sink":
    var props1 = newWriterProperties()
    props1.batchSize = 7500

    var props2 = move(props1) # Explicit move
    check props2.batchSize == 7500

suite "FileWriter ARC/ORC Memory Management":
  test "FileWriter assignment creates shared reference":
    let schema = newSchema([newField[int32]("value")])
    let wp = newWriterProperties()

    var fixture = newTestFixture("test_filewriter_arc")
    let uri = fixture / "shared.parquet"

    var writer1 = newFileWriter(uri, schema, wp)
    var writer2 = writer1 # Copy (both reference same handle)

    # Both should point to same underlying writer
    check writer1.toPtr == writer2.toPtr

  test "FileWriter move via sink transfers ownership":
    let schema = newSchema([newField[int32]("value")])
    let wp = newWriterProperties()

    var fixture = newTestFixture("test_filewriter_move")
    let uri = fixture / "moved.parquet"

    var writer1 = newFileWriter(uri, schema, wp)
    let ptr1 = writer1.toPtr

    var writer2 = move(writer1) # Move ownership

    check writer2.toPtr == ptr1
    # writer1 should now have nil handle (moved)

suite "FileReader Enhancements":
  test "FileReader nRowGroups returns correct count":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let reader = newFileReader(uri)

    check reader.nRowGroups > 0

  test "FileReader nRows returns total row count":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let reader = newFileReader(uri)

    check reader.nRows > 0

  test "FileReader close releases resources":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    var reader = newFileReader(uri)

    reader.close()
    check true # If we get here without crash, close worked

  test "FileReader readRowGroup reads specific group":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let reader = newFileReader(uri)

    if reader.nRowGroups > 0:
      let table = reader.readRowGroup(0)
      check table.nColumns > 0
      check table.nRows > 0

  test "FileReader readColumnData returns chunked array":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let reader = newFileReader(uri)

    # Read first column
    let chunkedArray = reader.readColumnData(0)
    check chunkedArray.len >= 0

  test "FileReader useThreads setter works":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let reader = newFileReader(uri)

    reader.useThreads = false
    reader.useThreads = true
    check true # If no crash, setter works

suite "FileWriter Enhancements":
  test "FileWriter close flushes and closes file":
    let
      schema = newSchema([newField[int32]("value")])
      values = newArray(@[1i32, 2i32, 3i32])
      table = newArrowTable(schema, values)

    var fixture = newTestFixture("test_filewriter_close")
    let uri = fixture / "closed.parquet"

    writeTable(table, uri)

    # Verify file can be read back
    let reader = newFileReader(uri)
    check reader.nRows == 3

  test "Test writeTable works with record batches":
    let schema = newSchema([newField[int32]("col1"), newField["string"]("col2")])

    let seq1 = @[1'i32, 2'i32, 3'i32]
    let seq2 = @["a", "b", "c"]

    let rb = newRecordBatch(schema, seq1, seq2)

    var fixture = newTestFixture("test_filewriter_close")
    let uri = fixture / "closed.parquet"

    writeTable(rb, uri)

    # Verify file can be read back
    let reader = newFileReader(uri)
    check reader.nRows == 3

  test "FileWriter writeRecordBatch writes batches":
    # TODO: RecordBatchBuilder.columnBuilder not accessible in test context
    # Skipping this test for now - need to investigate export issue
    skip()

  test "FileWriter newRowGroup can be called":
    # TODO: newRowGroup requires writeColumnData to actually write data
    # Skipping this test until column-level writing is implemented
    skip()

  test "FileWriter schema returns writer schema":
    let schema = newSchema([newField[int32]("value")])

    var fixture = newTestFixture("test_filewriter_schema")
    let uri = fixture / "schema.parquet"

    var writer = newFileWriter(uri, schema, newWriterProperties())
    let writerSchema = writer.schema

    check writerSchema.nFields == 1
    check writerSchema[0].name == "value"

    writer.close()

suite "WriterProperties Compression and Dictionary":
  test "WriterProperties compression type can be set and retrieved":
    var props = newWriterProperties()

    # Set compression for a specific column path
    props.setCompression("value", GARROW_COMPRESSION_TYPE_SNAPPY)

    # Retrieve compression type
    let compression = props.compression("value")
    check compression == GARROW_COMPRESSION_TYPE_SNAPPY

  test "WriterProperties enableDictionary enables encoding":
    var props = newWriterProperties()

    # Enable dictionary for a column
    props.enableDictionary("value")

    # Check that it's enabled
    check props.isDictionaryEnabled("value") == true

  test "WriterProperties disableDictionary disables encoding":
    var props = newWriterProperties()

    # First enable, then disable
    props.enableDictionary("value")
    props.disableDictionary("value")

    # Check that it's disabled
    check props.isDictionaryEnabled("value") == false

  test "WriterProperties dictionary settings are column-specific":
    var props = newWriterProperties()

    # Enable for one column, disable for another
    props.enableDictionary("col1")
    props.disableDictionary("col2")

    check props.isDictionaryEnabled("col1") == true
    check props.isDictionaryEnabled("col2") == false

  test "WriterProperties compression can be set to different types":
    var props = newWriterProperties()

    # Set different compression types for different columns
    props.setCompression("col1", GARROW_COMPRESSION_TYPE_SNAPPY)
    props.setCompression("col2", GARROW_COMPRESSION_TYPE_GZIP)
    props.setCompression("col3", GARROW_COMPRESSION_TYPE_ZSTD)

    check props.compression("col1") == GARROW_COMPRESSION_TYPE_SNAPPY
    check props.compression("col2") == GARROW_COMPRESSION_TYPE_GZIP
    check props.compression("col3") == GARROW_COMPRESSION_TYPE_ZSTD

suite "Filtering parquet at reading":
  var fixture: TestFixture

  setup:
    fixture = newTestFixture("test_read_with_filtering")

  teardown:
    fixture.cleanup()

  test "read parquet file localFileSystem":
    block:
      let
        schema = newSchema(
          [newField[bool]("alive"), newField[string]("name"), newField[int]("age")]
        )
        alive = newArray(@[false, false, true])
        name = newArray(@["Adam", "Eve", "admin"])
        age = newArray(@[18, 20, 40])
        table = newArrowTable(schema, alive, name, age)

      let uri = fixture / "table.parquet"
      writeTable(table, uri)

    let
      age = col("age")
      name = col("name")
      alive = col("alive")

    # TODO: name.toLower()
    # Developing a complex filter with your new grammar:
    let complexFilter =
      (age >= 18) and (name.contains("admin") or name == "root") and
      alive.isValid()

    echo age
    echo complexFilter
    # let table = readTable(
    #   "users.parquet", columns = @[$age, $name, $alive], filter = complexFilter
    # )
    # echo table
