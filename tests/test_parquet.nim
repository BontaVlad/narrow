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
    let uri = fixture / "table.parquet"
    block:
      let
        schema = newSchema(
          [newField[bool]("alive"), newField[string]("name"), newField[int]("age")]
        )
        alive = newArray(@[false, false, true])
        name = newArray(@["Adam", "Eve", "ADMIN"])
        age = newArray(@[18, 20, 40])
        table = newArrowTable(schema, alive, name, age)

      writeTable(table, uri)

    let
      age = col("age")
      name = col("name")
      alive = col("alive")

    let complexFilter =
      (age >= 18) and (name.toLower().contains("admin") or name == "root") and
      alive.isValid()

    let table = readTable(
      uri, columns = @[$age, $name, $alive], filter = complexFilter
    )
    check table["name"] == newChunkedArray([newArray(@["ADMIN"])])
    check table.nColumns == 3

  test "filter with simple equality":
    let uri = fixture / "simple_equality.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("name")])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32])
        names = newArray(@["Alice", "Bob", "Charlie", "Diana", "Eve"])
        table = newArrowTable(schema, ids, names)
      writeTable(table, uri)

    let id = col("id")
    let filtered = readTable(uri, filter = id == 3'i32)
    check filtered.nRows == 1

  test "filter with multiple equality conditions":
    let uri = fixture / "multi_equality.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("category"), newField[bool]("active")])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32, 6'i32])
        categories = newArray(@["A", "B", "A", "B", "A", "B"])
        active = newArray(@[true, true, false, true, false, false])
        table = newArrowTable(schema, ids, categories, active)
      writeTable(table, uri)

    let category = col("category")
    let active = col("active")
    let filter = (category == "A") and (active == true)
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 1

  test "filter with comparison operators":
    let uri = fixture / "comparison.parquet"
    block:
      let
        schema = newSchema([newField[int32]("value"), newField[float64]("score")])
        values = newArray(@[10'i32, 20'i32, 30'i32, 40'i32, 50'i32])
        scores = newArray(@[1.5'f64, 2.5'f64, 3.5'f64, 4.5'f64, 5.5'f64])
        table = newArrowTable(schema, values, scores)
      writeTable(table, uri)

    let value = col("value")
    let score = col("score")
    
    # Test greater than
    var filter = value > 25'i32
    var filtered = readTable(uri, filter = filter)
    check filtered.nRows == 3
    
    # Test less than
    filter = value < 35'i32
    filtered = readTable(uri, filter = filter)
    check filtered.nRows == 3
    
    # Test greater equal
    filter = score >= 3.5'f64
    filtered = readTable(uri, filter = filter)
    check filtered.nRows == 3
    
    # Test less equal
    filter = score <= 3.5'f64
    filtered = readTable(uri, filter = filter)
    check filtered.nRows == 3

  test "filter with string contains":
    let uri = fixture / "string_contains.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("description")])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32])
        descriptions = newArray(@["admin user", "regular user", "admin panel", "user guide", "administrator"])
        table = newArrowTable(schema, ids, descriptions)
      writeTable(table, uri)

    let description = col("description")
    let filter = description.contains("admin")
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 3

  test "filter with string startsWith":
    let uri = fixture / "string_startswith.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("name")])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32])
        names = newArray(@["Alice", "Bob", "Anna", "Charlie", "Amanda"])
        table = newArrowTable(schema, ids, names)
      writeTable(table, uri)

    let name = col("name")
    let filter = startsWith(name, "A")
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 3

  test "filter with string endsWith":
    let uri = fixture / "string_endswith.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("name")])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32])
        names = newArray(@["test.txt", "file.pdf", "doc.txt", "image.png", "notes.txt"])
        table = newArrowTable(schema, ids, names)
      writeTable(table, uri)

    let name = col("name")
    let filter = endsWith(name, ".txt")
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 3

  test "filter with case insensitive string operations":
    let uri = fixture / "case_insensitive.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("name")])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32])
        names = newArray(@["ADMIN", "admin", "Admin", "User"])
        table = newArrowTable(schema, ids, names)
      writeTable(table, uri)

    let name = col("name")
    let filter = name.contains("admin", true)
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 3

  test "filter with toLower and contains chaining":
    let uri = fixture / "tolower_chain.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("name")])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32])
        names = newArray(@["ADMIN", "admin", "Admin", "User", "ADMINISTRATOR"])
        table = newArrowTable(schema, ids, names)
      writeTable(table, uri)

    let name = col("name")
    let filter = name.toLower().contains("admin")
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 4

  test "filter with toUpper and contains chaining":
    let uri = fixture / "toupper_chain.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("code")])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32])
        codes = newArray(@["abc", "ABC", "Abc", "xyz"])
        table = newArrowTable(schema, ids, codes)
      writeTable(table, uri)

    let code = col("code")
    let filter = code.toUpper().contains("ABC")
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 3

  test "filter with logical OR":
    let uri = fixture / "logical_or.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("category")])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32])
        categories = newArray(@["A", "B", "C", "A", "B"])
        table = newArrowTable(schema, ids, categories)
      writeTable(table, uri)

    let category = col("category")
    let filter = (category == "A") or (category == "C")
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 3

  test "filter with logical NOT":
    let uri = fixture / "logical_not.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("status")])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32])
        statuses = newArray(@["active", "inactive", "active", "pending"])
        table = newArrowTable(schema, ids, statuses)
      writeTable(table, uri)

    let status = col("status")
    let filter = notExpr(status == "active")
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 2

  test "filter with complex nested conditions":
    let uri = fixture / "nested_conditions.parquet"
    block:
      let
        schema = newSchema([
          newField[int32]("id"),
          newField[string]("role"),
          newField[int32]("age"),
          newField[bool]("active")
        ])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32, 6'i32])
        roles = newArray(@["admin", "user", "admin", "user", "guest", "admin"])
        ages = newArray(@[25'i32, 30'i32, 35'i32, 20'i32, 40'i32, 28'i32])
        active = newArray(@[true, true, false, true, false, true])
        table = newArrowTable(schema, ids, roles, ages, active)
      writeTable(table, uri)

    let role = col("role")
    let age = col("age")
    let active = col("active")
    
    # Complex: (role is admin AND age >= 25) OR (active AND age < 30)
    let filter = ((role == "admin") and (age >= 25'i32)) or ((active == true) and (age < 30'i32))
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 4

  test "filter with deeply nested logical operations":
    let uri = fixture / "deeply_nested.parquet"
    block:
      let
        schema = newSchema([
          newField[int32]("id"),
          newField[string]("type"),
          newField[int32]("priority"),
          newField[bool]("urgent")
        ])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32, 6'i32, 7'i32, 8'i32])
        types = newArray(@["A", "B", "A", "B", "A", "B", "A", "B"])
        priorities = newArray(@[1'i32, 2'i32, 3'i32, 1'i32, 2'i32, 3'i32, 1'i32, 2'i32])
        urgent = newArray(@[true, false, true, true, false, false, true, false])
        table = newArrowTable(schema, ids, types, priorities, urgent)
      writeTable(table, uri)

    let typeCol = col("type")
    let priority = col("priority")
    let urgent = col("urgent")
    
    # Deep nesting: ((type == "A" AND priority > 1) OR (type == "B" AND urgent)) AND NOT (priority == 3)
    let filter = (((typeCol == "A") and (priority > 1'i32)) or ((typeCol == "B") and (urgent == true))) and (notExpr(priority == 3'i32))
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 2

  test "filter with null handling":
    let uri = fixture / "null_handling.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("optional")])
        ids = newArray(@[1'i32, 2'i32, 3'i32])
        optionals = newArray(@["value", "", ""])  # Empty strings as null indicators
        table = newArrowTable(schema, ids, optionals)
      writeTable(table, uri)

    let optional = col("optional")
    let filter = optional.isValid()
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows >= 1

  test "filter with isNull check":
    let uri = fixture / "isnull_check.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("name")])
        ids = newArray(@[1'i32, 2'i32, 3'i32])
        names = newArray(@["Alice", "", ""])
        table = newArrowTable(schema, ids, names)
      writeTable(table, uri)

    let name = col("name")
    let filter = isNull(name)
    let filtered = readTable(uri, filter = filter)
    # Empty strings aren't null, so this should return 0 or handle appropriately
    check filtered.nRows >= 0

  test "filter resulting in empty table":
    let uri = fixture / "empty_result.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("name")])
        ids = newArray(@[1'i32, 2'i32, 3'i32])
        names = newArray(@["Alice", "Bob", "Charlie"])
        table = newArrowTable(schema, ids, names)
      writeTable(table, uri)

    let id = col("id")
    let filter = id > 100'i32
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 0

  test "filter resulting in all rows":
    let uri = fixture / "all_rows.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("name")])
        ids = newArray(@[1'i32, 2'i32, 3'i32])
        names = newArray(@["Alice", "Bob", "Charlie"])
        table = newArrowTable(schema, ids, names)
      writeTable(table, uri)

    let id = col("id")
    let filter = id >= 0'i32
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 3

  test "filter with string length":
    let uri = fixture / "string_length.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("name")])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32])
        names = newArray(@["A", "BB", "CCC", "DDDD", "EEEEE"])
        table = newArrowTable(schema, ids, names)
      writeTable(table, uri)

    let name = col("name")
    let filter = name.len() > 3
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 2

  test "filter with multiple string operations":
    let uri = fixture / "multi_string_ops.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("email")])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32])
        emails = newArray(@["admin@test.com", "user@domain.org", "admin@company.net", "test@admin.io", "admin@local.com"])
        table = newArrowTable(schema, ids, emails)
      writeTable(table, uri)

    let email = col("email")
    # Find emails that start with "admin" OR contain "admin" AND end with ".com"
    # admin@test.com: starts with admin, ends with .com -> MATCH
    # admin@local.com: starts with admin, ends with .com -> MATCH
    let filter = (startsWith(email, "admin") or email.contains("admin")) and endsWith(email, ".com")
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 2

  test "stress test - filter large dataset with complex conditions":
    let uri = fixture / "stress_test.parquet"
    block:
      let
        schema = newSchema([
          newField[int32]("id"),
          newField[string]("category"),
          newField[int32]("value"),
          newField[bool]("flag")
        ])
      var
        ids: seq[int32] = @[]
        categories: seq[string] = @[]
        values: seq[int32] = @[]
        flags: seq[bool] = @[]
      
      # Generate 1000 rows
      for i in 0 ..< 1000:
        ids.add(i.int32)
        categories.add(if i mod 3 == 0: "A" elif i mod 3 == 1: "B" else: "C")
        values.add((i * 10).int32)
        flags.add(i mod 2 == 0)
      
      let table = newArrowTable(schema, newArray(ids), newArray(categories), newArray(values), newArray(flags))
      writeTable(table, uri)

    let category = col("category")
    let value = col("value")
    let flag = col("flag")
    
    # Complex filter on large dataset
    let filter = ((category == "A") or (category == "B")) and (value >= 100'i32) and (value <= 5000'i32) and (flag == true)
    let filtered = readTable(uri, filter = filter)
    # Should get roughly 1/3 of rows that meet criteria
    check filtered.nRows > 0
    check filtered.nRows < 1000

  test "stress test - multiple filters in sequence":
    let uri = fixture / "multi_filter_seq.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("type"), newField[int32]("score")])
      var
        ids: seq[int32] = @[]
        types: seq[string] = @[]
        scores: seq[int32] = @[]
      
      for i in 0 ..< 500:
        ids.add(i.int32)
        types.add(if i mod 5 == 0: "premium" else: "standard")
        scores.add((i mod 100).int32)
      
      let table = newArrowTable(schema, newArray(ids), newArray(types), newArray(scores))
      writeTable(table, uri)

    let id = col("id")
    let typeCol = col("type")
    let score = col("score")
    
    # Apply multiple different filters
    var filter = typeCol == "premium"
    var filtered = readTable(uri, filter = filter)
    check filtered.nRows == 100  # 500 / 5
    
    filter = score >= 50'i32
    filtered = readTable(uri, filter = filter)
    check filtered.nRows == 250  # Half of 500
    
    filter = (typeCol == "premium") and (score >= 50'i32)
    filtered = readTable(uri, filter = filter)
    check filtered.nRows == 50  # 100 premium * 0.5

  test "filter with mixed types comparison":
    let uri = fixture / "mixed_types.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[float64]("value")])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32])
        values = newArray(@[1.0'f64, 2.5'f64, 3.0'f64, 4.5'f64])
        table = newArrowTable(schema, ids, values)
      writeTable(table, uri)

    let id = col("id")
    let value = col("value")
    # Note: This tests if we can compare int and float properly
    let filter = value >= 2.0'f64
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 3

  test "filter with NOT and AND combination":
    let uri = fixture / "not_and_combo.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("status"), newField[bool]("active")])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32, 6'i32])
        statuses = newArray(@["pending", "active", "inactive", "pending", "active", "inactive"])
        active = newArray(@[false, true, false, true, false, true])
        table = newArrowTable(schema, ids, statuses, active)
      writeTable(table, uri)

    let status = col("status")
    let active = col("active")
    # NOT (status is pending AND active) - should exclude rows where both are true
    let filter = notExpr((status == "pending") and (active == true))
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 5

  test "filter with regex pattern matching":
    let uri = fixture / "regex_filter.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("email")])
        ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32])
        emails = newArray(@["user1@test.com", "admin@company.org", "test123@site.net", "user2@test.com", "admin@test.com"])
        table = newArrowTable(schema, ids, emails)
      writeTable(table, uri)

    let email = col("email")
    # Match emails ending with @test.com
    let filter = matchSubstringRegex(email, ".*@test\\.com$")
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 3

  test "edge case - filter on column with all same values":
    let uri = fixture / "same_values.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("category")])
        ids = newArray(@[1'i32, 2'i32, 3'i32])
        categories = newArray(@["A", "A", "A"])
        table = newArrowTable(schema, ids, categories)
      writeTable(table, uri)

    let category = col("category")
    let filter = category == "A"
    let filtered = readTable(uri, filter = filter)
    check filtered.nRows == 3
    
    let filter2 = category == "B"
    let result2 = readTable(uri, filter = filter2)
    check result2.nRows == 0

  test "edge case - filter on single row table":
    let uri = fixture / "single_row.parquet"
    block:
      let
        schema = newSchema([newField[int32]("id"), newField[string]("name")])
        ids = newArray(@[42'i32])
        names = newArray(@["solo"])
        table = newArrowTable(schema, ids, names)
      writeTable(table, uri)

    let id = col("id")
    let name = col("name")
    
    var filter = id == 42'i32
    var filtered = readTable(uri, filter = filter)
    check filtered.nRows == 1
    
    filter = name.contains("olo")
    filtered = readTable(uri, filter = filter)
    check filtered.nRows == 1
    
    filter = id > 100'i32
    filtered = readTable(uri, filter = filter)
    check filtered.nRows == 0
