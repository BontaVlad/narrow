import std/options
import unittest2
import testfixture
import ../src/narrow except check

suite "Feather File Format":
  var fixture: TestFixture

  setup:
    fixture = newTestFixture("test_io/feather")

  teardown:
    fixture.cleanup()

  test "write and read round-trip preserves data":
    let schema = newSchema(
      [
        newField[int32]("id"),
        newField[string]("name"),
        newField[bool]("active"),
        newField[float64]("score"),
      ]
    )
    let ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32])
    let names = newArray(@["alpha", "beta", "gamma", "delta", "epsilon"])
    let actives = newArray(@[true, false, true, false, true])
    let scores = newArray(@[95.5'f64, 87.2'f64, 92.1'f64, 78.5'f64, 88.0'f64])

    let original = newArrowTable(schema, ids, names, actives, scores)
    let uri = fixture / "roundtrip.feather"

    writeFeatherFile(uri, original)
    let restored = readFeatherFile(uri)

    check restored == original

  test "read specific columns by name":
    let schema = newSchema(
      [newField[int32]("id"), newField[string]("name"), newField[float64]("value")]
    )
    let ids = newArray(@[1'i32, 2'i32, 3'i32])
    let names = newArray(@["a", "b", "c"])
    let values = newArray(@[1.0'f64, 2.0'f64, 3.0'f64])

    let original = newArrowTable(schema, ids, names, values)
    let uri = fixture / "selective.feather"

    writeFeatherFile(uri, original)

    # Read only specific columns
    let partial = readFeatherFile(uri, ["id", "name"])
    check partial.nColumns == 2
    check partial.schema.tryGetField("id").isSome
    check partial.schema.tryGetField("name").isSome
    check partial.schema.tryGetField("value").isNone
    check partial.nRows == 3

  test "read specific columns by index":
    let schema = newSchema(
      [newField[int32]("col0"), newField[string]("col1"), newField[float64]("col2")]
    )
    let col0 = newArray(@[1'i32, 2'i32])
    let col1 = newArray(@["x", "y"])
    let col2 = newArray(@[10.0'f64, 20.0'f64])

    let original = newArrowTable(schema, col0, col1, col2)
    let uri = fixture / "byindex.feather"

    writeFeatherFile(uri, original)

    # Read columns 0 and 2 only
    let partial = readFeatherFile(uri, [0, 2])
    check partial.nColumns == 2
    check partial.nRows == 2

  test "low-level reader API":
    let schema = newSchema([newField[int64]("value"), newField[float64]("score")])
    let values = newArray(@[100'i64, 200'i64, 300'i64])
    let scores = newArray(@[1.5'f64, 2.5'f64, 3.5'f64])
    let original = newArrowTable(schema, values, scores)

    let uri = fixture / "lowlevel.feather"
    writeFeatherFile(uri, original)

    let fs = newFileSystem("file://" & fixture.basePath)
    let stream = fs.openInputFile(uri)
    let reader = newFeatherReader(stream)

    check reader.version >= 1

    let table = reader.read()
    check table.nRows == 3
    check table.nColumns == 2

  test "write with custom properties":
    let schema = newSchema([newField[int32]("data")])
    let data = newArray(@[1'i32, 2'i32, 3'i32])
    let original = newArrowTable(schema, data)

    let uri = fixture / "custom_props.feather"
    let props = newFeatherWriteProperties()
    writeFeatherFile(uri, original, props)

    let restored = readFeatherFile(uri)
    check restored == original

  test "compression getter and setter":
    let props = newFeatherWriteProperties()

    props.compression = GARROW_COMPRESSION_TYPE_ZSTD
    check props.compression == GARROW_COMPRESSION_TYPE_ZSTD

    props.compression= GARROW_COMPRESSION_TYPE_UNCOMPRESSED
    check props.compression == GARROW_COMPRESSION_TYPE_UNCOMPRESSED

  test "write and read with ZSTD compression":
    let schema = newSchema([newField[int32]("id"), newField[float64]("value")])
    let ids = newArray(@[1'i32, 2'i32, 3'i32])
    let values = newArray(@[1.1'f64, 2.2'f64, 3.3'f64])
    let original = newArrowTable(schema, ids, values)

    let uri = fixture / "zstd.feather"
    let props = newFeatherWriteProperties()
    props.compression = GARROW_COMPRESSION_TYPE_ZSTD
    writeFeatherFile(uri, original, props)

    let restored = readFeatherFile(uri)
    check restored == original

  test "handles empty column selection":
    let schema = newSchema([newField[int32]("id"), newField[string]("name")])
    let ids = newArray(@[1'i32, 2'i32])
    let names = newArray(@["a", "b"])
    let original = newArrowTable(schema, ids, names)

    let uri = fixture / "empty_select.feather"
    writeFeatherFile(uri, original)

    # Empty array should return full table
    let full = readFeatherFile(uri, newSeq[string](0))
    check full.nColumns == 2
    check full.nRows == 2

  test "preserves different data types":
    let schema = newSchema(
      [
        newField[bool]("bool_col"),
        newField[int8]("int8_col"),
        newField[int16]("int16_col"),
        newField[int32]("int32_col"),
        newField[int64]("int64_col"),
        newField[float32]("float32_col"),
        newField[float64]("float64_col"),
      ]
    )

    let bools = newArray(@[true, false, true])
    let int8s = newArray(@[1'i8, 2'i8, 3'i8])
    let int16s = newArray(@[100'i16, 200'i16, 300'i16])
    let int32s = newArray(@[1000'i32, 2000'i32, 3000'i32])
    let int64s = newArray(@[10000'i64, 20000'i64, 30000'i64])
    let float32s = newArray(@[1.5'f32, 2.5'f32, 3.5'f32])
    let float64s = newArray(@[1.5'f64, 2.5'f64, 3.5'f64])

    let original =
      newArrowTable(schema, bools, int8s, int16s, int32s, int64s, float32s, float64s)
    let uri = fixture / "types.feather"

    writeFeatherFile(uri, original)
    let restored = readFeatherFile(uri)

    check restored == original
