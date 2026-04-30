import unittest2
import testfixture
import ../src/narrow

suite "Filtering parquet at reading":
  var fixture: TestFixture

  setup:
    fixture = newTestFixture("test_read_with_filtering")

  teardown:
    fixture.cleanup()

  test "readTable with filter returns matching rows":
    let schema = newSchema(
      [newField[int32]("age"), newField[string]("name")]
    )
    let ages = newArray(@[10'i32, 25, 30, 15, 40])
    let names = newArray(@["child", "young", "adult", "teen", "senior"])
    let table = newArrowTable(schema, ages, names)

    let uri = fixture / "people.parquet"
    writeTable(table, uri)

    let filtered = readTable(uri, col("age") >= 18'i32)
    unittest2.check filtered.nRows == 3
    unittest2.check filtered.nColumns == 2

  test "readTable with filter and column projection":
    let schema = newSchema(
      [newField[int32]("age"), newField[string]("name"), newField[bool]("active")]
    )
    let ages = newArray(@[10'i32, 25, 30])
    let names = newArray(@["a", "b", "c"])
    let active = newArray(@[true, false, true])
    let table = newArrowTable(schema, ages, names, active)

    let uri = fixture / "projected.parquet"
    writeTable(table, uri)

    # Filter on age, requesting only name column
    let filtered = readTable(uri, col("age") >= 18'i32, @["name"])
    unittest2.check filtered.nRows == 2
    unittest2.check filtered.nColumns == 1
    unittest2.check filtered.schema[0].name == "name"

  test "readTable with columns only returns requested columns in order":
    let schema = newSchema(
      [newField[int32]("a"), newField[string]("b"), newField[bool]("c")]
    )
    let avals = newArray(@[1'i32, 2, 3])
    let bvals = newArray(@["x", "y", "z"])
    let cvals = newArray(@[true, false, true])
    let table = newArrowTable(schema, avals, bvals, cvals)

    let uri = fixture / "ordered.parquet"
    writeTable(table, uri)

    let projected = readTable(uri, @["c", "a"])
    unittest2.check projected.nColumns == 2
    unittest2.check projected.schema[0].name == "c"
    unittest2.check projected.schema[1].name == "a"

  test "readTable with filter preserves requested column order":
    let schema = newSchema(
      [newField[int32]("z"), newField[string]("y"), newField[bool]("x")]
    )
    let zvals = newArray(@[1'i32, 2, 3])
    let yvals = newArray(@["a", "b", "c"])
    let xvals = newArray(@[true, false, true])
    let table = newArrowTable(schema, zvals, yvals, xvals)

    let uri = fixture / "ordered_filter.parquet"
    writeTable(table, uri)

    # Filter on z, requesting y then x
    let filtered = readTable(uri, col("z") >= 2'i32, @["y", "x"])
    unittest2.check filtered.nRows == 2
    unittest2.check filtered.nColumns == 2
    unittest2.check filtered.schema[0].name == "y"
    unittest2.check filtered.schema[1].name == "x"

  test "readTable with filter and no columns returns all columns in schema order":
    let schema = newSchema(
      [newField[int32]("z"), newField[string]("y"), newField[bool]("x")]
    )
    let zvals = newArray(@[1'i32, 2, 3])
    let yvals = newArray(@["a", "b", "c"])
    let xvals = newArray(@[true, false, true])
    let table = newArrowTable(schema, zvals, yvals, xvals)

    let uri = fixture / "all_cols.parquet"
    writeTable(table, uri)

    let filtered = readTable(uri, col("z") >= 2'i32)
    unittest2.check filtered.nRows == 2
    unittest2.check filtered.nColumns == 3
    unittest2.check filtered.schema[0].name == "z"
    unittest2.check filtered.schema[1].name == "y"
    unittest2.check filtered.schema[2].name == "x"

  test "readTable with filter on non-existent column raises KeyError":
    let schema = newSchema([newField[int32]("x")])
    let xs = newArray(@[1'i32])
    let table = newArrowTable(schema, xs)
    let uri = fixture / "small.parquet"
    writeTable(table, uri)

    expect(KeyError):
      discard readTable(uri, col("nonexistent") > 5'i32)

  test "readTable with filter that matches nothing":
    let schema = newSchema([newField[int32]("v")])
    let vs = newArray(@[1'i32, 2, 3])
    let table = newArrowTable(schema, vs)

    let uri = fixture / "noresult.parquet"
    writeTable(table, uri)

    let filtered = readTable(uri, col("v") > 100'i32)
    unittest2.check filtered.nRows == 0

  test "readTable with columns parameter on non-existent column raises KeyError":
    let schema = newSchema([newField[int32]("x"), newField[string]("y")])
    let xs = newArray(@[1'i32, 2])
    let ys = newArray(@["a", "b"])
    let table = newArrowTable(schema, xs, ys)
    let uri = fixture / "twocols.parquet"
    writeTable(table, uri)

    expect(KeyError):
      discard readTable(uri, col("x") > 0'i32, @["nonexistent"])

  # test "foo":
  #   let filter = ((col("role") == "admin") and (col("age") >= 25'i32)) or ((col("active") == true) and (col("age") < 30'i32))
  #   echo "\nFilter expression:  \n", filter
  #   echo "\nTree:\n---------------"

  #   walk(filter, ExprVisitor(
  #       onCallPost: proc(e: Expression) = echo e
  #   ))
