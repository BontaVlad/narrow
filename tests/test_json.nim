import std/[os]
import unittest2
import testfixture
import ../src/narrow

suite "JSON Reader":
  test "readJSON reads simple JSON file":
    let uri = getCurrentDir() & "/tests/fixtures/simple.json"
    let table = readJSON(uri)
    check table.nRows == 3
    check table.nColumns == 4

    let idCol = table["id", int64]
    check idCol[0] == 1
    check idCol[1] == 2
    check idCol[2] == 3

    let nameCol = table["name", string]
    check nameCol[0] == "Alice"
    check nameCol[1] == "Bob"
    check nameCol[2] == "Charlie"

    let ageCol = table["age", int64]
    check ageCol[0] == 30
    check ageCol[1] == 25
    check ageCol[2] == 35

    let activeCol = table["active", bool]
    check activeCol[0] == true
    check activeCol[1] == false
    check activeCol[2] == true

  test "readJSON with InferType behavior":
    let uri = getCurrentDir() & "/tests/fixtures/simple.json"
    let options = newJsonReadOptions(InferType)
    let table = readJSON(uri, options)
    check table.nRows == 3
    check table.nColumns == 4

    let idCol = table["id", int64]
    check idCol[0] == 1
    check idCol[1] == 2
    check idCol[2] == 3

    let nameCol = table["name", string]
    check nameCol[0] == "Alice"
    check nameCol[1] == "Bob"
    check nameCol[2] == "Charlie"

  test "readJSON with Error behavior":
    let uri = getCurrentDir() & "/tests/fixtures/simple.json"
    let options = newJsonReadOptions(unexpectedFieldBehavior = Error)
    let table = readJSON(uri, options)
    check table.nRows == 3
    check table.nColumns == 4

    let idCol = table["id", int64]
    check idCol[0] == 1
    check idCol[1] == 2
    check idCol[2] == 3

    let nameCol = table["name", string]
    check nameCol[0] == "Alice"
    check nameCol[1] == "Bob"
    check nameCol[2] == "Charlie"

  test "JsonReadOptions property getters/setters":
    var options = newJsonReadOptions(Ignore)
    check options.unexpectedFieldBehavior == Ignore

    options.unexpectedFieldBehavior = InferType
    check options.unexpectedFieldBehavior == InferType

    options.unexpectedFieldBehavior = Error
    check options.unexpectedFieldBehavior == Error

  test "readJSON with absolute path":
    let uri = getCurrentDir() & "/tests/fixtures/simple.json"
    let table = readJSON(uri)
    check table.nRows == 3
    check table.nColumns == 4

    let idCol = table["id", int64]
    check idCol[0] == 1
    check idCol[1] == 2
    check idCol[2] == 3

    let nameCol = table["name", string]
    check nameCol[0] == "Alice"
    check nameCol[1] == "Bob"
    check nameCol[2] == "Charlie"

suite "JSON with null values":
  var fixture: TestFixture

  setup:
    fixture = newTestFixture("test_io/json")
    let nullJsonPath = fixture / "with_nulls.json"
    writeFile(nullJsonPath, """{"id": 1, "name": "Alice", "score": null}
{"id": 2, "name": null, "score": 95.5}
{"id": 3, "name": "Bob", "score": 87.0}""")

  teardown:
    fixture.cleanup()

  test "JSON file with missing/null values":
    let nullJsonPath = fixture / "with_nulls.json"
    let table = readJSON(nullJsonPath)
    check table.nRows == 3
    check table.nColumns == 3

    let idCol = table["id", int64]
    check idCol[0] == 1
    check idCol[1] == 2
    check idCol[2] == 3

    let scoreCol = table["score", float64]
    check scoreCol[1] == 95.5
    check scoreCol[2] == 87.0

suite "JSON Options":
  test "Empty JSON options construction":
    let options = newJsonReadOptions()
    check not options.handle.isNil

  test "Schema is correctly inferred from JSON":
    let uri = getCurrentDir() & "/tests/fixtures/simple.json"
    let table = readJSON(uri)
    let schema = table.schema

    check schema.nFields == 4
    check schema.getFieldByName("id").dataType.isCompatible(int64)
    check schema.getFieldByName("name").dataType.isCompatible(string)
    check schema.getFieldByName("age").dataType.isCompatible(int64)
    check schema.getFieldByName("active").dataType.isCompatible(bool)
