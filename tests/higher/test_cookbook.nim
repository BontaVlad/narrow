import std/[os, sequtils]
import unittest2
import ../testfixture

import ../../src/narrow/[column/primitive, column/metadata, tabular/table, io/parquet]

suite "Reading and Writing Data":

  var fixture: TestFixture

  setup:
    fixture = newTestFixture("test_parquet")

  teardown:
    fixture.cleanup()

  test "Write a parquet file":
    let schema = newSchema([
      newField[int]("id"),
    ])
    let data = newArray(toSeq(0 .. 99))

    let table = newArrowTable(schema, data)
    let uri = fixture / "table.parquet"
    writeTable(table, uri)

  test "Read a Parquet file":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let table = readTable(uri)
    check table["int64_col"].len == 10

  test "Reading a subset of Parquet data":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let table = readTable(uri, columns = @["int64_col", "string_col"])
    check table["int64_col"].len == 10

suite "Creating Arrow Objects":
  test "Creating Arrays":
    let arr = @[1, 2, 3, 4, 5]
    let garr = newArray[int](arr)
    check garr == arr

  test "Creating Arrays with mask to speccify which values should be considered null":
    let arr = @[1, 2, 3, 4, 5]
    let mask = @[true, false, true, false, true] # Mask to specify null values
    let garr = newArray[int](arr, mask)
    for i in 0 ..< arr.len:
      if mask[i]:
        check garr.isNull(i) # Check if the value is considered null
      else:
        check garr.isValid(i)
        check garr[i] == arr[i] # Check if the value is correctly included

  test "Creating Tables":
    let schema = newSchema([
      newField[int]("id"),
      newField[string]("name"),
      newField[float64]("value")
    ])

    let idArr = newArray(@[1, 2, 3, 4, 5])
    let nameArr = newArray(@["a", "b", "c", "d", "e"])
    let valueArr = newArray(@[1.0, 2.0, 3.0, 4.0, 5.0])
    
    let table = newArrowTable(schema, idArr, nameArr, valueArr)
    check table["id"] == newChunkedArray(@[idArr])
    check table["name"] == newChunkedArray(@[nameArr])
    check table[2] == newChunkedArray(@[valueArr])

  test "Creating Tables from tuples":
    let data = @[
        (col1: 1, col2: "a"),
        (col1: 2, col2: "b"),
        (col1: 3, col2: "c"),
        (col1: 4, col2: "d"),
        (col1: 5, col2: "e")
    ]
    let table = newArrowTable(data)
    check table["col1"] == newChunkedArray(@[newArray(@[1, 2, 3, 4, 5])])
    check table["col2"] == newChunkedArray(@[newArray(@["a", "b", "c", "d", "e"])])

