import std/[sequtils]
import unittest2
import ../src/narrow

suite "Array sortIndices":
  test "sortIndices returns correct permutation for ascending":
    let arr = newArray(@[3'i32, 1, 2])
    let indices = sortIndices(arr, Ascending)
    check indices.len == 3
    check indices[0] == 1'u64  # value 1 is at index 1
    check indices[1] == 2'u64  # value 2 is at index 2
    check indices[2] == 0'u64  # value 3 is at index 0

  test "sortIndices returns correct permutation for descending":
    let arr = newArray(@[3'i32, 1, 2])
    let indices = sortIndices(arr, Descending)
    check indices.len == 3
    check indices[0] == 0'u64  # value 3 is at index 0
    check indices[1] == 2'u64  # value 2 is at index 2
    check indices[2] == 1'u64  # value 1 is at index 1

  test "sortIndices on empty array returns empty":
    let arr = newArray[int32](@[])
    let indices = sortIndices(arr)
    check indices.len == 0

  test "sortIndices on single element":
    let arr = newArray(@[42'i32])
    let indices = sortIndices(arr)
    check indices.len == 1
    check indices[0] == 0'u64

  test "sortIndices on already sorted":
    let arr = newArray(@[1'i32, 2, 3, 4])
    let indices = sortIndices(arr)
    check indices[0] == 0'u64
    check indices[1] == 1'u64
    check indices[2] == 2'u64
    check indices[3] == 3'u64

  test "sortIndices on reverse sorted":
    let arr = newArray(@[4'i32, 3, 2, 1])
    let indices = sortIndices(arr)
    check indices[0] == 3'u64
    check indices[1] == 2'u64
    check indices[2] == 1'u64
    check indices[3] == 0'u64

  test "sortIndices on duplicate values preserves stable order":
    let arr = newArray(@[2'i32, 1, 2, 1])
    let indices = sortIndices(arr, Ascending)
    check indices.len == 4
    # Values 1 at indices 1 and 3, then values 2 at indices 0 and 2
    check indices[0] == 1'u64
    check indices[1] == 3'u64
    check indices[2] == 0'u64
    check indices[3] == 2'u64

  test "sortIndices on float array":
    let arr = newArray(@[3.5'f64, 1.2, 2.8])
    let indices = sortIndices(arr)
    check indices[0] == 1'u64  # 1.2
    check indices[1] == 2'u64  # 2.8
    check indices[2] == 0'u64  # 3.5

  test "sortIndices on string array":
    let arr = newArray(@["charlie", "alice", "bob"])
    let indices = sortIndices(arr)
    check indices[0] == 1'u64  # alice
    check indices[1] == 2'u64  # bob
    check indices[2] == 0'u64  # charlie

suite "Table sortIndices":
  test "sortIndices on table with single key":
    let schema = newSchema([
      newField[string]("name"),
      newField[int32]("age"),
    ])
    let names = newArray(@["charlie", "alice", "bob"])
    let ages = newArray(@[30'i32, 25, 27])
    let table = newArrowTable(schema, names, ages)

    let indices = sortIndices(table, @[newSortKey("age", Ascending)])
    check indices.len == 3
    check indices[0] == 1'u64  # alice, age 25
    check indices[1] == 2'u64  # bob, age 27
    check indices[2] == 0'u64  # charlie, age 30

  test "sortIndices on table with descending key":
    let schema = newSchema([
      newField[string]("name"),
      newField[int32]("age"),
    ])
    let names = newArray(@["charlie", "alice", "bob"])
    let ages = newArray(@[30'i32, 25, 27])
    let table = newArrowTable(schema, names, ages)

    let indices = sortIndices(table, @[newSortKey("age", Descending)])
    check indices[0] == 0'u64  # charlie, age 30
    check indices[1] == 2'u64  # bob, age 27
    check indices[2] == 1'u64  # alice, age 25

  test "sortIndices on table with multiple keys":
    let schema = newSchema([
      newField[string]("category"),
      newField[int32]("score"),
    ])
    let categories = newArray(@["b", "a", "a", "b"])
    let scores = newArray(@[20'i32, 30, 10, 15])
    let table = newArrowTable(schema, categories, scores)

    let indices = sortIndices(table, @[
      newSortKey("category", Ascending),
      newSortKey("score", Descending),
    ])
    check indices.len == 4
    # category a: score 30 (idx 1), score 10 (idx 2)
    # category b: score 20 (idx 0), score 15 (idx 3)
    check indices[0] == 1'u64
    check indices[1] == 2'u64
    check indices[2] == 0'u64
    check indices[3] == 3'u64

  test "sortIndices on empty table returns empty":
    let schema = newSchema([newField[int32]("x")])
    let empty = newArray[int32](@[])
    let table = newArrowTable(schema, empty)
    let indices = sortIndices(table, @[newSortKey("x")])
    check indices.len == 0

suite "Array take":
  test "take reorders elements by index":
    let arr = newArray(@[10'i32, 20, 30])
    let indices = newArray(@[2'u64, 0, 1])
    let result = take(arr, indices)
    check result.len == 3
    check result[0] == 30
    check result[1] == 10
    check result[2] == 20

  test "take with identity indices returns copy":
    let arr = newArray(@[10'i32, 20, 30])
    let indices = newArray(@[0'u64, 1, 2])
    let result = take(arr, indices)
    check result[0] == 10
    check result[1] == 20
    check result[2] == 30

  test "take with reverse indices":
    let arr = newArray(@[10'i32, 20, 30])
    let indices = newArray(@[2'u64, 1, 0])
    let result = take(arr, indices)
    check result[0] == 30
    check result[1] == 20
    check result[2] == 10

  test "take empty indices returns empty":
    let arr = newArray(@[10'i32, 20, 30])
    let indices = newArray[uint64](@[])
    let result = take(arr, indices)
    check result.len == 0

  test "take on empty array returns empty":
    let arr = newArray[int32](@[])
    let indices = newArray[uint64](@[])
    let result = take(arr, indices)
    check result.len == 0

suite "Table take":
  test "take reorders rows by index":
    let schema = newSchema([
      newField[string]("name"),
      newField[int32]("age"),
    ])
    let names = newArray(@["alice", "bob", "charlie"])
    let ages = newArray(@[25'i32, 30, 27])
    let table = newArrowTable(schema, names, ages)

    let indices = newArray(@[2'u64, 0, 1])
    let result = take(table, indices)
    check result.nRows == 3
    check result.nColumns == 2
    let resultNames = result["name", string]
    check resultNames[0] == "charlie"
    check resultNames[1] == "alice"
    check resultNames[2] == "bob"
    let resultAges = result["age", int32]
    check resultAges[0] == 27
    check resultAges[1] == 25
    check resultAges[2] == 30

  test "take subset of rows":
    let schema = newSchema([newField[int32]("x")])
    let data = newArray(@[100'i32, 200, 300, 400])
    let table = newArrowTable(schema, data)

    let indices = newArray(@[1'u64, 3])
    let result = take(table, indices)
    check result.nRows == 2
    let col = result["x", int32]
    check col[0] == 200
    check col[1] == 400

  test "take empty indices returns empty table":
    let schema = newSchema([newField[int32]("x")])
    let data = newArray(@[100'i32, 200])
    let table = newArrowTable(schema, data)

    let indices = newArray[uint64](@[])
    let result = take(table, indices)
    check result.nRows == 0
    check result.nColumns == 1

suite "Sort + Take composition":
  test "sort then take produces sorted table":
    let schema = newSchema([
      newField[string]("name"),
      newField[int32]("age"),
    ])
    let names = newArray(@["charlie", "alice", "bob"])
    let ages = newArray(@[30'i32, 25, 27])
    let table = newArrowTable(schema, names, ages)

    let indices = sortIndices(table, @[newSortKey("age", Ascending)])
    let sorted = take(table, indices)

    let sortedNames = sorted["name", string]
    check sortedNames[0] == "alice"
    check sortedNames[1] == "bob"
    check sortedNames[2] == "charlie"

    let sortedAges = sorted["age", int32]
    check sortedAges[0] == 25
    check sortedAges[1] == 27
    check sortedAges[2] == 30

  test "sort then take on array produces sorted array":
    let arr = newArray(@[3'i32, 1, 4, 1, 5])
    let indices = sortIndices(arr)
    let sorted = take(arr, indices)
    check sorted[0] == 1
    check sorted[1] == 1
    check sorted[2] == 3
    check sorted[3] == 4
    check sorted[4] == 5

suite "sortBy convenience":
  test "sortBy on table returns fully sorted table":
    let schema = newSchema([
      newField[string]("name"),
      newField[int32]("age"),
    ])
    let names = newArray(@["charlie", "alice", "bob"])
    let ages = newArray(@[30'i32, 25, 27])
    let table = newArrowTable(schema, names, ages)

    let sorted = sortBy(table, @[("age", Ascending)])
    check sorted.nRows == 3
    let sortedNames = sorted["name", string]
    check sortedNames[0] == "alice"
    check sortedNames[1] == "bob"
    check sortedNames[2] == "charlie"

  test "sortBy with descending":
    let schema = newSchema([newField[int32]("score")])
    let scores = newArray(@[50'i32, 90, 70])
    let table = newArrowTable(schema, scores)

    let sorted = sortBy(table, @[("score", Descending)])
    let col = sorted["score", int32]
    check col[0] == 90
    check col[1] == 70
    check col[2] == 50

suite "Null handling":
  test "sortIndices places nulls last by default":
    let arr = newArray(@[3'i32, 1, 2], mask = [false, true, false])
    # index 1 is null, indices 0 and 2 are valid
    let indices = sortIndices(arr)
    check indices.len == 3
    # null goes last in Arrow's default sort
    check indices[2] == 1'u64  # null at index 1

  test "take propagates nulls correctly":
    let arr = newArray(@[10'i32, 20, 30], mask = [false, true, false])
    let indices = newArray(@[0'u64, 1, 2])
    let result = take(arr, indices)
    check result[0] == 10
    check result[1] == 0  # null value default
    check result[2] == 30
