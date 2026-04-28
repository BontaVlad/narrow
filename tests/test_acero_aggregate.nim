import unittest2
import ../src/narrow

suite "Acero - Aggregate Table":
  test "global sum aggregation without group by":
    let schema = newSchema([
      newField[string]("category"),
      newField[int32]("amount"),
    ])
    let categories = newArray(@["a", "b", "a", "b", "a"])
    let amounts = newArray(@[10'i32, 20, 30, 40, 50])
    let table = newArrowTable(schema, categories, amounts)

    let result = aggregateTable(table, [], [
      (field: "amount", fn: "sum", output: "total"),
    ])

    unittest2.check result.nRows == 1
    unittest2.check result.nColumns == 1
    unittest2.check result.schema[0].name == "total"

  test "single-key group by with sum":
    let schema = newSchema([
      newField[string]("category"),
      newField[int32]("amount"),
    ])
    let categories = newArray(@["a", "b", "a", "b", "a"])
    let amounts = newArray(@[10'i32, 20, 30, 40, 50])
    let table = newArrowTable(schema, categories, amounts)

    let result = aggregateTable(table, ["category"], [
      (field: "amount", fn: "sum", output: "total"),
    ])

    unittest2.check result.nRows == 2
    unittest2.check result.nColumns == 2

  test "fluent groupBy aggregate API":
    let schema = newSchema([
      newField[string]("category"),
      newField[int32]("amount"),
    ])
    let categories = newArray(@["a", "b", "a", "b", "a"])
    let amounts = newArray(@[10'i32, 20, 30, 40, 50])
    let table = newArrowTable(schema, categories, amounts)

    let result = table.groupBy("category").aggregate([
      (field: "amount", fn: "sum", output: "total"),
    ])

    unittest2.check result.nRows == 2
    unittest2.check result.nColumns == 2

  test "multiple aggregations on same field":
    let schema = newSchema([
      newField[string]("category"),
      newField[int32]("amount"),
    ])
    let categories = newArray(@["a", "b", "a", "b", "a"])
    let amounts = newArray(@[10'i32, 20, 30, 40, 50])
    let table = newArrowTable(schema, categories, amounts)

    let result = table.groupBy("category").aggregate([
      (field: "amount", fn: "sum", output: "total"),
      (field: "amount", fn: "count", output: "n"),
    ])

    unittest2.check result.nRows == 2
    unittest2.check result.nColumns == 3

  test "empty aggregations list raises ValueError":
    let schema = newSchema([newField[int32]("x")])
    let xs = newArray(@[1'i32, 2, 3])
    let table = newArrowTable(schema, xs)

    expect ValueError:
      discard aggregateTable(table, [], [])
