import unittest2
import ../src/narrow

suite "Acero - Filter Table":
  test "filter table with simple comparison":
    let schema = newSchema([newField[int32]("age"), newField[string]("name")])
    let ages = newArray(@[10'i32, 25, 30, 15, 40])
    let names = newArray(@["child", "young", "adult", "teen", "senior"])
    let table = newArrowTable(schema, ages, names)

    let filter = col("age") >= 18'i32
    let filtered = filterTable(table, filter)

    unittest2.check filtered.nRows == 3  # 25, 30, 40

  test "filter with AND expression":
    let schema = newSchema([newField[int32]("x")])
    let xs = newArray(@[1'i32, 5, 10, 15, 20])
    let table = newArrowTable(schema, xs)

    let filter = (col("x") > 3'i32) and (col("x") < 16'i32)
    let filtered = filterTable(table, filter)

    unittest2.check filtered.nRows == 3  # 5, 10, 15

  test "filter that matches nothing returns empty table":
    let schema = newSchema([newField[int32]("v")])
    let vs = newArray(@[1'i32, 2, 3])
    let table = newArrowTable(schema, vs)

    let filter = col("v") > 100'i32
    let filtered = filterTable(table, filter)

    unittest2.check filtered.nRows == 0
    unittest2.check filtered.nColumns == 1  # schema preserved
