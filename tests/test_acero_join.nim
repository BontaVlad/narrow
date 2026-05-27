import unittest2
import ../src/narrow

suite "Acero - Hash Join":
  test "inner join on single integer key":
    let leftSchema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let rightSchema = newSchema([
      newField[int32]("id"),
      newField[string]("score"),
    ])
    let leftIds = newArray(@[1'i32, 2, 3, 4])
    let leftNames = newArray(@["a", "b", "c", "d"])
    let rightIds = newArray(@[2'i32, 3, 5])
    let rightScores = newArray(@["x", "y", "z"])

    let left = newArrowTable(leftSchema, leftIds, leftNames)
    let right = newArrowTable(rightSchema, rightIds, rightScores)

    let result = joinTables(left, right, jtInner, ["id"], ["id"])

    check result.nRows == 2   # ids 2, 3
    check result.nColumns == 4  # id, name, id, score

  test "inner join single string key":
    let leftSchema = newSchema([
      newField[string]("code"),
      newField[int32]("value"),
    ])
    let rightSchema = newSchema([
      newField[string]("code"),
      newField[string]("label"),
    ])
    let leftCodes = newArray(@["aa", "bb", "cc"])
    let leftValues = newArray(@[1'i32, 2, 3])
    let rightCodes = newArray(@["bb", "cc", "dd"])
    let rightLabels = newArray(@["BB", "CC", "DD"])

    let left = newArrowTable(leftSchema, leftCodes, leftValues)
    let right = newArrowTable(rightSchema, rightCodes, rightLabels)

    let result = joinTables(left, right, jtInner, ["code"], ["code"])

    check result.nRows == 2   # codes bb, cc
    check result.nColumns == 4  # code, value, code, label

  test "left outer join preserves all left rows":
    let leftSchema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let rightSchema = newSchema([
      newField[int32]("id"),
      newField[string]("score"),
    ])
    let leftIds = newArray(@[1'i32, 2, 3, 4])
    let leftNames = newArray(@["a", "b", "c", "d"])
    let rightIds = newArray(@[1'i32, 3])
    let rightScores = newArray(@["x", "y"])

    let left = newArrowTable(leftSchema, leftIds, leftNames)
    let right = newArrowTable(rightSchema, rightIds, rightScores)

    let result = joinTables(left, right, jtLeftOuter, ["id"], ["id"])

    check result.nRows == 4   # all 4 left rows
    check result.nColumns == 4  # id, name, id, score

  test "right outer join preserves all right rows":
    let leftSchema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let rightSchema = newSchema([
      newField[int32]("id"),
      newField[string]("score"),
    ])
    let leftIds = newArray(@[1'i32, 2])
    let leftNames = newArray(@["a", "b"])
    let rightIds = newArray(@[1'i32, 2, 3, 4])
    let rightScores = newArray(@["x", "y", "z", "w"])

    let left = newArrowTable(leftSchema, leftIds, leftNames)
    let right = newArrowTable(rightSchema, rightIds, rightScores)

    let result = joinTables(left, right, jtRightOuter, ["id"], ["id"])

    check result.nRows == 4   # all 4 right rows
    check result.nColumns == 4  # id, name, id, score

  test "full outer join":
    let leftSchema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let rightSchema = newSchema([
      newField[int32]("id"),
      newField[string]("score"),
    ])
    let leftIds = newArray(@[1'i32, 2, 3])
    let leftNames = newArray(@["a", "b", "c"])
    let rightIds = newArray(@[2'i32, 3, 4])
    let rightScores = newArray(@["x", "y", "z"])

    let left = newArrowTable(leftSchema, leftIds, leftNames)
    let right = newArrowTable(rightSchema, rightIds, rightScores)

    let result = joinTables(left, right, jtFullOuter, ["id"], ["id"])

    check result.nRows == 4   # 1, 2, 3, 4 join key values
    check result.nColumns == 4  # id, name, id, score

  test "multi-key join":
    let leftSchema = newSchema([
      newField[string]("region"),
      newField[int32]("year"),
      newField[string]("product"),
    ])
    let rightSchema = newSchema([
      newField[string]("region"),
      newField[int32]("year"),
      newField[int32]("revenue"),
    ])
    let leftRegions = newArray(@["US", "UK", "US", "UK"])
    let leftYears = newArray(@[2020'i32, 2020, 2021, 2021])
    let leftProducts = newArray(@["A", "B", "C", "D"])
    let rightRegions = newArray(@["US", "UK", "US"])
    let rightYears = newArray(@[2020'i32, 2021, 2020])
    let rightRevenues = newArray(@[100'i32, 200, 300])

    let left = newArrowTable(leftSchema, leftRegions, leftYears, leftProducts)
    let right = newArrowTable(rightSchema, rightRegions, rightYears, rightRevenues)

    let result = joinTables(left, right, jtInner, ["region", "year"],
                            ["region", "year"])

    check result.nRows == 3   # US/2020 appears twice on right
    check result.nColumns == 6  # region, year, product, region, year, revenue

  test "inner join with no matching keys returns empty table":
    let leftSchema = newSchema([
      newField[int32]("id"),
      newField[string]("data"),
    ])
    let rightSchema = newSchema([
      newField[int32]("id"),
      newField[string]("info"),
    ])
    let leftIds = newArray(@[1'i32, 2, 3])
    let leftData = newArray(@["a", "b", "c"])
    let rightIds = newArray(@[99'i32, 100])
    let rightInfo = newArray(@["x", "y"])

    let left = newArrowTable(leftSchema, leftIds, leftData)
    let right = newArrowTable(rightSchema, rightIds, rightInfo)

    let result = joinTables(left, right, jtInner, ["id"], ["id"])

    check result.nRows == 0
    check result.nColumns == 4  # id, data, id, info

  test "left semi join":
    let leftSchema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let rightSchema = newSchema([
      newField[int32]("id"),
    ])
    let leftIds = newArray(@[1'i32, 2, 3, 4, 5])
    let leftNames = newArray(@["a", "b", "c", "d", "e"])
    let rightIds = newArray(@[2'i32, 4])

    let left = newArrowTable(leftSchema, leftIds, leftNames)
    let right = newArrowTable(rightSchema, rightIds)

    let result = joinTables(left, right, jtLeftSemi, ["id"], ["id"])

    check result.nRows == 2   # only rows with id 2, 4
    check result.nColumns == 2  # only left columns (name, id)

  test "left anti join":
    let leftSchema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let rightSchema = newSchema([
      newField[int32]("id"),
    ])
    let leftIds = newArray(@[1'i32, 2, 3, 4, 5])
    let leftNames = newArray(@["a", "b", "c", "d", "e"])
    let rightIds = newArray(@[2'i32, 4])

    let left = newArrowTable(leftSchema, leftIds, leftNames)
    let right = newArrowTable(rightSchema, rightIds)

    let result = joinTables(left, right, jtLeftAnti, ["id"], ["id"])

    check result.nRows == 3   # ids 1, 3, 5 (in left but not in right)
    check result.nColumns == 2  # only left columns
