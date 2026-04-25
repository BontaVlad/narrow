import criterion
import ../src/narrow
import ./config

let cfg = narrowConfig()

benchmark cfg:

  proc benchBuildSimpleExpression {.measure.} =
    let age = col("age")
    let threshold = newLiteralExpression(18'i32)
    discard age >= threshold

  proc benchBuildComplexExpression {.measure.} =
    let age = col("age")
    let name = col("name")
    let salary = col("salary")
    discard (age >= 18'i32) and (salary > 50000.0) and (name == "Alice")

  proc benchExpressionTreeWalk {.measure.} =
    let expr = (col("a") >= 1'i32) and (col("b") < 100'i32) or (col("c") == "test")
    var count = 0
    proc countNodes(e: Expression) =
      if e.isNil:
        return
      count += 1
      for child in e.children:
        countNodes(child)
    countNodes(expr)

  proc benchReferencedFields {.measure.} =
    let expr = (col("a") >= 1'i32) and (col("b") < 100'i32) or (col("c") == "test")
    discard expr.referencedFields()

  proc benchParseFilter {.measure.} =
    let filters = @[
      (field: "age", op: ">=", value: "18"),
      (field: "name", op: "==", value: "Alice"),
      (field: "salary", op: ">", value: "50000"),
    ]
    discard parse(filters)
