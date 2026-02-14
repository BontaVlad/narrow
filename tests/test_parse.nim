import unittest2
import ../src/narrow/[core/ffi, compute/expressions, types/gtypes]

suite "parseValue - Type Detection":
  test "Parse boolean true":
    let parsed = parseValue("true")
    let dsl = newLiteralExpression(true)
    check $parsed == $dsl

  test "Parse boolean false (case insensitive)":
    let parsed = parseValue("FALSE")
    let dsl = newLiteralExpression(false)
    check $parsed == $dsl

  test "Parse int32 value":
    let parsed = parseValue("42")
    let dsl = newLiteralExpression(42'i32)
    check $parsed == $dsl

  test "Parse negative int32":
    let parsed = parseValue("-100")
    let dsl = newLiteralExpression(-100'i32)
    check $parsed == $dsl

  test "Parse large int64 value":
    let parsed = parseValue("9999999999")  # > int32.max
    let dsl = newLiteralExpression(9999999999'i64)
    check $parsed == $dsl

  test "Parse float64 value":
    let parsed = parseValue("3.14159")
    let dsl = newLiteralExpression(3.14159'f64)
    check $parsed == $dsl

  test "Parse scientific notation float":
    let parsed = parseValue("1.5e10")
    let dsl = newLiteralExpression(1.5e10'f64)
    check $parsed == $dsl

  test "Parse string value":
    let parsed = parseValue("hello world")
    let dsl = newLiteralExpression("hello world")
    check $parsed == $dsl

  test "Parse mixed alphanumeric as string":
    let parsed = parseValue("abc123")
    let dsl = newLiteralExpression("abc123")
    check $parsed == $dsl

suite "parseFilter - Single Filter Parsing":
  test "Parse equality filter with int":
    let parsed = parseFilter(("age", "==", "25"))
    let age = newFieldExpression("age")
    let dsl = eq(age, 25'i32)
    check $parsed == $dsl

  test "Parse greater-than filter with float":
    let parsed = parseFilter(("salary", ">", "50000.50"))
    let salary = newFieldExpression("salary")
    let dsl = gt(salary, 50000.50'f64)
    check $parsed == $dsl

  test "Parse less-than-or-equal filter":
    let parsed = parseFilter(("score", "<=", "100"))
    let score = newFieldExpression("score")
    let dsl = le(score, 100'i32)
    check $parsed == $dsl

  test "Parse not-equal filter with string":
    let parsed = parseFilter(("status", "!=", "inactive"))
    let status = newFieldExpression("status")
    let dsl = neq(status, "inactive")
    check $parsed == $dsl

  test "Parse contains filter":
    let parsed = parseFilter(("name", "contains", "Alice"))
    let name = newFieldExpression("name")
    let dsl = strContains(name, "Alice")
    check $parsed == $dsl

  test "Parse greater-than-or-equal filter with bool":
    let parsed = parseFilter(("active", ">=", "true"))
    let active = newFieldExpression("active")
    let dsl = ge(active, true)
    check $parsed == $dsl

  test "Unknown operator raises ValueError":
    expect(ValueError):
      discard parseFilter(("field", "unknown", "value"))

suite "parse - Multiple Filter Combination":
  test "Single filter returns valid expression":
    let filters: seq[FilterClause] = @[("age", ">=", "18")]
    let parsed = parse(filters)
    let age = newFieldExpression("age")
    let dsl = ge(age, 18'i32)
    check $parsed == $dsl

  test "Two filters combined with AND":
    let filters: seq[FilterClause] = @[
      ("age", ">=", "18"),
      ("active", "==", "true")
    ]
    let parsed = parse(filters)
    let age = newFieldExpression("age")
    let active = newFieldExpression("active")
    let dsl = andExpr(ge(age, 18'i32), eq(active, true))
    check $parsed == $dsl

  test "Three filters combined with AND":
    let filters: seq[FilterClause] = @[
      ("age", ">=", "18"),
      ("salary", ">", "50000.0"),
      ("name", "contains", "Smith")
    ]
    let parsed = parse(filters)
    let age = newFieldExpression("age")
    let salary = newFieldExpression("salary")
    let name = newFieldExpression("name")
    let dsl = andExpr(
      andExpr(ge(age, 18'i32), gt(salary, 50000.0'f64)),
      strContains(name, "Smith")
    )
    check $parsed == $dsl

  test "Empty filter sequence raises ValueError":
    expect(ValueError):
      discard parse(@[])

suite "parse - End-to-End Type Detection":
  test "Complex filter with mixed types":
    let filters: seq[FilterClause] = @[
      ("user_id", "==", "12345"),           # int
      ("rating", ">=", "4.5"),              # float
      ("verified", "==", "true"),           # bool
      ("email", "contains", "@gmail.com")   # string
    ]
    let parsed = parse(filters)
    let userId = newFieldExpression("user_id")
    let rating = newFieldExpression("rating")
    let verified = newFieldExpression("verified")
    let email = newFieldExpression("email")
    let dsl = andExpr(
      andExpr(
        andExpr(eq(userId, 12345'i32), ge(rating, 4.5'f64)),
        eq(verified, true)
      ),
      strContains(email, "@gmail.com")
    )
    check $parsed == $dsl

  test "Edge case - zero values":
    let filters: seq[FilterClause] = @[
      ("count", ">=", "0"),
      ("temperature", ">", "-10.5")
    ]
    let parsed = parse(filters)
    let count = newFieldExpression("count")
    let temp = newFieldExpression("temperature")
    let dsl = andExpr(ge(count, 0'i32), gt(temp, -10.5'f64))
    check $parsed == $dsl

  test "Edge case - max int32 boundary":
    let filters: seq[FilterClause] = @[("id", "<", "2147483647")]  # int32.max
    let parsed = parse(filters)
    let id = newFieldExpression("id")
    let dsl = lt(id, 2147483647'i32)
    check $parsed == $dsl

  test "Edge case - beyond int32 becomes int64":
    let filters: seq[FilterClause] = @[("big_id", ">", "2147483648")]  # > int32.max
    let parsed = parse(filters)
    let bigId = newFieldExpression("big_id")
    let dsl = gt(bigId, 2147483648'i64)
    check $parsed == $dsl
