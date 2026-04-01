import std/strutils, options
import unittest2
import testfixture
import ../src/narrow/[core/ffi, compute/expressions, types/gtypes, column/primitive, tabular/table, tabular/batch, column/metadata, io/parquet]

suite "Datum - Construction and Basic Operations":
  test "Create Datum from Array and verify type checking":
    let arr = newArray(@[1'i32, 2'i32, 3'i32])
    let dt = newDatum(arr)

    check dt.toPtr != nil
    check dt.isArray
    check not dt.isScalar
    check dt.isArrayLike
    check dt.isValue
    check dt.kind == DatumKind.dkArray

  test "Create Datum from ChunkedArray and verify type checking":
    let arr1 = newArray(@[1'i32, 2'i32])
    let arr2 = newArray(@[3'i32, 4'i32])
    let chunked = newChunkedArray([arr1, arr2])
    let dt = newDatum(chunked)

    check dt.kind == DatumKind.dkChunkedArray
    check dt.kind != DatumKind.dkScalar

  test "Create Datum from Table and verify properties":
    let schema = newSchema([newField[int32]("col1"), newField[float64]("col2")])
    let arr1 = newArray(@[1'i32, 2'i32, 3'i32])
    let arr2 = newArray(@[1.0'f64, 2.0'f64, 3.0'f64])
    let table = newArrowTable(schema, arr1, arr2)
    let dt = newDatum(table)

    check dt.kind == DatumKind.dkTable

  test "Create Datum from RecordBatch and verify properties":
    let schema = newSchema([newField[string]("name"), newField[int32]("age")])
    let nameArr = newArray(@["Alice", "Bob", "Charlie"])
    let ageArr = newArray(@[30'i32, 25'i32, 35'i32])
    let rb = newRecordBatch(schema, nameArr, ageArr)
    let dt = newDatum(rb)

    check dt.kind == DatumKind.dkRecordBatch

  test "Create Datum from string scalar":
    let dt = newDatum("hello world")
    check dt.isScalar
    check dt.kind == DatumKind.dkScalar

suite "Datum - Equality and String Representation":
  test "Equal datums from same array are equal":
    let arr = newArray(@[1'i32, 2'i32, 3'i32])
    let dt1 = newDatum(arr)
    let dt2 = newDatum(arr)

    check dt1 == dt2

  test "Different datums from different arrays are not equal":
    let arr1 = newArray(@[1'i32, 2'i32, 3'i32])
    let arr2 = newArray(@[4'i32, 5'i32, 6'i32])
    let dt1 = newDatum(arr1)
    let dt2 = newDatum(arr2)

    check dt1 != dt2

  test "Datum string representation contains expected content":
    let arr = newArray(@[42'i32, 43'i32])
    let dt = newDatum(arr)
    let str = $dt

    check str.len > 0
    check "42" in str


suite "Datum - Memory Management (ARC Hooks)":
  test "Datum can be copied and both copies are valid":
    let arr = newArray(@[1'i32, 2'i32, 3'i32, 4'i32, 5'i32])
    let dt1 = newDatum(arr)
    let dt2 = dt1  # Copy

    # Both should be valid and equal
    check dt1 == dt2
    check dt1.isArray
    check dt2.isArray
    check dt1.kind == DatumKind.dkArray
    check dt2.kind == DatumKind.dkArray

  test "Datum works correctly when moved (sink)":
    var dt: Datum
    block:
      let arr = newArray(@[100'i32, 200'i32])
      let temp = newDatum(arr)
      dt = temp  # Move

    # dt should still be valid after temp goes out of scope
    check dt.isArray
    check dt.kind == DatumKind.dkArray
    check dt.toPtr != nil

  test "Multiple copies of Datum are independent but equal":
    let arr = newArray(@[10'i32, 20'i32, 30'i32])
    let original = newDatum(arr)

    var copies: seq[Datum] = @[]
    for i in 0..5:
      copies.add(original)

    # All copies should be equal and valid
    for cp in copies:
      check cp == original
      check cp.isArray
      check cp.toPtr != nil

suite "Scalar - Constructors and Value Extraction":
  test "Create bool scalar and extract value":
    let sc = newScalar(true)
    check sc.isValid
    check sc.getBool == true
    let sc2 = newScalar(false)
    check sc2.getBool == false

  test "Create int8 scalar and extract value":
    let sc = newScalar(42'i8)
    check sc.isValid
    check sc.getInt8 == 42'i8

  test "Create string scalar":
    let sc = newScalar("hello")
    check sc.isValid
    check $sc == "hello"

  test "Create binary scalar from bytes":
    let sc = newScalar(@[byte(1), byte(2), byte(3)])
    check sc.isValid

  test "Create temporal scalars":
    let date32 = newScalar(Date32(100))
    let date64 = newScalar(Date64(86400000'i64))
    let interval = newScalar(MonthInterval(12))
    check date32.isValid
    check date64.isValid
    check interval.isValid

suite "Scalar - Equality":
  test "Equal scalars are equal":
    let sc1 = newScalar(42'i32)
    let sc2 = newScalar(42'i32)
    check sc1 == sc2

  test "Different scalars are not equal":
    let sc1 = newScalar(42'i32)
    let sc2 = newScalar(43'i32)
    check sc1 != sc2

  test "Scalars of different types are not equal":
    let intSc = newScalar(42'i32)
    let floatSc = newScalar(42.0'f64)
    check intSc != floatSc

suite "Scalar - String Representation":
  test "Scalar string representation contains value":
    let sc = newScalar(123'i32)
    let str = $sc
    check str.len > 0
    check "123" in str

suite "Scalar - Memory Management":
  test "Scalar can be copied and both copies are valid":
    let sc1 = newScalar(999'i64)
    let sc2 = sc1  # Copy

    check sc1 == sc2
    check sc1.isValid
    check sc2.isValid
    check sc1.toPtr != nil
    check sc2.toPtr != nil

  test "Scalar works correctly when moved":
    var sc: Scalar
    block:
      let temp = newScalar(3.14159'f64)
      sc = temp  # Move

    check sc.isValid
    check sc.toPtr != nil
    check sc.getFloat64 == 3.14159'f64

  test "String scalar can be copied and moved":
    let sc1 = newScalar("test")
    let sc2 = sc1
    var sc3: Scalar
    block:
      let temp = newScalar("moved")
      sc3 = temp
    check sc1 == sc2
    check $sc3 == "moved"

suite "Expression - Literal and Field Expressions":
  test "Create literal expression from int":
    let expr = newLiteralExpression(42'i32)
    check expr.toPtr != nil

  test "Create literal expression from string":
    let expr = newLiteralExpression("hello")
    check expr.toPtr != nil

  test "Create field expression by name":
    let expr = newFieldExpression("age")
    check expr.toPtr != nil

  # test "Field expression equality":
  #   let f1 = newFieldExpression("col1")
  #   let f2 = newFieldExpression("col1")
  #   let f3 = newFieldExpression("col2")
  #   check f1 == f2
  #   check f1 != f3

  test "Expression string representation":
    let expr = newLiteralExpression(42'i32)
    let str = $expr
    check str.len > 0

suite "Expression - Call Expressions":
  test "Create comparison expression (eq)":
    let age = newFieldExpression("age")
    let threshold = 18'i32
    let expr = eq(age, 18'i32)
    check expr.toPtr != nil

  test "Create arithmetic expression (add)":
    let a = newFieldExpression("a")
    let b = newFieldExpression("b")
    let expr = add(a, b)
    check expr.toPtr != nil

  test "Create logical expression (and)":
    let isActive = newFieldExpression("is_active")
    let isVerified = newFieldExpression("is_verified")
    let expr = andExpr(isActive, isVerified)
    check expr.toPtr != nil

  test "Chained expressions work correctly":
    let age = newFieldExpression("age")
    let minAge = 18'i32
    let maxAge = 65'i32
    let gteMin = ge(age, minAge)
    let lteMax = le(age, maxAge)
    let validAge = andExpr(gteMin, lteMax)
    check validAge.toPtr != nil

suite "Push-Down Filtering":

  var fixture: TestFixture
  var uri: string

  setup:
    fixture = newTestFixture("test_parquet")
    uri = fixture / "table.parquet"
    let
      schema = newSchema([
        newField[int32]("id"),
        newField[string]("category"),
        newField[int32]("value"),
        newField[bool]("flag")
      ])
    var
      ids: seq[int32] = @[]
      categories: seq[string] = @[]
      values: seq[int32] = @[]
      flags: seq[bool] = @[]
    
    # Generate 1000 rows
    for i in 0 ..< 1000:
      ids.add(i.int32)
      categories.add(if i mod 3 == 0: "A" elif i mod 3 == 1: "B" else: "C")
      values.add((i * 10).int32)
      flags.add(i mod 2 == 0)
    
    let table = newArrowTable(schema, newArray(ids), newArray(categories), newArray(values), newArray(flags))
    writeTable(table, uri)

  teardown:
    fixture.cleanup()

  test "statistics to expression":
    # Create mock statistics with known min/max
    # Verify the generated expression is correct
    
    let reader = newFileReader(uri)
    let schema = reader.schema
    let metadata = reader.metadata

    let colIdx = schema.getFieldIndex("id")

    let columnMeta = metadata.rowGroup(0).columnChunk(colIdx)
    let stats = columnMeta.statistics
    let expr = statisticsAsExpression("id", stats)
    check $expr.get == "and((id >= 0), (id <= 999))"

  test "simplify equal within bounds":
    let pred = col("age") == 30'i32
    let guarantee = andExpr(col("age") >= 25'i32, col("age") <= 50'i32)
    let simplified = simplifyWithGuarantee(pred, guarantee)
    check simplified.isSatisfiable  # 30 is within [25, 50]

  test "simplify equal outside bounds":
    let pred = col("age") == 10'i32
    let guarantee = andExpr(col("age") >= 25'i32, col("age") <= 50'i32)
    let simplified = simplifyWithGuarantee(pred, guarantee)
    check not simplified.isSatisfiable  # 10 < 25, impossible

  test "complex OR with mixed satisfiability":
    # Age is in [35, 50], so:
    # - First branch: age >= 25 (always true in this range) AND active == true
    # - Second branch: age < 30 (always false in this range)
    let pred = orExpr(
      andExpr(col("age") >= 25'i32, col("active") == true),
      andExpr(col("age") < 30'i32, col("flag") == true)
    )
    let guarantee = andExpr(col("age") >= 35'i32, col("age") <= 50'i32)
    let simplified = simplifyWithGuarantee(pred, guarantee)
    # Second branch is always false due to age constraint
    # First branch simplifies to just "active == true" since age >= 25 is always true
    check simplified.isSatisfiable  # First branch can still be true


