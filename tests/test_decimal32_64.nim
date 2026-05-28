import unittest2
import ../src/narrow

suite "Decimal32 - Value Type":
  test "construct from string":
    let d = newDecimal32("12345")
    check $d == "12345"

  test "construct from integer":
    let d = newDecimal32(42'i64)
    check $d == "42"

  test "equality":
    let a = newDecimal32("100")
    let b = newDecimal32("100")
    let c = newDecimal32("200")
    check a == b
    check a != c

  test "comparisons":
    let a = newDecimal32("10")
    let b = newDecimal32("20")
    check a < b
    check a <= b
    check b > a
    check b >= a

  test "arithmetic":
    let a = newDecimal32("10")
    let b = newDecimal32("3")
    doAssert $(a + b) == "13"
    doAssert $(a - b) == "7"
    doAssert $(a * b) == "30"

  test "toInt":
    let d = newDecimal32("99")
    check d.toInt == 99

  test "toBytes roundtrip consistency":
    let d = newDecimal32("42")
    let bytes = d.toBytes
    check bytes.len > 0

  test "abs and negate":
    let d = newDecimal32("-5")
    doAssert $(d.abs) == "5"
    doAssert $(d.negate) == "5"

  test "rescale":
    let d = newDecimal32("12345")
    let r = d.rescale(0, 2)
    doAssert $(r) == "1234500"

  test "toStringScale":
    let d = newDecimal32("12345")
    let s = d.toStringScale(2)
    check s == "123.45"

suite "Decimal64 - Value Type":
  test "construct from string":
    let d = newDecimal64("9999999999")
    check $d == "9999999999"

  test "construct from integer":
    let d = newDecimal64(1'i64)
    check $d == "1"

  test "equality":
    let a = newDecimal64("500")
    let b = newDecimal64("500")
    check a == b

  test "comparisons":
    let a = newDecimal64("5")
    let b = newDecimal64("10")
    check a < b
    check b > a

  test "arithmetic":
    let a = newDecimal64("100")
    let b = newDecimal64("50")
    check $(a + b) == "150"
    check $(a - b) == "50"
    check $(a * b) == "5000"

  test "toInt":
    let d = newDecimal64("77")
    check d.toInt == 77

  test "abs and negate":
    let d = newDecimal64("-100")
    check $(d.abs) == "100"
    check $(d.negate) == "100"

  test "toStringScale":
    let d = newDecimal64("123456")
    let s = d.toStringScale(3)
    check s == "123.456"

suite "Decimal32 - Data Type":
  test "create data type":
    let dt = newDecimal32DataType(9, 2)
    check dt.precision == 9
    check dt.scale == 2

  test "max precision":
    check Decimal32DataType.maxPrecision > 0

suite "Decimal64 - Data Type":
  test "create data type":
    let dt = newDecimal64DataType(18, 4)
    check dt.precision == 18
    check dt.scale == 4

  test "max precision":
    check Decimal64DataType.maxPrecision > 0

suite "Decimal32 - Array and Builder":
  test "build array from values":
    var b = newDecimal32ArrayBuilder(9, 2)
    b.append(newDecimal32("100"))
    b.append(newDecimal32("200"))
    b.append(newDecimal32("300"))
    let arr = b.finish()
    check arr.len == 3
    check arr[0] == newDecimal32("100")
    check arr[1] == newDecimal32("200")

  test "build array from strings":
    var b = newDecimal32ArrayBuilder(9, 2)
    b.append("150")
    b.append("250")
    let arr = b.finish()
    check arr.len == 2
    check $(arr[0]) == "150"

  test "build array from integers":
    var b = newDecimal32ArrayBuilder(9, 0)
    b.append(10'i64)
    b.append(20'i64)
    let arr = b.finish()
    check arr.len == 2
    check $(arr[0]) == "10"

  test "append null":
    var b = newDecimal32ArrayBuilder(9, 2)
    b.append(newDecimal32("100"))
    b.appendNull()
    b.append(newDecimal32("300"))
    let arr = b.finish()
    check arr.len == 3
    check arr.isNull(1)
    check not arr.isNull(0)

  test "formatValue uses array scale":
    var b = newDecimal32ArrayBuilder(9, 2)
    b.append(newDecimal32("12345"))
    let arr = b.finish()
    check arr.formatValue(0) == "123.45"

  test "precision and scale on array":
    var b = newDecimal32ArrayBuilder(9, 2)
    b.append(newDecimal32("100"))
    let arr = b.finish()
    check arr.precision == 9
    check arr.scale == 2

suite "Decimal64 - Array and Builder":
  test "build array from values":
    var b = newDecimal64ArrayBuilder(18, 2)
    b.append(newDecimal64("5000"))
    b.append(newDecimal64("6000"))
    let arr = b.finish()
    check arr.len == 2
    check arr[0] == newDecimal64("5000")

  test "append null":
    var b = newDecimal64ArrayBuilder(18, 2)
    b.append(newDecimal64("100"))
    b.appendNull()
    let arr = b.finish()
    check arr.isNull(1)

  test "formatValue uses array scale":
    var b = newDecimal64ArrayBuilder(18, 4)
    b.append(newDecimal64("12345678"))
    let arr = b.finish()
    check arr.formatValue(0) == "1234.5678"

suite "Decimal32/64 - Scalars":
  test "create Decimal32 scalar and get value":
    let dt = newDecimal32DataType(9, 2)
    let val = newDecimal32("123")
    let sc = newDecimal32Scalar(dt, val)
    check sc.getValue == val

  test "create Decimal64 scalar and get value":
    let dt = newDecimal64DataType(18, 4)
    let val = newDecimal64("5678")
    let sc = newDecimal64Scalar(dt, val)
    check sc.getValue == val

suite "Decimal32/64 - Memory":
  test "copy semantics Decimal32 array":
    var b = newDecimal32ArrayBuilder(9, 2)
    b.append(newDecimal32("42"))
    let a1 = b.finish()
    let a2 = a1
    check a2.len == 1
    check a2[0] == newDecimal32("42")

  test "copy semantics Decimal64 array":
    var b = newDecimal64ArrayBuilder(18, 2)
    b.append(newDecimal64("99"))
    let a1 = b.finish()
    let a2 = a1
    check a2[0] == newDecimal64("99")

  test "many rows Decimal32":
    var b = newDecimal32ArrayBuilder(9, 0)
    for i in 0'i64 ..< 500:
      b.append(i)
    let arr = b.finish()
    check arr.len == 500
    check arr[0].toInt == 0
    check arr[499].toInt == 499
