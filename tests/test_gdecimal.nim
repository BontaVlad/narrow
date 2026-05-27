import std/strutils
import unittest2
import ../src/narrow

suite "Decimal128 - Value Type":
  test "construct from string returns raw integer":
    let d = newDecimal128("123.45")
    check $d == "12345"

  test "construct from int64":
    let d = newDecimal128(42)
    check $d == "42"

  test "construct negative value":
    let d = newDecimal128("-10")
    check $d == "-10"

  test "string constructor strips decimal point":
    let d = newDecimal128("0.001")
    check $d == "1"

  test "comparison operators":
    let a = newDecimal128("10")
    let b = newDecimal128("20")
    let c = newDecimal128("10")
    check a < b
    check a <= b
    check not (a > b)
    check a >= a
    check a == c

  test "arithmetic addition":
    let a = newDecimal128("100")
    let b = newDecimal128("200")
    check $(a + b) == "300"

  test "arithmetic subtraction":
    let a = newDecimal128("500")
    let b = newDecimal128("100")
    check $(a - b) == "400"

  test "arithmetic multiplication":
    let a = newDecimal128("12")
    let b = newDecimal128("3")
    check $(a * b) == "36"

  test "arithmetic division":
    let a = newDecimal128("100")
    let b = newDecimal128("4")
    check $(a / b) == "25"

  test "absolute value":
    let a = newDecimal128("-42")
    let b = newDecimal128("42")
    check a.abs == b

  test "negate":
    let a = newDecimal128("7")
    check $(a.negate) == "-7"
    check $(a.negate.negate) == "7"

  test "toInt truncates":
    let d = newDecimal128("123")
    check d.toInt == 123

  test "toBytes round-trip size":
    let d = newDecimal128("0")
    check d.toBytes.len == 16

  test "rescale multiplies raw value":
    let d = newDecimal128("12345")
    let rescaled = d.rescale(0, 2)
    check $(rescaled) == "1234500"

suite "Decimal256 - Value Type":
  test "construct from string returns raw integer":
    let d = newDecimal256("123.45")
    check $d == "12345"

  test "construct from int64":
    let d = newDecimal256(42)
    check $d == "42"

  test "comparison operators":
    let a = newDecimal256("10")
    let b = newDecimal256("20")
    check a < b
    check b > a

  test "arithmetic addition":
    let a = newDecimal256("100")
    let b = newDecimal256("200")
    check $(a + b) == "300"

  test "arithmetic multiplication":
    let a = newDecimal256("12")
    let b = newDecimal256("3")
    check $(a * b) == "36"

  test "absolute value":
    let a = newDecimal256("-42")
    let b = newDecimal256("42")
    check a.abs == b

  test "negate":
    let a = newDecimal256("7")
    check $(a.negate) == "-7"

  test "toBytes round-trip size":
    let d = newDecimal256("0")
    check d.toBytes.len == 32

suite "Decimal128 - Array Builder and Array":
  test "build array from string values":
    var builder = newDecimal128ArrayBuilder(10, 2)
    builder.append("123.45")
    builder.append("67.89")
    builder.append("-1.00")
    let arr = builder.finish()

    check arr.len == 3
    check arr.precision == 10
    check arr.scale == 2

  test "build array from int64 values":
    var builder = newDecimal128ArrayBuilder(10, 0)
    builder.append(123'i64)
    builder.append(456'i64)
    let arr = builder.finish()

    check arr.len == 2
    check $(arr[0]) == "123"
    check $(arr[1]) == "456"

  test "build array from Decimal128 values":
    let a = newDecimal128("3.14")
    let b = newDecimal128("2.71")
    var builder = newDecimal128ArrayBuilder(10, 2)
    builder.append(a)
    builder.append(b)
    let arr = builder.finish()

    check arr.len == 2
    check arr[0] == a
    check arr[1] == b

  test "index out of bounds":
    var builder = newDecimal128ArrayBuilder(5, 1)
    builder.append("1.0")
    let arr = builder.finish()

    expect IndexDefect:
      discard arr[-1]
    expect IndexDefect:
      discard arr[100]

  test "isNull returns false for valid values":
    var builder = newDecimal128ArrayBuilder(5, 1)
    builder.append("1.0")
    let arr = builder.finish()
    check not arr.isNull(0)

  test "isNull out of bounds":
    var builder = newDecimal128ArrayBuilder(5, 1)
    builder.append("1.0")
    let arr = builder.finish()
    expect IndexDefect:
      discard arr.isNull(-1)

  test "appendNull creates null value":
    var builder = newDecimal128ArrayBuilder(5, 1)
    builder.append("1.0")
    builder.appendNull()
    builder.append("3.0")
    let arr = builder.finish()

    check arr.len == 3
    check not arr.isNull(0)
    check arr.isNull(1)
    check not arr.isNull(2)

  test "formatValue with scale":
    var builder = newDecimal128ArrayBuilder(38, 3)
    builder.append("12345")
    let arr = builder.finish()
    check arr.formatValue(0) == "12.345"

  test "empty array":
    var builder = newDecimal128ArrayBuilder(10, 2)
    let arr = builder.finish()
    check arr.len == 0

  test "string representation of array":
    var builder = newDecimal128ArrayBuilder(10, 2)
    builder.append("1.00")
    builder.append("2.50")
    let arr = builder.finish()
    let s = $arr
    check "1.00" in s
    check "2.50" in s

  test "high precision value with formatValue":
    var builder = newDecimal128ArrayBuilder(38, 10)
    builder.append("1234567890123456.1234567890")
    let arr = builder.finish()
    check arr.len == 1
    check arr.formatValue(0) == "1234567890123456.1234567890"

suite "Decimal256 - Array Builder and Array":
  test "build array from string values":
    var builder = newDecimal256ArrayBuilder(20, 2)
    builder.append("123.45")
    builder.append("67.89")
    let arr = builder.finish()

    check arr.len == 2
    check arr.precision == 20
    check arr.scale == 2

  test "index into array":
    var builder = newDecimal256ArrayBuilder(20, 0)
    builder.append("42")
    let arr = builder.finish()
    check arr[0] == newDecimal256(42)

  test "appendNull":
    var builder = newDecimal256ArrayBuilder(20, 0)
    builder.appendNull()
    builder.append("7")
    let arr = builder.finish()
    check arr.len == 2
    check arr.isNull(0)
    check not arr.isNull(1)

  test "empty array":
    var builder = newDecimal256ArrayBuilder(20, 2)
    let arr = builder.finish()
    check arr.len == 0
