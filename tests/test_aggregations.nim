import std/[sequtils, options]
import unittest2
import ../src/narrow

suite "Scalar Reductions":
  test "mean on int32 array":
    let arr = newArray(@[1'i32, 2, 3, 4, 5])
    check mean(arr) == 3.0

  test "mean on int64 array":
    let arr = newArray(@[1'i64, 2, 3, 4, 5])
    check mean(arr) == 3.0

  test "mean on float32 array":
    let arr = newArray(@[1.0'f32, 2.0, 3.0, 4.0, 5.0])
    check mean(arr) == 3.0

  test "mean on float64 array":
    let arr = newArray(@[1.0'f64, 2.0, 3.0, 4.0, 5.0])
    check mean(arr) == 3.0

  test "mean with nulls":
    let arr = newArray(@[1'i32, 2, 3, 4, 5], mask = @[false, false, true, false, false])
    # mean of [1, 2, null, 4, 5] = 12 / 4 = 3.0 (nulls are skipped)
    check mean(arr) == 3.0

  test "sum on int32 returns int64":
    let arr = newArray(@[1'i32, 2, 3, 4, 5])
    check sum(arr) == 15'i64

  test "sum on int64 returns int64":
    let arr = newArray(@[1'i64, 2, 3, 4, 5])
    check sum(arr) == 15'i64

  test "sum on uint32 returns uint64":
    let arr = newArray(@[1'u32, 2, 3, 4, 5])
    check sum(arr) == 15'u64

  test "sum on float32 returns float64":
    let arr = newArray(@[1.0'f32, 2.0, 3.0])
    check sum(arr) == 6.0

  test "sum on float64 returns float64":
    let arr = newArray(@[1.0'f64, 2.0, 3.0])
    check sum(arr) == 6.0

  test "count with All mode":
    let arr = newArray(@[1'i32, 2, 3])
    check count(arr) == 3
    check count(arr, newCountOptions(All)) == 3

  test "count with OnlyValid mode":
    let arr = newArray(@[1'i32, 2, 3], mask = @[false, true, false])
    # [1, null, 3] -> 2 valid
    check count(arr, newCountOptions(OnlyValid)) == 2

  test "count with OnlyNull mode":
    let arr = newArray(@[1'i32, 2, 3], mask = @[false, true, false])
    # [1, null, 3] -> 1 null
    check count(arr, newCountOptions(OnlyNull)) == 1

  test "countValues returns StructArray":
    let arr = newArray(@[1'i32, 2, 1, 2, 1])
    let cv = countValues(arr)
    # count_values returns one row per distinct value
    check cv.len == 2
    check cv.fieldCount == 2
    let fields = cv.fields
    check fields[0].name == "values"
    check fields[1].name == "counts"

suite "Element-Wise Helpers":
  test "multiply two int32 arrays":
    let a = newArray(@[1'i32, 2, 3])
    let b = newArray(@[10'i32, 20, 30])
    let result = multiply(a, b).toArray()
    check result == newArray(@[10'i32, 40, 90])

  test "multiply array by scalar":
    let a = newArray(@[1'i32, 2, 3])
    let result = multiply(a, 2'i32).toArray()
    check result == newArray(@[2'i32, 4, 6])

  test "equal two arrays":
    let a = newArray(@[1'i32, 2, 3])
    let b = newArray(@[1'i32, 4, 3])
    let result = equal(a, b).toArray()
    let expected = newArray(@[true, false, true])
    check result == expected

  test "greater array vs scalar":
    let a = newArray(@[1'i32, 5, 3, 8])
    let result = greater(a, 3'i32).toArray()
    let expected = newArray(@[false, true, false, true])
    check result == expected

  test "less array vs array":
    let a = newArray(@[1'i32, 5, 3])
    let b = newArray(@[2'i32, 4, 6])
    let result = less(a, b).toArray()
    let expected = newArray(@[true, false, true])
    check result == expected

  test "greaterEqual array vs scalar":
    let a = newArray(@[1'i32, 3, 5])
    let result = greaterEqual(a, 3'i32).toArray()
    let expected = newArray(@[false, true, true])
    check result == expected

  test "lessEqual array vs scalar":
    let a = newArray(@[1'i32, 3, 5])
    let result = lessEqual(a, 3'i32).toArray()
    let expected = newArray(@[true, true, false])
    check result == expected

  test "subtract arrays":
    let a = newArray(@[10'i32, 20, 30])
    let b = newArray(@[1'i32, 2, 3])
    let result = subtract(a, b).toArray()
    check result == newArray(@[9'i32, 18, 27])

  test "divide array by scalar":
    let a = newArray(@[10.0'f64, 20.0, 30.0])
    let result = divide(a, 2.0'f64).toArray()
    check result == newArray(@[5.0'f64, 10.0, 15.0])
