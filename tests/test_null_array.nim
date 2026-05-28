import unittest2
import ../src/narrow

suite "NullArray":
  test "newNullArray creates array of given length":
    let arr = newNullArray(5)
    check arr.len == 5

  test "every element is null":
    let arr = newNullArray(5)
    for i in 0 ..< arr.len:
      check arr.isNull(i)

  test "newNullArray with zero length":
    let arr = newNullArray(0)
    check arr.len == 0

  test "out of bounds raises":
    let arr = newNullArray(3)
    expect(IndexDefect):
      discard arr.isNull(-1)
    expect(IndexDefect):
      discard arr.isNull(3)

  test "string representation":
    let arr = newNullArray(3)
    let s = $arr
    check s.len > 0

  test "builder: single appendNull":
    var builder = newNullArrayBuilder()
    builder.appendNull()
    builder.appendNull()
    let arr = builder.finish()
    check arr.len == 2
    check arr.isNull(0)
    check arr.isNull(1)

  test "builder: appendNulls batch":
    var builder = newNullArrayBuilder()
    builder.appendNulls(10)
    let arr = builder.finish()
    check arr.len == 10
    for i in 0 ..< 10:
      check arr.isNull(i)

  test "builder: zero nulls produces empty array":
    var builder = newNullArrayBuilder()
    let arr = builder.finish()
    check arr.len == 0

  test "builder: appendNulls then appendNull":
    var builder = newNullArrayBuilder()
    builder.appendNulls(3)
    builder.appendNull()
    let arr = builder.finish()
    check arr.len == 4

  test "null scalar creation":
    let scalar = newNullScalar()
    check not isNil(scalar.handle)

  test "NullArray data type can be created":
    let dt = newNullGType()
    check not isNil(dt.handle)

  test "NullArray from builder using convenience constructor":
    var builder = newNullArrayBuilder()
    builder.appendNulls(4)
    let arr = newNullArray(builder)
    check arr.len == 4

suite "NullArray - Memory Stress":
  test "many nulls":
    let n = 100_000
    var builder = newNullArrayBuilder()
    builder.appendNulls(n)
    let arr = builder.finish()
    check arr.len == n

  test "many single appends":
    let n = 10_000
    var builder = newNullArrayBuilder()
    for i in 0 ..< n:
      builder.appendNull()
    let arr = builder.finish()
    check arr.len == n

  test "copy semantics":
    var builder1 = newNullArrayBuilder()
    builder1.appendNulls(3)
    let arr1 = builder1.finish()
    let arr2 = arr1  # =copy
    check arr2.len == 3
    check arr2.isNull(0)
