import unittest2
import ../src/narrow

suite "FixedSizeBinary - Data Type":
  test "newFixedSizeBinaryDataType creates type with byte width":
    let dt = newFixedSizeBinaryDataType(16)
    check dt.byteWidth == 16

  test "data type with zero byte width":
    let dt = newFixedSizeBinaryDataType(0)
    check dt.byteWidth == 0

suite "FixedSizeBinary - Array Builder":
  test "builder creates array with correct byte width":
    var builder = newFixedSizeBinaryArrayBuilder(4)
    let arr = builder.finish()
    check arr.byteWidth == 4
    check arr.len == 0

  test "append single value":
    var builder = newFixedSizeBinaryArrayBuilder(4)
    builder.append(@[1'u8, 2, 3, 4])
    let arr = builder.finish()
    check arr.len == 1
    check arr[0] == @[1'u8, 2, 3, 4]

  test "append multiple values":
    var builder = newFixedSizeBinaryArrayBuilder(4)
    builder.append(@[1'u8, 0, 0, 0])
    builder.append(@[2'u8, 0, 0, 0])
    builder.append(@[3'u8, 0, 0, 0])
    let arr = builder.finish()
    check arr.len == 3
    check arr[0] == @[1'u8, 0, 0, 0]
    check arr[1] == @[2'u8, 0, 0, 0]
    check arr[2] == @[3'u8, 0, 0, 0]

  test "append null":
    var builder = newFixedSizeBinaryArrayBuilder(4)
    builder.append(@[1'u8, 2, 3, 4])
    builder.appendNull()
    builder.append(@[5'u8, 6, 7, 8])
    let arr = builder.finish()
    check arr.len == 3
    check arr[0] == @[1'u8, 2, 3, 4]
    check arr.isNull(1)
    check not arr.isNull(0)
    check arr[2] == @[5'u8, 6, 7, 8]

  test "builder with different byte widths":
    for bw in [1, 8, 16, 32]:
      var builder = newFixedSizeBinaryArrayBuilder(bw.int32)
      var emptySeq = newSeq[byte](bw)
      builder.append(emptySeq)
      let arr = builder.finish()
      check arr.byteWidth == bw.int32
      check arr.len == 1

suite "FixedSizeBinary - Array Accessors":
  test "array length":
    var builder = newFixedSizeBinaryArrayBuilder(4)
    builder.append(@[1'u8, 2, 3, 4])
    builder.append(@[5'u8, 6, 7, 8])
    let arr = builder.finish()
    check arr.len == 2

  test "index out of bounds raises":
    var builder = newFixedSizeBinaryArrayBuilder(4)
    builder.append(@[1'u8, 2, 3, 4])
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr[1]
    expect(IndexDefect):
      discard arr[-1]

  test "empty array index raises":
    var builder = newFixedSizeBinaryArrayBuilder(4)
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr[0]

  test "string representation":
    var builder = newFixedSizeBinaryArrayBuilder(4)
    builder.append(@[1'u8, 2, 3, 4])
    let arr = builder.finish()
    let s = $arr
    check s.len > 0

  test "toSeq conversion":
    var builder = newFixedSizeBinaryArrayBuilder(4)
    builder.append(@[1'u8, 2, 3, 4])
    builder.append(@[5'u8, 6, 7, 8])
    let arr = builder.finish()
    let s = arr.toSeq
    check s.len == 2
    check s[0] == @[1'u8, 2, 3, 4]
    check s[1] == @[5'u8, 6, 7, 8]

  test "@ macro conversion":
    var builder = newFixedSizeBinaryArrayBuilder(4)
    builder.append(@[1'u8, 2, 3, 4])
    let arr = builder.finish()
    let s = @arr
    check s.len == 1
    check s[0] == @[1'u8, 2, 3, 4]

  test "items iterator":
    var builder = newFixedSizeBinaryArrayBuilder(4)
    builder.append(@[1'u8, 2, 3, 4])
    builder.append(@[5'u8, 6, 7, 8])
    let arr = builder.finish()
    var collected: seq[seq[byte]]
    for item in arr:
      collected.add(item)
    check collected.len == 2
    check collected[0] == @[1'u8, 2, 3, 4]
    check collected[1] == @[5'u8, 6, 7, 8]

  test "isNull returns true for null indices":
    var builder = newFixedSizeBinaryArrayBuilder(4)
    builder.appendNull()
    builder.append(@[1'u8, 2, 3, 4])
    let arr = builder.finish()
    check arr.isNull(0)
    check not arr.isNull(1)

  test "isNull out of bounds raises":
    var builder = newFixedSizeBinaryArrayBuilder(4)
    builder.append(@[1'u8, 2, 3, 4])
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr.isNull(-1)
    expect(IndexDefect):
      discard arr.isNull(1)

suite "FixedSizeBinary - Scalar":
  test "create scalar from bytes":
    let scalar = newFixedSizeBinaryScalar(@[1'u8, 2, 3, 4])
    check not isNil(scalar.handle)

  test "create scalar from empty bytes":
    let scalar = newFixedSizeBinaryScalar(@[])
    check not isNil(scalar.handle)

suite "FixedSizeBinary - Memory":
  test "copy semantics":
    var builder = newFixedSizeBinaryArrayBuilder(4)
    builder.append(@[1'u8, 2, 3, 4])
    let arr1 = builder.finish()
    let arr2 = arr1
    check arr2.len == 1
    check arr2[0] == @[1'u8, 2, 3, 4]

  test "many rows":
    let n = 1000
    var builder = newFixedSizeBinaryArrayBuilder(8)
    for i in 0 ..< n:
      builder.append(@[(i and 0xFF).uint8, 0, 0, 0, 0, 0, 0, 0])
    let arr = builder.finish()
    check arr.len == n
    check arr[0] == @[0'u8, 0, 0, 0, 0, 0, 0, 0]
