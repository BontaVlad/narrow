import unittest2
import ../src/narrow

suite "LargeBinaryArray":
  test "builder creates empty array":
    var b = newLargeBinaryArrayBuilder()
    let arr = b.finish()
    check arr.len == 0

  test "append and read back":
    var b = newLargeBinaryArrayBuilder()
    b.append(@[1'u8, 2, 3])
    b.append(@[4'u8, 5])
    let arr = b.finish()
    check arr.len == 2
    check arr[0] == @[1'u8, 2, 3]
    check arr[1] == @[4'u8, 5]

  test "append null":
    var b = newLargeBinaryArrayBuilder()
    b.append(@[1'u8, 2])
    b.appendNull()
    b.append(@[3'u8])
    let arr = b.finish()
    check arr.len == 3
    check arr[0] == @[1'u8, 2]
    check arr.isNull(1)
    check not arr.isNull(0)
    check arr[2] == @[3'u8]

  test "index out of bounds":
    var b = newLargeBinaryArrayBuilder()
    b.append(@[1'u8])
    let arr = b.finish()
    expect(IndexDefect):
      discard arr[1]
    expect(IndexDefect):
      discard arr[-1]

  test "string representation":
    var b = newLargeBinaryArrayBuilder()
    b.append(@[0xDE'u8, 0xAD])
    let arr = b.finish()
    let s = $arr
    check s.len > 0

  test "toSeq and @ conversion":
    var b = newLargeBinaryArrayBuilder()
    b.append(@[1'u8])
    b.append(@[2'u8, 3])
    let arr = b.finish()
    let s = arr.toSeq
    check s == @[@[1'u8], @[2'u8, 3]]
    check @arr == @[@[1'u8], @[2'u8, 3]]

  test "items iterator":
    var b = newLargeBinaryArrayBuilder()
    b.append(@[1'u8])
    b.append(@[2'u8])
    let arr = b.finish()
    var col: seq[seq[byte]]
    for v in arr:
      col.add(v)
    check col == @[@[1'u8], @[2'u8]]

suite "LargeStringArray":
  test "builder creates empty array":
    var b = newLargeStringArrayBuilder()
    let arr = b.finish()
    check arr.len == 0

  test "append and read back":
    var b = newLargeStringArrayBuilder()
    b.append("hello")
    b.append("world")
    let arr = b.finish()
    check arr.len == 2
    check arr[0] == "hello"
    check arr[1] == "world"

  test "append null":
    var b = newLargeStringArrayBuilder()
    b.append("a")
    b.appendNull()
    b.append("c")
    let arr = b.finish()
    check arr.len == 3
    check arr[0] == "a"
    check arr.isNull(1)
    check arr[2] == "c"

  test "index out of bounds":
    var b = newLargeStringArrayBuilder()
    b.append("x")
    let arr = b.finish()
    expect(IndexDefect):
      discard arr[1]
    expect(IndexDefect):
      discard arr[-1]

  test "large strings work":
    var b = newLargeStringArrayBuilder()
    var big = newString(10000)
    for i in 0 ..< 10000:
      big[i] = 'a'
    b.append(big)
    let arr = b.finish()
    check arr.len == 1
    check arr[0] == big

  test "toSeq and @ conversion":
    var b = newLargeStringArrayBuilder()
    b.append("a")
    b.append("b")
    let arr = b.finish()
    check arr.toSeq == @["a", "b"]
    check @arr == @["a", "b"]

  test "items iterator":
    var b = newLargeStringArrayBuilder()
    b.append("x")
    b.append("y")
    let arr = b.finish()
    var col: seq[string]
    for v in arr:
      col.add(v)
    check col == @["x", "y"]

  test "string representation":
    var b = newLargeStringArrayBuilder()
    b.append("test")
    let arr = b.finish()
    let s = $arr
    check s.len > 0

suite "LargeListArray":
  test "builder creates empty array":
    let field = newField[int32]("item")
    var b = newLargeListArrayBuilder(field)
    let arr = b.finish()
    check arr.len == 0

  test "build list of ints":
    let field = newField[int32]("item")
    var b = newLargeListArrayBuilder(field)
    let vb = cast[ptr GArrowInt32ArrayBuilder](b.getValueBuilder())
    b.append()
    verify garrow_int32_array_builder_append_value(vb, 1'i32)
    verify garrow_int32_array_builder_append_value(vb, 2'i32)
    b.append()
    verify garrow_int32_array_builder_append_value(vb, 3'i32)
    let arr = b.finish()
    check arr.len == 2

  test "append null":
    let field = newField[int32]("item")
    var b = newLargeListArrayBuilder(field)
    let vb = cast[ptr GArrowInt32ArrayBuilder](b.getValueBuilder())
    b.append()
    verify garrow_int32_array_builder_append_value(vb, 42'i32)
    b.appendNull()
    let arr = b.finish()
    check arr.len == 2
    check arr.isNull(1)
    check not arr.isNull(0)

  test "getValueOffset and getValueLength":
    let field = newField[int32]("item")
    var b = newLargeListArrayBuilder(field)
    let vb = cast[ptr GArrowInt32ArrayBuilder](b.getValueBuilder())
    b.append()
    verify garrow_int32_array_builder_append_value(vb, 10'i32)
    verify garrow_int32_array_builder_append_value(vb, 20'i32)
    b.append()
    verify garrow_int32_array_builder_append_value(vb, 30'i32)
    let arr = b.finish()
    check arr.getValueOffset(0) == 0
    check arr.getValueLength(0) == 2
    check arr.getValueOffset(1) == 2
    check arr.getValueLength(1) == 1

  test "getValue returns child array":
    let field = newField[int32]("item")
    var b = newLargeListArrayBuilder(field)
    let vb = cast[ptr GArrowInt32ArrayBuilder](b.getValueBuilder())
    b.append()
    verify garrow_int32_array_builder_append_value(vb, 1'i32)
    let arr = b.finish()
    let child = arr.getValue(0)
    check not isNil(child)

suite "Large Types - Memory":
  test "copy semantics LargeBinary":
    var b = newLargeBinaryArrayBuilder()
    b.append(@[1'u8])
    let a1 = b.finish()
    let a2 = a1
    check a2.len == 1

  test "copy semantics LargeString":
    var b = newLargeStringArrayBuilder()
    b.append("hi")
    let a1 = b.finish()
    let a2 = a1
    check a2[0] == "hi"

  test "many rows large string":
    var b = newLargeStringArrayBuilder()
    for i in 0 ..< 1000:
      b.append("row_" & $i)
    let arr = b.finish()
    check arr.len == 1000
    check arr[0] == "row_0"
    check arr[999] == "row_999"

  test "many rows large binary":
    var b = newLargeBinaryArrayBuilder()
    for i in 0 ..< 500:
      b.append(@[i.uint8, (i+1).uint8])
    let arr = b.finish()
    check arr.len == 500
