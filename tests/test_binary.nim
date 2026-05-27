import unittest2
import std/options
import ../src/narrow

suite "Binary Array (seq[byte])":
  test "Create builder and append":
    var builder = newArrayBuilder[seq[byte]]()
    builder.append(@[byte 1, 2, 3])
    builder.append(@[byte 4, 5])
    let arr = builder.finish()
    check arr.len == 2

  test "Index access":
    var builder = newArrayBuilder[seq[byte]]()
    builder.append(@[byte 1, 2, 3])
    builder.append(@[byte 4, 5, 6, 7])
    let arr = builder.finish()
    check arr[0] == @[byte 1, 2, 3]
    check arr[1] == @[byte 4, 5, 6, 7]

  test "Empty byte sequence":
    var builder = newArrayBuilder[seq[byte]]()
    builder.append(newSeq[byte]())
    builder.append(@[byte 1])
    let arr = builder.finish()
    check arr.len == 2
    check arr[0] == newSeq[byte]()
    check arr[1] == @[byte 1]

  test "Null handling":
    var builder = newArrayBuilder[seq[byte]]()
    builder.append(some(@[byte 1, 2]))
    builder.append(none(seq[byte]))
    builder.append(some(@[byte 3]))
    let arr = builder.finish()
    check arr.len == 3
    check arr[0] == @[byte 1, 2]
    check arr.isNull(1)
    check not arr.isNull(0)
    check arr[2] == @[byte 3]

  test "String representation":
    var builder = newArrayBuilder[seq[byte]]()
    builder.append(@[byte 0x48, 0x65, 0x6C, 0x6C, 0x6F])  # "Hello"
    builder.appendNull()
    let arr = builder.finish()
    let str = $arr
    check str.len > 0

  test "toSeq conversion":
    var builder = newArrayBuilder[seq[byte]]()
    builder.append(@[byte 10, 20])
    builder.append(@[byte 30])
    let arr = builder.finish()
    let s = arr.toSeq
    check s.len == 2
    check s[0] == @[byte 10, 20]
    check s[1] == @[byte 30]

  test "Slice":
    var builder = newArrayBuilder[seq[byte]]()
    builder.append(@[byte 1])
    builder.append(@[byte 2])
    builder.append(@[byte 3])
    builder.append(@[byte 4])
    let arr = builder.finish()
    let sliced = arr[0 .. 2]
    check sliced.len == 3
    check sliced[0] == @[byte 1]
    check sliced[1] == @[byte 2]
    check sliced[2] == @[byte 3]

  test "Builder appendValues (openArray)":
    var builder = newArrayBuilder[seq[byte]]()
    builder.appendValues([@[byte 1], @[byte 2, 3], @[byte 4, 5, 6]])
    let arr = builder.finish()
    check arr.len == 3
    check arr[0] == @[byte 1]
    check arr[1] == @[byte 2, 3]
    check arr[2] == @[byte 4, 5, 6]

  test "Copy from another binary array":
    var builder1 = newArrayBuilder[seq[byte]]()
    builder1.append(@[byte 1, 2])
    builder1.append(@[byte 3])
    let arr1 = builder1.finish()
    var builder2 = newArrayBuilder[seq[byte]]()
    builder2.appendValues(arr1)
    let arr2 = builder2.finish()
    check arr2.len == 2
    check arr2[0] == @[byte 1, 2]
    check arr2[1] == @[byte 3]

  test "newArray from seq":
    let arr = newArray(@[@[byte 1, 2], @[byte 3, 4, 5]])
    check arr.len == 2
    check arr[0] == @[byte 1, 2]
    check arr[1] == @[byte 3, 4, 5]

  test "Memory management stress":
    for i in 0 ..< 20:
      var builder = newArrayBuilder[seq[byte]]()
      builder.append(@[byte 1, 2, 3])
      builder.appendNull()
      builder.append(@[byte 4, 5])
      discard builder.finish()

  test "Bounds checking":
    var builder = newArrayBuilder[seq[byte]]()
    builder.append(@[byte 1])
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr[-1]
    expect(IndexDefect):
      discard arr[5]

  test "Bulk appendValues with null mask":
    var builder = newArrayBuilder[seq[byte]]()
    let values = [@[byte 1], @[byte 2], @[byte 3]]
    builder.appendValues(values)
    let arr = builder.finish()
    check arr.len == 3

  test "Large binary values":
    var builder = newArrayBuilder[seq[byte]]()
    var large = newSeq[byte](10000)
    for i in 0 ..< 10000:
      large[i] = byte(i and 0xFF)
    builder.append(large)
    let arr = builder.finish()
    check arr.len == 1
    let retrieved = arr[0]
    check retrieved.len == 10000
    check retrieved[0] == 0
    check retrieved[5000] == byte(5000 and 0xFF)

  test "Binary array items iterator":
    var builder = newArrayBuilder[seq[byte]]()
    builder.append(@[byte 1])
    builder.append(@[byte 2, 3])
    let arr = builder.finish()
    var vals: seq[seq[byte]]
    for v in arr:
      vals.add(v)
    check vals.len == 2
    check vals[0] == @[byte 1]
    check vals[1] == @[byte 2, 3]
