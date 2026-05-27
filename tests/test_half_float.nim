import unittest2
import ../src/narrow

suite "Half-Float Array":
  const
    hf1_0 = HalfFloat(0x3c00'u16)   # 1.0
    hf2_0 = HalfFloat(0x4000'u16)   # 2.0
    hf3_0 = HalfFloat(0x4200'u16)   # 3.0
    hfNeg = HalfFloat(0xbc00'u16)   # -1.0
    hfZero = HalfFloat(0x0000'u16)  # 0.0

  test "newHalfFloatArray from seq":
    let arr = newHalfFloatArray(@[hf1_0, hf2_0, hf3_0])
    check arr.len == 3
    check arr[0] == hf1_0
    check arr[1] == hf2_0
    check arr[2] == hf3_0

  test "newHalfFloatArray empty":
    let emptySeq: seq[HalfFloat] = @[]
    let arr = newHalfFloatArray(emptySeq)
    check arr.len == 0

  test "empty array index raises IndexDefect":
    let emptySeq: seq[HalfFloat] = @[]
    let arr = newHalfFloatArray(emptySeq)
    expect(IndexDefect):
      discard arr[0]

  test "isNull out of bounds raises":
    let arr = newHalfFloatArray(@[hf1_0])
    expect(IndexDefect):
      discard isNull(arr, -1)
    expect(IndexDefect):
      discard isNull(arr, 5)

  test "scalar create and get value":
    let scalar = newHalfFloatScalar(hf1_0)
    check scalar.getValue == hf1_0

  test "scalar zero value":
    let scalar = newHalfFloatScalar(hfZero)
    check scalar.getValue == hfZero

  test "scalar negative value":
    let scalar = newHalfFloatScalar(hfNeg)
    check scalar.getValue == hfNeg

  test "half-float data type creation":
    let dt = newHalfFloatGType()
    check not isNil(dt.handle)
    check $dt == "halffloat"

  test "all null array via mask":
    let arr = newHalfFloatArray(@[hf1_0, hf2_0, hf3_0],
                                 [true, true, true])
    check arr.len == 3
    for i in 0 ..< 3:
      check isNull(arr, i)

  test "newHalfFloatArray with null mask":
    let arr = newHalfFloatArray(@[hf1_0, hf2_0, hf3_0],
                                 [false, true, false])
    check arr.len == 3
    check isNull(arr, 0) == false
    check isNull(arr, 1) == true
    check isNull(arr, 2) == false
    check arr[0] == hf1_0
    check arr[2] == hf3_0

  test "builder append and finish":
    var builder = newHalfFloatArrayBuilder()
    builder.append(hf1_0)
    builder.append(hf2_0)
    builder.append(hf3_0)
    let arr = builder.finish()
    check arr.len == 3
    check arr[0] == hf1_0
    check arr[1] == hf2_0
    check arr[2] == hf3_0

  test "builder appendNull":
    var builder = newHalfFloatArrayBuilder()
    builder.append(hf1_0)
    builder.appendNull()
    builder.append(hf3_0)
    let arr = builder.finish()
    check arr.len == 3
    check isNull(arr, 0) == false
    check isNull(arr, 1) == true
    check isNull(arr, 2) == false
    check arr[0] == hf1_0
    check arr[2] == hf3_0

  test "builder appendValues":
    var builder = newHalfFloatArrayBuilder()
    builder.appendValues(@[hf1_0, hf2_0, hf3_0])
    let arr = builder.finish()
    check arr.len == 3
    check arr[0] == hf1_0
    check arr[1] == hf2_0
    check arr[2] == hf3_0

  test "toSeq round-trip":
    let original = @[hf1_0, hf2_0, hf3_0, hfNeg, hfZero]
    let arr = newHalfFloatArray(original)
    let restored = arr.toSeq
    check restored == original

  test "@ converter":
    let original = @[hf1_0, hf2_0, hf3_0]
    let arr = newHalfFloatArray(original)
    check @arr == original

  test "iterator items":
    let original = @[hf1_0, hf2_0, hf3_0]
    let arr = newHalfFloatArray(original)
    var collected: seq[HalfFloat]
    for v in arr:
      collected.add v
    check collected == original

  test "$ produces string representation":
    let arr = newHalfFloatArray(@[hf1_0, hf2_0])
    let s = $arr
    check s.len > 0

  test "index out of bounds raises IndexDefect":
    let arr = newHalfFloatArray(@[hf1_0])
    expect(IndexDefect):
      discard arr[1]

  test "len returns correct count":
    check newHalfFloatArray(@[hf1_0]).len == 1
    check newHalfFloatArray(@[hf1_0, hf2_0]).len == 2
