import unittest2
import ../src/narrow

suite "REE - Data Type":
  test "create REE data type":
    let runType = newGType(int32)
    let valType = newGType(string)
    let dt = newRunEndEncodedDataType(runType, valType)
    check not isNil(dt.handle)

  test "runEndDataType and valueDataType accessors":
    let dt = newRunEndEncodedDataType(newGType(int16), newGType(float64))
    check not isNil(dt.runEndDataType)
    check not isNil(dt.valueDataType)

suite "REE - Array":
  test "create REE array":
    let runEnds = newArray(@[3'i32, 5, 7])
    let values = newArray(@["a", "b", "c"])
    let dt = newRunEndEncodedDataType(newGType(int32), newGType(string))
    let arr = newRunEndEncodedArray(dt, 7, runEnds, values)
    check arr.len == 7

  test "runEnds and values accessors":
    let runEnds = newArray(@[2'i32, 4])
    let values = newArray(@["x", "y"])
    let dt = newRunEndEncodedDataType(newGType(int32), newGType(string))
    let arr = newRunEndEncodedArray(dt, 4, runEnds, values)
    check not isNil(arr.runEnds)
    check not isNil(arr.values)

  test "logicalRunEnds and logicalValues":
    let runEnds = newArray(@[2'i32, 4])
    let values = newArray(@["x", "y"])
    let dt = newRunEndEncodedDataType(newGType(int32), newGType(string))
    let arr = newRunEndEncodedArray(dt, 4, runEnds, values)
    check not isNil(arr.logicalRunEnds)
    check not isNil(arr.logicalValues)

  test "decode returns expanded array":
    let runEnds = newArray(@[3'i32, 5, 7])
    let values = newArray(@["a", "b", "c"])
    let dt = newRunEndEncodedDataType(newGType(int32), newGType(string))
    let arr = newRunEndEncodedArray(dt, 7, runEnds, values)
    let decoded = arr.decode
    check not isNil(decoded)

  test "findPhysicalOffset and findPhysicalLength":
    let runEnds = newArray(@[3'i32, 5, 7])
    let values = newArray(@["a", "b", "c"])
    let dt = newRunEndEncodedDataType(newGType(int32), newGType(string))
    let arr = newRunEndEncodedArray(dt, 7, runEnds, values)
    discard arr.findPhysicalOffset
    discard arr.findPhysicalLength

  test "string representation":
    let runEnds = newArray(@[2'i32, 4])
    let values = newArray(@["x", "y"])
    let dt = newRunEndEncodedDataType(newGType(int32), newGType(string))
    let arr = newRunEndEncodedArray(dt, 4, runEnds, values)
    let s = $arr
    check s.len > 0

suite "REE - Memory":
  test "copy semantics":
    let runEnds = newArray(@[1'i32, 2])
    let values = newArray(@["a", "b"])
    let dt = newRunEndEncodedDataType(newGType(int32), newGType(string))
    let arr1 = newRunEndEncodedArray(dt, 2, runEnds, values)
    let arr2 = arr1
    check arr2.len == 2
