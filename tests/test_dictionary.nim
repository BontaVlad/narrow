import unittest2
import ../src/narrow

suite "Dictionary - Data Type":
  test "create dictionary data type":
    let idxType = newGType(int32)
    let valType = newGType(string)
    let dt = newDictionaryDataType(idxType, valType)
    check not isNil(dt.handle)

  test "ordered dictionary data type":
    let idxType = newGType(int8)
    let valType = newGType(string)
    let orderedDt = newDictionaryDataType(idxType, valType, ordered = true)
    check orderedDt.isOrdered
    let unorderedDt = newDictionaryDataType(idxType, valType)
    check not unorderedDt.isOrdered

  test "default is unordered":
    let idxType = newGType(int32)
    let valType = newGType(float64)
    let dt = newDictionaryDataType(idxType, valType)
    check not dt.isOrdered

  test "index and value type accessors":
    let idxType = newGType(int16)
    let valType = newGType(string)
    let dt = newDictionaryDataType(idxType, valType)
    let idx = dt.indexDataType
    let val = dt.valueDataType
    check not isNil(idx)
    check not isNil(val)

suite "Dictionary - Array":
  test "create dictionary array from indices and values":
    let indices = newArray(@[0'i32, 1, 2, 0, 1])
    let dictionary = newArray(@["alpha", "beta", "gamma"])
    let idxType = newGType(int32)
    let valType = newGType(string)
    let dt = newDictionaryDataType(idxType, valType)
    let arr = newDictionaryArray(dt, indices, dictionary)
    check arr.len == 5

  test "get indices and dictionary from array":
    let indices = newArray(@[0'i32, 1, 2, 0, 1])
    let dictionary = newArray(@["alpha", "beta", "gamma"])
    let idxType = newGType(int32)
    let valType = newGType(string)
    let dt = newDictionaryDataType(idxType, valType)
    let arr = newDictionaryArray(dt, indices, dictionary)
    let outIndicesHandle = arr.indices
    let outDictHandle = arr.dictionary
    check not isNil(outIndicesHandle)
    check not isNil(outDictHandle)

  test "dictionary array with null":
    let indices = newArray(@[0'i32, 1, 2], [false, true, false])
    let dictionary = newArray(@["a", "b", "c"])
    let idxType = newGType(int32)
    let valType = newGType(string)
    let dt = newDictionaryDataType(idxType, valType)
    let arr = newDictionaryArray(dt, indices, dictionary)
    check arr.len == 3
    check arr.isNull(1)
    check not arr.isNull(0)

  test "empty dictionary array":
    let empty: seq[int32] = @[]
    let indices = newArray(empty)
    let dictionary = newArray(@["a"])
    let idxType = newGType(int32)
    let valType = newGType(string)
    let dt = newDictionaryDataType(idxType, valType)
    let arr = newDictionaryArray(dt, indices, dictionary)
    check arr.len == 0

  test "string representation":
    let indices = newArray(@[0'i32, 1])
    let dictionary = newArray(@["hello", "world"])
    let idxType = newGType(int32)
    let valType = newGType(string)
    let dt = newDictionaryDataType(idxType, valType)
    let arr = newDictionaryArray(dt, indices, dictionary)
    let s = $arr
    check s.len > 0

  test "dictionaryDataType accessor":
    let indices = newArray(@[0'i32, 1])
    let dictionary = newArray(@["x", "y"])
    let idxType = newGType(int32)
    let valType = newGType(string)
    let dt = newDictionaryDataType(idxType, valType)
    let arr = newDictionaryArray(dt, indices, dictionary)
    let outDt = arr.dictionaryDataType
    check not isNil(outDt)

suite "Dictionary - Encode Options":
  test "create encode options":
    let opts = newDictionaryEncodeOptions()
    check not isNil(opts.handle)

suite "Dictionary - Memory":
  test "copy semantics for array":
    let indices = newArray(@[0'i32, 1, 2])
    let dictionary = newArray(@["a", "b", "c"])
    let idxType = newGType(int32)
    let valType = newGType(string)
    let dt = newDictionaryDataType(idxType, valType)
    let arr1 = newDictionaryArray(dt, indices, dictionary)
    let arr2 = arr1
    check arr2.len == 3

  test "many values":
    let n = 1000
    var idxVals = newSeq[int32](n)
    for i in 0 ..< n:
      idxVals[i] = int32(i mod 10)
    let indices = newArray(idxVals)
    var dictVals = newSeq[string](10)
    for i in 0 ..< 10:
      dictVals[i] = "val_" & $i
    let dictionary = newArray(dictVals)
    let idxType = newGType(int32)
    let valType = newGType(string)
    let dt = newDictionaryDataType(idxType, valType)
    let arr = newDictionaryArray(dt, indices, dictionary)
    check arr.len == n
