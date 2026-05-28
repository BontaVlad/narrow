import unittest2
import ../src/narrow

template typeCodes(codes: varargs[int8]): (ptr int8, int) =
  (unsafeAddr codes[0], codes.len)

suite "Union - Data Types":
  test "create sparse union data type":
    let f1 = newField[int32]("a")
    let f2 = newField[string]("b")
    var codes = [0'i8, 1]
    let dt = newSparseUnionDataType([f1.toPtr, f2.toPtr],
      addr codes[0], codes.len)
    check not isNil(dt.handle)

  test "create dense union data type":
    let f1 = newField[int32]("x")
    let f2 = newField[float64]("y")
    var codes = [0'i8, 1]
    let dt = newDenseUnionDataType([f1.toPtr, f2.toPtr],
      addr codes[0], codes.len)
    check not isNil(dt.handle)

suite "Union - Sparse Array":
  test "create sparse union array":
    let f1 = newField[int32]("a")
    let f2 = newField[string]("b")
    var codes = [0'i8, 1]
    let typeIds = newArray(@[0'i8, 1, 0])
    let arr1 = newArray(@[10'i32, 20, 30])
    let arr2 = newArray(@["x", "y", "z"])
    let dt = newSparseUnionDataType([f1.toPtr, f2.toPtr],
      addr codes[0], codes.len)
    let arr = newSparseUnionArray(dt,
      cast[ptr GArrowInt8Array](typeIds.toPtr),
      [arr1.toPtr, arr2.toPtr])
    check arr.len == 3

  test "sparse union array string representation":
    let f1 = newField[int32]("a")
    let f2 = newField[string]("b")
    var codes = [0'i8, 1]
    let typeIds = newArray(@[0'i8])
    let arr1 = newArray(@[42'i32])
    let arr2 = newArray(@["hello"])
    let dt = newSparseUnionDataType([f1.toPtr, f2.toPtr],
      addr codes[0], codes.len)
    let arr = newSparseUnionArray(dt,
      cast[ptr GArrowInt8Array](typeIds.toPtr),
      [arr1.toPtr, arr2.toPtr])
    let s = $arr
    check s.len > 0

suite "Union - Dense Array":
  test "create dense union array":
    let f1 = newField[int32]("a")
    let f2 = newField[string]("b")
    var codes = [0'i8, 1]
    let typeIds = newArray(@[0'i8, 1, 0])
    let offsets = newArray(@[0'i32, 0, 1])
    let arr1 = newArray(@[10'i32, 20])
    let arr2 = newArray(@["x", "y"])
    let dt = newDenseUnionDataType([f1.toPtr, f2.toPtr],
      addr codes[0], codes.len)
    let arr = newDenseUnionArray(dt,
      cast[ptr GArrowInt8Array](typeIds.toPtr),
      cast[ptr GArrowInt32Array](offsets.toPtr),
      [arr1.toPtr, arr2.toPtr])
    check arr.len == 3

  test "getValueOffset returns offset for element":
    let f1 = newField[int32]("a")
    let f2 = newField[string]("b")
    var codes = [0'i8, 1]
    let typeIds = newArray(@[0'i8, 1, 0])
    let offsets = newArray(@[0'i32, 0, 1])
    let arr1 = newArray(@[10'i32, 20])
    let arr2 = newArray(@["x", "y"])
    let dt = newDenseUnionDataType([f1.toPtr, f2.toPtr],
      addr codes[0], codes.len)
    let arr = newDenseUnionArray(dt,
      cast[ptr GArrowInt8Array](typeIds.toPtr),
      cast[ptr GArrowInt32Array](offsets.toPtr),
      [arr1.toPtr, arr2.toPtr])
    check arr.getValueOffset(0) == 0
    check arr.getValueOffset(1) == 0
    check arr.getValueOffset(2) == 1

  test "dense union array string representation":
    let f1 = newField[int32]("a")
    var codes = [0'i8]
    let typeIds = newArray(@[0'i8])
    let offsets = newArray(@[0'i32])
    let arr1 = newArray(@[42'i32])
    let dt = newDenseUnionDataType([f1.toPtr],
      addr codes[0], codes.len)
    let arr = newDenseUnionArray(dt,
      cast[ptr GArrowInt8Array](typeIds.toPtr),
      cast[ptr GArrowInt32Array](offsets.toPtr),
      [arr1.toPtr])
    let s = $arr
    check s.len > 0

suite "Union - Scalars":
  test "create sparse union scalar":
    let f1 = newField[int32]("a")
    let f2 = newField[string]("b")
    var codes = [0'i8, 1]
    let dt = newSparseUnionDataType([f1.toPtr, f2.toPtr],
      addr codes[0], codes.len)
    let valSc = garrow_int32_scalar_new(42'i32)
    let sc = newSparseUnionScalar(dt, 0'i8, cast[ptr GArrowScalar](valSc))
    check not isNil(sc.handle)
    g_object_unref(valSc)

  test "create dense union scalar":
    let f1 = newField[int32]("a")
    var codes = [0'i8]
    let dt = newDenseUnionDataType([f1.toPtr],
      addr codes[0], codes.len)
    let valSc = garrow_int32_scalar_new(10'i32)
    let sc = newDenseUnionScalar(dt, 0'i8, cast[ptr GArrowScalar](valSc))
    check not isNil(sc.handle)
    g_object_unref(valSc)

suite "Union - Memory":
  test "copy semantics for sparse array":
    let f1 = newField[int32]("a")
    var codes = [0'i8]
    let typeIds = newArray(@[0'i8])
    let arr1 = newArray(@[42'i32])
    let dt = newSparseUnionDataType([f1.toPtr],
      addr codes[0], codes.len)
    let a1 = newSparseUnionArray(dt,
      cast[ptr GArrowInt8Array](typeIds.toPtr), [arr1.toPtr])
    let a2 = a1
    check a2.len == 1

  test "copy semantics for dense array":
    let f1 = newField[int32]("a")
    var codes = [0'i8]
    let typeIds = newArray(@[0'i8])
    let offsets = newArray(@[0'i32])
    let arr1 = newArray(@[42'i32])
    let dt = newDenseUnionDataType([f1.toPtr],
      addr codes[0], codes.len)
    let a1 = newDenseUnionArray(dt,
      cast[ptr GArrowInt8Array](typeIds.toPtr),
      cast[ptr GArrowInt32Array](offsets.toPtr), [arr1.toPtr])
    let a2 = a1
    check a2.len == 1
