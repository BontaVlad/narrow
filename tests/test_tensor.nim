import unittest2
import ../src/narrow

suite "Tensor - Creation":
  test "create 1D tensor from float64 data":
    let values = @[1'f64, 2, 3, 4, 5, 6]
    let buf = newBuffer(cast[pointer](values[0].unsafeAddr), values.len.int64 * 8)
    let dt = newGType(float64)
    let tensor = newTensor(dt, buf, [6'i64])
    check tensor.nDimensions == 1
    check tensor.shape == @[6'i64]
    check tensor.size == 6

  test "create 2D tensor with explicit strides":
    let values = @[1'f64, 2, 3, 4, 5, 6]
    let buf = newBuffer(cast[pointer](values[0].unsafeAddr), values.len.int64 * 8)
    let dt = newGType(float64)
    let tensor = newTensor(dt, buf, [2'i64, 3], [24'i64, 8])
    check tensor.nDimensions == 2
    check tensor.shape == @[2'i64, 3]
    check tensor.strides == @[24'i64, 8]

  test "create tensor with dimension names":
    let values = @[1'f64, 2, 3, 4]
    let buf = newBuffer(cast[pointer](values[0].unsafeAddr), values.len.int64 * 8)
    let dt = newGType(float64)
    let tensor = newTensor(dt, buf, [2'i64, 2], dimNames = ["rows", "cols"])
    check tensor.nDimensions == 2
    check tensor.dimensionName(0) == "rows"
    check tensor.dimensionName(1) == "cols"

  test "create tensor with int32 data":
    let values = @[10'i32, 20, 30]
    let buf = newBuffer(cast[pointer](values[0].unsafeAddr), values.len.int64 * 4)
    let dt = newGType(int32)
    let tensor = newTensor(dt, buf, [3'i64])
    check tensor.nDimensions == 1
    check tensor.size == 3

suite "Tensor - Properties":
  test "isContiguous for simple 1D tensor":
    let values = @[1'f32, 2, 3, 4]
    let buf = newBuffer(cast[pointer](values[0].unsafeAddr), values.len.int64 * 4)
    let dt = newGType(float32)
    let tensor = newTensor(dt, buf, [4'i64])
    check tensor.isContiguous

  test "isRowMajor for 2D tensor":
    let nrows = 3'i64
    let ncols = 4'i64
    var values = newSeq[float64](int(nrows * ncols))
    for i in 0 ..< values.len:
      values[i] = float64(i)
    let buf = newBuffer(cast[pointer](values[0].unsafeAddr), values.len.int64 * 8)
    let dt = newGType(float64)
    let tensor = newTensor(dt, buf, [nrows, ncols])
    check tensor.isRowMajor
    check tensor.isContiguous

  test "value type accessors":
    let values = @[1'i32, 2, 3]
    let buf = newBuffer(cast[pointer](values[0].unsafeAddr), values.len.int64 * 4)
    let dt = newGType(int32)
    let tensor = newTensor(dt, buf, [3'i64])
    check tensor.valueType == GARROW_TYPE_INT32

  test "buffer access returns tensor's data buffer":
    let values = @[1'f64, 2, 3]
    let buf = newBuffer(cast[pointer](values[0].unsafeAddr), values.len.int64 * 8)
    let dt = newGType(float64)
    let tensor = newTensor(dt, buf, [3'i64])
    let outBuf = tensor.buffer
    check outBuf.dataSize == 24'i64  # 3 * 8

  test "equal tensors":
    let values = @[1'f64, 2, 3, 4]
    let buf1 = newBuffer(cast[pointer](values[0].unsafeAddr), values.len.int64 * 8)
    let buf2 = newBuffer(cast[pointer](values[0].unsafeAddr), values.len.int64 * 8)
    let dt = newGType(float64)
    let t1 = newTensor(dt, buf1, [4'i64])
    let t2 = newTensor(dt, buf2, [4'i64])
    check t1 == t2

  test "dimension name out of bounds raises":
    let values = @[1'f64, 2, 3]
    let buf = newBuffer(cast[pointer](values[0].unsafeAddr), values.len.int64 * 8)
    let dt = newGType(float64)
    let tensor = newTensor(dt, buf, [3'i64])
    expect(IndexDefect):
      discard tensor.dimensionName(-1)
    expect(IndexDefect):
      discard tensor.dimensionName(1)

suite "Tensor - Memory":
  test "many dimensions":
    let n = 10_000'i64
    var values = newSeq[float64](n.int)
    let buf = newBuffer(cast[pointer](values[0].unsafeAddr), n * 8)
    let dt = newGType(float64)
    let tensor = newTensor(dt, buf, [n])
    check tensor.size == n

  test "copy semantics":
    let values = @[1'f64, 2, 3]
    let buf = newBuffer(cast[pointer](values[0].unsafeAddr), values.len.int64 * 8)
    let dt = newGType(float64)
    let t1 = newTensor(dt, buf, [3'i64])
    let t2 = t1
    check t2.size == 3
    check t2.shape == @[3'i64]
