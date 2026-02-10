import std/options
import unittest2
import ../src/[ffi, garray, gchunkedarray, gtypes]

suite "ChunkedArray - Construction":
  test "Empty chunked array with data type":
    let chunkedArray = newChunkedArray[bool]()
    check chunkedArray.getValueType() == GArrowType.GARROW_TYPE_BOOLEAN
    check chunkedArray.nChunks() == 1
    check chunkedArray.len() == 0

  test "Create from single chunk":
    let chunks = [newArray[bool](@[true, false, true])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.getValueType() == GArrowType.GARROW_TYPE_BOOLEAN
    check chunkedArray.nChunks() == 1
    check chunkedArray.len() == 3

  test "Create from multiple chunks":
    let chunks = [
      newArray[bool](@[true, false]),
      newArray[bool](@[true]),
      newArray[bool](@[false, true, false]),
    ]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.nChunks() == 3
    check chunkedArray.len() == 6
    check chunkedArray.getChunk(1).len == 1

  test "Create from empty sequence of chunks":
    let chunks: seq[Array[bool]] = @[]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.nChunks() == 1
    check chunkedArray.len() == 0

suite "ChunkedArray - Equality":
  test "Equal chunked arrays with same chunking":
    let chunks1 = [newArray[bool](@[true, false]), newArray[bool](@[true])]
    let chunks2 = [newArray[bool](@[true, false]), newArray[bool](@[true])]
    check newChunkedArray(chunks1) == newChunkedArray(chunks2)

  test "Equal chunked arrays with different chunking":
    let chunks1 = [newArray[bool](@[true, false]), newArray[bool](@[true])]
    let chunks2 = [newArray[bool](@[true]), newArray[bool](@[false, true])]
    check newChunkedArray(chunks1) == newChunkedArray(chunks2)

  test "Not equal - different values":
    let chunks1 = [newArray[int32](@[1'i32, 2]), newArray[int32](@[3'i32])]
    let chunks2 = [newArray[int32](@[1'i32, 2]), newArray[int32](@[4'i32])]
    check newChunkedArray(chunks1) != newChunkedArray(chunks2)

  test "Not equal - different lengths":
    let chunks1 = [newArray[int32](@[1'i32, 2, 3])]
    let chunks2 = [newArray[int32](@[1'i32, 2])]
    check newChunkedArray(chunks1) != newChunkedArray(chunks2)

  test "Empty chunked arrays are equal":
    let ca1 = newChunkedArray[int32]()
    let ca2 = newChunkedArray[int32]()
    check ca1 == ca2

  test "Different number of chunks but same values are equal":
    let chunks1 = [newArray[int32](@[1'i32, 2, 3, 4, 5])]
    let chunks2 = [
      newArray[int32](@[1'i32, 2]),
      newArray[int32](@[3'i32]),
      newArray[int32](@[4'i32, 5]),
    ]
    check newChunkedArray(chunks1) == newChunkedArray(chunks2)

  test "Same values with different chunk boundaries are equal":
    let chunks1 =
      [newArray[int32](@[1'i32]), newArray[int32](@[2'i32]), newArray[int32](@[3'i32])]
    let chunks2 = [newArray[int32](@[1'i32, 2, 3])]
    check newChunkedArray(chunks1) == newChunkedArray(chunks2)

suite "ChunkedArray - Data Type and Type":
  test "value_data_type for boolean":
    let chunks = [newArray[bool](@[true, false]), newArray[bool](@[true])]
    check newChunkedArray(chunks).getValueType() == GArrowType.GARROW_TYPE_BOOLEAN

  test "value_data_type for int32":
    let chunks = [newArray[int32](@[1'i32, 2, 3])]
    check newChunkedArray(chunks).getValueType() == GArrowType.GARROW_TYPE_INT32

  test "value_data_type for int64":
    let chunks = [newArray[int64](@[1'i64, 2, 3])]
    check newChunkedArray(chunks).getValueType() == GArrowType.GARROW_TYPE_INT64

  test "value_data_type for float64":
    let chunks = [newArray[float64](@[1.5, 2.5, 3.5])]
    check newChunkedArray(chunks).getValueType() == GArrowType.GARROW_TYPE_DOUBLE

  test "value_data_type for string":
    let chunks = [newArray[string](@["hello", "world"])]
    check newChunkedArray(chunks).getValueType() == GArrowType.GARROW_TYPE_STRING

suite "ChunkedArray - Dimensions":
  test "n_rows with single chunk":
    let chunks = [newArray[bool](@[true, false, true])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.nRows() == 3

  test "n_rows with multiple chunks":
    let chunks = [newArray[bool](@[true, false]), newArray[bool](@[true])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.nRows() == 3

  test "n_rows empty":
    let chunkedArray = newChunkedArray[bool]()
    check chunkedArray.nRows() == 0

  test "len equals n_rows":
    let chunks = [newArray[int32](@[1'i32, 2, 3, 4, 5])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == int(chunkedArray.nRows())

suite "ChunkedArray - Null Handling":
  test "n_nulls with no nulls":
    let chunks = [newArray[bool](@[true, false]), newArray[bool](@[true])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.nNulls() == 0

  test "n_nulls with nulls in single chunk":
    var builder = newArrayBuilder[bool]()
    builder.append(true)
    builder.appendNull()
    builder.append(false)
    let chunks = [builder.finish()]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.nNulls() == 1

  test "n_nulls with nulls in multiple chunks":
    var builder1 = newArrayBuilder[bool]()
    builder1.append(true)
    builder1.appendNull()
    builder1.append(false)

    var builder2 = newArrayBuilder[bool]()
    builder2.appendNull()
    builder2.appendNull()
    builder2.append(true)

    let chunks = [builder1.finish(), builder2.finish()]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.nNulls() == 3

  test "n_nulls with all nulls":
    var builder = newArrayBuilder[int32]()
    for i in 0 .. 9:
      builder.appendNull()
    let chunks = [builder.finish()]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.nNulls() == 10

suite "ChunkedArray - Chunks Access":
  test "n_chunks":
    let chunks = [newArray[bool](@[true]), newArray[bool](@[false])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.nChunks() == 2

  test "get_chunk":
    let chunks = [newArray[bool](@[true, false]), newArray[bool](@[false])]
    let chunkedArray = newChunkedArray(chunks)
    let chunk = chunkedArray.getChunk(0)
    check chunk.len() == 2
    check chunk[0] == true
    check chunk[1] == false

  test "get_chunk second chunk":
    let chunks = [newArray[bool](@[true, false]), newArray[bool](@[false])]
    let chunkedArray = newChunkedArray(chunks)
    let chunk = chunkedArray.getChunk(1)
    check chunk.len() == 1
    check chunk[0] == false

  test "get_chunk out of bounds":
    let chunks = [newArray[bool](@[true])]
    let chunkedArray = newChunkedArray(chunks)
    expect(IndexDefect):
      discard chunkedArray.getChunk(10)

  test "chunks iterator":
    let chunks = [newArray[int32](@[1'i32, 2]), newArray[int32](@[3'i32, 4, 5])]
    let chunkedArray = newChunkedArray(chunks)

    var lengths: seq[int]
    for chunk in chunkedArray.chunks:
      lengths.add(chunk.len())

    check lengths == @[2, 3]

  test "chunks iterator - collect values":
    let chunks = [newArray[int32](@[1'i32, 2]), newArray[int32](@[3'i32])]
    let chunkedArray = newChunkedArray(chunks)

    var allValues: seq[int32]
    for chunk in chunkedArray.chunks:
      for val in chunk:
        allValues.add(val)

    check allValues == @[1'i32, 2, 3]

suite "ChunkedArray - Slicing":
  test "slice with offset and length":
    let chunks = [newArray[int32](@[1'i32, 2, 3, 4, 5])]
    let chunkedArray = newChunkedArray(chunks)
    let sliced = chunkedArray.slice(1'u64, 3'u64)
    check sliced.len() == 3
    check sliced[0] == 2
    check sliced[1] == 3
    check sliced[2] == 4

  test "slice with HSlice":
    let chunks = [newArray[int32](@[1'i32, 2, 3, 4, 5])]
    let chunkedArray = newChunkedArray(chunks)
    let sliced = chunkedArray.slice(1 .. 3)
    check sliced.len() == 3
    check sliced[0] == 2
    check sliced[1] == 3
    check sliced[2] == 4

  test "slice across chunks":
    let chunks = [
      newArray[int32](@[1'i32, 2]),
      newArray[int32](@[3'i32, 4]),
      newArray[int32](@[5'i32, 6]),
    ]
    let chunkedArray = newChunkedArray(chunks)
    let sliced = chunkedArray.slice(1 .. 4)
    check sliced.len() == 4
    check sliced[0] == 2
    check sliced[3] == 5

suite "ChunkedArray - Combine":
  test "combine single chunk":
    let chunks = [newArray[int32](@[1'i32, 2, 3])]
    let chunkedArray = newChunkedArray(chunks)
    let combined = chunkedArray.combine()
    check combined.len() == 3
    check combined[0] == 1
    check combined[1] == 2
    check combined[2] == 3

  test "combine multiple chunks":
    let chunks = [
      newArray[int32](@[1'i32, 2]),
      newArray[int32](@[3'i32]),
      newArray[int32](@[4'i32, 5]),
    ]
    let chunkedArray = newChunkedArray(chunks)
    let combined = chunkedArray.combine()
    check combined.len() == 5
    check combined[0] == 1
    check combined[4] == 5

suite "ChunkedArray - String Representation":
  test "to_s for boolean chunked array":
    let chunks = [newArray[bool](@[true, false]), newArray[bool](@[true])]
    let chunkedArray = newChunkedArray(chunks)
    let str = $chunkedArray
    check str.len > 0

  test "to_s for int32 chunked array":
    let chunks = [newArray[int32](@[1'i32, 2]), newArray[int32](@[3'i32, 4])]
    let chunkedArray = newChunkedArray(chunks)
    let str = $chunkedArray
    check str.len > 0

  test "to_s for empty chunked array":
    let chunkedArray = newChunkedArray[int32]()
    let str = $chunkedArray
    check str.len > 0

  test "to_s for string chunked array":
    let chunks = [newArray[string](@["hello", "world"]), newArray[string](@["test"])]
    let chunkedArray = newChunkedArray(chunks)
    let str = $chunkedArray
    check str.len > 0

suite "ChunkedArray - Indexing":
  test "index access in first chunk":
    let chunks = [newArray[int32](@[1'i32, 2]), newArray[int32](@[3'i32, 4])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray[0] == 1
    check chunkedArray[1] == 2

  test "index access in second chunk":
    let chunks = [newArray[int32](@[1'i32, 2]), newArray[int32](@[3'i32, 4])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray[2] == 3
    check chunkedArray[3] == 4

  test "index out of bounds":
    let chunks = [newArray[int32](@[1'i32, 2])]
    let chunkedArray = newChunkedArray(chunks)
    expect(IndexDefect):
      discard chunkedArray[5]

  test "isNull check":
    var builder = newArrayBuilder[int32]()
    builder.append(1'i32)
    builder.appendNull()
    builder.append(3'i32)

    let chunks = [builder.finish()]
    let chunkedArray = newChunkedArray(chunks)

    check not chunkedArray.isNull(0)
    check chunkedArray.isNull(1)
    check not chunkedArray.isNull(2)

  test "isValid check":
    var builder = newArrayBuilder[int32]()
    builder.append(1'i32)
    builder.appendNull()
    builder.append(3'i32)

    let chunks = [builder.finish()]
    let chunkedArray = newChunkedArray(chunks)

    check chunkedArray.isValid(0)
    check not chunkedArray.isValid(1)
    check chunkedArray.isValid(2)

  test "tryGet with valid value":
    let chunks = [newArray[int32](@[1'i32, 2, 3])]
    let chunkedArray = newChunkedArray(chunks)
    let val = chunkedArray.tryGet(1)
    check val.isSome
    check val.get() == 2

  test "tryGet with null value":
    var builder = newArrayBuilder[int32]()
    builder.append(1'i32)
    builder.appendNull()
    builder.append(3'i32)

    let chunks = [builder.finish()]
    let chunkedArray = newChunkedArray(chunks)
    let val = chunkedArray.tryGet(1)
    check val.isNone

  test "tryGet out of bounds":
    let chunks = [newArray[int32](@[1'i32, 2])]
    let chunkedArray = newChunkedArray(chunks)
    let val = chunkedArray.tryGet(10)
    check val.isNone

suite "ChunkedArray - Iteration":
  test "items iterator across chunks":
    let chunks = [newArray[int32](@[1'i32, 2]), newArray[int32](@[3'i32, 4, 5])]
    let chunkedArray = newChunkedArray(chunks)

    var values: seq[int32]
    for val in chunkedArray:
      values.add(val)

    check values == @[1'i32, 2, 3, 4, 5]

  test "items iterator with single chunk":
    let chunks = [newArray[int32](@[1'i32, 2, 3, 4, 5])]
    let chunkedArray = newChunkedArray(chunks)

    var sum = 0'i32
    for val in chunkedArray:
      sum += val

    check sum == 15

  test "items iterator with strings":
    let chunks = [newArray[string](@["a", "b"]), newArray[string](@["c"])]
    let chunkedArray = newChunkedArray(chunks)

    var concat = ""
    for val in chunkedArray:
      concat &= val

    check concat == "abc"

  test "items iterator empty chunked array":
    let chunkedArray = newChunkedArray[int32]()

    var count = 0
    for val in chunkedArray:
      count += 1

    check count == 0

suite "ChunkedArray - Conversion":
  test "to seq - single chunk":
    let chunks = [newArray[int32](@[1'i32, 2, 3, 4, 5])]
    let chunkedArray = newChunkedArray(chunks)
    let s = @chunkedArray
    check s == @[1'i32, 2, 3, 4, 5]

  test "to seq - multiple chunks":
    let chunks = [
      newArray[int32](@[1'i32, 2]),
      newArray[int32](@[3'i32, 4]),
      newArray[int32](@[5'i32]),
    ]
    let chunkedArray = newChunkedArray(chunks)
    let s = @chunkedArray
    check s == @[1'i32, 2, 3, 4, 5]

suite "ChunkedArray - Different Types":
  test "int8 chunked array":
    let chunks = [newArray[int8](@[1'i8, 2]), newArray[int8](@[3'i8, 4, 5])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 5
    check chunkedArray.nChunks() == 2
    check chunkedArray.getValueType() == GArrowType.GARROW_TYPE_INT8

  test "int16 chunked array":
    let chunks = [newArray[int16](@[100'i16, 200]), newArray[int16](@[300'i16])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 3
    check chunkedArray.getValueType() == GArrowType.GARROW_TYPE_INT16

  test "int64 chunked array":
    let chunks = [newArray[int64](@[1000000'i64, 2000000])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 2
    check chunkedArray.getValueType() == GArrowType.GARROW_TYPE_INT64

  test "uint8 chunked array":
    let chunks = [newArray[uint8](@[1'u8, 2, 3])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.getValueType() == GArrowType.GARROW_TYPE_UINT8

  test "uint32 chunked array":
    let chunks = [newArray[uint32](@[1000'u32, 2000])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 2
    check chunkedArray.getValueType() == GArrowType.GARROW_TYPE_UINT32

  test "uint64 chunked array":
    let chunks = [newArray[uint64](@[1000000'u64])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 1
    check chunkedArray.getValueType() == GArrowType.GARROW_TYPE_UINT64

  test "float32 chunked array":
    let chunks = [newArray[float32](@[1.5'f32, 2.5]), newArray[float32](@[3.5'f32])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 3
    check chunkedArray.getValueType() == GArrowType.GARROW_TYPE_FLOAT

  test "float64 chunked array":
    let chunks = [newArray[float64](@[1.5, 2.5, 3.5])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 3
    check chunkedArray.getValueType() == GArrowType.GARROW_TYPE_DOUBLE

suite "ChunkedArray - Edge Cases":
  test "Single element total":
    let chunks = [newArray[int32](@[42'i32])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 1
    check chunkedArray.nChunks() == 1

  test "Many small chunks":
    var chunks: seq[Array[int32]]
    for i in 0 .. 99:
      chunks.add(newArray[int32](@[i.int32]))
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 100
    check chunkedArray.nChunks() == 100

  test "One large chunk":
    var values: seq[int32]
    for i in 0 ..< 10000:
      values.add(i.int32)
    let chunks = [newArray[int32](values)]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 10000
    check chunkedArray.nChunks() == 1

  test "Mix of chunk sizes":
    let chunks = [
      newArray[int32](@[1'i32]),
      newArray[int32](@[2'i32, 3]),
      newArray[int32](@[4'i32, 5, 6]),
      newArray[int32](@[7'i32, 8, 9, 10]),
    ]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 10
    check chunkedArray.nChunks() == 4

  test "Empty strings in chunks":
    let chunks = [
      newArray[string](@["", "hello"]),
      newArray[string](@["", ""]),
      newArray[string](@["world"]),
    ]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 5

  test "Unicode in string chunks":
    let chunks =
      [newArray[string](@["Hello 👋", "World 🌍"]), newArray[string](@["Nim 🎯"])]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 3

  test "Special float values across chunks":
    let chunks = [
      newArray[float64](@[Inf, -Inf]),
      newArray[float64](@[0.0, -0.0]),
      newArray[float64](@[1.0]),
    ]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 5

  test "Integer edge values":
    let chunks = [
      newArray[int32](@[high(int32)]),
      newArray[int32](@[low(int32)]),
      newArray[int32](@[0'i32]),
    ]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 3

suite "ChunkedArray - Memory Management":
  test "Create and destroy many chunked arrays":
    for i in 0 .. 1000:
      let chunks = [newArray[int32](@[1'i32, 2, 3]), newArray[int32](@[4'i32, 5])]
      let chunkedArray = newChunkedArray(chunks)
      check chunkedArray.len() == 5

  test "Large number of chunks":
    var chunks: seq[Array[int32]]
    for i in 0 ..< 1000:
      chunks.add(newArray[int32](@[i.int32]))
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.nChunks() == 1000

  test "Deep iteration":
    let chunks = [newArray[int32](@[1'i32, 2]), newArray[int32](@[3'i32, 4])]
    let chunkedArray = newChunkedArray(chunks)

    for cycle in 0 .. 100:
      var count = 0
      for val in chunkedArray:
        count += 1
      check count == 4

  test "Multiple chunked arrays from same chunks":
    let chunks = [newArray[int32](@[1'i32, 2, 3])]
    let ca1 = newChunkedArray(chunks)
    let ca2 = newChunkedArray(chunks)
    check ca1 == ca2

suite "ChunkedArray - Complex Scenarios":
  test "Mixed null and value chunks":
    var nullBuilder = newArrayBuilder[int32]()
    nullBuilder.appendNull()
    nullBuilder.appendNull()

    var mixedBuilder = newArrayBuilder[int32]()
    mixedBuilder.append(1'i32)
    mixedBuilder.appendNull()
    mixedBuilder.append(2'i32)

    let chunks =
      [nullBuilder.finish(), newArray[int32](@[3'i32, 4]), mixedBuilder.finish()]
    let chunkedArray = newChunkedArray(chunks)
    check chunkedArray.len() == 7
    check chunkedArray.nNulls() == 3

  test "Collect all values skipping nulls":
    var builder = newArrayBuilder[int32]()
    builder.append(1'i32)
    builder.appendNull()
    builder.append(2'i32)

    let chunks = [builder.finish(), newArray[int32](@[3'i32, 4])]
    let chunkedArray = newChunkedArray(chunks)

    var values: seq[int32]
    var chunkIdx = 0'u
    for chunk in chunkedArray.chunks:
      for i in 0 ..< chunk.len():
        if not chunk.isNull(i):
          values.add(chunk[i])
      chunkIdx += 1

    check values == @[1'i32, 2, 3, 4]

  test "Count valid values per chunk":
    var builder = newArrayBuilder[bool]()
    builder.append(true)
    builder.appendNull()

    let chunks =
      [newArray(@[true, false]), builder.finish(), newArray(@[false, true, false])]
    let chunkedArray = newChunkedArray(chunks)

    var validCounts: seq[int]
    for chunk in chunkedArray.chunks:
      var count = 0
      for i in 0 ..< chunk.len():
        if chunk.isValid(i):
          count += 1
      validCounts.add(count)

    check validCounts == @[2, 1, 3]

  test "String chunks with mixed content":
    let chunks =
      [newArray(@["", "hello"]), newArray(@["world", "", "test"]), newArray(@[""])]
    let chunkedArray = newChunkedArray(chunks)

    var nonEmpty = 0
    for val in chunkedArray:
      if val.len > 0:
        nonEmpty += 1

    check nonEmpty == 3

  test "Verify chunk boundaries":
    let chunks = [newArray(@[1'i32, 2]), newArray(@[3'i32, 4, 5]), newArray(@[6'i32])]
    let chunkedArray = newChunkedArray(chunks)

    check chunkedArray.getChunk(0).len() == 2
    check chunkedArray.getChunk(1).len() == 3
    check chunkedArray.getChunk(2).len() == 1

suite "ChunkedArray - Error Handling":
  test "Empty chunked array operations":
    let chunkedArray = newChunkedArray[int32]()
    check chunkedArray.len() == 0
    check chunkedArray.nChunks() == 1
    check chunkedArray.nNulls() == 0
    check chunkedArray.nRows() == 0

  test "Access chunk beyond bounds":
    let chunks = [newArray(@[1'i32, 2, 3])]
    let chunkedArray = newChunkedArray(chunks)

    expect(IndexDefect):
      discard chunkedArray.getChunk(5)

  test "Iteration over empty chunked array":
    let chunkedArray = newChunkedArray[int32]()

    var count = 0
    for val in chunkedArray:
      count += 1

    check count == 0

  test "Chunks iterator on empty":
    let chunkedArray = newChunkedArray[int32]()

    var chunkCount = 0
    for chunk in chunkedArray.chunks:
      chunkCount += 1

    check chunkCount == 0
