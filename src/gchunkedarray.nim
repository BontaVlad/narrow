import std/[options]
import ./[ffi, gtypes, error, garray, glist]

type ChunkedArray*[T] = object
  handle: ptr GArrowChunkedArray

proc toPtr*[T](c: ChunkedArray[T]): ptr GArrowChunkedArray {.inline.} =
  c.handle

proc `=destroy`*[T](c: ChunkedArray[T]) =
  if not isNil(c.toPtr):
    g_object_unref(cast[gpointer](c.toPtr))

proc newChunkedArray*[T](chunks: openArray[Array[T]]): ChunkedArray[T] =
  let cList = newGList(chunks)
  var handle: ptr GArrowChunkedArray
  if len(cList) == 0:
    handle = check garrow_chunked_array_new_empty(newGType(T).toPtr)
  else:
    handle = check garrow_chunked_array_new(cList.list)
  result = ChunkedArray[T](handle: handle)

proc newChunkedArray*[T](): ChunkedArray[T] {.inline.} =
  let dataType = newGType(T)
  let handle = check garrow_chunked_array_new_empty(dataType.toPtr)
  result = ChunkedArray[T](handle: handle)

proc newChunkedArray*(cAbiArrayStream: pointer): ChunkedArray =
  let handle = check garrow_chunked_array_import(cAbiArrayStream)
  result = ChunkedArray(handle: handle)

proc newChunkedArray*(rawPtr: ptr GArrowChunkedArray): ChunkedArray =
  result = ChunkedArray(handle: rawPtr)

proc `==`*(chunkedArray: ChunkedArray, other: ChunkedArray): bool =
  result = garrow_chunked_array_equal(chunkedArray.toPtr, other.toPtr) != 0

proc getValueDataType*(chunkedArray: ChunkedArray): GADType =
  result = cast[GADType](garrow_chunked_array_get_value_data_type(chunkedArray.toPtr))

proc getValueType*(chunkedArray: ChunkedArray): GArrowType =
  result = garrow_chunked_array_get_value_type(chunkedArray.toPtr)

proc len*(chunkedArray: ChunkedArray): int =
  result = int(garrow_chunked_array_get_length(chunkedArray.toPtr))

proc getNRows*(chunkedArray: ChunkedArray): uint64 =
  result = garrow_chunked_array_get_n_rows(chunkedArray.toPtr)

proc getNNulls*(chunkedArray: ChunkedArray): uint64 =
  result = garrow_chunked_array_get_n_nulls(chunkedArray.toPtr)

proc nChunks*(chunkedArray: ChunkedArray): uint =
  result = garrow_chunked_array_get_n_chunks(chunkedArray.toPtr)

proc getChunk*[T](chunkedArray: ChunkedArray[T], i: uint): Array[T] =
  if i.int >= chunkedArray.len:
    raise newException(IndexDefect, "Chunk index out of bounds")
  let handle = garrow_chunked_array_get_chunk(chunkedArray.toPtr, i.guint)
  result = newArray[T](handle)

proc getChunks*[T](chunkedArray: ChunkedArray[T]): ptr GList =
  result = garrow_chunked_array_get_chunks(chunkedArray.toPtr)

proc slice*(chunkedArray: ChunkedArray, offset: uint64, length: uint64): ChunkedArray =
  let handle =
    garrow_chunked_array_slice(chunkedArray.toPtr, offset.guint64, length.guint64)
  result = ChunkedArray(handle: handle)

proc slice*(chunkedArray: ChunkedArray, slice: HSlice[int, int]): ChunkedArray =
  let start = uint64(slice.a)
  let length = uint64(slice.b - slice.a + 1)
  result = chunkedArray.slice(start, length)

proc `$`*(chunkedArray: ChunkedArray): string =
  let cStr = check garrow_chunked_array_to_string(chunkedArray.toPtr)
  result = $newGString(cStr)

proc combine*[T](chunkedArray: ChunkedArray[T]): Array[T] =
  let handle = check garrow_chunked_array_combine(chunkedArray.toPtr)
  result = newArray[T](handle)

proc exportCArray*(chunkedArray: ChunkedArray): pointer =
  result = check garrow_chunked_array_export(chunkedArray.toPtr)

# TODO: use getChunks, don't iterate with getChunk, maybe profile and see what is the overhead
iterator chunks*[T](chunkedArray: ChunkedArray[T]): Array[T] =
  let nChunks = chunkedArray.nChunks()
  if len(chunkedArray) > 0:
    for i in 0.uint ..< nChunks:
      yield chunkedArray.getChunk(i)

# Indexing support (across all chunks)
proc `[]`*[T](chunkedArray: ChunkedArray[T], i: int): T =
  if i < 0 or i >= chunkedArray.len:
    raise newException(IndexDefect, "Index out of bounds")

  var currentIndex = i
  let nChunks = chunkedArray.nChunks()

  for chunkIdx in 0.uint ..< nChunks:
    let chunk = chunkedArray.getChunk(chunkIdx)
    let chunkLen = chunk.len

    if currentIndex < chunkLen:
      return chunk[currentIndex]
    else:
      currentIndex -= chunkLen

  raise newException(IndexDefect, "Index out of bounds")

# Check if value at index is null (across all chunks)
proc isNull*(chunkedArray: ChunkedArray, i: int): bool =
  if i < 0 or i >= chunkedArray.len:
    raise newException(IndexDefect, "Index out of bounds")

  var currentIndex = i
  let nChunks = chunkedArray.nChunks()

  for chunkIdx in 0.uint ..< nChunks:
    let chunk = chunkedArray.getChunk(chunkIdx)
    let chunkLen = chunk.len

    if currentIndex < chunkLen:
      return chunk.isNull(currentIndex)
    else:
      currentIndex -= chunkLen

  raise newException(IndexDefect, "Index out of bounds")

# Check if value at index is valid (across all chunks)
proc isValid*(chunkedArray: ChunkedArray, i: int): bool =
  result = not chunkedArray.isNull(i)

# Safe getter with Option-like semantics
proc tryGet*[T](chunkedArray: ChunkedArray[T], i: int): Option[T] =
  if i < 0 or i >= chunkedArray.len:
    return none(T)
  if chunkedArray.isNull(i):
    return none(T)
  return some(chunkedArray[i])

# Convert to sequence
proc `@`*[T](chunkedArray: ChunkedArray[T]): seq[T] =
  result = newSeq[T](chunkedArray.len)
  var idx = 0
  for chunk in chunkedArray.chunks:
    for item in chunk:
      result[idx] = item
      inc idx

iterator items*[T](chunkedArray: ChunkedArray[T]): lent T =
  if len(chunkedArray) > 0:
    for chunk in chunkedArray.chunks:
      for item in chunk:
        yield item
