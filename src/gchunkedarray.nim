import ./[ffi, gtypes, error, garray, glist]

type ChunkedArray* = distinct ptr GArrowChunkedArray

# TODO: remove converter
converter toGArrow*(chunkedArray: ChunkedArray): ptr GArrowChunkedArray =
  cast[ptr GArrowChunkedArray](chunkedArray)

proc toPtr*(chunkedArray: ChunkedArray): ptr GArrowChunkedArray =
  cast[ptr GArrowChunkedArray](chunkedArray)

proc `=destroy`*(chunkedArray: ChunkedArray) =
  if not isNil(chunkedArray):
    g_object_unref(cast[gpointer](chunkedArray))

proc newChunkedArray*[T](chunks: openArray[Array[T]]): ChunkedArray =
  let cList = newGList(chunks)
  let handle = check garrow_chunked_array_new(cList.list)
  result = ChunkedArray(handle)

proc newChuckedArray*(dataType: GADType): ChunkedArray =
  let handle = check garrow_chunked_array_new_empty(dataType.addr)
  result = ChunkedArray(handle)

proc newChunkedArray*(cAbiArrayStream: pointer): ChunkedArray =
  let handle = check garrow_chunked_array_import(cAbiArrayStream)
  result = ChunkedArray(handle)

proc newChunkedArray*(rawPtr: ptr GArrowChunkedArray): ChunkedArray =
  result = cast[ChunkedArray](rawPtr)

proc `==`*(chunkedArray: ChunkedArray, other: ChunkedArray): bool =
  result = garrow_chunked_array_equal(chunkedArray, other) != 0

proc getValueDataType*(chunkedArray: ChunkedArray): GADType =
  result = cast[GADType](garrow_chunked_array_get_value_data_type(chunkedArray))

# proc getValueType*(chunkedArray: ChunkedArray): GArrowType =
#   result = garrow_chunked_array_get_value_type(chunkedArray)

proc len*(chunkedArray: ChunkedArray): int =
  result = int(garrow_chunked_array_get_length(chunkedArray))

proc getNRows*(chunkedArray: ChunkedArray): uint64 =
  result = garrow_chunked_array_get_n_rows(chunkedArray)

proc getNNulls*(chunkedArray: ChunkedArray): uint64 =
  result = garrow_chunked_array_get_n_nulls(chunkedArray)

proc nChunks*(chunkedArray: ChunkedArray): uint =
  result = garrow_chunked_array_get_n_chunks(chunkedArray)

proc getChunk*[T](chunkedArray: ChunkedArray, i: uint): Array[T] =
  let handle = garrow_chunked_array_get_chunk(chunkedArray, i)
  if isNil(handle):
    raise newException(IndexDefect, "Chunk index out of bounds")
  result = newArray[T](handle)

proc getChunks*(chunkedArray: ChunkedArray): ptr GList =
  result = garrow_chunked_array_get_chunks(chunkedArray)

# proc slice*(chunkedArray: ChunkedArray, offset: uint64, length: uint64): ChunkedArray =
#   let handle = garrow_chunked_array_slice(chunkedArray.toPtr, offset, length)
#   if isNil(handle):
#     raise newException(OperationError, "Error slicing chunked array")
#   result = ChunkedArray(handle)

# proc slice*(chunkedArray: ChunkedArray, slice: HSlice[int, int]): ChunkedArray =
#   let start = uint64(slice.a)
#   let length = uint64(slice.b - slice.a + 1)
#   result = chunkedArray.slice(start, length)

proc `$`*(chunkedArray: ChunkedArray): string =
  let cStr = check garrow_chunked_array_to_string(chunkedArray)
  result = $newGString(cStr)

# proc combine*[T](chunkedArray: ChunkedArray): Array[T] =
#   var err: ptr GError
#   let handle = garrow_chunked_array_combine(chunkedArray.toPtr, err.addr)
#   if isNil(handle) or not isNil(err):
#     if not isNil(err):
#       let msg = $err.message
#       gErrorFree(err)
#       raise newException(OperationError, "Error combining chunked array: " & msg)
#     else:
#       raise newException(OperationError, "Error combining chunked array")
#   result = newArray[T](handle)

# proc export*(chunkedArray: ChunkedArray): pointer =
#   var err: ptr GError
#   result = garrow_chunked_array_export(chunkedArray.toPtr, err.addr)
#   if not isNil(err):
#     let msg = $err.message
#     gErrorFree(err)
#     raise newException(OperationError, "Error exporting chunked array: " & msg)

iterator chunks*[T](chunkedArray: ChunkedArray): Array[T] =
  let nChunks = chunkedArray.getNChunks()
  for i in 0.uint ..< nChunks:
    yield chunkedArray.getChunk[T](i)

# # Indexing support (across all chunks)
# proc `[]`*[T](chunkedArray: ChunkedArray, i: int): T =
#   if i < 0 or i >= chunkedArray.len:
#     raise newException(IndexDefect, "Index out of bounds")

#   var currentIndex = i
#   let nChunks = chunkedArray.getNChunks()

#   for chunkIdx in 0.uint ..< nChunks:
#     let chunk = chunkedArray.getChunk[T](chunkIdx)
#     let chunkLen = chunk.len

#     if currentIndex < chunkLen:
#       return chunk[currentIndex]
#     else:
#       currentIndex -= chunkLen

#   raise newException(IndexDefect, "Index out of bounds")

# # Check if value at index is null (across all chunks)
# proc isNull*[T](chunkedArray: ChunkedArray, i: int): bool =
#   if i < 0 or i >= chunkedArray.len:
#     raise newException(IndexDefect, "Index out of bounds")

#   var currentIndex = i
#   let nChunks = chunkedArray.getNChunks()

#   for chunkIdx in 0.uint ..< nChunks:
#     let chunk = chunkedArray.getChunk[T](chunkIdx)
#     let chunkLen = chunk.len

#     if currentIndex < chunkLen:
#       return chunk.isNull(currentIndex)
#     else:
#       currentIndex -= chunkLen

#   raise newException(IndexDefect, "Index out of bounds")

# # Check if value at index is valid (across all chunks)
# proc isValid*[T](chunkedArray: ChunkedArray, i: int): bool =
#   result = not chunkedArray.isNull[T](i)

# # Safe getter with Option-like semantics
# proc tryGet*[T](chunkedArray: ChunkedArray, i: int): Option[T] =
#   if i < 0 or i >= chunkedArray.len:
#     return none(T)
#   if chunkedArray.isNull[T](i):
#     return none(T)
#   return some(chunkedArray[T](i))

# # Convert to sequence
proc `@`*[T](chunkedArray: ChunkedArray): seq[T] =
  result = newSeq[T](chunkedArray.len)
  var idx = 0
  for chunk in chunkedArray.chunks[T]():
    for item in chunk:
      result[idx] = item
      inc idx

iterator items*[T](chunkedArray: ChunkedArray): lent T =
  for chunk in chunkedArray.chunks[T]():
    for item in chunk:
      yield item
