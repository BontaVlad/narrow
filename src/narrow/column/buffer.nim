import ../core/[ffi, utils]

arcGObject:
  type GBuffer* = object
    handle*: ptr GArrowBuffer

proc newBuffer*(data: pointer, size: int64): GBuffer =
  ## Creates a buffer that **copies** the input data.
  result = GBuffer(handle: garrow_buffer_new(cast[ptr uint8](data), size.gint64))

proc newBuffer*(data: pointer, size: int64, copy: bool): GBuffer =
  ## Creates a buffer. When `copy` is `true`, data is copied.
  ## When `copy` is `false`, the buffer wraps the existing memory
  ## zero-copy (the caller must ensure the memory outlives the buffer).
  if copy:
    result = GBuffer(handle: garrow_buffer_new(cast[ptr uint8](data), size.gint64))
  else:
    let gbytes = g_bytes_new_static(data, size.gsize)
    result = GBuffer(handle: garrow_buffer_new_bytes(gbytes))
    g_bytes_unref(gbytes)

proc slice*(buffer: GBuffer, offset, size: int64): GBuffer =
  ## Returns a zero-copy sub-buffer view covering
  ## `[offset, offset + size)` of the original buffer.
  let handle = garrow_buffer_slice(buffer.handle, offset.gint64, size.gint64)
  result = GBuffer(handle: handle)

proc dataSize*(buffer: GBuffer): int64 =
  ## Returns the size of the buffer in bytes.
  ## Returns 0 if the buffer handle is nil.
  if buffer.handle == nil:
    return 0
  garrow_buffer_get_size(buffer.handle).int64

proc dataPointer*(buffer: GBuffer): pointer =
  ## Returns a read-only pointer to the buffer's data.
  ## The pointer is only valid while the buffer lives.
  ## Returns nil if the buffer handle is nil.
  if buffer.handle == nil:
    return nil
  let gbytes = garrow_buffer_get_data(buffer.handle)
  var size: gsize
  result = cast[pointer](g_bytes_get_data(gbytes, addr size))
  g_bytes_unref(gbytes)
