import ../core/[ffi, error]
import ../tabular/table
import ./filesystem

type
  JsonUnexpectedFieldBehavior* = enum
    Ignore = GARROW_JSON_READ_IGNORE
    Error = GARROW_JSON_READ_ERROR
    InferType = GARROW_JSON_READ_INFER_TYPE

  JsonReadOptions* = object
    handle*: ptr GArrowJSONReadOptions

  JsonReader* = object
    handle*: ptr GArrowJSONReader

proc newJsonReadOptions*(): JsonReadOptions =
  let handle = garrow_json_read_options_new()
  if handle.isNil:
    raise newException(IOError, "Failed to create JsonReadOptions")
  result.handle = handle

proc newJsonReadOptions*(
    unexpectedFieldBehavior: JsonUnexpectedFieldBehavior
): JsonReadOptions =
  let handle = garrow_json_read_options_new()
  if handle.isNil:
    raise newException(IOError, "Failed to create JsonReadOptions")
  result.handle = handle

  # Set the unexpected field behavior
  g_object_set(
    result.handle, "unexpected-field-behavior", gint(unexpectedFieldBehavior.ord), nil
  )

proc `=destroy`*(o: JsonReadOptions) =
  if o.handle != nil:
    g_object_unref(o.handle)

proc `=wasMoved`*(o: var JsonReadOptions) =
  o.handle = nil

proc `=dup`*(o: JsonReadOptions): JsonReadOptions =
  result.handle = o.handle
  if o.handle != nil:
    discard g_object_ref(o.handle)

proc `=copy`*(dest: var JsonReadOptions, src: JsonReadOptions) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc unexpectedFieldBehavior*(options: JsonReadOptions): JsonUnexpectedFieldBehavior =
  var value: gint
  g_object_get(options.handle, "unexpected-field-behavior", addr value, nil)
  result = JsonUnexpectedFieldBehavior(value)

proc `unexpectedFieldBehavior=`*(
    options: JsonReadOptions, value: JsonUnexpectedFieldBehavior
) =
  g_object_set(options.handle, "unexpected-field-behavior", gint(value.ord), nil)

proc `=destroy`*(r: JsonReader) =
  if r.handle != nil:
    g_object_unref(r.handle)

proc `=wasMoved`*(r: var JsonReader) =
  r.handle = nil

proc `=dup`*(r: JsonReader): JsonReader =
  result.handle = r.handle
  if r.handle != nil:
    discard g_object_ref(r.handle)

proc `=copy`*(dest: var JsonReader, src: JsonReader) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc toPtr*(r: JsonReader): ptr GArrowJSONReader {.inline.} =
  r.handle

proc newJsonReader*(stream: InputStream, options: JsonReadOptions): JsonReader =
  let handle = verify garrow_json_reader_new(stream.handle, options.handle)
  result.handle = handle

proc read*(reader: JsonReader): ArrowTable =
  let tablePtr = verify garrow_json_reader_read(reader.handle)
  result = newArrowTable(tablePtr)

proc readJSON*(uri: string, options: JsonReadOptions): ArrowTable =
  let fs = newFileSystem(uri)
  with fs.openInputStream(uri), stream:
    let reader = newJsonReader(stream, options)
    result = reader.read()

proc readJSON*(uri: string): ArrowTable =
  let options = newJsonReadOptions()
  return readJSON(uri, options)
