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

# ============================================================================
# JsonReadOptions - Construction & ARC Hooks
# ============================================================================
#
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

proc `=sink`*(dest: var JsonReadOptions, src: JsonReadOptions) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var JsonReadOptions, src: JsonReadOptions) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

# ============================================================================
# JsonReadOptions - Property Getters/Setters
# ============================================================================

proc unexpectedFieldBehavior*(options: JsonReadOptions): JsonUnexpectedFieldBehavior =
  var value: gint
  g_object_get(options.handle, "unexpected-field-behavior", addr value, nil)
  result = JsonUnexpectedFieldBehavior(value)

proc `unexpectedFieldBehavior=`*(
    options: JsonReadOptions, value: JsonUnexpectedFieldBehavior
) =
  g_object_set(options.handle, "unexpected-field-behavior", gint(value.ord), nil)

# ============================================================================
# JsonReader - Construction & ARC Hooks
# ============================================================================

proc `=destroy`*(r: JsonReader) =
  if r.handle != nil:
    g_object_unref(r.handle)

proc `=sink`*(dest: var JsonReader, src: JsonReader) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

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
  let handle = check garrow_json_reader_new(stream.handle, options.handle)
  result.handle = handle

# ============================================================================
# JsonReader - Read Operations
# ============================================================================

proc read*(reader: JsonReader): ArrowTable =
  let tablePtr = check garrow_json_reader_read(reader.handle)
  result = newArrowTable(tablePtr)

# ============================================================================
# High-level API
# ============================================================================

proc readJSON*(uri: string, options: JsonReadOptions): ArrowTable =
  let fs = newFileSystem(uri)
  with fs.openInputStream(uri), stream:
    let reader = newJsonReader(stream, options)
    result = reader.read()

proc readJSON*(uri: string): ArrowTable =
  let options = newJsonReadOptions()
  return readJSON(uri, options)
