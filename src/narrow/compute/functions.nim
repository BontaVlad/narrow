import ../core/[ffi, error]
import ../types/[gtypes, glist]
import ./expressions

type
  Function* = object
    handle: ptr GArrowFunction

  FunctionOptions* = object
    handle: ptr GArrowFunctionOptions

  FunctionDoc* = object
    handle: ptr GArrowFunctionDoc

# ============================================================================
# ARC Hooks - Function
# ============================================================================

proc `=destroy`*(fn: Function) =
  if not isNil(fn.handle):
    g_object_unref(fn.handle)

proc `=wasMoved`*(fn: var Function) =
  fn.handle = nil

proc `=dup`*(fn: Function): Function =
  result.handle = fn.handle
  if not isNil(fn.handle):
    discard g_object_ref(fn.handle)

proc `=copy`*(dest: var Function, src: Function) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

# ============================================================================
# ARC Hooks - FunctionOptions
# ============================================================================

proc `=destroy`*(options: FunctionOptions) =
  if not isNil(options.handle):
    g_object_unref(options.handle)

proc `=wasMoved`*(options: var FunctionOptions) =
  options.handle = nil

proc `=dup`*(options: FunctionOptions): FunctionOptions =
  result.handle = options.handle
  if not isNil(options.handle):
    discard g_object_ref(options.handle)

proc `=copy`*(dest: var FunctionOptions, src: FunctionOptions) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

# ============================================================================
# ARC Hooks - FunctionDoc
# ============================================================================

proc `=destroy`*(doc: FunctionDoc) =
  if not isNil(doc.handle):
    g_object_unref(doc.handle)

proc `=wasMoved`*(doc: var FunctionDoc) =
  doc.handle = nil

proc `=dup`*(doc: FunctionDoc): FunctionDoc =
  result.handle = doc.handle
  if not isNil(doc.handle):
    discard g_object_ref(doc.handle)

proc `=copy`*(dest: var FunctionDoc, src: FunctionDoc) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

# ============================================================================
# Pointer Converters
# ============================================================================

proc toPtr*(fn: Function): ptr GArrowFunction {.inline.} =
  fn.handle

proc toPtr*(options: FunctionOptions): ptr GArrowFunctionOptions {.inline.} =
  options.handle

proc toPtr*(doc: FunctionDoc): ptr GArrowFunctionDoc {.inline.} =
  doc.handle

# ============================================================================
# Function Discovery
# ============================================================================

proc find*(name: string): Function =
  let handle = garrow_function_find(name.cstring)
  if handle.isNil:
    raise newException(ValueError, "Function not found: " & name)
  result.handle = handle

proc name*(fn: Function): string =
  let cstr = garrow_function_get_name(fn.handle)
  result = $cstr
  # result = $newGString(cstr)

proc listFunctions*(): seq[Function] =
  let glist = newGList[ptr GArrowFunction](garrow_function_all())
  result = newSeqOfCap[Function](glist.len)
  for f in glist:
    result.add(Function(handle: f))

proc `$`*(fn: Function): string {.inline.} =
  $newGString(garrow_function_to_string(fn.handle))

proc `==`*(a, b: Function): bool =
  garrow_function_equal(a.handle, b.handle).bool

# ============================================================================
# Function Introspection
# ============================================================================

proc doc*(fn: Function): FunctionDoc =
  ## Returns the documentation for this function
  let handle = garrow_function_get_doc(fn.handle)
  if not isNil(handle):
    result.handle = cast[ptr GArrowFunctionDoc](g_object_ref(handle))

proc summary*(doc: FunctionDoc): string =
  ## Returns a one-line summary of the function
  result = $newGString(garrow_function_doc_get_summary(doc.handle))

proc description*(doc: FunctionDoc): string =
  ## Returns a detailed description of the function
  result = $newGString(garrow_function_doc_get_description(doc.handle))

proc defaultOptions*(fn: Function): FunctionOptions =
  ## Returns the default options for this function
  let handle = garrow_function_get_default_options(fn.handle)
  if not isNil(handle):
    result.handle = cast[ptr GArrowFunctionOptions](g_object_ref(handle))

proc optionsType*(fn: Function): GType =
  ## Returns the GType of the options class for this function
  garrow_function_get_options_type(fn.handle)

# ============================================================================
# Function Execution
# ============================================================================

proc execute*(
    fn: Function,
    args: openArray[Datum],
    options: FunctionOptions = FunctionOptions(),
    ctx: ptr GArrowExecuteContext = nil,
): Datum =
  ## Execute a function with the given arguments
  ##
  ## Parameters:
  ##   fn: The function to execute
  ##   args: Array of Datum arguments
  ##   options: Function-specific options (use defaultOptions(fn) for defaults)
  ##   ctx: Execution context (nil for default)
  ##
  ## Returns:
  ##   The result as a Datum. Use `.isArray`, `.isScalar`, etc. to check the kind,
  ##   then extract the value appropriately.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let addFn = find("add")
  ##     let a = newDatum(newArray(@[1'i32, 2, 3]))
  ##     let b = newDatum(newArray(@[10'i32, 20, 30]))
  ##     let result = addFn.execute([a, b])
  ##     if result.isArray:
  ##       echo "Result is an array"

  # Build argument list
  var argList = newGList[ptr GArrowDatum]()
  for arg in args:
    argList.append(arg.toPtr)

  # Execute the function
  let optionsPtr = if options.handle.isNil: nil else: options.handle
  let resultHandle =
    verify garrow_function_execute(fn.handle, argList.toPtr, optionsPtr, ctx)

  result = newDatum(resultHandle)

# ============================================================================
# Convenience API - Direct call by name
# ============================================================================

proc call*(
    name: string,
    args: varargs[Datum],
    options: FunctionOptions = FunctionOptions(),
    ctx: ptr GArrowExecuteContext = nil,
): Datum =
  ## Convenience function to call an Arrow function by name
  ##
  ## Parameters:
  ##   name: Function name (e.g., "add", "sum", "equal")
  ##   args: Array of Datum arguments
  ##   options: Function-specific options
  ##   ctx: Execution context (nil for default)
  ##
  ## Returns:
  ##   The result as a Datum. Use `.isArray`, `.isScalar`, etc. to check the kind.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let a = newDatum(newArray(@[1'i32, 2, 3]))
  ##     let b = newDatum(newArray(@[10'i32, 20, 30]))
  ##     let result = call("add", [a, b])
  ##     if result.isArray:
  ##       echo "Result is an array"

  let fn = find(name)
  result = fn.execute(args, options, ctx)
