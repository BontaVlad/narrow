import ../core/[ffi, error, utils]
import ../types/[gtypes, glist]
import ./expressions

arcGObject:
  type
    Function* = object
      handle: ptr GArrowFunction

    FunctionOptions* = object
      handle*: ptr GArrowFunctionOptions

    FunctionDoc* = object
      handle: ptr GArrowFunctionDoc

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
  let cstr = garrow_function_to_string(fn.handle)
  result = $newGString(cstr, owned = true)

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
  let cstr = garrow_function_doc_get_summary(doc.handle)
  result = $newGString(cstr, owned = true)

proc description*(doc: FunctionDoc): string =
  ## Returns a detailed description of the function
  let cstr = garrow_function_doc_get_description(doc.handle)
  result = $newGString(cstr, owned = true)

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
