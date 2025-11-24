
import macros
import ./[ffi]

type
  OperationError* = object of CatchableError
  GErrorWrapper = object
    error: ptr GError

proc `=destroy`(x: var GErrorWrapper) =
  ## Automatic cleanup when the wrapper goes out of scope
  if not isNil(x.error):
    gErrorFree(x.error)

proc newError(): GErrorWrapper =
  GErrorWrapper(error: nil)

converter toBool*(wrapper: GErrorWrapper): bool =
  ## Check if an error occurred
  not isNil(wrapper.error)

proc `$`*(wrapper: GErrorWrapper): string =
  if wrapper:
    $wrapper.error.message
  else:
    ""

macro check*(callable: untyped, message: static string = ""): untyped =
  expectKind(callable, nnkCall)
  expectMinLen(callable, 1)

  let funcName = callable[0]
  var args = newSeq[NimNode]()

  # Copy existing arguments
  for i in 1 ..< callable.len:
    args.add(callable[i])

  # Create the error variable identifier
  let errorVar = genSym(nskVar, "error")

  # Add the error parameter to the arguments
  args.add(
    newDotExpr(newDotExpr(errorVar, newIdentNode("error")), newIdentNode("addr"))
  )

  # Convert static string to string literal node
  let messageNode = newLit(message)

  # Build the new function call with the error parameter
  let newCall = newCall(funcName, args)

  # Generate the complete code block
  result = quote:
    var `errorVar` = newError()
    let callResult = `newCall`

    # Always check for errors first
    if `errorVar`:
      let errorMessage = `messageNode` & " " & $`errorVar`
      raise newException(OperationError, errorMessage)

    # Type-specific handling
    when typeof(callResult) is gboolean:
      # For gboolean, check the result but don't return it
      if callResult != 1:
        let errorMessage = `messageNode` & " operation failed"
        raise newException(OperationError, errorMessage)
    else:
      # For other types, return the actual result
      callResult

when isMainModule:
  let builder = garrow_string_array_builder_new()

  check(
    garrow_boolean_array_builder_append_value(
      cast[ptr GArrowBooleanArrayBuilder](builder), 0.gboolean
    )
  )
