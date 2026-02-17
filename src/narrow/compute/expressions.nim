import std/[strutils, sets]
import ../column/primitive
import ../types/gtypes
import ../types/glist
import ../tabular/table
import ../tabular/batch
import ../core/ffi
import ../core/error
import ./match_substring_options

# ============================================================================
# Kinds
# ============================================================================

type DatumKind* = enum
  none
  array
  chunkedArray
  scalar
  recordBatch
  table

# ============================================================================
# Core Types
# ============================================================================

type
  Datum*[K: static DatumKind = DatumKind.none] = object
    handle: ptr GArrowDatum

  Scalar*[T: ArrowPrimitive = void] = object
    handle*: ptr GArrowScalar

  ExpressionObj* = object of RootObj
    handle: ptr GArrowExpression
    referencedFields*: seq[string] ## Field names referenced in this expression tree

  LiteralExpression* = object of ExpressionObj
    ## Represents a literal/constant value in an expression

  FieldExpression* = object of ExpressionObj

  CallExpression* = object of ExpressionObj ## Represents a function call with arguments
    functionName*: string ## The function being called (e.g., "equal", "greater")

  DatumCompatible = ArrowPrimitive | Array | ChunkedArray | ArrowTable | RecordBatch

  FilterClause* = tuple[field: string, op: string, value: string]

# ============================================================================
# ARC Hooks — Datum
# ============================================================================

proc `=destroy`*[K](dt: Datum[K]) =
  if not isNil(dt.handle):
    g_object_unref(dt.handle)

proc `=sink`*[K](dest: var Datum[K], src: Datum[K]) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*[K](dest: var Datum[K], src: Datum[K]) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# ============================================================================
# ARC Hooks — Scalar
# ============================================================================

proc `=destroy`*[T](sc: Scalar[T]) =
  if not isNil(sc.handle):
    g_object_unref(sc.handle)

proc `=sink`*[T](dest: var Scalar[T], src: Scalar[T]) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*[T](dest: var Scalar[T], src: Scalar[T]) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# ============================================================================
# Expression Base Type - ARC Hooks
# ============================================================================

proc `=destroy`*(expr: ExpressionObj) =
  if expr.handle != nil:
    g_object_unref(expr.handle)

proc `=sink`*(dest: var ExpressionObj, src: ExpressionObj) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle
  dest.referencedFields = src.referencedFields

proc `=copy`*(dest: var ExpressionObj, src: ExpressionObj) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)
  dest.referencedFields = src.referencedFields

# ============================================================================
# Pointer Converters
# ============================================================================

proc toPtr*[K](dt: Datum[K]): ptr GArrowDatum {.inline.} =
  dt.handle

proc toPtr*[T](sc: Scalar[T]): ptr GArrowScalar {.inline.} =
  sc.handle

proc toPtr*(expr: ExpressionObj): ptr GArrowExpression {.inline.} =
  ## Returns the underlying GArrowExpression pointer
  expr.handle

# ============================================================================
# Expression Operations
# ============================================================================

proc `$`*[T: ExpressionObj](expr: T | ExpressionObj): string =
  ## Returns a string representation of the expression
  if expr.handle == nil:
    result = "Expression(nil)"
  else:
    result = $newGString(garrow_expression_to_string(expr.handle))

proc `==`*[T: ExpressionObj](a, b: T): bool {.inline.} =
  ## Compares two expressions (or any subtype) for equality
  garrow_expression_equal(a.handle, b.handle) != 0

# ============================================================================
# Scalar Constructors (typed)
# ============================================================================

proc newScalar*(): Scalar =
  result.handle = cast[ptr GArrowScalar](garrow_null_scalar_new())

proc newScalar*(v: bool): Scalar[bool] =
  result.handle = cast[ptr GArrowScalar](garrow_boolean_scalar_new(v.gboolean))

proc newScalar*(v: int8): Scalar[int8] =
  result.handle = cast[ptr GArrowScalar](garrow_int8_scalar_new(v.gint8))

proc newScalar*(v: int16): Scalar[int16] =
  result.handle = cast[ptr GArrowScalar](garrow_int16_scalar_new(v.gint16))

proc newScalar*(v: int32): Scalar[int32] =
  result.handle = cast[ptr GArrowScalar](garrow_int32_scalar_new(v.gint32))

proc newScalar*(v: int64): Scalar[int64] =
  result.handle = cast[ptr GArrowScalar](garrow_int64_scalar_new(v.gint64))

proc newScalar*(v: uint8): Scalar[uint8] =
  result.handle = cast[ptr GArrowScalar](garrow_uint8_scalar_new(v.guint8))

proc newScalar*(v: uint16): Scalar[uint16] =
  result.handle = cast[ptr GArrowScalar](garrow_uint16_scalar_new(v.guint16))

proc newScalar*(v: uint32): Scalar[uint32] =
  result.handle = cast[ptr GArrowScalar](garrow_uint32_scalar_new(v.guint32))

proc newScalar*(v: uint64): Scalar[uint64] =
  result.handle = cast[ptr GArrowScalar](garrow_uint64_scalar_new(v.guint64))

proc newScalar*(v: float32): Scalar[float32] =
  result.handle = cast[ptr GArrowScalar](garrow_float_scalar_new(v.gfloat))

proc newScalar*(v: float64): Scalar[float64] =
  result.handle = cast[ptr GArrowScalar](garrow_double_scalar_new(v.gdouble))

proc newScalar*(v: string): Scalar[string] =
  ## Creates a string scalar from a Nim string
  let buffer = garrow_buffer_new(cast[ptr guint8](v.cstring), v.len.gint64)
  result.handle = cast[ptr GArrowScalar](garrow_string_scalar_new(buffer))
  g_object_unref(buffer)

proc newScalar*(v: seq[byte]): Scalar[seq[byte]] =
  ## Creates a binary scalar from a byte sequence
  let buffer = garrow_buffer_new(cast[ptr guint8](v[0].unsafeAddr), v.len.gint64)
  result.handle = cast[ptr GArrowScalar](garrow_binary_scalar_new(buffer))
  g_object_unref(buffer)

proc newScalar*(v: Date32): Scalar[Date32] =
  ## Creates a date32 scalar (days since epoch)
  result.handle = cast[ptr GArrowScalar](garrow_date32_scalar_new(v.int32.gint32))

proc newScalar*(v: Date64): Scalar[Date64] =
  ## Creates a date64 scalar (milliseconds since epoch)
  result.handle = cast[ptr GArrowScalar](garrow_date64_scalar_new(v.int64.gint64))

proc newScalar*(v: MonthInterval): Scalar[MonthInterval] =
  ## Creates a month interval scalar
  result.handle =
    cast[ptr GArrowScalar](garrow_month_interval_scalar_new(v.int32.gint32))

# ============================================================================
# Datum Constructors
# ============================================================================

proc newDatum*[T](sc: Scalar[T]): Datum[DatumKind.scalar] =
  result.handle = cast[ptr GArrowDatum](garrow_scalar_datum_new(sc.toPtr))

proc newDatum*[T: ArrowPrimitive](value: T): Datum[DatumKind.scalar] =
  ## Creates a datum from a primitive value (auto-creates a scalar)
  let sc = newScalar(value)
  result.handle = cast[ptr GArrowDatum](garrow_scalar_datum_new(sc.toPtr))

proc newDatum*[T](arr: Array[T]): Datum[DatumKind.array] =
  result.handle = cast[ptr GArrowDatum](garrow_array_datum_new(arr.toPtr))

proc newDatum*[T](ca: ChunkedArray[T]): Datum[DatumKind.chunkedArray] =
  result.handle = cast[ptr GArrowDatum](garrow_chunked_array_datum_new(ca.toPtr))

proc newDatum*(tb: ArrowTable): Datum[DatumKind.table] =
  result.handle = cast[ptr GArrowDatum](garrow_table_datum_new(tb.toPtr))

proc newDatum*(rb: RecordBatch): Datum[DatumKind.recordBatch] =
  result.handle = cast[ptr GArrowDatum](garrow_record_batch_datum_new(rb.toPtr))

# ============================================================================
# Datum Runtime Type Checks
# ============================================================================

proc isArray*(dt: Datum): bool {.inline.} =
  garrow_datum_is_array(dt.handle) != 0

proc isArrayLike*(dt: Datum): bool {.inline.} =
  garrow_datum_is_array_like(dt.handle) != 0

proc isScalar*(dt: Datum): bool {.inline.} =
  garrow_datum_is_scalar(dt.handle) != 0

proc isValue*(dt: Datum): bool {.inline.} =
  garrow_datum_is_value(dt.handle) != 0

proc `==`*[T](a, b: Datum[T]): bool {.inline.} =
  garrow_datum_equal(a.toPtr, b.toPtr) != 0

proc `$`*[T](dt: Datum[T]): string =
  result = $newGString(garrow_datum_to_string(dt.handle))

# ============================================================================
# Datum Kind (compile-time first, runtime fallback)
# ============================================================================

proc kind*[K: static DatumKind](dt: Datum[K]): DatumKind {.inline.} =
  when K != DatumKind.none:
    K
  else:
    if dt.isArray:
      DatumKind.array
    elif dt.isScalar:
      DatumKind.scalar
    elif dt.isArrayLike:
      DatumKind.chunkedArray
    else:
      raise newException(ValueError, "Unknown datum kind")

# ============================================================================
# Scalar Methods
# ============================================================================

proc isValid*[T](sc: Scalar[T]): bool {.inline.} =
  garrow_scalar_is_valid(sc.handle) != 0

proc `==`*[T, K](a: Scalar[T], b: Scalar[K]): bool {.inline.} =
  garrow_scalar_equal(a.handle, b.handle) != 0

proc `$`*[T](sc: Scalar[T]): string =
  result = $newGString(garrow_scalar_to_string(sc.handle))

# ============================================================================
# Raw Value Extractors (private helpers)
# ============================================================================

proc getBool*(sc: Scalar): bool =
  garrow_boolean_scalar_get_value(cast[ptr GArrowBooleanScalar](sc.handle)) != 0

proc getInt8*(sc: Scalar): int8 =
  garrow_int8_scalar_get_value(cast[ptr GArrowInt8Scalar](sc.handle))

proc getInt16*(sc: Scalar): int16 =
  garrow_int16_scalar_get_value(cast[ptr GArrowInt16Scalar](sc.handle))

proc getInt32*(sc: Scalar): int32 =
  garrow_int32_scalar_get_value(cast[ptr GArrowInt32Scalar](sc.handle))

proc getInt64*(sc: Scalar): int64 =
  garrow_int64_scalar_get_value(cast[ptr GArrowInt64Scalar](sc.handle))

proc getUInt8*(sc: Scalar): uint8 =
  garrow_uint8_scalar_get_value(cast[ptr GArrowUInt8Scalar](sc.handle))

proc getUInt16*(sc: Scalar): uint16 =
  garrow_uint16_scalar_get_value(cast[ptr GArrowUInt16Scalar](sc.handle))

proc getUInt32*(sc: Scalar): uint32 =
  garrow_uint32_scalar_get_value(cast[ptr GArrowUInt32Scalar](sc.handle))

proc getUInt64*(sc: Scalar): uint64 =
  garrow_uint64_scalar_get_value(cast[ptr GArrowUInt64Scalar](sc.handle))

proc getFloat32*(sc: Scalar): float32 =
  garrow_float_scalar_get_value(cast[ptr GArrowFloatScalar](sc.handle))

proc getFloat64*(sc: Scalar): float64 =
  garrow_double_scalar_get_value(cast[ptr GArrowDoubleScalar](sc.handle))

# ============================================================================
# Compile-time Value Type Mapping
# ============================================================================

template valueType*(T: typedesc): typedesc =
  when T is bool:
    bool
  elif T is int8:
    int8
  elif T is int16:
    int16
  elif T is int32:
    int32
  elif T is int64:
    int64
  elif T is uint8:
    uint8
  elif T is uint16:
    uint16
  elif T is uint32:
    uint32
  elif T is uint64:
    uint64
  elif T is float32:
    float32
  elif T is float64:
    float64
  else:
    void

# ============================================================================
# Scalar value() — compile-time or runtime
# ============================================================================

proc value*[T](sc: Scalar[T]): valueType(T) {.inline.} =
  when T is not void:
    when T is bool:
      sc.getBool()
    elif T is int8:
      sc.getInt8()
    elif T is int16:
      sc.getInt16()
    elif T is int32:
      sc.getInt32()
    elif T is int64:
      sc.getInt64()
    elif T is uint8:
      sc.getUInt8()
    elif T is uint16:
      sc.getUInt16()
    elif T is uint32:
      sc.getUInt32()
    elif T is uint64:
      sc.getUInt64()
    elif T is float32:
      sc.getFloat32()
    elif T is float64:
      sc.getFloat64()
  else:
    raise newException(ValueError, "Cannot extract value from untyped scalar")

# ============================================================================
# Expression Constructors
# ============================================================================

proc newLiteralExpression*(dt: Datum): LiteralExpression =
  result.handle = cast[ptr GArrowExpression](garrow_literal_expression_new(dt.toPtr))
  result.referencedFields = @[] # Literals don't reference fields

proc newLiteralExpression*[T: DatumCompatible](value: T): LiteralExpression =
  ## Creates a literal expression from a scalar value
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let lit42 = newLiteralExpression(42'i32)
  ##     let litStr = newLiteralExpression("hello")
  ##     let litTrue = newLiteralExpression(true)

  let datum = newDatum(value)
  result = newLiteralExpression(datum)

proc newFieldExpression*(name: string): FieldExpression =
  ## Creates a field expression that references a column by name
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let ageField = newFieldExpression("age")
  ##     let nameField = newFieldExpression("name")

  result.handle =
    cast[ptr GArrowExpression](check garrow_field_expression_new(name.cstring))
  result.referencedFields = @[name] # This field references itself

proc newCallExpression*(
    function: string, args: varargs[ExpressionObj]
): CallExpression =
  ## Creates a call expression for invoking compute functions
  ##
  ## Common functions: "equal", "not_equal", "less", "less_equal", "greater",
  ## "greater_equal", "add", "subtract", "multiply", "divide", "and", "or", "not"
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let age = newFieldExpression("age")
  ##     let threshold = newLiteralExpression(21'i32)
  ##     let isAdult = newCallExpression("greater", age, threshold)

  var argList = newGList[ptr GArrowExpression]()
  var allFields: seq[string]
  for arg in args:
    argList.append(arg.toPtr)
    # Merge field references from all arguments
    for f in arg.referencedFields:
      if f notin allFields:
        allFields.add(f)
  result.handle = cast[ptr GArrowExpression](garrow_call_expression_new(
    function.cstring, argList.toPtr, nil
  ))
  result.functionName = function
  result.referencedFields = allFields

proc newCallExpressionWithOptions*(
    function: string, options: MatchSubstringOptions, args: varargs[ExpressionObj]
): CallExpression =
  ## Creates a call expression with MatchSubstringOptions for functions like
  ## match_substring, starts_with, ends_with, match_substring_regex
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let name = newFieldExpression("name")
  ##     let opts = newMatchSubstringOptions("pattern")
  ##     let expr = newCallExpressionWithOptions("match_substring", opts, name)

  var argList = newGList[ptr GArrowExpression]()
  var allFields: seq[string]
  for arg in args:
    argList.append(arg.toPtr)
    # Merge field references from all arguments
    for f in arg.referencedFields:
      if f notin allFields:
        allFields.add(f)
  result.handle = cast[ptr GArrowExpression](garrow_call_expression_new(
    function.cstring, argList.toPtr, cast[ptr GArrowFunctionOptions](options.toPtr)
  ))
  result.functionName = function
  result.referencedFields = allFields

# ============================================================================
# Convenience Constructors for Common Operations
# ============================================================================

proc eq*[T: DatumCompatible](field: FieldExpression, value: T): CallExpression =
  ## Creates an equality comparison expression: field == value
  newCallExpression("equal", field, newLiteralExpression(value))

proc eq*(a, b: ExpressionObj): CallExpression =
  ## Creates an equality comparison expression: a == b
  newCallExpression("equal", a, b)

proc neq*[T: DatumCompatible](field: FieldExpression, value: T): CallExpression =
  ## Creates a not-equal comparison expression: field != value
  newCallExpression("not_equal", field, newLiteralExpression(value))

proc neq*(a, b: ExpressionObj): CallExpression =
  ## Creates a not-equal comparison expression: a != b
  newCallExpression("not_equal", a, b)

proc lt*[T: DatumCompatible](field: FieldExpression, value: T): CallExpression =
  ## Creates a less-than comparison expression: field < value
  newCallExpression("less", field, newLiteralExpression(value))

proc lt*(a, b: ExpressionObj): CallExpression =
  ## Creates a less-than comparison expression: a < b
  newCallExpression("less", a, b)

proc le*[T: DatumCompatible](field: FieldExpression, value: T): CallExpression =
  ## Creates a less-than-or-equal comparison expression: field <= value
  newCallExpression("less_equal", field, newLiteralExpression(value))

proc le*(a, b: ExpressionObj): CallExpression =
  ## Creates a less-than-or-equal comparison expression: a <= b
  newCallExpression("less_equal", a, b)

proc gt*[T: DatumCompatible](field: FieldExpression, value: T): CallExpression =
  ## Creates a greater-than comparison expression: field > value
  newCallExpression("greater", field, newLiteralExpression(value))

proc gt*(a, b: ExpressionObj): CallExpression =
  ## Creates a greater-than comparison expression: a > b
  newCallExpression("greater", a, b)

proc ge*[T: DatumCompatible](field: FieldExpression, value: T): CallExpression =
  ## Creates a greater-than-or-equal comparison expression: field >= value
  newCallExpression("greater_equal", field, newLiteralExpression(value))

proc ge*(a, b: ExpressionObj): CallExpression =
  ## Creates a greater-than-or-equal comparison expression: a >= b
  newCallExpression("greater_equal", a, b)

proc andExpr*(a, b: ExpressionObj): CallExpression =
  ## Creates a logical AND expression
  newCallExpression("and", a, b)

proc orExpr*(a, b: ExpressionObj): CallExpression =
  ## Creates a logical OR expression
  newCallExpression("or", a, b)

proc notExpr*(expr: ExpressionObj): CallExpression =
  ## Creates a logical NOT expression
  newCallExpression("invert", expr) # "invert" is Arrow's name for boolean NOT

proc add*(a, b: ExpressionObj): CallExpression =
  ## Creates an addition expression: a + b
  newCallExpression("add", a, b)

proc sub*(a, b: ExpressionObj): CallExpression =
  ## Creates a subtraction expression: a - b
  newCallExpression("subtract", a, b)

proc mul*(a, b: ExpressionObj): CallExpression =
  ## Creates a multiplication expression: a * b
  newCallExpression("multiply", a, b)

proc divide*(a, b: ExpressionObj): CallExpression =
  ## Creates a division expression: a / b
  newCallExpression("divide", a, b)

proc isNull*(field: FieldExpression): CallExpression =
  ## Creates an is-null check expression
  newCallExpression("is_null", field)

proc isValid*(field: FieldExpression): CallExpression =
  ## Creates an is-valid (not null) check expression
  newCallExpression("is_valid", field)

# ============================================================================
# String Operations
# ============================================================================

proc strLength*(expr: ExpressionObj): CallExpression =
  ## Creates a string length expression
  newCallExpression("utf8_length", expr)

proc strUpper*(expr: ExpressionObj): CallExpression =
  ## Creates an uppercase expression
  newCallExpression("utf8_upper", expr)

proc strLower*(expr: ExpressionObj): CallExpression =
  ## Creates a lowercase expression
  newCallExpression("utf8_lower", expr)

proc strContains*(
    expr: ExpressionObj, substr: string, ignoreCase: bool = false
): CallExpression =
  ## Creates a string contains expression (match_substring function)
  ## Returns true if the substring is found in the input value
  let options = newMatchSubstringOptions(substr, ignoreCase)
  newCallExpressionWithOptions("match_substring", options, expr)

proc startsWith*(
    expr: ExpressionObj, prefix: string, ignoreCase: bool = false
): CallExpression =
  ## Creates a starts_with expression
  ## Returns true if the input value starts with the given prefix
  let options = newMatchSubstringOptions(prefix, ignoreCase)
  newCallExpressionWithOptions("starts_with", options, expr)

proc endsWith*(
    expr: ExpressionObj, suffix: string, ignoreCase: bool = false
): CallExpression =
  ## Creates an ends_with expression
  ## Returns true if the input value ends with the given suffix
  let options = newMatchSubstringOptions(suffix, ignoreCase)
  newCallExpressionWithOptions("ends_with", options, expr)

proc matchSubstringRegex*(
    expr: ExpressionObj, pattern: string, ignoreCase: bool = false
): CallExpression =
  ## Creates a match_substring_regex expression
  ## Returns true if the regex pattern matches within the input value
  let options = newMatchSubstringOptions(pattern, ignoreCase)
  newCallExpressionWithOptions("match_substring_regex", options, expr)

# ============================================================================
# DSL Entry Point
# ============================================================================

proc col*(name: string): FieldExpression =
  ## Shortcut to create a field reference
  newFieldExpression(name)

# ============================================================================
# Operator Overloading (Comparison)
# ============================================================================

# These map Nim's infix operators to your Arrow convenience constructors
proc `==`*[T: DatumCompatible](a: FieldExpression, b: T): CallExpression =
  eq(a, b)

proc `!=`*[T: DatumCompatible](a: FieldExpression, b: T): CallExpression =
  neq(a, b)

proc `<`*[T: DatumCompatible](a: FieldExpression, b: T): CallExpression =
  lt(a, b)

proc `<=`*[T: DatumCompatible](a: FieldExpression, b: T): CallExpression =
  le(a, b)

proc `>`*[T: DatumCompatible](a: FieldExpression, b: T): CallExpression =
  gt(a, b)

proc `>=`*[T: DatumCompatible](a: FieldExpression, b: T): CallExpression =
  ge(a, b)

# Comparison operators for ExpressionObj (enables chaining like name.len() > 3)
proc `==`*[T: DatumCompatible](a: ExpressionObj, b: T): CallExpression =
  eq(a, newLiteralExpression(b))

proc `!=`*[T: DatumCompatible](a: ExpressionObj, b: T): CallExpression =
  neq(a, newLiteralExpression(b))

proc `<`*[T: DatumCompatible](a: ExpressionObj, b: T): CallExpression =
  lt(a, newLiteralExpression(b))

proc `<=`*[T: DatumCompatible](a: ExpressionObj, b: T): CallExpression =
  le(a, newLiteralExpression(b))

proc `>`*[T: DatumCompatible](a: ExpressionObj, b: T): CallExpression =
  gt(a, newLiteralExpression(b))

proc `>=`*[T: DatumCompatible](a: ExpressionObj, b: T): CallExpression =
  ge(a, newLiteralExpression(b))

# Comparison operators for CallExpression (needed for method chaining)
proc `==`*[T: DatumCompatible](a: CallExpression, b: T): CallExpression =
  eq(a.ExpressionObj, newLiteralExpression(b))

proc `!=`*[T: DatumCompatible](a: CallExpression, b: T): CallExpression =
  neq(a.ExpressionObj, newLiteralExpression(b))

proc `<`*[T: DatumCompatible](a: CallExpression, b: T): CallExpression =
  lt(a.ExpressionObj, newLiteralExpression(b))

proc `<=`*[T: DatumCompatible](a: CallExpression, b: T): CallExpression =
  le(a.ExpressionObj, newLiteralExpression(b))

proc `>`*[T: DatumCompatible](a: CallExpression, b: T): CallExpression =
  gt(a.ExpressionObj, newLiteralExpression(b))

proc `>=`*[T: DatumCompatible](a: CallExpression, b: T): CallExpression =
  ge(a.ExpressionObj, newLiteralExpression(b))

# ============================================================================
# Operator Overloading (Logical)
# ============================================================================

# Nim allows overloading 'and', 'or', and 'not' for non-boolean types
proc `and`*(a, b: ExpressionObj): CallExpression =
  andExpr(a, b)

proc `or`*(a, b: ExpressionObj): CallExpression =
  orExpr(a, b)

proc `not`*(a: ExpressionObj): CallExpression =
  notExpr(a)

# ============================================================================
# Arithmetic (Optional Grammar)
# ============================================================================

proc `+`*(a, b: ExpressionObj): CallExpression =
  add(a, b)

proc `-`*(a, b: ExpressionObj): CallExpression =
  sub(a, b)

proc `*`*(a, b: ExpressionObj): CallExpression =
  mul(a, b)

proc `/`*(a, b: ExpressionObj): CallExpression =
  divide(a, b)

# ============================================================================
# String DSL Extensions
# ============================================================================

proc contains*(
    expr: ExpressionObj, substr: string, ignoreCase: bool = false
): CallExpression =
  ## Usage: name.contains("pattern") or name.contains("pattern", true)
  strContains(expr, substr, ignoreCase)

proc len*(expr: ExpressionObj): CallExpression =
  ## Usage: name.len() > 5
  strLength(expr)

proc toUpper*(expr: ExpressionObj): CallExpression =
  ## Usage: name.toUpper() == "ALICE"
  strUpper(expr)

proc toLower*(expr: ExpressionObj): CallExpression =
  ## Usage: name.toLower() == "alice"
  strLower(expr)

# ============================================================================
# Field Reference Extraction
# ============================================================================

proc extractFieldReferences*(expr: ExpressionObj): seq[string] =
  ## Returns all unique field names referenced in the expression tree.
  ## 
  ## Example:
  ##   .. code-block:: nim
  ##     let expr = (col("age") >= 18) and (col("name") == "Alice")
  ##     let fields = extractFieldReferences(expr)
  ##     # fields == @["age", "name"]
  var seen: HashSet[string]
  result = @[]
  for f in expr.referencedFields:
    if f notin seen:
      result.add(f)
      seen.incl(f)

proc fieldName*(expr: ExpressionObj): string =
  ## Extracts the field name from a FieldExpression.
  ## Raises ValueError if the expression references 0 or more than 1 fields.
  if expr.referencedFields.len != 1:
    raise newException(ValueError, "Expected single field expression, got: " & $expr)
  result = expr.referencedFields[0]

# ============================================================================
# Filter Parser - Parse string tuples into expressions
# ============================================================================

proc parseValue*(value: string): LiteralExpression =
  ## Parses a string value into the appropriate ArrowPrimitive type.
  ## Tries bool, then int (int32 or int64 based on range), then float64, then string.

  # Try bool first
  if value.toLowerAscii == "true":
    return newLiteralExpression(true)
  elif value.toLowerAscii == "false":
    return newLiteralExpression(false)

  # Try float (contains '.' or 'e'/'E')
  if value.contains('.') or value.toLowerAscii.contains('e'):
    try:
      let f = parseFloat(value)
      return newLiteralExpression(f.float64)
    except ValueError:
      discard

  # Try int
  var intVal: int64
  try:
    intVal = parseInt(value)
    # Check if it fits in int32
    if intVal >= int32.low.int64 and intVal <= int32.high.int64:
      return newLiteralExpression(intVal.int32)
    else:
      return newLiteralExpression(intVal)
  except ValueError:
    discard

  # Fallback to string
  return newLiteralExpression(value)

proc parseFilter*(cl: FilterClause): CallExpression =
  ## Parses a single filter tuple (field, operator, value) into an expression.
  ## Supported operators: "==", "!=", "<", "<=", ">", ">=", "contains"

  let fieldExpr = newFieldExpression(cl.field)
  let valueExpr = parseValue(cl.value)

  case cl.op
  of "==":
    return newCallExpression("equal", fieldExpr, valueExpr)
  of "!=":
    return newCallExpression("not_equal", fieldExpr, valueExpr)
  of "<":
    return newCallExpression("less", fieldExpr, valueExpr)
  of "<=":
    return newCallExpression("less_equal", fieldExpr, valueExpr)
  of ">":
    return newCallExpression("greater", fieldExpr, valueExpr)
  of ">=":
    return newCallExpression("greater_equal", fieldExpr, valueExpr)
  of "contains":
    # valueExpr is a LiteralExpression containing the substring to search for
    # Extract the string value from the scalar within the literal expression's datum
    # For now, we know the value string is in cl.value
    return strContains(fieldExpr, cl.value)
  else:
    raise newException(ValueError, "Unknown operator: " & cl.op)

proc parse*(filters: seq[FilterClause]): ExpressionObj =
  ## Parses a sequence of filter tuples into a combined expression.
  ## Multiple filters are combined with AND logic.
  ## 
  ## Example:
  ##   .. code-block:: nim
  ##     let expr = parse(@[
  ##       ("age", ">=", "18"),
  ##       ("name", "contains", "Alice")
  ##     ])

  if filters.len == 0:
    raise newException(ValueError, "Empty filter sequence")

  if filters.len == 1:
    return parseFilter(filters[0])

  # Build AND chain from left to right
  result = parseFilter(filters[0])
  for i in 1 ..< filters.len:
    let nextExpr = parseFilter(filters[i])
    result = newCallExpression("and", result, nextExpr)
