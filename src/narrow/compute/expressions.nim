import ../column/primitive
import ../types/gtypes
import ../types/glist
import ../tabular/table
import ../tabular/batch
import ../core/ffi
import ../core/error

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

  ExpressionObj = object of RootObj
    handle: ptr GArrowExpression

  LiteralExpression* = object of ExpressionObj
    ## Represents a literal/constant value in an expression

  FieldExpression* = object of ExpressionObj
    ## Represents a reference to a field/column by name

  CallExpression* = object of ExpressionObj ## Represents a function call with arguments

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

proc `=copy`*(dest: var ExpressionObj, src: ExpressionObj) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

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

# TODO: this might need different types declared
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

proc newLiteralExpression*[T](value: T): LiteralExpression =
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
  for arg in args:
    argList.append(arg.toPtr)
  result.handle = cast[ptr GArrowExpression](garrow_call_expression_new(
    function.cstring, argList.toPtr, nil
  ))

# ============================================================================
# Expression Operations
# ============================================================================

proc `$`*(expr: ExpressionObj): string =
  ## Returns a string representation of the expression
  if expr.handle == nil:
    result = "Expression(nil)"
  else:
    let cStr = garrow_expression_to_string(expr.handle)
    result = $cStr

proc `==`*[T: ExpressionObj](a, b: T): bool {.inline.} =
  ## Compares two expressions (or any subtype) for equality
  garrow_expression_equal(a.handle, b.handle) != 0

# ============================================================================
# Convenience Constructors for Common Operations
# ============================================================================

proc eq*(field: FieldExpression, value: auto): CallExpression =
  ## Creates an equality comparison expression: field == value
  newCallExpression("equal", field, newLiteralExpression(value))

proc eq*(a, b: ExpressionObj): CallExpression =
  ## Creates an equality comparison expression: a == b
  newCallExpression("equal", a, b)

proc neq*(field: FieldExpression, value: auto): CallExpression =
  ## Creates a not-equal comparison expression: field != value
  newCallExpression("not_equal", field, newLiteralExpression(value))

proc neq*(a, b: ExpressionObj): CallExpression =
  ## Creates a not-equal comparison expression: a != b
  newCallExpression("not_equal", a, b)

proc lt*(field: FieldExpression, value: auto): CallExpression =
  ## Creates a less-than comparison expression: field < value
  newCallExpression("less", field, newLiteralExpression(value))

proc lt*(a, b: ExpressionObj): CallExpression =
  ## Creates a less-than comparison expression: a < b
  newCallExpression("less", a, b)

proc le*(field: FieldExpression, value: auto): CallExpression =
  ## Creates a less-than-or-equal comparison expression: field <= value
  newCallExpression("less_equal", field, newLiteralExpression(value))

proc le*(a, b: ExpressionObj): CallExpression =
  ## Creates a less-than-or-equal comparison expression: a <= b
  newCallExpression("less_equal", a, b)

proc gt*(field: FieldExpression, value: auto): CallExpression =
  ## Creates a greater-than comparison expression: field > value
  newCallExpression("greater", field, newLiteralExpression(value))

proc gt*(a, b: ExpressionObj): CallExpression =
  ## Creates a greater-than comparison expression: a > b
  newCallExpression("greater", a, b)

proc ge*(field: FieldExpression, value: auto): CallExpression =
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

proc strLength*(field: FieldExpression): CallExpression =
  ## Creates a string length expression
  newCallExpression("utf8_length", field)

proc strUpper*(field: FieldExpression): CallExpression =
  ## Creates an uppercase expression
  newCallExpression("utf8_upper", field)

proc strLower*(field: FieldExpression): CallExpression =
  ## Creates a lowercase expression
  newCallExpression("utf8_lower", field)

proc strContains*(field: FieldExpression, substr: string): CallExpression =
  ## Creates a string contains expression
  newCallExpression("match_substring", field, newLiteralExpression(substr))
