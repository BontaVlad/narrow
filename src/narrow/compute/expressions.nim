import std/[strutils, sets, options, hashes]
import ../column/primitive
import ../types/gtypes
import ../types/glist
import ../tabular/table
import ../tabular/batch
import ../core/ffi
import ../core/error
import ./match_substring_options
import ../compute/statistics

# ============================================================================
# Kinds
# ============================================================================

type
  DatumKind* = enum
    dkNone
    dkArray
    dkChunkedArray
    dkScalar
    dkRecordBatch
    dkTable

  ScalarKind* = enum
    skNull
    skBool
    skInt8
    skInt16
    skInt32
    skInt64
    skUInt8
    skUInt16
    skUInt32
    skUInt64
    skFloat32
    skFloat64
    skString
    skBinary
    skDate32
    skDate64
    skMonthInterval

  ExpressionKind* = enum
    ekLiteral ## A constant value (wraps a Datum/Scalar)
    ekField ## A column reference by name
    ekCall ## A function invocation with arguments

  ## Result of evaluating a comparison against known bounds
  BoundResult* = enum
    brAlwaysTrue ## The comparison is always satisfied given the bounds
    brAlwaysFalse ## The comparison can never be satisfied given the bounds
    brIndeterminate ## Can't determine — must read the data

type
  Datum* = object
    handle: ptr GArrowDatum

  Scalar* = object
    handle*: ptr GArrowScalar
    kind*: ScalarKind

  DatumCompatible* = ArrowPrimitive | Array | ChunkedArray | ArrowTable | RecordBatch

  FilterClause* = tuple[field: string, op: string, value: string]

type
  ExpressionObj = object
    handle*: ptr GArrowExpression
    case kind*: ExpressionKind
    of ekLiteral:
      datum*: Datum
    of ekField:
      fieldName*: string
    of ekCall:
      functionName*: string
      args*: seq[Expression]

  Expression* = ref ExpressionObj

proc `=destroy`*(dt: Datum) =
  if not isNil(dt.handle):
    g_object_unref(dt.handle)

proc `=sink`*(dest: var Datum, src: Datum) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var Datum, src: Datum) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# ============================================================================
# ARC Hooks — Scalar
# ============================================================================

proc `=destroy`*(sc: Scalar) =
  if not isNil(sc.handle):
    g_object_unref(sc.handle)

proc `=sink`*(dest: var Scalar, src: Scalar) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle
  dest.kind = src.kind

proc `=copy`*(dest: var Scalar, src: Scalar) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.kind = src.kind
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# proc `=destroy`*(expr: ExpressionObj) =
#     if not isNil(expr.handle):
#       g_object_unref(expr.handle)

# ============================================================================
# Expression ref — prevent double-free of GLib handle via ref semantics
# ============================================================================
# Expression is a `ref object`, so Nim's GC handles lifetime.
# We add a destructor on the ref target via invoke mechanism.

# proc finalizeExpression(expr: ExpressionObj) =
#   if expr.handle != nil:
#     g_object_unref(expr.handle)

# proc newExpressionRef(kind: ExpressionKind): Expression =
#   ## Internal helper to allocate an Expression ref with destructor
#   case kind
#   of ekLiteral:
#     new(result, proc(x: ExpressionObj) = finalizeExpression(x))
#     result[] = ExpressionObj(kind: ekLiteral, handle: nil)
#   of ekField:
#     new(result, proc(x: ExpressionObj) = finalizeExpression(x))
#     result[] = ExpressionObj(kind: ekField, handle: nil, fieldName: "")
#   of ekCall:
#     new(result, proc(x: ExpressionObj) = finalizeExpression(x))
#     result[] = ExpressionObj(kind: ekCall, handle: nil, functionName: "", args: @[])

# ============================================================================
# Pointer Converters
# ============================================================================

proc toPtr*(dt: Datum): ptr GArrowDatum {.inline.} =
  dt.handle

proc toPtr*(sc: Scalar): ptr GArrowScalar {.inline.} =
  sc.handle

proc toPtr*(expr: Expression): ptr GArrowExpression {.inline.} =
  if expr.isNil: nil else: expr.handle

# ============================================================================
# Expression — Identity & Display
# ============================================================================

proc `$`*(expr: Expression): string =
  if expr.isNil or expr.handle == nil:
    return "Expression(nil)"
  result = $newGString(garrow_expression_to_string(expr.handle))

proc `==`*(a, b: Expression): bool =
  if a.isNil and b.isNil:
    return true
  if a.isNil or b.isNil:
    return false
  garrow_expression_equal(a.handle, b.handle) != 0

# ============================================================================
# Expression — Tree Queries
# ============================================================================

proc isLiteral*(expr: Expression): bool {.inline.} =
  not expr.isNil and expr.kind == ekLiteral

proc isField*(expr: Expression): bool {.inline.} =
  not expr.isNil and expr.kind == ekField

proc isCall*(expr: Expression): bool {.inline.} =
  not expr.isNil and expr.kind == ekCall

proc isComparison*(expr: Expression): bool {.inline.} =
  expr.isCall and
    expr.functionName in
    ["equal", "not_equal", "less", "less_equal", "greater", "greater_equal"]

proc isLogical*(expr: Expression): bool {.inline.} =
  expr.isCall and expr.functionName in ["and", "or", "invert"]

proc isArithmetic*(expr: Expression): bool {.inline.} =
  expr.isCall and expr.functionName in ["add", "subtract", "multiply", "divide"]

proc arity*(expr: Expression): int {.inline.} =
  ## Number of child arguments (0 for literals/fields)
  if expr.isCall: expr.args.len else: 0

proc children*(expr: Expression): seq[Expression] {.inline.} =
  ## Returns child expressions. Empty for leaf nodes.
  if expr.isCall:
    expr.args
  else:
    @[]

# ============================================================================
# Expression — Recursive Field Collection
# ============================================================================

proc collectFieldsImpl(expr: Expression, acc: var HashSet[string]) =
  if expr.isNil:
    return
  case expr.kind
  of ekLiteral:
    discard
  of ekField:
    acc.incl(expr.fieldName)
  of ekCall:
    for child in expr.args:
      collectFieldsImpl(child, acc)

proc referencedFields*(expr: Expression): HashSet[string] =
  ## Recursively collects all field names referenced in the expression tree.
  result = initHashSet[string]()
  collectFieldsImpl(expr, result)

proc referencedFieldSeq*(expr: Expression): seq[string] =
  ## Returns referenced fields as an ordered seq (insertion order).
  var seen = initHashSet[string]()
  var res: seq[string] = @[]

  proc walk(e: Expression) =
    if e.isNil:
      return
    case e.kind
    of ekLiteral:
      discard
    of ekField:
      if e.fieldName notin seen:
        seen.incl(e.fieldName)
        res.add(e.fieldName)
    of ekCall:
      for child in e.args:
        walk(child)

  walk(expr)
  result = res

proc fieldName*(expr: Expression): string =
  ## Returns the field name for a field expression, or the single field
  ## referenced in a comparison expression. Raises ValueError if there
  ## isn't exactly one field.
  if expr.isNil:
    raise newException(ValueError, "Cannot get fieldName from nil expression")
  if expr.isField:
    return expr.fieldName
  let fields = referencedFieldSeq(expr)
  if fields.len == 0:
    raise newException(ValueError, "Expression has no field references")
  if fields.len > 1:
    raise newException(ValueError, "Expression references multiple fields: " & $fields)
  return fields[0]

# ============================================================================
# Expression — Tree Depth & Size
# ============================================================================

proc depth*(expr: Expression): int =
  ## Returns the maximum depth of the expression tree.
  if expr.isNil:
    return 0
  case expr.kind
  of ekLiteral, ekField:
    return 1
  of ekCall:
    var maxChild = 0
    for child in expr.args:
      maxChild = max(maxChild, depth(child))
    return 1 + maxChild

proc nodeCount*(expr: Expression): int =
  ## Returns total number of nodes in the expression tree.
  if expr.isNil:
    return 0
  case expr.kind
  of ekLiteral, ekField:
    return 1
  of ekCall:
    result = 1
    for child in expr.args:
      result += nodeCount(child)

# ============================================================================
# Scalar Constructors (typed)
# ============================================================================

proc newScalar*(): Scalar =
  Scalar(handle: cast[ptr GArrowScalar](garrow_null_scalar_new()), kind: skNull)

proc newScalar*(handle: ptr GArrowScalar): Scalar =
  Scalar(handle: handle, kind: skNull)

proc newScalar*(v: bool): Scalar =
  Scalar(
    handle: cast[ptr GArrowScalar](garrow_boolean_scalar_new(v.gboolean)), kind: skBool
  )

proc newScalar*(v: int8): Scalar =
  Scalar(handle: cast[ptr GArrowScalar](garrow_int8_scalar_new(v.gint8)), kind: skInt8)

proc newScalar*(v: int16): Scalar =
  Scalar(
    handle: cast[ptr GArrowScalar](garrow_int16_scalar_new(v.gint16)), kind: skInt16
  )

proc newScalar*(v: int32): Scalar =
  Scalar(
    handle: cast[ptr GArrowScalar](garrow_int32_scalar_new(v.gint32)), kind: skInt32
  )

proc newScalar*(v: int64): Scalar =
  Scalar(
    handle: cast[ptr GArrowScalar](garrow_int64_scalar_new(v.gint64)), kind: skInt64
  )

proc newScalar*(v: uint8): Scalar =
  Scalar(
    handle: cast[ptr GArrowScalar](garrow_uint8_scalar_new(v.guint8)), kind: skUInt8
  )

proc newScalar*(v: uint16): Scalar =
  Scalar(
    handle: cast[ptr GArrowScalar](garrow_uint16_scalar_new(v.guint16)), kind: skUInt16
  )

proc newScalar*(v: uint32): Scalar =
  Scalar(
    handle: cast[ptr GArrowScalar](garrow_uint32_scalar_new(v.guint32)), kind: skUInt32
  )

proc newScalar*(v: uint64): Scalar =
  Scalar(
    handle: cast[ptr GArrowScalar](garrow_uint64_scalar_new(v.guint64)), kind: skUInt64
  )

proc newScalar*(v: float32): Scalar =
  Scalar(
    handle: cast[ptr GArrowScalar](garrow_float_scalar_new(v.gfloat)), kind: skFloat32
  )

proc newScalar*(v: float64): Scalar =
  Scalar(
    handle: cast[ptr GArrowScalar](garrow_double_scalar_new(v.gdouble)), kind: skFloat64
  )

proc newScalar*(v: string): Scalar =
  let buffer = garrow_buffer_new(cast[ptr guint8](v.cstring), v.len.gint64)
  result = Scalar(
    handle: cast[ptr GArrowScalar](garrow_string_scalar_new(buffer)), kind: skString
  )
  g_object_unref(buffer)

proc newScalar*(v: seq[byte]): Scalar =
  let buffer = garrow_buffer_new(cast[ptr guint8](v[0].unsafeAddr), v.len.gint64)
  result = Scalar(
    handle: cast[ptr GArrowScalar](garrow_binary_scalar_new(buffer)), kind: skBinary
  )
  g_object_unref(buffer)

proc newScalar*(v: Date32): Scalar =
  Scalar(
    handle: cast[ptr GArrowScalar](garrow_date32_scalar_new(v.int32.gint32)),
    kind: skDate32,
  )

proc newScalar*(v: Date64): Scalar =
  Scalar(
    handle: cast[ptr GArrowScalar](garrow_date64_scalar_new(v.int64.gint64)),
    kind: skDate64,
  )

proc newScalar*(v: MonthInterval): Scalar =
  Scalar(
    handle: cast[ptr GArrowScalar](garrow_month_interval_scalar_new(v.int32.gint32)),
    kind: skMonthInterval,
  )

# ============================================================================
# Datum Constructors
# ============================================================================

proc newDatum*(sc: Scalar): Datum =
  result.handle = cast[ptr GArrowDatum](garrow_scalar_datum_new(sc.toPtr))

proc newDatum*[T: ArrowPrimitive](value: T): Datum =
  let sc = newScalar(value)
  result.handle = cast[ptr GArrowDatum](garrow_scalar_datum_new(sc.toPtr))

proc newDatum*[T](arr: Array[T]): Datum =
  result.handle = cast[ptr GArrowDatum](garrow_array_datum_new(arr.toPtr))

proc newDatum*[T](ca: ChunkedArray[T]): Datum =
  result.handle = cast[ptr GArrowDatum](garrow_chunked_array_datum_new(ca.toPtr))

proc newDatum*(tb: ArrowTable): Datum =
  result.handle = cast[ptr GArrowDatum](garrow_table_datum_new(tb.toPtr))

proc newDatum*(rb: RecordBatch): Datum =
  result.handle = cast[ptr GArrowDatum](garrow_record_batch_datum_new(rb.toPtr))

proc newDatum*(handle: ptr GArrowDatum): Datum =
  result.handle = handle

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

proc `==`*(a, b: Datum): bool {.inline.} =
  garrow_datum_equal(a.toPtr, b.toPtr) != 0

proc `$`*(dt: Datum): string =
  result = $newGString(garrow_datum_to_string(dt.handle))

# ============================================================================
# GObject Type Detection
# ============================================================================

proc detectScalarKind*(handle: ptr GArrowScalar): ScalarKind =
  ## Detects the ScalarKind from a GArrowScalar handle using GObject type.
  if handle.isNil:
    return skNull
  let inst = cast[ptr GTypeInstance](handle)
  if g_type_check_instance_is_a(inst, garrow_null_scalar_get_type()) != 0:
    return skNull
  if g_type_check_instance_is_a(inst, garrow_boolean_scalar_get_type()) != 0:
    return skBool
  if g_type_check_instance_is_a(inst, garrow_int8_scalar_get_type()) != 0:
    return skInt8
  if g_type_check_instance_is_a(inst, garrow_int16_scalar_get_type()) != 0:
    return skInt16
  if g_type_check_instance_is_a(inst, garrow_int32_scalar_get_type()) != 0:
    return skInt32
  if g_type_check_instance_is_a(inst, garrow_int64_scalar_get_type()) != 0:
    return skInt64
  if g_type_check_instance_is_a(inst, garrow_uint8_scalar_get_type()) != 0:
    return skUInt8
  if g_type_check_instance_is_a(inst, garrow_uint16_scalar_get_type()) != 0:
    return skUInt16
  if g_type_check_instance_is_a(inst, garrow_uint32_scalar_get_type()) != 0:
    return skUInt32
  if g_type_check_instance_is_a(inst, garrow_uint64_scalar_get_type()) != 0:
    return skUInt64
  if g_type_check_instance_is_a(inst, garrow_float_scalar_get_type()) != 0:
    return skFloat32
  if g_type_check_instance_is_a(inst, garrow_double_scalar_get_type()) != 0:
    return skFloat64
  if g_type_check_instance_is_a(inst, garrow_string_scalar_get_type()) != 0:
    return skString
  if g_type_check_instance_is_a(inst, garrow_large_string_scalar_get_type()) != 0:
    return skString
  if g_type_check_instance_is_a(inst, garrow_binary_scalar_get_type()) != 0:
    return skBinary
  if g_type_check_instance_is_a(inst, garrow_large_binary_scalar_get_type()) != 0:
    return skBinary
  if g_type_check_instance_is_a(inst, garrow_date32_scalar_get_type()) != 0:
    return skDate32
  if g_type_check_instance_is_a(inst, garrow_date64_scalar_get_type()) != 0:
    return skDate64
  if g_type_check_instance_is_a(inst, garrow_month_interval_scalar_get_type()) != 0:
    return skMonthInterval
  return skNull

proc kind*(sc: Scalar): ScalarKind {.inline.} =
  detectScalarKind(sc.handle)
# ============================================================================
# Datum Extraction Methods
# ============================================================================

proc toScalar*(dt: Datum): Scalar =
  if not dt.isScalar:
    raise newException(ValueError, "Datum is not a scalar")
  var scalarPtr: ptr GArrowScalar
  g_object_get(dt.handle, "value", addr scalarPtr, nil)
  result = Scalar(handle: scalarPtr, kind: detectScalarKind(scalarPtr))

# ============================================================================
# Datum Kind (runtime)
# ============================================================================

proc kind*(dt: Datum): DatumKind {.inline.} =
  if dt.handle.isNil:
    return dkNone
  let inst = cast[ptr GTypeInstance](dt.handle)
  if g_type_check_instance_is_a(inst, garrow_scalar_datum_get_type()) != 0:
    return dkScalar
  if g_type_check_instance_is_a(inst, garrow_array_datum_get_type()) != 0:
    return dkArray
  if g_type_check_instance_is_a(inst, garrow_chunked_array_datum_get_type()) != 0:
    return dkChunkedArray
  if g_type_check_instance_is_a(inst, garrow_record_batch_datum_get_type()) != 0:
    return dkRecordBatch
  if g_type_check_instance_is_a(inst, garrow_table_datum_get_type()) != 0:
    return dkTable
  return dkNone

# ============================================================================
# Scalar Methods
# ============================================================================

proc isValid*(sc: Scalar): bool {.inline.} =
  garrow_scalar_is_valid(sc.handle) != 0

proc `==`*(a, b: Scalar): bool {.inline.} =
  garrow_scalar_equal(a.handle, b.handle) != 0

proc `<`*[T: SomeNumber](sc: Scalar, val: T): bool =
  ## Compare scalar < value. Only works for numeric scalars.
  case sc.kind
  of skInt8: sc.getInt8() < val
  of skInt16: sc.getInt16() < val
  of skInt32: sc.getInt32() < val
  of skInt64: sc.getInt64() < val
  of skUInt8: sc.getUInt8() < val
  of skUInt16: sc.getUInt16() < val
  of skUInt32: sc.getUInt32() < val
  of skUInt64: sc.getUInt64() < val
  of skFloat32: sc.getFloat32() < val
  of skFloat64: sc.getFloat64() < val
  else:
    raise newException(ValueError, "Cannot compare scalar of kind " & $sc.kind & " with <")

proc `<=`*[T: SomeNumber](sc: Scalar, val: T): bool =
  ## Compare scalar <= value. Only works for numeric scalars.
  case sc.kind
  of skInt8: sc.getInt8() <= val
  of skInt16: sc.getInt16() <= val
  of skInt32: sc.getInt32() <= val
  of skInt64: sc.getInt64() <= val
  of skUInt8: sc.getUInt8() <= val
  of skUInt16: sc.getUInt16() <= val
  of skUInt32: sc.getUInt32() <= val
  of skUInt64: sc.getUInt64() <= val
  of skFloat32: sc.getFloat32() <= val
  of skFloat64: sc.getFloat64() <= val
  else:
    raise newException(ValueError, "Cannot compare scalar of kind " & $sc.kind & " with <=")

proc `>`*[T: SomeNumber](sc: Scalar, val: T): bool =
  ## Compare scalar > value. Only works for numeric scalars.
  case sc.kind
  of skInt8: sc.getInt8() > val
  of skInt16: sc.getInt16() > val
  of skInt32: sc.getInt32() > val
  of skInt64: sc.getInt64() > val
  of skUInt8: sc.getUInt8() > val
  of skUInt16: sc.getUInt16() > val
  of skUInt32: sc.getUInt32() > val
  of skUInt64: sc.getUInt64() > val
  of skFloat32: sc.getFloat32() > val
  of skFloat64: sc.getFloat64() > val
  else:
    raise newException(ValueError, "Cannot compare scalar of kind " & $sc.kind & " with >")

proc `>=`*[T: SomeNumber](sc: Scalar, val: T): bool =
  ## Compare scalar >= value. Only works for numeric scalars.
  case sc.kind
  of skInt8: sc.getInt8() >= val
  of skInt16: sc.getInt16() >= val
  of skInt32: sc.getInt32() >= val
  of skInt64: sc.getInt64() >= val
  of skUInt8: sc.getUInt8() >= val
  of skUInt16: sc.getUInt16() >= val
  of skUInt32: sc.getUInt32() >= val
  of skUInt64: sc.getUInt64() >= val
  of skFloat32: sc.getFloat32() >= val
  of skFloat64: sc.getFloat64() >= val
  else:
    raise newException(ValueError, "Cannot compare scalar of kind " & $sc.kind & " with >=")

proc `$`*(sc: Scalar): string =
  result = $newGString(garrow_scalar_to_string(sc.handle))

# ============================================================================
# Scalar Value Extractors
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
# Runtime Value Extraction
# ============================================================================

proc value*[T: bool](sc: Scalar, _: typedesc[T]): bool =
  if sc.kind != skBool:
    raise newException(ValueError, "Scalar is not a bool, got: " & $sc.kind)
  sc.getBool()

proc value*[T: int8](sc: Scalar, _: typedesc[T]): int8 =
  if sc.kind != skInt8:
    raise newException(ValueError, "Scalar is not an int8, got: " & $sc.kind)
  sc.getInt8()

proc value*[T: int16](sc: Scalar, _: typedesc[T]): int16 =
  if sc.kind != skInt16:
    raise newException(ValueError, "Scalar is not an int16, got: " & $sc.kind)
  sc.getInt16()

proc value*[T: int32](sc: Scalar, _: typedesc[T]): int32 =
  if sc.kind != skInt32:
    raise newException(ValueError, "Scalar is not an int32, got: " & $sc.kind)
  sc.getInt32()

proc value*[T: int64](sc: Scalar, _: typedesc[T]): int64 =
  if sc.kind != skInt64:
    raise newException(ValueError, "Scalar is not an int64, got: " & $sc.kind)
  sc.getInt64()

proc value*[T: uint8](sc: Scalar, _: typedesc[T]): uint8 =
  if sc.kind != skUInt8:
    raise newException(ValueError, "Scalar is not a uint8, got: " & $sc.kind)
  sc.getUInt8()

proc value*[T: uint16](sc: Scalar, _: typedesc[T]): uint16 =
  if sc.kind != skUInt16:
    raise newException(ValueError, "Scalar is not a uint16, got: " & $sc.kind)
  sc.getUInt16()

proc value*[T: uint32](sc: Scalar, _: typedesc[T]): uint32 =
  if sc.kind != skUInt32:
    raise newException(ValueError, "Scalar is not a uint32, got: " & $sc.kind)
  sc.getUInt32()

proc value*[T: uint64](sc: Scalar, _: typedesc[T]): uint64 =
  if sc.kind != skUInt64:
    raise newException(ValueError, "Scalar is not a uint64, got: " & $sc.kind)
  sc.getUInt64()

proc value*[T: float32](sc: Scalar, _: typedesc[T]): float32 =
  if sc.kind != skFloat32:
    raise newException(ValueError, "Scalar is not a float32, got: " & $sc.kind)
  sc.getFloat32()

proc value*[T: float64](sc: Scalar, _: typedesc[T]): float64 =
  if sc.kind != skFloat64:
    raise newException(ValueError, "Scalar is not a float64, got: " & $sc.kind)
  sc.getFloat64()

proc value*[T: int](sc: Scalar, _: typedesc[T]): int =
  ## Extract int value with automatic dispatch to appropriate size.
  ## Raises ValueError if scalar is not an integer type.
  case sc.kind
  of skInt8: sc.getInt8().int
  of skInt16: sc.getInt16().int
  of skInt32: sc.getInt32().int
  of skInt64: sc.getInt64().int
  of skUInt8: sc.getUInt8().int
  of skUInt16: sc.getUInt16().int
  of skUInt32: sc.getUInt32().int
  of skUInt64: sc.getUInt64().int
  else:
    raise newException(ValueError, "Scalar is not an integer type, got: " & $sc.kind)

# ============================================================================
# Expression Constructors — Leaf Nodes
# ============================================================================

proc newLiteralExpression*(dt: Datum): Expression =
  new(result)
  result[] = ExpressionObj(
    kind: ekLiteral,
    datum: dt,
    handle: cast[ptr GArrowExpression](garrow_literal_expression_new(dt.toPtr)),
  )

proc newLiteralExpression*[T: DatumCompatible](value: T): Expression =
  ## Creates a literal expression from a primitive value.
  ##
  ## Example:
  ##   ```nim
  ##   let lit42 = newLiteralExpression(42'i32)
  ##   let litStr = newLiteralExpression("hello")
  ##   ```
  let datum = newDatum(value)
  result = newLiteralExpression(datum)

proc newFieldExpression*(name: string): Expression =
  ## Creates a field reference expression.
  ##
  ## Example:
  ##   ```nim
  ##   let ageField = newFieldExpression("age")
  ##   ```
  new(result)
  result[] = ExpressionObj(
    kind: ekField,
    handle: cast[ptr GArrowExpression](check garrow_field_expression_new(name.cstring)),
    fieldName: name,
  )

# ============================================================================
# Expression Constructors — Call Nodes
# ============================================================================

proc newCallExpression*(function: string, args: varargs[Expression]): Expression =
  ## Creates a call expression node.
  ##
  ## Common functions: "equal", "not_equal", "less", "less_equal",
  ## "greater", "greater_equal", "add", "subtract", "multiply",
  ## "divide", "and", "or", "invert"
  ##
  ## Example:
  ##   ```nim
  ##   let age = newFieldExpression("age")
  ##   let threshold = newLiteralExpression(21'i32)
  ##   let isAdult = newCallExpression("greater_equal", age, threshold)
  ##   ```
  var argList = newGList[ptr GArrowExpression]()
  var childExprs: seq[Expression] = @[]
  for arg in args:
    argList.append(arg.toPtr)
    childExprs.add(arg)

  new(result)
  result[] = ExpressionObj(
    kind: ekCall,
    handle: cast[ptr GArrowExpression](garrow_call_expression_new(
      function.cstring, argList.toPtr, nil
    )),
    functionName: function,
    args: childExprs,
  )

proc newCallExpressionWithOptions*(
    function: string, options: MatchSubstringOptions, args: varargs[Expression]
): Expression =
  ## Creates a call expression with MatchSubstringOptions.
  var argList = newGList[ptr GArrowExpression]()
  var childExprs: seq[Expression] = @[]
  for arg in args:
    argList.append(arg.toPtr)
    childExprs.add(arg)

  new(result)
  result[] = ExpressionObj(
    kind: ekCall,
    handle: cast[ptr GArrowExpression](garrow_call_expression_new(
      function.cstring, argList.toPtr, cast[ptr GArrowFunctionOptions](options.toPtr)
    )),
    functionName: function,
    args: childExprs,
  )

# ============================================================================
# Convenience — Comparisons
# ============================================================================

proc eq*[T: DatumCompatible](field: Expression, value: T): Expression =
  newCallExpression("equal", field, newLiteralExpression(value))

proc eq*(a, b: Expression): Expression =
  newCallExpression("equal", a, b)

proc neq*[T: DatumCompatible](field: Expression, value: T): Expression =
  newCallExpression("not_equal", field, newLiteralExpression(value))

proc neq*(a, b: Expression): Expression =
  newCallExpression("not_equal", a, b)

proc lt*[T: DatumCompatible](field: Expression, value: T): Expression =
  newCallExpression("less", field, newLiteralExpression(value))

proc lt*(a, b: Expression): Expression =
  newCallExpression("less", a, b)

proc le*[T: DatumCompatible](field: Expression, value: T): Expression =
  newCallExpression("less_equal", field, newLiteralExpression(value))

proc le*(a, b: Expression): Expression =
  newCallExpression("less_equal", a, b)

proc gt*[T: DatumCompatible](field: Expression, value: T): Expression =
  newCallExpression("greater", field, newLiteralExpression(value))

proc gt*(a, b: Expression): Expression =
  newCallExpression("greater", a, b)

proc ge*[T: DatumCompatible](field: Expression, value: T): Expression =
  newCallExpression("greater_equal", field, newLiteralExpression(value))

proc ge*(a, b: Expression): Expression =
  newCallExpression("greater_equal", a, b)

# ============================================================================
# Convenience — Logical
# ============================================================================

proc andExpr*(a, b: Expression): Expression =
  newCallExpression("and", a, b)

proc orExpr*(a, b: Expression): Expression =
  newCallExpression("or", a, b)

proc notExpr*(expr: Expression): Expression =
  newCallExpression("invert", expr)

# ============================================================================
# Convenience — Arithmetic
# ============================================================================

proc add*(a, b: Expression): Expression =
  newCallExpression("add", a, b)

proc sub*(a, b: Expression): Expression =
  newCallExpression("subtract", a, b)

proc mul*(a, b: Expression): Expression =
  newCallExpression("multiply", a, b)

proc divide*(a, b: Expression): Expression =
  newCallExpression("divide", a, b)

# ============================================================================
# Convenience — Null checks
# ============================================================================

proc isNull*(field: Expression): Expression =
  newCallExpression("is_null", field)

proc isValid*(field: Expression): Expression =
  newCallExpression("is_valid", field)

# ============================================================================
# String Operations
# ============================================================================

proc strLength*(expr: Expression): Expression =
  newCallExpression("utf8_length", expr)

proc strUpper*(expr: Expression): Expression =
  newCallExpression("utf8_upper", expr)

proc strLower*(expr: Expression): Expression =
  newCallExpression("utf8_lower", expr)

proc strContains*(
    expr: Expression, substr: string, ignoreCase: bool = false
): Expression =
  let options = newMatchSubstringOptions(substr, ignoreCase)
  newCallExpressionWithOptions("match_substring", options, expr)

proc startsWith*(
    expr: Expression, prefix: string, ignoreCase: bool = false
): Expression =
  let options = newMatchSubstringOptions(prefix, ignoreCase)
  newCallExpressionWithOptions("starts_with", options, expr)

proc endsWith*(expr: Expression, suffix: string, ignoreCase: bool = false): Expression =
  let options = newMatchSubstringOptions(suffix, ignoreCase)
  newCallExpressionWithOptions("ends_with", options, expr)

proc matchSubstringRegex*(
    expr: Expression, pattern: string, ignoreCase: bool = false
): Expression =
  let options = newMatchSubstringOptions(pattern, ignoreCase)
  newCallExpressionWithOptions("match_substring_regex", options, expr)

# ============================================================================
# DSL Entry Point
# ============================================================================

proc col*(name: string): Expression =
  newFieldExpression(name)

# ============================================================================
# Operator Overloading — Comparison
# ============================================================================

proc `==`*[T: DatumCompatible](a: Expression, b: T): Expression =
  eq(a, b)

proc `!=`*[T: DatumCompatible](a: Expression, b: T): Expression =
  neq(a, b)

proc `<`*[T: DatumCompatible](a: Expression, b: T): Expression =
  lt(a, b)

proc `<=`*[T: DatumCompatible](a: Expression, b: T): Expression =
  le(a, b)

proc `>`*[T: DatumCompatible](a: Expression, b: T): Expression =
  gt(a, b)

proc `>=`*[T: DatumCompatible](a: Expression, b: T): Expression =
  ge(a, b)

# ============================================================================
# Operator Overloading — Logical
# ============================================================================

proc `and`*(a, b: Expression): Expression =
  andExpr(a, b)

proc `or`*(a, b: Expression): Expression =
  orExpr(a, b)

proc `not`*(a: Expression): Expression =
  notExpr(a)

# ============================================================================
# Operator Overloading — Arithmetic
# ============================================================================

proc `+`*(a, b: Expression): Expression =
  add(a, b)

proc `-`*(a, b: Expression): Expression =
  sub(a, b)

proc `*`*(a, b: Expression): Expression =
  mul(a, b)

proc `/`*(a, b: Expression): Expression =
  divide(a, b)

# ============================================================================
# String DSL Extensions
# ============================================================================

proc contains*(expr: Expression, substr: string, ignoreCase: bool = false): Expression =
  strContains(expr, substr, ignoreCase)

proc len*(expr: Expression): Expression =
  strLength(expr)

proc toUpper*(expr: Expression): Expression =
  strUpper(expr)

proc toLower*(expr: Expression): Expression =
  strLower(expr)

# ============================================================================
# Tree Walking — Pattern Matching Helpers
# ============================================================================

proc findFieldAndLiteral*(
    expr: Expression
): Option[tuple[fieldExpr: Expression, litExpr: Expression, flipped: bool]] =
  ## For a binary call expression, identifies which argument is the field
  ## reference and which is the literal. Returns `flipped = true` if the
  ## literal was on the left (e.g., `5 < age` → field=age, flipped=true).
  if not expr.isCall or expr.args.len != 2:
    return none(tuple[fieldExpr: Expression, litExpr: Expression, flipped: bool])

  let a = expr.args[0]
  let b = expr.args[1]

  if a.isField and b.isLiteral:
    return some((fieldExpr: a, litExpr: b, flipped: false))
  elif b.isField and a.isLiteral:
    return some((fieldExpr: b, litExpr: a, flipped: true))
  else:
    return none(tuple[fieldExpr: Expression, litExpr: Expression, flipped: bool])

proc flipOp*(op: string): string =
  ## Flips a comparison operator (when operands are swapped).
  ## `equal` and `not_equal` are symmetric so unchanged.
  case op
  of "less": "greater"
  of "less_equal": "greater_equal"
  of "greater": "less"
  of "greater_equal": "less_equal"
  else: op

proc negateOp*(op: string): string =
  ## Returns the negated comparison operator (for NOT push-through).
  case op
  of "equal": "not_equal"
  of "not_equal": "equal"
  of "less": "greater_equal"
  of "less_equal": "greater"
  of "greater": "less_equal"
  of "greater_equal": "less"
  else: op

# ============================================================================
# Pretty Print — Debug tree view
# ============================================================================

proc treeRepr*(expr: Expression, indent: int = 0): string =
  ## Returns a human-readable tree representation of the expression.
  ##
  ## Example output:
  ##   ```
  ##   Call(and)
  ##     Call(greater_equal)
  ##       Field(age)
  ##       Literal(18)
  ##     Call(equal)
  ##       Field(name)
  ##       Literal("Alice")
  ##   ```
  let pad = "  ".repeat(indent)
  if expr.isNil:
    return pad & "<nil>"

  case expr.kind
  of ekLiteral:
    result = pad & "Literal(" & $expr & ")"
  of ekField:
    result = pad & "Field(" & expr.fieldName & ")"
  of ekCall:
    result = pad & "Call(" & expr.functionName & ")"
    for child in expr.args:
      result &= "\n" & treeRepr(child, indent + 1)

# ============================================================================
# Expression Visitor (generic tree walker)
# ============================================================================

type ExprVisitor* = object
  onLiteral*: proc(expr: Expression)
  onField*: proc(expr: Expression)
  onCallPre*: proc(expr: Expression) ## Called before visiting children
  onCallPost*: proc(expr: Expression) ## Called after visiting children

proc walk*(expr: Expression, visitor: ExprVisitor) =
  ## Walks the expression tree, invoking visitor callbacks at each node.
  if expr.isNil:
    return
  case expr.kind
  of ekLiteral:
    if visitor.onLiteral != nil:
      visitor.onLiteral(expr)
  of ekField:
    if visitor.onField != nil:
      visitor.onField(expr)
  of ekCall:
    if visitor.onCallPre != nil:
      visitor.onCallPre(expr)
    for child in expr.args:
      walk(child, visitor)
    if visitor.onCallPost != nil:
      visitor.onCallPost(expr)

# ============================================================================
# Expression Map / Transform (creates new tree)
# ============================================================================

proc mapTree*(expr: Expression, fn: proc(e: Expression): Expression): Expression =
  ## Recursively transforms the expression tree bottom-up.
  ## The transform function receives each node after its children have
  ## already been transformed.
  if expr.isNil:
    return nil

  case expr.kind
  of ekLiteral, ekField:
    return fn(expr)
  of ekCall:
    var newArgs: seq[Expression] = @[]
    for child in expr.args:
      newArgs.add(mapTree(child, fn))
    # Build a new call node with transformed children
    let mapped = newCallExpression(expr.functionName, newArgs)
    return fn(mapped)

proc extractFieldReferences*(expr: Expression): HashSet[string] =
  ## Extracts all field names referenced in the expression tree.
  var refs = initHashSet[string]()
  walk(
    expr,
    ExprVisitor(
      onField: proc(e: Expression) =
        refs.incl(e.fieldName)
    ),
  )
  result = refs

# ============================================================================
# Filter Parser — Parse string tuples into expressions
# ============================================================================

proc parseValue*(value: string): Expression =
  ## Parses a string value into the appropriate typed literal expression.
  if value.toLowerAscii == "true":
    return newLiteralExpression(true)
  elif value.toLowerAscii == "false":
    return newLiteralExpression(false)

  if value.contains('.') or value.toLowerAscii.contains('e'):
    try:
      let f = parseFloat(value)
      return newLiteralExpression(f.float64)
    except ValueError:
      discard

  try:
    let intVal = parseInt(value)
    if intVal >= int32.low.int64 and intVal <= int32.high.int64:
      return newLiteralExpression(intVal.int32)
    else:
      return newLiteralExpression(intVal)
  except ValueError:
    discard

  return newLiteralExpression(value)

proc parseFilter*(cl: FilterClause): Expression =
  let fieldExpr = newFieldExpression(cl.field)
  let valueExpr = parseValue(cl.value)
  case cl.op
  of "==":
    newCallExpression("equal", fieldExpr, valueExpr)
  of "!=":
    newCallExpression("not_equal", fieldExpr, valueExpr)
  of "<":
    newCallExpression("less", fieldExpr, valueExpr)
  of "<=":
    newCallExpression("less_equal", fieldExpr, valueExpr)
  of ">":
    newCallExpression("greater", fieldExpr, valueExpr)
  of ">=":
    newCallExpression("greater_equal", fieldExpr, valueExpr)
  of "contains":
    strContains(fieldExpr, cl.value)
  else:
    raise newException(ValueError, "Unknown operator: " & cl.op)

proc parse*(filters: seq[FilterClause]): Expression =
  if filters.len == 0:
    raise newException(ValueError, "Empty filter sequence")
  result = parseFilter(filters[0])
  for i in 1 ..< filters.len:
    result = newCallExpression("and", result, parseFilter(filters[i]))

proc extractGuaranteeBounds*(
    guarantee: Expression, fieldName: string
): Option[tuple[minExpr: Expression, maxExpr: Expression]] =
  ## From a guarantee expression, extract the min/max bound expressions
  ## for a given field. The guarantee is expected to be of the form:
  ##   and(greater_equal(field, min_literal), less_equal(field, max_literal))
  ## possibly nested with other field constraints via AND.

  var minExpr, maxExpr: Expression

  proc extract(expr: Expression) =
    if expr.isNil:
      return
    if expr.isCall:
      case expr.functionName
      of "and", "and_kleene":
        for child in expr.args:
          extract(child)
      of "greater_equal":
        # greater_equal(field, min) → field >= min
        if expr.args.len == 2 and expr.args[0].isField and
            expr.args[0].fieldName == fieldName and expr.args[1].isLiteral:
          minExpr = expr.args[1]
      of "less_equal":
        # less_equal(field, max) → field <= max
        if expr.args.len == 2 and expr.args[0].isField and
            expr.args[0].fieldName == fieldName and expr.args[1].isLiteral:
          maxExpr = expr.args[1]
      of "equal":
        # equal(field, val) → min = max = val
        if expr.args.len == 2 and expr.args[0].isField and
            expr.args[0].fieldName == fieldName and expr.args[1].isLiteral:
          minExpr = expr.args[1]
          maxExpr = expr.args[1]
      else:
        discard

  extract(guarantee)

  if not minExpr.isNil and not maxExpr.isNil:
    return some((minExpr: minExpr, maxExpr: maxExpr))
  return none(tuple[minExpr: Expression, maxExpr: Expression])

proc isLiteralTrue*(expr: Expression): bool =
  ## Checks if an expression is the literal `true`.
  if expr.isNil or not expr.isLiteral:
    return false

  let dt = expr.datum
  let sc = dt.toScalar()
  if sc.kind == skBool:
    return sc.getBool()
  return false

proc isLiteralFalse*(expr: Expression): bool =
  ## Checks if an expression is the literal `false`.
  if expr.isNil or not expr.isLiteral:
    return false

  let dt = expr.datum
  let sc = dt.toScalar()
  if sc.kind == skBool:
    return not sc.getBool()
  return false

proc isSatisfiable*(expr: Expression): bool =
  ## Returns true unless the expression is definitely `literal(false)`.
  ## This is the final check that determines whether a row group is read.
  not isLiteralFalse(expr)

proc evaluateComparisonAgainstBounds*(
    op: string,
    bounds: tuple[minExpr: Expression, maxExpr: Expression],
    literal: Expression,
): BoundResult =
  ## Evaluates whether a comparison `field <op> literal` can be
  ## determined from the known bounds [min, max] of the field.
  ##
  ## Key insight from Arrow's implementation:
  ##
  ## | Predicate         | Always True when      | Always False when      |
  ## |-------------------|-----------------------|------------------------|
  ## | field == lit       | min == max == lit      | lit < min OR lit > max |
  ## | field != lit       | lit < min OR lit > max | min == max == lit      |
  ## | field < lit        | max < lit             | min >= lit             |
  ## | field <= lit       | max <= lit            | min > lit              |
  ## | field > lit        | min > lit             | max <= lit             |
  ## | field >= lit       | min >= lit            | max < lit              |
  ##
  ## We compare using the string representations of the literal expressions
  ## as a proxy. For a production implementation, you would compare the
  ## underlying scalar values directly.

  let minDatum = bounds.minExpr.datum
  let maxDatum = bounds.maxExpr.datum
  let litDatum = literal.datum

  if minDatum.handle.isNil or maxDatum.handle.isNil or litDatum.handle.isNil:
    return brIndeterminate

  # Extract scalars from datums for comparison
  let minSc = minDatum.toScalar()
  let maxSc = maxDatum.toScalar()
  let litSc = litDatum.toScalar()

  # Helper to compare a scalar with a literal value based on scalar type
  template compareScalar(sc: Scalar, litVal: typed): bool =
    case sc.kind
    of skInt8: sc.getInt8() < litVal
    of skInt16: sc.getInt16() < litVal
    of skInt32: sc.getInt32() < litVal
    of skInt64: sc.getInt64() < litVal
    of skUInt8: sc.getUInt8() < litVal
    of skUInt16: sc.getUInt16() < litVal
    of skUInt32: sc.getUInt32() < litVal
    of skUInt64: sc.getUInt64() < litVal
    of skFloat32: sc.getFloat32() < litVal
    of skFloat64: sc.getFloat64() < litVal
    else: false

  template compareScalars(a, b: Scalar): int =
    # Returns -1 if a < b, 0 if a == b, 1 if a > b
    # For simplicity, compare as float64
    let aVal = case a.kind
      of skInt8: a.getInt8().float64
      of skInt16: a.getInt16().float64
      of skInt32: a.getInt32().float64
      of skInt64: a.getInt64().float64
      of skUInt8: a.getUInt8().float64
      of skUInt16: a.getUInt16().float64
      of skUInt32: a.getUInt32().float64
      of skUInt64: a.getUInt64().float64
      of skFloat32: a.getFloat32().float64
      of skFloat64: a.getFloat64()
      else: return brIndeterminate
    let bVal = case b.kind
      of skInt8: b.getInt8().float64
      of skInt16: b.getInt16().float64
      of skInt32: b.getInt32().float64
      of skInt64: b.getInt64().float64
      of skUInt8: b.getUInt8().float64
      of skUInt16: b.getUInt16().float64
      of skUInt32: b.getUInt32().float64
      of skUInt64: b.getUInt64().float64
      of skFloat32: b.getFloat32().float64
      of skFloat64: b.getFloat64()
      else: return brIndeterminate
    if aVal < bVal: -1
    elif aVal > bVal: 1
    else: 0

  case op
  of "equal":
    # Always false if lit < min or lit > max
    if compareScalars(litSc, minSc) < 0 or compareScalars(litSc, maxSc) > 0:
      return brAlwaysFalse
    # Always true if min == max == lit
    if compareScalars(minSc, maxSc) == 0 and compareScalars(minSc, litSc) == 0:
      return brAlwaysTrue
    return brIndeterminate
  of "not_equal":
    if compareScalars(litSc, minSc) < 0 or compareScalars(litSc, maxSc) > 0:
      return brAlwaysTrue
    if compareScalars(minSc, maxSc) == 0 and compareScalars(minSc, litSc) == 0:
      return brAlwaysFalse
    return brIndeterminate
  of "less":
    # field < lit → always true if max < lit
    if compareScalars(maxSc, litSc) < 0:
      return brAlwaysTrue
    # always false if min >= lit
    if compareScalars(minSc, litSc) >= 0:
      return brAlwaysFalse
    return brIndeterminate
  of "less_equal":
    if compareScalars(maxSc, litSc) <= 0:
      return brAlwaysTrue
    if compareScalars(minSc, litSc) > 0:
      return brAlwaysFalse
    return brIndeterminate
  of "greater":
    if compareScalars(minSc, litSc) > 0:
      return brAlwaysTrue
    if compareScalars(maxSc, litSc) <= 0:
      return brAlwaysFalse
    return brIndeterminate
  of "greater_equal":
    if compareScalars(minSc, litSc) >= 0:
      return brAlwaysTrue
    if compareScalars(maxSc, litSc) < 0:
      return brAlwaysFalse
    return brIndeterminate
  else:
    return brIndeterminate

proc simplifyWithGuarantee*(predicate: Expression, guarantee: Expression): Expression =
  ## Recursively simplifies a predicate expression given known guarantees
  ## from row group statistics.
  ##
  ## This mirrors Arrow C++'s `SimplifyWithGuarantee`:
  ## - For comparison nodes (field op literal), evaluate against bounds
  ## - For AND nodes, simplify both sides; if either is false → false
  ## - For OR nodes, simplify both sides; if either is true → true
  ## - For NOT nodes, simplify inner and negate
  ##
  ## Returns:
  ##   - literal(true) if the predicate is always satisfied
  ##   - literal(false) if the predicate can never be satisfied
  ##   - a simplified expression otherwise

  if predicate.isNil:
    return newLiteralExpression(true)

  case predicate.kind
  of ekLiteral:
    return predicate # Already a constant
  of ekField:
    return predicate # Can't simplify a bare field reference
  of ekCall:
    case predicate.functionName

    # ----- Logical AND -----
    of "and", "and_kleene":
      let left = simplifyWithGuarantee(predicate.args[0], guarantee)
      let right = simplifyWithGuarantee(predicate.args[1], guarantee)

      # Short-circuit: if either side is always false, whole AND is false
      if isLiteralFalse(left) or isLiteralFalse(right):
        return newLiteralExpression(false)
      # If one side is always true, result is the other side
      if isLiteralTrue(left):
        return right
      if isLiteralTrue(right):
        return left
      return andExpr(left, right)

    # ----- Logical OR -----
    of "or", "or_kleene":
      let left = simplifyWithGuarantee(predicate.args[0], guarantee)
      let right = simplifyWithGuarantee(predicate.args[1], guarantee)

      # Short-circuit: if either side is always true, whole OR is true
      if isLiteralTrue(left) or isLiteralTrue(right):
        return newLiteralExpression(true)
      if isLiteralFalse(left):
        return right
      if isLiteralFalse(right):
        return left
      return orExpr(left, right)

    # ----- Logical NOT -----
    of "invert", "not":
      let inner = simplifyWithGuarantee(predicate.args[0], guarantee)
      if isLiteralTrue(inner):
        return newLiteralExpression(false)
      if isLiteralFalse(inner):
        return newLiteralExpression(true)
      return notExpr(inner)

    # ----- Comparison operators -----
    of "equal", "not_equal", "less", "less_equal", "greater", "greater_equal":
      let pair = findFieldAndLiteral(predicate)
      if pair.isNone:
        return predicate # Can't simplify complex comparisons

      let (fieldExpr, litExpr, flipped) = pair.get
      let op =
        if flipped:
          flipOp(predicate.functionName)
        else:
          predicate.functionName
      let fName = fieldExpr.fieldName

      let bounds = extractGuaranteeBounds(guarantee, fName)
      if bounds.isNone:
        return predicate # No statistics for this field

      let result = evaluateComparisonAgainstBounds(op, bounds.get, litExpr)
      case result
      of brAlwaysTrue:
        return newLiteralExpression(true)
      of brAlwaysFalse:
        return newLiteralExpression(false)
      of brIndeterminate:
        return predicate

    # ----- is_null / is_valid -----
    of "is_null":
      # If statistics show no nulls, is_null is always false
      # (Would need null count from statistics — extend later)
      return predicate
    of "is_valid":
      return predicate
    else:
      # Unknown function — can't simplify
      return predicate

# ============================================================================
# Statistics to Expression Conversion
# ============================================================================

proc statisticsAsExpression*(fieldName: string, stats: Statistics): Option[Expression] =
  ## Converts column statistics into a guarantee expression of the form:
  ##   and(greater_equal(field, min), less_equal(field, max))
  ##
  ## This encodes the invariant: all values in this row group satisfy
  ##   min ≤ field ≤ max
  ##
  ## Returns none if statistics are not available or have no min/max.

  if not stats.hasMinMax:
    return none(Expression)

  let field = col(fieldName)

  if stats.isBooleanStatistics:
    let s = stats.toBooleanStatistics()
    let minVal = s.min
    let maxVal = s.max
    # For boolean: if min == max, the column is constant
    if minVal == maxVal:
      return some(field == minVal)
    else:
      # Both true and false present — no useful constraint
      return none(Expression)
  elif stats.isInt32Statistics:
    let s = stats.toInt32Statistics()
    return some(andExpr(field >= s.min, field <= s.max))
  elif stats.isInt64Statistics:
    let s = stats.toInt64Statistics()
    return some(andExpr(field >= s.min, field <= s.max))
  elif stats.isFloatStatistics:
    let s = stats.toFloatStatistics()
    return some(andExpr(field >= s.min, field <= s.max))
  elif stats.isDoubleStatistics:
    let s = stats.toDoubleStatistics()
    return some(andExpr(field >= s.min, field <= s.max))
  elif stats.isByteArrayStatistics:
    let s = stats.toByteArrayStatistics()
    let minStr = s.min.toString
    let maxStr = s.max.toString
    if minStr.len > 0 or maxStr.len > 0:
      return some(andExpr(field >= minStr, field <= maxStr))
    return none(Expression)
  elif stats.isFixedLengthByteArrayStatistics:
    let s = stats.toFixedLengthByteArrayStatistics()
    let minStr = s.min.toString
    let maxStr = s.max.toString
    if minStr.len > 0 or maxStr.len > 0:
      return some(andExpr(field >= minStr, field <= maxStr))
    return none(Expression)
  else:
    return none(Expression)
