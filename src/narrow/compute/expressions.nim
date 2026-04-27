import std/[strutils, sets, options, parseutils]
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

  ScalarObj = object
    handle*: ptr GArrowScalar
    kind*: ScalarKind

  Scalar* = ref ScalarObj

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

proc `=wasMoved`*(dt: var Datum) =
  dt.handle = nil

proc `=dup`*(dt: Datum): Datum =
  result.handle = dt.handle
  if not isNil(dt.handle):
    discard g_object_ref(dt.handle)

proc `=copy`*(dest: var Datum, src: Datum) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# ============================================================================
# ARC Hooks — Scalar (ref type, only need destroy for the object)
# ============================================================================

proc `=destroy`*(sc: ScalarObj) =
  if not isNil(sc.handle):
    g_object_unref(sc.handle)

proc `=destroy`*(expr: ExpressionObj) =
  if not isNil(expr.handle):
    g_object_unref(expr.handle)

proc newScalar*(handle: ptr GArrowScalar, kind: ScalarKind): Scalar =
  result = Scalar(handle: handle, kind: kind)

# ============================================================================
# Pointer Converters
# ============================================================================

func toPtr*(dt: Datum): ptr GArrowDatum {.inline.} =
  dt.handle

func toPtr*(sc: Scalar): ptr GArrowScalar {.inline.} =
  if sc.isNil: nil else: sc.handle

func toPtr*(expr: Expression): ptr GArrowExpression {.inline.} =
  if expr.isNil: nil else: expr.handle

# ============================================================================
# Expression — Identity & Display
# ============================================================================

proc `$`*(expr: Expression): string =
  if expr.isNil or expr.handle == nil:
    return "Expression(nil)"
  result = $newGString(garrow_expression_to_string(expr.handle))

func `==`*(a, b: Expression): bool {.inline.} =
  if a.isNil and b.isNil:
    return true
  if a.isNil or b.isNil:
    return false
  garrow_expression_equal(a.handle, b.handle) != 0

# ============================================================================
# Expression — Tree Queries
# ============================================================================

func isLiteral*(expr: Expression): bool {.inline.} =
  not expr.isNil and expr.kind == ekLiteral

func isField*(expr: Expression): bool {.inline.} =
  not expr.isNil and expr.kind == ekField

func isCall*(expr: Expression): bool {.inline.} =
  not expr.isNil and expr.kind == ekCall

func isComparison*(expr: Expression): bool {.inline.} =
  expr.isCall and
    expr.functionName in
    ["equal", "not_equal", "less", "less_equal", "greater", "greater_equal"]

func isLogical*(expr: Expression): bool {.inline.} =
  expr.isCall and expr.functionName in ["and", "or", "invert"]

func isArithmetic*(expr: Expression): bool {.inline.} =
  expr.isCall and expr.functionName in ["add", "subtract", "multiply", "divide"]

func arity*(expr: Expression): int {.inline.} =
  ## Number of child arguments (0 for literals/fields)
  if expr.isCall: expr.args.len else: 0

let emptyExprSeq: seq[Expression] = @[]

proc children*(expr: Expression): seq[Expression] {.inline.} =
  ## Returns child expressions. Empty for leaf nodes.
  if expr.isCall: expr.args else: emptyExprSeq

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

func referencedFields*(expr: Expression): HashSet[string] =
  ## Recursively collects all field names referenced in the expression tree.
  result = initHashSet[string]()
  collectFieldsImpl(expr, result)

proc referencedFieldSeqWalk(
    e: Expression, seen: var HashSet[string], res: var seq[string]
) =
  ## Helper for `referencedFieldSeq` — recursively walks expression tree.
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
      referencedFieldSeqWalk(child, seen, res)

func referencedFieldSeq*(expr: Expression): seq[string] =
  ## Returns referenced fields as an ordered seq (insertion order).
  var seen = initHashSet[string]()
  result = @[]
  referencedFieldSeqWalk(expr, seen, result)

func fieldName*(expr: Expression): string =
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

func depth*(expr: Expression): int =
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

func nodeCount*(expr: Expression): int =
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
  new(result)
  result.handle = cast[ptr GArrowScalar](garrow_null_scalar_new())
  result.kind = skNull

proc newScalar*(handle: ptr GArrowScalar): Scalar =
  new(result)
  result.handle = handle
  result.kind = skNull

proc newScalar*(v: bool): Scalar =
  new(result)
  result.handle = cast[ptr GArrowScalar](garrow_boolean_scalar_new(v.gboolean))
  result.kind = skBool

proc newScalar*(v: int8): Scalar =
  new(result)
  result.handle = cast[ptr GArrowScalar](garrow_int8_scalar_new(v.gint8))
  result.kind = skInt8

proc newScalar*(v: int16): Scalar =
  new(result)
  result.handle = cast[ptr GArrowScalar](garrow_int16_scalar_new(v.gint16))
  result.kind = skInt16

proc newScalar*(v: int32): Scalar =
  new(result)
  result.handle = cast[ptr GArrowScalar](garrow_int32_scalar_new(v.gint32))
  result.kind = skInt32

proc newScalar*(v: int64): Scalar =
  new(result)
  result.handle = cast[ptr GArrowScalar](garrow_int64_scalar_new(v.gint64))
  result.kind = skInt64

proc newScalar*(v: uint8): Scalar =
  new(result)
  result.handle = cast[ptr GArrowScalar](garrow_uint8_scalar_new(v.guint8))
  result.kind = skUInt8

proc newScalar*(v: uint16): Scalar =
  new(result)
  result.handle = cast[ptr GArrowScalar](garrow_uint16_scalar_new(v.guint16))
  result.kind = skUInt16

proc newScalar*(v: uint32): Scalar =
  new(result)
  result.handle = cast[ptr GArrowScalar](garrow_uint32_scalar_new(v.guint32))
  result.kind = skUInt32

proc newScalar*(v: uint64): Scalar =
  new(result)
  result.handle = cast[ptr GArrowScalar](garrow_uint64_scalar_new(v.guint64))
  result.kind = skUInt64

proc newScalar*(v: float32): Scalar =
  new(result)
  result.handle = cast[ptr GArrowScalar](garrow_float_scalar_new(v.gfloat))
  result.kind = skFloat32

proc newScalar*(v: float64): Scalar =
  new(result)
  result.handle = cast[ptr GArrowScalar](garrow_double_scalar_new(v.gdouble))
  result.kind = skFloat64

# proc newScalar*(v: string): Scalar =
#   let buffer = garrow_buffer_new(cast[ptr guint8](v.cstring), v.len.gint64)
#   new(result)
#   result.handle = cast[ptr GArrowScalar](garrow_string_scalar_new(buffer))
#   result.kind = skString
#   g_object_unref(buffer)
proc newScalar*(v: string): Scalar =
  new(result)
  # garrow_buffer_new_bytes with GBytes creates a buffer that OWNS a copy
  let gbytes = g_bytes_new(cast[gconstpointer](v.cstring), v.len.gsize)
  let buffer = garrow_buffer_new_bytes(gbytes)
  g_bytes_unref(gbytes) # buffer holds a ref to gbytes internally
  result.handle = cast[ptr GArrowScalar](garrow_string_scalar_new(buffer))
  result.kind = skString
  g_object_unref(buffer)

# proc newScalar*(v: seq[byte]): Scalar =
#   let buffer = garrow_buffer_new(cast[ptr guint8](v[0].unsafeAddr), v.len.gint64)
#   new(result)
#   result.handle = cast[ptr GArrowScalar](garrow_binary_scalar_new(buffer))
#   result.kind = skBinary
#   g_object_unref(buffer)

proc newScalar*(v: seq[byte]): Scalar =
  new(result)
  let gbytes = g_bytes_new(cast[gconstpointer](v[0].unsafeAddr), v.len.gsize)
  let buffer = garrow_buffer_new_bytes(gbytes)
  g_bytes_unref(gbytes)
  result.handle = cast[ptr GArrowScalar](garrow_binary_scalar_new(buffer))
  result.kind = skBinary
  g_object_unref(buffer)

proc newScalar*(v: Date32): Scalar =
  new(result)
  result.handle = cast[ptr GArrowScalar](garrow_date32_scalar_new(v.int32.gint32))
  result.kind = skDate32

proc newScalar*(v: Date64): Scalar =
  new(result)
  result.handle = cast[ptr GArrowScalar](garrow_date64_scalar_new(v.int64.gint64))
  result.kind = skDate64

proc newScalar*(v: MonthInterval): Scalar =
  new(result)
  result.handle =
    cast[ptr GArrowScalar](garrow_month_interval_scalar_new(v.int32.gint32))
  result.kind = skMonthInterval

# ============================================================================
# Datum Constructors
# ============================================================================

proc newDatum*(sc: Scalar): Datum =
  result.handle = cast[ptr GArrowDatum](garrow_scalar_datum_new(sc.toPtr))

proc newDatum*[T: ArrowPrimitive](value: T): Datum =
  let scalar =
    when T is bool:
      cast[ptr GArrowScalar](garrow_boolean_scalar_new(value.gboolean))
    elif T is int8:
      cast[ptr GArrowScalar](garrow_int8_scalar_new(value.gint8))
    elif T is uint8:
      cast[ptr GArrowScalar](garrow_uint8_scalar_new(value.guint8))
    elif T is int16:
      cast[ptr GArrowScalar](garrow_int16_scalar_new(value.gint16))
    elif T is uint16:
      cast[ptr GArrowScalar](garrow_uint16_scalar_new(value.guint16))
    elif T is int32:
      cast[ptr GArrowScalar](garrow_int32_scalar_new(value.gint32))
    elif T is uint32:
      cast[ptr GArrowScalar](garrow_uint32_scalar_new(value.guint32))
    elif T is int64 or T is int:
      cast[ptr GArrowScalar](garrow_int64_scalar_new(value.gint64))
    elif T is uint64 or T is uint:
      cast[ptr GArrowScalar](garrow_uint64_scalar_new(value.guint64))
    elif T is float32:
      cast[ptr GArrowScalar](garrow_float_scalar_new(value.gfloat))
    elif T is float64:
      cast[ptr GArrowScalar](garrow_double_scalar_new(value.gdouble))
    elif T is string:
      let gbytes = g_bytes_new(cast[gconstpointer](value.cstring), value.len.gsize)
      let buffer = garrow_buffer_new_bytes(gbytes)
      g_bytes_unref(gbytes)
      let sc = cast[ptr GArrowScalar](garrow_string_scalar_new(buffer))
      g_object_unref(buffer)
      sc
    elif T is seq[byte]:
      let gbytes =
        g_bytes_new(cast[gconstpointer](value[0].unsafeAddr), value.len.gsize)
      let buffer = garrow_buffer_new_bytes(gbytes)
      g_bytes_unref(gbytes)
      let sc = cast[ptr GArrowScalar](garrow_binary_scalar_new(buffer))
      g_object_unref(buffer)
      sc
    elif T is Date32:
      cast[ptr GArrowScalar](garrow_date32_scalar_new(value.int32.gint32))
    elif T is Date64:
      cast[ptr GArrowScalar](garrow_date64_scalar_new(value.int64.gint64))
    elif T is MonthInterval:
      cast[ptr GArrowScalar](garrow_month_interval_scalar_new(value.int32.gint32))
    else:
      nil
  result.handle = cast[ptr GArrowDatum](garrow_scalar_datum_new(scalar))
  if not scalar.isNil:
    g_object_unref(scalar)

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

func isArray*(dt: Datum): bool {.inline.} =
  garrow_datum_is_array(dt.handle) != 0

func isArrayLike*(dt: Datum): bool {.inline.} =
  garrow_datum_is_array_like(dt.handle) != 0

func isScalar*(dt: Datum): bool {.inline.} =
  garrow_datum_is_scalar(dt.handle) != 0

func isValue*(dt: Datum): bool {.inline.} =
  garrow_datum_is_value(dt.handle) != 0

func `==`*(a, b: Datum): bool {.inline.} =
  garrow_datum_equal(a.toPtr, b.toPtr) != 0

proc `$`*(dt: Datum): string =
  result = $newGString(garrow_datum_to_string(dt.handle))

# ============================================================================
# GObject Type Detection
# ============================================================================

func detectScalarKind*(handle: ptr GArrowScalar): ScalarKind =
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

func kind*(sc: Scalar): ScalarKind {.inline.} =
  if sc.isNil:
    return skNull
  detectScalarKind(sc.handle)

# ============================================================================
# Datum Extraction Methods
# ============================================================================

proc toScalar*(dt: Datum): Scalar =
  if not dt.isScalar:
    raise newException(ValueError, "Datum is not a scalar")
  var scalarPtr: ptr GArrowScalar
  g_object_get(dt.handle, "value", addr scalarPtr, nil)
  new(result)
  result.handle = scalarPtr
  result.kind = detectScalarKind(scalarPtr)

# ============================================================================
# Datum Kind (runtime)
# ============================================================================

func kind*(dt: Datum): DatumKind {.inline.} =
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

proc toChunkedArray*(dt: Datum): ChunkedArray[void] =
  if dt.kind != dkChunkedArray:
    raise newException(ValueError, "Datum is not a chunked array")
  var caPtr: ptr GArrowChunkedArray
  g_object_get(dt.handle, "value", addr caPtr, nil)
  result = newChunkedArray[void](caPtr)

# ============================================================================
# Scalar Methods
# ============================================================================

func isValid*(sc: Scalar): bool {.inline.} =
  if sc.isNil or sc.handle.isNil:
    return false
  garrow_scalar_is_valid(sc.handle) != 0

func `==`*(a, b: Scalar): bool {.inline.} =
  if a.isNil and b.isNil:
    return true
  if a.isNil or b.isNil:
    return false
  if a.handle.isNil and b.handle.isNil:
    return true
  if a.handle.isNil or b.handle.isNil:
    return false
  garrow_scalar_equal(a.handle, b.handle) != 0

proc `$`*(sc: Scalar): string =
  if sc.isNil or sc.handle.isNil:
    return "Scalar(nil)"
  result = $newGString(garrow_scalar_to_string(sc.handle))

# ============================================================================
# Scalar Value Extractors
# ============================================================================

func getBool*(sc: Scalar): bool {.inline.} =
  if sc.isNil or sc.handle.isNil:
    return false
  garrow_boolean_scalar_get_value(cast[ptr GArrowBooleanScalar](sc.handle)) != 0

func getInt8*(sc: Scalar): int8 {.inline.} =
  if sc.isNil or sc.handle.isNil:
    return 0
  garrow_int8_scalar_get_value(cast[ptr GArrowInt8Scalar](sc.handle))

func getInt16*(sc: Scalar): int16 {.inline.} =
  if sc.isNil or sc.handle.isNil:
    return 0
  garrow_int16_scalar_get_value(cast[ptr GArrowInt16Scalar](sc.handle))

func getInt32*(sc: Scalar): int32 {.inline.} =
  if sc.isNil or sc.handle.isNil:
    return 0
  garrow_int32_scalar_get_value(cast[ptr GArrowInt32Scalar](sc.handle))

func getInt64*(sc: Scalar): int64 {.inline.} =
  if sc.isNil or sc.handle.isNil:
    return 0
  garrow_int64_scalar_get_value(cast[ptr GArrowInt64Scalar](sc.handle))

func getUInt8*(sc: Scalar): uint8 {.inline.} =
  if sc.isNil or sc.handle.isNil:
    return 0
  garrow_uint8_scalar_get_value(cast[ptr GArrowUInt8Scalar](sc.handle))

func getUInt16*(sc: Scalar): uint16 {.inline.} =
  if sc.isNil or sc.handle.isNil:
    return 0
  garrow_uint16_scalar_get_value(cast[ptr GArrowUInt16Scalar](sc.handle))

func getUInt32*(sc: Scalar): uint32 {.inline.} =
  if sc.isNil or sc.handle.isNil:
    return 0
  garrow_uint32_scalar_get_value(cast[ptr GArrowUInt32Scalar](sc.handle))

func getUInt64*(sc: Scalar): uint64 {.inline.} =
  if sc.isNil or sc.handle.isNil:
    return 0
  garrow_uint64_scalar_get_value(cast[ptr GArrowUInt64Scalar](sc.handle))

func getFloat32*(sc: Scalar): float32 {.inline.} =
  if sc.isNil or sc.handle.isNil:
    return 0.0
  garrow_float_scalar_get_value(cast[ptr GArrowFloatScalar](sc.handle))

func getFloat64*(sc: Scalar): float64 {.inline.} =
  if sc.isNil or sc.handle.isNil:
    return 0.0
  garrow_double_scalar_get_value(cast[ptr GArrowDoubleScalar](sc.handle))

# proc getString*(sc: Scalar): string =
# Extract string value from a string scalar.
# Returns empty string if scalar is not a string type.

# let buffer = garrow_base_binary_scalar_get_value(cast[ptr GArrowBaseBinaryScalar](sc.handle))
# if buffer.isNil:
#   return ""
# let gbytes = garrow_buffer_get_data(buffer)
# if gbytes.isNil:
#   return ""
# var size: gsize = 0
# let data = g_bytes_get_data(gbytes, addr size)
# if data.isNil or size == 0:
#   g_bytes_unref(gbytes)
#   return ""
# result = newString(size)
# copyMem(result[0].unsafeAddr, data, size)
# g_bytes_unref(gbytes)

# ============================================================================
# Runtime Value Extraction
# ============================================================================

func value*(sc: Scalar, _: typedesc[bool]): bool {.inline.} =
  if sc.isNil:
    raise newException(ValueError, "Scalar is nil")
  if sc.kind != skBool:
    raise newException(ValueError, "Scalar is not a bool, got: " & $sc.kind)
  sc.getBool()

func value*(sc: Scalar, _: typedesc[int8]): int8 {.inline.} =
  if sc.isNil:
    raise newException(ValueError, "Scalar is nil")
  if sc.kind != skInt8:
    raise newException(ValueError, "Scalar is not an int8, got: " & $sc.kind)
  sc.getInt8()

func value*(sc: Scalar, _: typedesc[int16]): int16 {.inline.} =
  if sc.isNil:
    raise newException(ValueError, "Scalar is nil")
  if sc.kind != skInt16:
    raise newException(ValueError, "Scalar is not an int16, got: " & $sc.kind)
  sc.getInt16()

func value*(sc: Scalar, _: typedesc[int32]): int32 {.inline.} =
  if sc.isNil:
    raise newException(ValueError, "Scalar is nil")
  case sc.kind
  of skInt32:
    sc.getInt32()
  of skDate32, skMonthInterval:
    sc.getInt32()
  else:
    raise newException(
      ValueError, "Scalar is not an int32/date32/month_interval, got: " & $sc.kind
    )

func value*(sc: Scalar, _: typedesc[int64]): int64 {.inline.} =
  if sc.isNil:
    raise newException(ValueError, "Scalar is nil")
  case sc.kind
  of skInt64:
    sc.getInt64()
  of skDate64:
    sc.getInt64()
  else:
    raise newException(ValueError, "Scalar is not an int64/date64, got: " & $sc.kind)

func value*(sc: Scalar, _: typedesc[uint8]): uint8 {.inline.} =
  if sc.isNil:
    raise newException(ValueError, "Scalar is nil")
  if sc.kind != skUInt8:
    raise newException(ValueError, "Scalar is not a uint8, got: " & $sc.kind)
  sc.getUInt8()

func value*(sc: Scalar, _: typedesc[uint16]): uint16 {.inline.} =
  if sc.isNil:
    raise newException(ValueError, "Scalar is nil")
  if sc.kind != skUInt16:
    raise newException(ValueError, "Scalar is not a uint16, got: " & $sc.kind)
  sc.getUInt16()

func value*(sc: Scalar, _: typedesc[uint32]): uint32 {.inline.} =
  if sc.isNil:
    raise newException(ValueError, "Scalar is nil")
  if sc.kind != skUInt32:
    raise newException(ValueError, "Scalar is not a uint32, got: " & $sc.kind)
  sc.getUInt32()

func value*(sc: Scalar, _: typedesc[uint64]): uint64 {.inline.} =
  if sc.isNil:
    raise newException(ValueError, "Scalar is nil")
  if sc.kind != skUInt64:
    raise newException(ValueError, "Scalar is not a uint64, got: " & $sc.kind)
  sc.getUInt64()

func value*(sc: Scalar, _: typedesc[float32]): float32 {.inline.} =
  if sc.isNil:
    raise newException(ValueError, "Scalar is nil")
  if sc.kind != skFloat32:
    raise newException(ValueError, "Scalar is not a float32, got: " & $sc.kind)
  sc.getFloat32()

proc value*(sc: Scalar, _: typedesc[float64]): float64 =
  if sc.isNil:
    raise newException(ValueError, "Scalar is nil")
  if sc.kind != skFloat64:
    raise newException(ValueError, "Scalar is not a float64, got: " & $sc.kind)
  sc.getFloat64()

proc value*(sc: Scalar, _: typedesc[string]): string =
  if sc.isNil:
    raise newException(ValueError, "Scalar is nil")
  case sc.kind
  of skString, skBinary:
    $sc
  else:
    raise newException(ValueError, "Scalar is not a string/binary, got: " & $sc.kind)

# ============================================================================
# Scalar-to-Scalar Comparison Operators
# ============================================================================

proc `<`*(a, b: Scalar): bool =
  ## Compare scalar < scalar. Only works for numeric scalars of compatible types.
  case a.kind
  of skInt8:
    a.getInt8() < b.getInt8()
  of skInt16:
    a.getInt16() < b.getInt16()
  of skInt32:
    a.getInt32() < b.getInt32()
  of skInt64:
    a.getInt64() < b.getInt64()
  of skUInt8:
    a.getUInt8() < b.getUInt8()
  of skUInt16:
    a.getUInt16() < b.getUInt16()
  of skUInt32:
    a.getUInt32() < b.getUInt32()
  of skUInt64:
    a.getUInt64() < b.getUInt64()
  of skFloat32:
    a.getFloat32() < b.getFloat32()
  of skFloat64:
    a.getFloat64() < b.getFloat64()
  of skString:
    let left = $a
    let right = $b
    left < right
  else:
    raise
      newException(ValueError, "Cannot compare scalar of kind " & $a.kind & " with <")

proc `<=`*(a, b: Scalar): bool =
  ## Compare scalar <= scalar. Only works for numeric scalars.
  a < b or a == b

proc `>`*(a, b: Scalar): bool =
  ## Compare scalar > scalar. Only works for numeric scalars.
  b < a

proc `>=`*(a, b: Scalar): bool =
  ## Compare scalar >= scalar. Only works for numeric scalars.
  b < a or a == b

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

proc newFieldExpression*(name: sink string): Expression =
  ## Creates a field reference expression.
  ##
  ## Example:
  ##   ```nim
  ##   let ageField = newFieldExpression("age")
  ##   ```
  let cname = name.cstring
  new(result)
  result[] = ExpressionObj(
    kind: ekField,
    handle: cast[ptr GArrowExpression](verify garrow_field_expression_new(cname)),
    fieldName: move(name),
  )

# ============================================================================
# Expression Constructors — Call Nodes
# ============================================================================

proc newCallExpression*(
    function: sink string, args: openArray[Expression]
): Expression =
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
  ##   let isAdult = newCallExpression("greater_equal", [age, threshold])
  ##   ```
  let cfunc = function.cstring
  var argList = newGList[ptr GArrowExpression]()
  var childExprs = newSeqOfCap[Expression](args.len)
  for arg in args:
    argList.append(arg.toPtr)
    childExprs.add(arg)

  new(result)
  result[] = ExpressionObj(
    kind: ekCall,
    handle:
      cast[ptr GArrowExpression](garrow_call_expression_new(cfunc, argList.toPtr, nil)),
    functionName: move(function),
    args: move(childExprs),
  )

proc newCallExpressionWithOptions*(
    function: sink string, options: MatchSubstringOptions, args: openArray[Expression]
): Expression =
  ## Creates a call expression with MatchSubstringOptions.
  let cfunc = function.cstring
  var argList = newGList[ptr GArrowExpression]()
  var childExprs = newSeqOfCap[Expression](args.len)
  for arg in args:
    argList.append(arg.toPtr)
    childExprs.add(arg)

  new(result)
  result[] = ExpressionObj(
    kind: ekCall,
    handle: cast[ptr GArrowExpression](garrow_call_expression_new(
      cfunc, argList.toPtr, cast[ptr GArrowFunctionOptions](options.toPtr)
    )),
    functionName: move(function),
    args: move(childExprs),
  )

# ============================================================================
# Convenience — Comparisons
# ============================================================================

proc eq*[T: DatumCompatible](field: Expression, value: T): Expression =
  newCallExpression("equal", [field, newLiteralExpression(value)])

proc eq*(a, b: Expression): Expression =
  newCallExpression("equal", [a, b])

proc neq*[T: DatumCompatible](field: Expression, value: T): Expression =
  newCallExpression("not_equal", [field, newLiteralExpression(value)])

proc neq*(a, b: Expression): Expression =
  newCallExpression("not_equal", [a, b])

proc lt*[T: DatumCompatible](field: Expression, value: T): Expression =
  newCallExpression("less", [field, newLiteralExpression(value)])

proc lt*(a, b: Expression): Expression =
  newCallExpression("less", [a, b])

proc le*[T: DatumCompatible](field: Expression, value: T): Expression =
  newCallExpression("less_equal", [field, newLiteralExpression(value)])

proc le*(a, b: Expression): Expression =
  newCallExpression("less_equal", [a, b])

proc gt*[T: DatumCompatible](field: Expression, value: T): Expression =
  newCallExpression("greater", [field, newLiteralExpression(value)])

proc gt*(a, b: Expression): Expression =
  newCallExpression("greater", [a, b])

proc ge*[T: DatumCompatible](field: Expression, value: T): Expression =
  newCallExpression("greater_equal", [field, newLiteralExpression(value)])

proc ge*(a, b: Expression): Expression =
  newCallExpression("greater_equal", [a, b])

# ============================================================================
# Convenience — Logical
# ============================================================================

proc andExpr*(a, b: Expression): Expression =
  newCallExpression("and", [a, b])

proc orExpr*(a, b: Expression): Expression =
  newCallExpression("or", [a, b])

proc notExpr*(expr: Expression): Expression =
  newCallExpression("invert", [expr])

# ============================================================================
# Convenience — Arithmetic
# ============================================================================

proc add*(a, b: Expression): Expression =
  newCallExpression("add", [a, b])

proc sub*(a, b: Expression): Expression =
  newCallExpression("subtract", [a, b])

proc mul*(a, b: Expression): Expression =
  newCallExpression("multiply", [a, b])

proc divide*(a, b: Expression): Expression =
  newCallExpression("divide", [a, b])

# ============================================================================
# Convenience — Null checks
# ============================================================================

proc isNull*(field: Expression): Expression =
  newCallExpression("is_null", [field])

proc isValid*(field: Expression): Expression =
  newCallExpression("is_valid", [field])

# ============================================================================
# String Operations
# ============================================================================

proc strLength*(expr: Expression): Expression =
  newCallExpression("utf8_length", [expr])

proc strUpper*(expr: Expression): Expression =
  newCallExpression("utf8_upper", [expr])

proc strLower*(expr: Expression): Expression =
  newCallExpression("utf8_lower", [expr])

proc strContains*(
    expr: Expression, substr: string, ignoreCase: bool = false
): Expression =
  let options = newMatchSubstringOptions(substr, ignoreCase)
  newCallExpressionWithOptions("match_substring", options, [expr])

proc startsWith*(
    expr: Expression, prefix: string, ignoreCase: bool = false
): Expression =
  let options = newMatchSubstringOptions(prefix, ignoreCase)
  newCallExpressionWithOptions("starts_with", options, [expr])

proc endsWith*(expr: Expression, suffix: string, ignoreCase: bool = false): Expression =
  let options = newMatchSubstringOptions(suffix, ignoreCase)
  newCallExpressionWithOptions("ends_with", options, [expr])

proc matchSubstringRegex*(
    expr: Expression, pattern: string, ignoreCase: bool = false
): Expression =
  let options = newMatchSubstringOptions(pattern, ignoreCase)
  newCallExpressionWithOptions("match_substring_regex", options, [expr])

# ============================================================================
# DSL Entry Point
# ============================================================================

proc col*(name: sink string): Expression =
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
      result.add("\n")
      result.add(treeRepr(child, indent + 1))

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
    var newArgs = newSeqOfCap[Expression](expr.args.len)
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
  ## Uses exception-free parsing via ``parseutils`` to avoid allocation
  ## overhead on non-numeric inputs.
  template eqIgnoreCase(s: string, target: static string): bool =
    s.len == target.len and cmpIgnoreCase(s, target) == 0

  if eqIgnoreCase(value, "true"):
    return newLiteralExpression(true)
  elif eqIgnoreCase(value, "false"):
    return newLiteralExpression(false)

  if value.contains('.') or value.contains('e') or value.contains('E'):
    var f: float
    if parseFloat(value, f) == value.len:
      return newLiteralExpression(f.float64)

  var intVal: int64
  var intChars = 0
  try:
    intChars = parseBiggestInt(value, intVal)
  except ValueError:
    discard
  if intChars == value.len:
    if intVal >= int32.low.int64 and intVal <= int32.high.int64:
      return newLiteralExpression(intVal.int32)
    else:
      return newLiteralExpression(intVal)

  return newLiteralExpression(value)

proc parseFilter*(cl: FilterClause): Expression =
  let fieldExpr = newFieldExpression(cl.field)
  let valueExpr = parseValue(cl.value)
  case cl.op
  of "==":
    newCallExpression("equal", [fieldExpr, valueExpr])
  of "!=":
    newCallExpression("not_equal", [fieldExpr, valueExpr])
  of "<":
    newCallExpression("less", [fieldExpr, valueExpr])
  of "<=":
    newCallExpression("less_equal", [fieldExpr, valueExpr])
  of ">":
    newCallExpression("greater", [fieldExpr, valueExpr])
  of ">=":
    newCallExpression("greater_equal", [fieldExpr, valueExpr])
  of "contains":
    strContains(fieldExpr, cl.value)
  else:
    raise newException(ValueError, "Unknown operator: " & cl.op)

proc parse*(filters: seq[FilterClause]): Expression =
  if filters.len == 0:
    raise newException(ValueError, "Empty filter sequence")
  result = parseFilter(filters[0])
  for i in 1 ..< filters.len:
    result = newCallExpression("and", [result, parseFilter(filters[i])])

proc extractGuaranteeBoundsExtract(
    expr: Expression,
    fieldName: string,
    minExpr: var Expression,
    maxExpr: var Expression,
) =
  ## Helper for `extractGuaranteeBounds` — recursively extracts bound expressions.
  if expr.isNil:
    return
  if expr.isCall:
    case expr.functionName
    of "and", "and_kleene":
      for child in expr.args:
        extractGuaranteeBoundsExtract(child, fieldName, minExpr, maxExpr)
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

proc extractGuaranteeBounds*(
    guarantee: Expression, fieldName: string
): Option[tuple[minExpr: Expression, maxExpr: Expression]] =
  ## From a guarantee expression, extract the min/max bound expressions
  ## for a given field. The guarantee is expected to be of the form:
  ##   and(greater_equal(field, min_literal), less_equal(field, max_literal))
  ## possibly nested with other field constraints via AND.

  var minExpr, maxExpr: Expression
  extractGuaranteeBoundsExtract(guarantee, fieldName, minExpr, maxExpr)

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

proc areScalarsComparable(a, b: Scalar): bool =
  ## Check if two scalars can be compared with ordering operators.
  ## Returns true only if kinds match exactly (numeric or string).
  ## Cross-type numeric comparisons (e.g. int32 vs int64) are rejected
  ## to avoid crashes in kind-specific getters.
  template isNumeric(k: ScalarKind): bool =
    k in {
      skInt8, skInt16, skInt32, skInt64, skUInt8, skUInt16, skUInt32, skUInt64,
      skFloat32, skFloat64,
    }

  if a.kind != b.kind:
    return false
  if isNumeric(a.kind):
    return true
  if a.kind == skString:
    return true
  return false

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

  let minDatum = bounds.minExpr.datum
  let maxDatum = bounds.maxExpr.datum
  let litDatum = literal.datum

  if minDatum.handle.isNil or maxDatum.handle.isNil or litDatum.handle.isNil:
    return brIndeterminate

  # Extract scalars from datums for comparison
  let minSc = minDatum.toScalar()
  let maxSc = maxDatum.toScalar()
  let litSc = litDatum.toScalar()

  # Check if all scalars are comparable (all numeric or all string)
  if not (
    areScalarsComparable(minSc, maxSc) and areScalarsComparable(minSc, litSc) and
    areScalarsComparable(maxSc, litSc)
  ):
    return brIndeterminate

  case op
  of "equal":
    # Always false if lit < min or lit > max
    if litSc < minSc or litSc > maxSc:
      return brAlwaysFalse
    # Always true if min == max == lit
    if minSc == maxSc and minSc == litSc:
      return brAlwaysTrue
    return brIndeterminate
  of "not_equal":
    if litSc < minSc or litSc > maxSc:
      return brAlwaysTrue
    if minSc == maxSc and minSc == litSc:
      return brAlwaysFalse
    return brIndeterminate
  of "less":
    # field < lit → always true if max < lit
    if maxSc < litSc:
      return brAlwaysTrue
    # always false if min >= lit
    if minSc >= litSc:
      return brAlwaysFalse
    return brIndeterminate
  of "less_equal":
    if maxSc <= litSc:
      return brAlwaysTrue
    if minSc > litSc:
      return brAlwaysFalse
    return brIndeterminate
  of "greater":
    if minSc > litSc:
      return brAlwaysTrue
    if maxSc <= litSc:
      return brAlwaysFalse
    return brIndeterminate
  of "greater_equal":
    if minSc >= litSc:
      return brAlwaysTrue
    if maxSc < litSc:
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

      let res = evaluateComparisonAgainstBounds(op, bounds.get, litExpr)
      case res
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
