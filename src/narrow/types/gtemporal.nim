import std/[options, times]
import ../core/[ffi, error]
import ./gtypes

# Time units mapping
type
  Date32* = object
    value*: int32

  Date64* = object
    value*: int64

  Timestamp* = object
    value*: int64
    unit*: GArrowTimeUnit
    tz*: string

  Duration* = object
    value*: int64
    unit*: GArrowTimeUnit

  Time32* = object
    value*: int32

  Time64* = object
    value*: int64

  MonthInterval* = object
    months*: int32

  DayTimeInterval* = object
    days*: int32
    millis*: int32

  MonthDayNanoInterval* = object
    months*: int32
    days*: int32
    nanos*: int64

  Time32Array* = object
    handle: ptr GArrowTime32Array
    unit*: GArrowTimeUnit

  Time32ArrayBuilder* = object
    handle: ptr GArrowTime32ArrayBuilder
    unit*: GArrowTimeUnit

  Time64Array* = object
    handle: ptr GArrowTime64Array
    unit*: GArrowTimeUnit

  Time64ArrayBuilder* = object
    handle: ptr GArrowTime64ArrayBuilder
    unit*: GArrowTimeUnit

  TimestampArray* = object
    handle: ptr GArrowTimestampArray
    unit*: GArrowTimeUnit
    tz*: string

  TimestampArrayBuilder* = object
    handle: ptr GArrowTimestampArrayBuilder
    unit*: GArrowTimeUnit
    tz*: string

  Date32Array* = object
    handle: ptr GArrowDate32Array

  Date32ArrayBuilder* = object
    handle: ptr GArrowDate32ArrayBuilder

proc toPtr*(ta: TimestampArray): ptr GArrowTimestampArray {.inline.} =
  ta.handle

proc toPtr*(tab: TimestampArrayBuilder): ptr GArrowTimestampArrayBuilder {.inline.} =
  tab.handle

proc toPtr*(d32a: Date32Array): ptr GArrowDate32Array {.inline.} =
  d32a.handle

proc toPtr*(d32ab: Date32ArrayBuilder): ptr GArrowDate32ArrayBuilder {.inline.} =
  d32ab.handle

proc toPtr*(t32a: Time32Array): ptr GArrowTime32Array {.inline.} =
  t32a.handle

proc toPtr*(t32ab: Time32ArrayBuilder): ptr GArrowTime32ArrayBuilder {.inline.} =
  t32ab.handle

proc toPtr*(t64a: Time64Array): ptr GArrowTime64Array {.inline.} =
  t64a.handle

proc toPtr*(t64ab: Time64ArrayBuilder): ptr GArrowTime64ArrayBuilder {.inline.} =
  t64ab.handle

# Helper to convert Time to DateTime
# Conversion implementations
proc toDateTime*(d: Date32): DateTime {.inline.} =
  ## Convert Date32 (days since epoch) to DateTime
  let baseTime = dateTime(1970, mJan, 1)
  baseTime + initDuration(days = d.value)

proc toDateTime*(d: Date64): DateTime {.inline.} =
  ## Convert Date64 (milliseconds since epoch) to DateTime
  let baseTime = dateTime(1970, mJan, 1)
  baseTime + initDuration(milliseconds = d.value)

proc toDateTime*(ts: Timestamp): DateTime {.inline.} =
  ## Convert Timestamp to DateTime
  let baseTime = dateTime(1970, mJan, 1)
  let nanos =
    case ts.unit
    of GArrowTimeUnit.GARROW_TIME_UNIT_SECOND:
      ts.value * 1_000_000_000
    of GArrowTimeUnit.GARROW_TIME_UNIT_MILLI:
      ts.value * 1_000_000
    of GArrowTimeUnit.GARROW_TIME_UNIT_MICRO:
      ts.value * 1_000
    of GArrowTimeUnit.GARROW_TIME_UNIT_NANO:
      ts.value
  let durMillis = nanos div 1_000_000
  baseTime + initDuration(milliseconds = durMillis)

# Date32 constructors
proc newDate32*(days: int32): Date32 =
  Date32(value: days)

proc newDate32*(dt: DateTime): Date32 =
  ## Convert DateTime to Date32 (days since epoch)
  let seconds = dt.toTime.toUnixFloat.int64
  Date32(value: int32(seconds div 86400))

proc toDays*(d: Date32): int32 =
  d.value

proc `$`*(d: Date32): string =
  $d.toDateTime()

# Date64 constructors
proc newDate64*(ms: int64): Date64 =
  Date64(value: ms)

proc newDate64*(dt: DateTime): Date64 =
  Date64(value: dt.toTime.toUnixFloat.int64 * 1000)

proc toMs*(d: Date64): int64 =
  d.value

proc `$`*(d: Date64): string =
  $d.toDateTime()

# Timestamp constructors
proc newTimestamp*(
    val: int64, unit: GArrowTimeUnit = GARROW_TIME_UNIT_NANO, tz: string = "UTC"
): Timestamp =
  Timestamp(value: val, unit: unit, tz: tz)

proc newTimestamp*(
    dt: DateTime, unit: GArrowTimeUnit = GARROW_TIME_UNIT_NANO, tz: string = "UTC"
): Timestamp =
  let unixNano = dt.toTime.toUnixFloat.int64 * 1_000_000_000
  let scaled =
    case unit
    of GARROW_TIME_UNIT_SECOND:
      unixNano div 1_000_000_000
    of GARROW_TIME_UNIT_MILLI:
      unixNano div 1_000_000
    of GARROW_TIME_UNIT_MICRO:
      unixNano div 1_000
    of GARROW_TIME_UNIT_NANO:
      unixNano
  Timestamp(value: scaled, unit: unit, tz: tz)

proc `$`*(ts: Timestamp): string =
  $ts.toDateTime() & " [" & ts.tz & "]"

# Duration - time intervals
proc newDuration*(val: int64, unit: GArrowTimeUnit = GARROW_TIME_UNIT_NANO): Duration =
  Duration(value: val, unit: unit)

proc toNanos*(d: Duration): int64 =
  case d.unit
  of GARROW_TIME_UNIT_SECOND:
    d.value * 1_000_000_000
  of GARROW_TIME_UNIT_MILLI:
    d.value * 1_000_000
  of GARROW_TIME_UNIT_MICRO:
    d.value * 1_000
  of GARROW_TIME_UNIT_NANO:
    d.value

proc `$`*(d: Duration): string =
  let nanos = d.toNanos()
  let secs = nanos div 1_000_000_000
  let ms = (nanos mod 1_000_000_000) div 1_000_000
  $secs & "." & $ms & "s"

# TimestampArray memory management
proc `=destroy`*(ta: TimestampArray) =
  if not isNil(ta.handle):
    g_object_unref(cast[ptr GObject](ta.handle))

proc `=sink`*(dest: var TimestampArray, src: TimestampArray) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(cast[ptr GObject](dest.handle))
  dest.handle = src.handle
  dest.unit = src.unit
  dest.tz = src.tz

proc `=copy`*(dest: var TimestampArray, src: TimestampArray) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(cast[ptr GObject](dest.handle))
    dest.handle = src.handle
    dest.unit = src.unit
    dest.tz = src.tz
    if not isNil(dest.handle):
      discard g_object_ref(cast[ptr GObject](dest.handle))

# TimestampArrayBuilder memory management
proc `=destroy`*(tab: TimestampArrayBuilder) =
  if not isNil(tab.handle):
    g_object_unref(cast[ptr GObject](tab.handle))

proc `=sink`*(dest: var TimestampArrayBuilder, src: TimestampArrayBuilder) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(cast[ptr GObject](dest.handle))
  dest.handle = src.handle
  dest.unit = src.unit
  dest.tz = src.tz

proc `=copy`*(dest: var TimestampArrayBuilder, src: TimestampArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(cast[ptr GObject](dest.handle))
    dest.handle = src.handle
    dest.unit = src.unit
    dest.tz = src.tz
    if not isNil(dest.handle):
      discard g_object_ref(cast[ptr GObject](dest.handle))

# Date32Array memory management
proc `=destroy`*(d32a: Date32Array) =
  if not isNil(d32a.handle):
    g_object_unref(cast[ptr GObject](d32a.handle))

proc `=sink`*(dest: var Date32Array, src: Date32Array) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(cast[ptr GObject](dest.handle))
  dest.handle = src.handle

proc `=copy`*(dest: var Date32Array, src: Date32Array) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(cast[ptr GObject](dest.handle))
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(cast[ptr GObject](dest.handle))

# Date32ArrayBuilder memory management
proc `=destroy`*(d32ab: Date32ArrayBuilder) =
  if not isNil(d32ab.handle):
    g_object_unref(cast[ptr GObject](d32ab.handle))

proc `=sink`*(dest: var Date32ArrayBuilder, src: Date32ArrayBuilder) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(cast[ptr GObject](dest.handle))
  dest.handle = src.handle

proc `=copy`*(dest: var Date32ArrayBuilder, src: Date32ArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(cast[ptr GObject](dest.handle))
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(cast[ptr GObject](dest.handle))

# Time32Array memory management
proc `=destroy`*(t32a: Time32Array) =
  if not isNil(t32a.handle):
    g_object_unref(cast[ptr GObject](t32a.handle))

proc `=sink`*(dest: var Time32Array, src: Time32Array) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(cast[ptr GObject](dest.handle))
  dest.handle = src.handle
  dest.unit = src.unit

proc `=copy`*(dest: var Time32Array, src: Time32Array) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(cast[ptr GObject](dest.handle))
    dest.handle = src.handle
    dest.unit = src.unit
    if not isNil(dest.handle):
      discard g_object_ref(cast[ptr GObject](dest.handle))

# Time32ArrayBuilder memory management
proc `=destroy`*(t32ab: Time32ArrayBuilder) =
  if not isNil(t32ab.handle):
    g_object_unref(cast[ptr GObject](t32ab.handle))

proc `=sink`*(dest: var Time32ArrayBuilder, src: Time32ArrayBuilder) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(cast[ptr GObject](dest.handle))
  dest.handle = src.handle
  dest.unit = src.unit

proc `=copy`*(dest: var Time32ArrayBuilder, src: Time32ArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(cast[ptr GObject](dest.handle))
    dest.handle = src.handle
    dest.unit = src.unit
    if not isNil(dest.handle):
      discard g_object_ref(cast[ptr GObject](dest.handle))

# Time64Array memory management
proc `=destroy`*(t64a: Time64Array) =
  if not isNil(t64a.handle):
    g_object_unref(cast[ptr GObject](t64a.handle))

proc `=sink`*(dest: var Time64Array, src: Time64Array) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(cast[ptr GObject](dest.handle))
  dest.handle = src.handle
  dest.unit = src.unit

proc `=copy`*(dest: var Time64Array, src: Time64Array) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(cast[ptr GObject](dest.handle))
    dest.handle = src.handle
    dest.unit = src.unit
    if not isNil(dest.handle):
      discard g_object_ref(cast[ptr GObject](dest.handle))

# Time64ArrayBuilder memory management
proc `=destroy`*(t64ab: Time64ArrayBuilder) =
  if not isNil(t64ab.handle):
    g_object_unref(cast[ptr GObject](t64ab.handle))

proc `=sink`*(dest: var Time64ArrayBuilder, src: Time64ArrayBuilder) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(cast[ptr GObject](dest.handle))
  dest.handle = src.handle
  dest.unit = src.unit

proc `=copy`*(dest: var Time64ArrayBuilder, src: Time64ArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(cast[ptr GObject](dest.handle))
    dest.handle = src.handle
    dest.unit = src.unit
    if not isNil(dest.handle):
      discard g_object_ref(cast[ptr GObject](dest.handle))

# TimestampArray creators
proc newTimestampArray*(
    handle: ptr GArrowTimestampArray, unit: GArrowTimeUnit, tz: string
): TimestampArray =
  TimestampArray(handle: handle, unit: unit, tz: tz)

proc newTimestampArrayBuilder*(
    unit: GArrowTimeUnit, tz: string = "UTC"
): TimestampArrayBuilder =
  # Create a GTimeZone from the timezone string
  let tzCstr = tz.cstring
  let gTz = g_time_zone_new(tzCstr)
  if gTz.isNil:
    raise newException(OperationError, "Failed to create timezone")

  # Create a timestamp data type
  let tsType = garrow_timestamp_data_type_new(unit, gTz)
  g_time_zone_unref(gTz) # Release timezone reference as data type owns it

  if tsType.isNil:
    raise newException(OperationError, "Failed to create timestamp data type")

  let handle = garrow_timestamp_array_builder_new(tsType)
  if handle.isNil:
    g_object_unref(tsType)
    raise newException(OperationError, "Failed to create TimestampArrayBuilder")

  g_object_unref(tsType) # release reference as builder owns it

  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)
  TimestampArrayBuilder(handle: handle, unit: unit, tz: tz)

# Date32Array creators
proc newDate32ArrayBuilder*(): Date32ArrayBuilder =
  let handle = garrow_date32_array_builder_new()
  if handle.isNil:
    raise newException(OperationError, "Failed to create Date32ArrayBuilder")
  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)
  Date32ArrayBuilder(handle: handle)

# TimestampArray operations
proc len*(ta: TimestampArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](ta.handle))

# Array operations for Time types
proc len*(t32a: Time32Array): int =
  garrow_array_get_length(cast[ptr GArrowArray](t32a.handle))

proc len*(t64a: Time64Array): int =
  garrow_array_get_length(cast[ptr GArrowArray](t64a.handle))

proc append*(tab: TimestampArrayBuilder, val: int64) =
  check garrow_timestamp_array_builder_append_value(tab.handle, val)

proc appendNull*(tab: TimestampArrayBuilder) =
  check garrow_timestamp_array_builder_append_null(tab.handle)

proc append*(tab: TimestampArrayBuilder, val: Timestamp) =
  if val.unit != tab.unit:
    raise newException(ValueError, "Timestamp unit mismatch")
  if val.tz != tab.tz:
    raise newException(ValueError, "Timestamp timezone mismatch")
  tab.append(val.value)

proc append*(tab: TimestampArrayBuilder, val: Option[Timestamp]) =
  if val.isSome:
    tab.append(val.get())
  else:
    tab.appendNull()

proc finish*(tab: TimestampArrayBuilder): TimestampArray =
  let handle =
    check garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](tab.handle))
  newTimestampArray(cast[ptr GArrowTimestampArray](handle), tab.unit, tab.tz)

# Date32Array operations
proc append*(d32ab: Date32ArrayBuilder, val: int32) =
  check garrow_date32_array_builder_append_value(d32ab.handle, val)

proc appendNull*(d32ab: Date32ArrayBuilder) =
  check garrow_date32_array_builder_append_null(d32ab.handle)

proc append*(d32ab: Date32ArrayBuilder, val: Date32) =
  d32ab.append(val.value)

proc append*(d32ab: Date32ArrayBuilder, val: Option[Date32]) =
  if val.isSome:
    d32ab.append(val.get())
  else:
    d32ab.appendNull()

proc finish*(d32ab: Date32ArrayBuilder): Date32Array =
  let handle =
    check garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](d32ab.handle))
  Date32Array(handle: cast[ptr GArrowDate32Array](handle))

# Array operations
proc `[]`*(ta: TimestampArray, idx: int): int64 =
  if idx < 0 or idx >= ta.len:
    raise newException(IndexDefect, "Index out of bounds")
  garrow_timestamp_array_get_value(ta.handle, idx.gint64)

proc `[]`*(t: Time64Array, idx: int): int64 =
  if idx < 0 or idx >= t.len:
    raise newException(IndexDefect, "Index out of bounds")
  garrow_time64_array_get_value(t.handle, idx.gint64)

proc `[]`*(t: Time32Array, idx: int): int32 =
  if idx < 0 or idx >= t.len:
    raise newException(IndexDefect, "Index out of bounds")
  garrow_time32_array_get_value(t.handle, idx.gint64)

proc `[]`*(d32a: Date32Array, idx: int): int32 =
  if idx < 0 or idx >= garrow_array_get_length(cast[ptr GArrowArray](d32a.handle)):
    raise newException(IndexDefect, "Index out of bounds")
  garrow_date32_array_get_value(d32a.handle, idx.gint64)

proc isNull*(ta: TimestampArray, idx: int): bool =
  if idx < 0 or idx >= ta.len:
    raise newException(IndexDefect, "Index out of bounds")
  garrow_array_is_null(cast[ptr GArrowArray](ta.handle), idx) != 0

proc `$`*(ta: TimestampArray): string =
  let cStr = check garrow_array_to_string(cast[ptr GArrowArray](ta.handle))
  $newGString(cStr)

proc `$`*(d32a: Date32Array): string =
  let cStr = check garrow_array_to_string(cast[ptr GArrowArray](d32a.handle))
  $newGString(cStr)

# Time32 constructors and operations
proc newTime32*(seconds: int32): Time32 =
  Time32(value: seconds)

proc newTime32FromSeconds*(secs: float): Time32 =
  Time32(value: int32(secs))

proc toSeconds*(t: Time32): float =
  float(t.value)

proc `$`*(t: Time32): string =
  let secs = t.value
  let hours = secs div 3600
  let mins = (secs mod 3600) div 60
  let s = secs mod 60
  $hours & ":" & $mins & ":" & $s

# Time64 constructors and operations
proc newTime64*(nanos: int64): Time64 =
  Time64(value: nanos)

proc newTime64FromMicros*(micros: float): Time64 =
  Time64(value: int64(micros * 1000))

proc toMicros*(t: Time64): float =
  float(t.value) / 1000.0

proc `$`*(t: Time64): string =
  let micros = t.value
  let millis = micros div 1000
  $millis & " ms"

# Time32Array builders and operations
proc newTime32ArrayBuilder*(
    unit: GArrowTimeUnit = GARROW_TIME_UNIT_SECOND
): Time32ArrayBuilder =
  var err: ptr GError
  let tsType = garrow_time32_data_type_new(unit, addr err)

  if not isNil(err):
    let msg =
      if not isNil(err.message):
        $err.message
      else:
        "Failed to create time32 data type"
    g_error_free(err)
    raise newException(OperationError, msg)

  if tsType.isNil:
    raise newException(OperationError, "Failed to create time32 data type")

  let handle = garrow_time32_array_builder_new(tsType)
  if handle.isNil:
    g_object_unref(tsType)
    raise newException(OperationError, "Failed to create Time32ArrayBuilder")

  g_object_unref(tsType)

  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)
  Time32ArrayBuilder(handle: handle, unit: unit)

proc append*(t32ab: Time32ArrayBuilder, val: int32) =
  check garrow_time32_array_builder_append_value(t32ab.handle, val)

proc appendNull*(t32ab: Time32ArrayBuilder) =
  check garrow_time32_array_builder_append_null(t32ab.handle)

proc append*(t32ab: Time32ArrayBuilder, val: Time32) =
  t32ab.append(val.value)

proc append*(t32ab: Time32ArrayBuilder, val: Option[Time32]) =
  if val.isSome:
    t32ab.append(val.get())
  else:
    t32ab.appendNull()

proc finish*(t32ab: Time32ArrayBuilder): Time32Array =
  let handle =
    check garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](t32ab.handle))
  Time32Array(handle: cast[ptr GArrowTime32Array](handle), unit: t32ab.unit)

# Time64Array builders and operations
proc newTime64ArrayBuilder*(
    unit: GArrowTimeUnit = GARROW_TIME_UNIT_MICRO
): Time64ArrayBuilder =
  var err: ptr GError
  let tsType = garrow_time64_data_type_new(unit, addr err)

  if not isNil(err):
    let msg =
      if not isNil(err.message):
        $err.message
      else:
        "Failed to create time64 data type"
    g_error_free(err)
    raise newException(OperationError, msg)

  if tsType.isNil:
    raise newException(OperationError, "Failed to create time64 data type")

  let handle = garrow_time64_array_builder_new(tsType)
  if handle.isNil:
    g_object_unref(tsType)
    raise newException(OperationError, "Failed to create Time64ArrayBuilder")

  g_object_unref(tsType)

  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)
  Time64ArrayBuilder(handle: handle, unit: unit)

proc append*(t64ab: Time64ArrayBuilder, val: int64) =
  check garrow_time64_array_builder_append_value(t64ab.handle, val)

proc appendNull*(t64ab: Time64ArrayBuilder) =
  check garrow_time64_array_builder_append_null(t64ab.handle)

proc append*(t64ab: Time64ArrayBuilder, val: Time64) =
  t64ab.append(val.value)

proc append*(t64ab: Time64ArrayBuilder, val: Option[Time64]) =
  if val.isSome:
    t64ab.append(val.get())
  else:
    t64ab.appendNull()

proc finish*(t64ab: Time64ArrayBuilder): Time64Array =
  let handle =
    check garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](t64ab.handle))
  Time64Array(handle: cast[ptr GArrowTime64Array](handle), unit: t64ab.unit)

# Duration operations (scalar only, array support via Time64 or Timestamp)
proc toDuration*(dur: Duration): string =
  ## Format duration as a human-readable string
  let nanos = dur.toNanos()
  let seconds = nanos div 1_000_000_000
  let remaining = nanos mod 1_000_000_000
  if remaining == 0:
    return $seconds & "s"
  let millis = remaining div 1_000_000
  if millis == 0:
    return $seconds & "s"
  $seconds & "." & $millis & "s"

proc `$`*(t64a: Time64Array): string =
  let cStr = check garrow_array_to_string(cast[ptr GArrowArray](t64a.handle))
  $newGString(cStr)

# Interval type constructors and operations
proc newMonthInterval*(months: int32): MonthInterval =
  MonthInterval(months: months)

proc newDayTimeInterval*(days: int32, millis: int32): DayTimeInterval =
  DayTimeInterval(days: days, millis: millis)

proc newMonthDayNanoInterval*(
    months: int32, days: int32, nanos: int64
): MonthDayNanoInterval =
  MonthDayNanoInterval(months: months, days: days, nanos: nanos)

proc `$`*(mi: MonthInterval): string =
  $mi.months & " months"

proc `$`*(dti: DayTimeInterval): string =
  $dti.days & " days " & $dti.millis & " ms"

proc `$`*(mdni: MonthDayNanoInterval): string =
  $mdni.months & " months " & $mdni.days & " days " & $mdni.nanos & " ns"
