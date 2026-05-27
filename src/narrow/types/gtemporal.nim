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

  DurationArray* = object
    handle: ptr GArrowDurationArray
    unit*: GArrowTimeUnit

  DurationArrayBuilder* = object
    handle: ptr GArrowDurationArrayBuilder
    unit*: GArrowTimeUnit

  MonthIntervalArray* = object
    handle: ptr GArrowMonthIntervalArray

  MonthIntervalArrayBuilder* = object
    handle: ptr GArrowMonthIntervalArrayBuilder

  DayMillisecondObj* = object
    handle: ptr GArrowDayMillisecond

  MonthDayNanoObj* = object
    handle: ptr GArrowMonthDayNano

  DayTimeIntervalArray* = object
    handle: ptr GArrowDayTimeIntervalArray

  DayTimeIntervalArrayBuilder* = object
    handle: ptr GArrowDayTimeIntervalArrayBuilder

  MonthDayNanoIntervalArray* = object
    handle: ptr GArrowMonthDayNanoIntervalArray

  MonthDayNanoIntervalArrayBuilder* = object
    handle: ptr GArrowMonthDayNanoIntervalArrayBuilder

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

func toPtr*(ta: TimestampArray): ptr GArrowTimestampArray {.inline.} =
  ta.handle

func toPtr*(tab: TimestampArrayBuilder): ptr GArrowTimestampArrayBuilder {.inline.} =
  tab.handle

func toPtr*(d32a: Date32Array): ptr GArrowDate32Array {.inline.} =
  d32a.handle

func toPtr*(d32ab: Date32ArrayBuilder): ptr GArrowDate32ArrayBuilder {.inline.} =
  d32ab.handle

func toPtr*(t32a: Time32Array): ptr GArrowTime32Array {.inline.} =
  t32a.handle

func toPtr*(t32ab: Time32ArrayBuilder): ptr GArrowTime32ArrayBuilder {.inline.} =
  t32ab.handle

func toPtr*(t64a: Time64Array): ptr GArrowTime64Array {.inline.} =
  t64a.handle

func toPtr*(t64ab: Time64ArrayBuilder): ptr GArrowTime64ArrayBuilder {.inline.} =
  t64ab.handle

func toPtr*(da: DurationArray): ptr GArrowDurationArray {.inline.} =
  da.handle

func toPtr*(dab: DurationArrayBuilder): ptr GArrowDurationArrayBuilder {.inline.} =
  dab.handle

func toPtr*(mia: MonthIntervalArray): ptr GArrowMonthIntervalArray {.inline.} =
  mia.handle

func toPtr*(miab: MonthIntervalArrayBuilder): ptr GArrowMonthIntervalArrayBuilder {.inline.} =
  miab.handle

func toPtr*(dmo: DayMillisecondObj): ptr GArrowDayMillisecond {.inline.} =
  dmo.handle

func toPtr*(mdno: MonthDayNanoObj): ptr GArrowMonthDayNano {.inline.} =
  mdno.handle

func toPtr*(dtia: DayTimeIntervalArray): ptr GArrowDayTimeIntervalArray {.inline.} =
  dtia.handle

func toPtr*(dtiab: DayTimeIntervalArrayBuilder): ptr GArrowDayTimeIntervalArrayBuilder {.inline.} =
  dtiab.handle

func toPtr*(mdnia: MonthDayNanoIntervalArray): ptr GArrowMonthDayNanoIntervalArray {.inline.} =
  mdnia.handle

func toPtr*(mdniab: MonthDayNanoIntervalArrayBuilder): ptr GArrowMonthDayNanoIntervalArrayBuilder {.inline.} =
  mdniab.handle

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
func newDate32*(days: int32): Date32 {.inline.} =
  Date32(value: days)

proc newDate32*(dt: DateTime): Date32 =
  ## Convert DateTime to Date32 (days since epoch)
  let seconds = dt.toTime.toUnixFloat.int64
  Date32(value: int32(seconds div 86400))

func toDays*(d: Date32): int32 {.inline.} =
  d.value

proc `$`*(d: Date32): string {.inline.} =
  $d.toDateTime()

# Date64 constructors
func newDate64*(ms: int64): Date64 {.inline.} =
  Date64(value: ms)

proc newDate64*(dt: DateTime): Date64 =
  Date64(value: dt.toTime.toUnixFloat.int64 * 1000)

func toMs*(d: Date64): int64 {.inline.} =
  d.value

proc `$`*(d: Date64): string {.inline.} =
  $d.toDateTime()

# Timestamp constructors
proc newTimestamp*(
    val: int64, unit: GArrowTimeUnit = GARROW_TIME_UNIT_NANO, tz: sink string = "UTC"
): Timestamp =
  Timestamp(value: val, unit: unit, tz: tz)

proc newTimestamp*(
    dt: DateTime, unit: GArrowTimeUnit = GARROW_TIME_UNIT_NANO, tz: sink string = "UTC"
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
  result = $ts.toDateTime()
  result.add(" [")
  result.add(ts.tz)
  result.add("]")

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
  result = $secs
  result.add(".")
  result.add($ms)
  result.add("s")

# TimestampArray memory management
proc `=destroy`*(ta: TimestampArray) =
  if not isNil(ta.handle):
    g_object_unref(ta.handle)

proc `=wasMoved`*(ta: var TimestampArray) =
  ta.handle = nil

proc `=dup`*(ta: TimestampArray): TimestampArray =
  result.handle = ta.handle
  result.unit = ta.unit
  result.tz = ta.tz
  if not isNil(ta.handle):
    discard g_object_ref(ta.handle)

proc `=copy`*(dest: var TimestampArray, src: TimestampArray) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.unit = src.unit
    dest.tz = src.tz
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# TimestampArrayBuilder memory management
proc `=destroy`*(tab: TimestampArrayBuilder) =
  if not isNil(tab.handle):
    g_object_unref(tab.handle)

proc `=wasMoved`*(tab: var TimestampArrayBuilder) =
  tab.handle = nil

proc `=dup`*(tab: TimestampArrayBuilder): TimestampArrayBuilder =
  result.handle = tab.handle
  result.unit = tab.unit
  result.tz = tab.tz
  if not isNil(tab.handle):
    discard g_object_ref(tab.handle)

proc `=copy`*(dest: var TimestampArrayBuilder, src: TimestampArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.unit = src.unit
    dest.tz = src.tz
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# Date32Array memory management
proc `=destroy`*(d32a: Date32Array) =
  if not isNil(d32a.handle):
    g_object_unref(d32a.handle)

proc `=wasMoved`*(d32a: var Date32Array) =
  d32a.handle = nil

proc `=dup`*(d32a: Date32Array): Date32Array =
  result.handle = d32a.handle
  if not isNil(d32a.handle):
    discard g_object_ref(d32a.handle)

proc `=copy`*(dest: var Date32Array, src: Date32Array) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# Date32ArrayBuilder memory management
proc `=destroy`*(d32ab: Date32ArrayBuilder) =
  if not isNil(d32ab.handle):
    g_object_unref(d32ab.handle)

proc `=wasMoved`*(d32ab: var Date32ArrayBuilder) =
  d32ab.handle = nil

proc `=dup`*(d32ab: Date32ArrayBuilder): Date32ArrayBuilder =
  result.handle = d32ab.handle
  if not isNil(d32ab.handle):
    discard g_object_ref(d32ab.handle)

proc `=copy`*(dest: var Date32ArrayBuilder, src: Date32ArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# Time32Array memory management
proc `=destroy`*(t32a: Time32Array) =
  if not isNil(t32a.handle):
    g_object_unref(t32a.handle)

proc `=wasMoved`*(t32a: var Time32Array) =
  t32a.handle = nil

proc `=dup`*(t32a: Time32Array): Time32Array =
  result.handle = t32a.handle
  result.unit = t32a.unit
  if not isNil(t32a.handle):
    discard g_object_ref(t32a.handle)

proc `=copy`*(dest: var Time32Array, src: Time32Array) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.unit = src.unit
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# Time32ArrayBuilder memory management
proc `=destroy`*(t32ab: Time32ArrayBuilder) =
  if not isNil(t32ab.handle):
    g_object_unref(t32ab.handle)

proc `=wasMoved`*(t32ab: var Time32ArrayBuilder) =
  t32ab.handle = nil

proc `=dup`*(t32ab: Time32ArrayBuilder): Time32ArrayBuilder =
  result.handle = t32ab.handle
  result.unit = t32ab.unit
  if not isNil(t32ab.handle):
    discard g_object_ref(t32ab.handle)

proc `=copy`*(dest: var Time32ArrayBuilder, src: Time32ArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.unit = src.unit
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# Time64Array memory management
proc `=destroy`*(t64a: Time64Array) =
  if not isNil(t64a.handle):
    g_object_unref(t64a.handle)

proc `=wasMoved`*(t64a: var Time64Array) =
  t64a.handle = nil

proc `=dup`*(t64a: Time64Array): Time64Array =
  result.handle = t64a.handle
  result.unit = t64a.unit
  if not isNil(t64a.handle):
    discard g_object_ref(t64a.handle)

proc `=copy`*(dest: var Time64Array, src: Time64Array) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.unit = src.unit
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# Time64ArrayBuilder memory management
proc `=destroy`*(t64ab: Time64ArrayBuilder) =
  if not isNil(t64ab.handle):
    g_object_unref(t64ab.handle)

proc `=wasMoved`*(t64ab: var Time64ArrayBuilder) =
  t64ab.handle = nil

proc `=dup`*(t64ab: Time64ArrayBuilder): Time64ArrayBuilder =
  result.handle = t64ab.handle
  result.unit = t64ab.unit
  if not isNil(t64ab.handle):
    discard g_object_ref(t64ab.handle)

proc `=copy`*(dest: var Time64ArrayBuilder, src: Time64ArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.unit = src.unit
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# DurationArray memory management
proc `=destroy`*(da: DurationArray) =
  if not isNil(da.handle):
    g_object_unref(da.handle)

proc `=wasMoved`*(da: var DurationArray) =
  da.handle = nil

proc `=dup`*(da: DurationArray): DurationArray =
  result.handle = da.handle
  result.unit = da.unit
  if not isNil(da.handle):
    discard g_object_ref(da.handle)

proc `=copy`*(dest: var DurationArray, src: DurationArray) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.unit = src.unit
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# DurationArrayBuilder memory management
proc `=destroy`*(dab: DurationArrayBuilder) =
  if not isNil(dab.handle):
    g_object_unref(dab.handle)

proc `=wasMoved`*(dab: var DurationArrayBuilder) =
  dab.handle = nil

proc `=dup`*(dab: DurationArrayBuilder): DurationArrayBuilder =
  result.handle = dab.handle
  result.unit = dab.unit
  if not isNil(dab.handle):
    discard g_object_ref(dab.handle)

proc `=copy`*(dest: var DurationArrayBuilder, src: DurationArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.unit = src.unit
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# MonthIntervalArray memory management
proc `=destroy`*(mia: MonthIntervalArray) =
  if not isNil(mia.handle):
    g_object_unref(mia.handle)

proc `=wasMoved`*(mia: var MonthIntervalArray) =
  mia.handle = nil

proc `=dup`*(mia: MonthIntervalArray): MonthIntervalArray =
  result.handle = mia.handle
  if not isNil(mia.handle):
    discard g_object_ref(mia.handle)

proc `=copy`*(dest: var MonthIntervalArray, src: MonthIntervalArray) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# MonthIntervalArrayBuilder memory management
proc `=destroy`*(miab: MonthIntervalArrayBuilder) =
  if not isNil(miab.handle):
    g_object_unref(miab.handle)

proc `=wasMoved`*(miab: var MonthIntervalArrayBuilder) =
  miab.handle = nil

proc `=dup`*(miab: MonthIntervalArrayBuilder): MonthIntervalArrayBuilder =
  result.handle = miab.handle
  if not isNil(miab.handle):
    discard g_object_ref(miab.handle)

proc `=copy`*(dest: var MonthIntervalArrayBuilder, src: MonthIntervalArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# DayMillisecondObj memory management
proc `=destroy`*(dmo: DayMillisecondObj) =
  if not isNil(dmo.handle):
    g_object_unref(dmo.handle)

proc `=wasMoved`*(dmo: var DayMillisecondObj) =
  dmo.handle = nil

proc `=dup`*(dmo: DayMillisecondObj): DayMillisecondObj =
  result.handle = dmo.handle
  if not isNil(dmo.handle):
    discard g_object_ref(dmo.handle)

proc `=copy`*(dest: var DayMillisecondObj, src: DayMillisecondObj) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# MonthDayNanoObj memory management
proc `=destroy`*(mdno: MonthDayNanoObj) =
  if not isNil(mdno.handle):
    g_object_unref(mdno.handle)

proc `=wasMoved`*(mdno: var MonthDayNanoObj) =
  mdno.handle = nil

proc `=dup`*(mdno: MonthDayNanoObj): MonthDayNanoObj =
  result.handle = mdno.handle
  if not isNil(mdno.handle):
    discard g_object_ref(mdno.handle)

proc `=copy`*(dest: var MonthDayNanoObj, src: MonthDayNanoObj) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# DayTimeIntervalArray memory management
proc `=destroy`*(dtia: DayTimeIntervalArray) =
  if not isNil(dtia.handle):
    g_object_unref(dtia.handle)

proc `=wasMoved`*(dtia: var DayTimeIntervalArray) =
  dtia.handle = nil

proc `=dup`*(dtia: DayTimeIntervalArray): DayTimeIntervalArray =
  result.handle = dtia.handle
  if not isNil(dtia.handle):
    discard g_object_ref(dtia.handle)

proc `=copy`*(dest: var DayTimeIntervalArray, src: DayTimeIntervalArray) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# DayTimeIntervalArrayBuilder memory management
proc `=destroy`*(dtiab: DayTimeIntervalArrayBuilder) =
  if not isNil(dtiab.handle):
    g_object_unref(dtiab.handle)

proc `=wasMoved`*(dtiab: var DayTimeIntervalArrayBuilder) =
  dtiab.handle = nil

proc `=dup`*(dtiab: DayTimeIntervalArrayBuilder): DayTimeIntervalArrayBuilder =
  result.handle = dtiab.handle
  if not isNil(dtiab.handle):
    discard g_object_ref(dtiab.handle)

proc `=copy`*(dest: var DayTimeIntervalArrayBuilder, src: DayTimeIntervalArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# MonthDayNanoIntervalArray memory management
proc `=destroy`*(mdnia: MonthDayNanoIntervalArray) =
  if not isNil(mdnia.handle):
    g_object_unref(mdnia.handle)

proc `=wasMoved`*(mdnia: var MonthDayNanoIntervalArray) =
  mdnia.handle = nil

proc `=dup`*(mdnia: MonthDayNanoIntervalArray): MonthDayNanoIntervalArray =
  result.handle = mdnia.handle
  if not isNil(mdnia.handle):
    discard g_object_ref(mdnia.handle)

proc `=copy`*(dest: var MonthDayNanoIntervalArray, src: MonthDayNanoIntervalArray) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

# MonthDayNanoIntervalArrayBuilder memory management
proc `=destroy`*(mdniab: MonthDayNanoIntervalArrayBuilder) =
  if not isNil(mdniab.handle):
    g_object_unref(mdniab.handle)

proc `=wasMoved`*(mdniab: var MonthDayNanoIntervalArrayBuilder) =
  mdniab.handle = nil

proc `=dup`*(mdniab: MonthDayNanoIntervalArrayBuilder): MonthDayNanoIntervalArrayBuilder =
  result.handle = mdniab.handle
  if not isNil(mdniab.handle):
    discard g_object_ref(mdniab.handle)

proc `=copy`*(dest: var MonthDayNanoIntervalArrayBuilder, src: MonthDayNanoIntervalArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

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

  TimestampArrayBuilder(handle: handle, unit: unit, tz: tz)

# Date32Array creators
proc newDate32ArrayBuilder*(): Date32ArrayBuilder =
  let handle = garrow_date32_array_builder_new()
  if handle.isNil:
    raise newException(OperationError, "Failed to create Date32ArrayBuilder")
  Date32ArrayBuilder(handle: handle)

# DurationArrayBuilder creators
proc newDurationArrayBuilder*(
    unit: GArrowTimeUnit = GARROW_TIME_UNIT_NANO
): DurationArrayBuilder =
  let dtype = garrow_duration_data_type_new(unit)
  if dtype.isNil:
    raise newException(OperationError, "Failed to create duration data type")
  let handle = garrow_duration_array_builder_new(dtype)
  g_object_unref(dtype)
  if handle.isNil:
    raise newException(OperationError, "Failed to create DurationArrayBuilder")
  DurationArrayBuilder(handle: handle, unit: unit)

# MonthIntervalArrayBuilder creator
proc newMonthIntervalArrayBuilder*(): MonthIntervalArrayBuilder =
  let handle = garrow_month_interval_array_builder_new()
  if handle.isNil:
    raise newException(OperationError, "Failed to create MonthIntervalArrayBuilder")
  MonthIntervalArrayBuilder(handle: handle)

# DayTimeIntervalArrayBuilder creator
proc newDayTimeIntervalArrayBuilder*(): DayTimeIntervalArrayBuilder =
  let handle = garrow_day_time_interval_array_builder_new()
  if handle.isNil:
    raise newException(OperationError,
      "Failed to create DayTimeIntervalArrayBuilder")
  DayTimeIntervalArrayBuilder(handle: handle)

# MonthDayNanoIntervalArrayBuilder creator
proc newMonthDayNanoIntervalArrayBuilder*(): MonthDayNanoIntervalArrayBuilder =
  let handle = garrow_month_day_nano_interval_array_builder_new()
  if handle.isNil:
    raise newException(OperationError,
      "Failed to create MonthDayNanoIntervalArrayBuilder")
  MonthDayNanoIntervalArrayBuilder(handle: handle)

# TimestampArray operations
proc len*(ta: TimestampArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](ta.handle))

# Array operations for Time types
proc len*(t32a: Time32Array): int =
  garrow_array_get_length(cast[ptr GArrowArray](t32a.handle))

proc len*(t64a: Time64Array): int =
  garrow_array_get_length(cast[ptr GArrowArray](t64a.handle))

proc append*(tab: TimestampArrayBuilder, val: int64) =
  verify garrow_timestamp_array_builder_append_value(tab.handle, val)

proc appendNull*(tab: TimestampArrayBuilder) =
  verify garrow_timestamp_array_builder_append_null(tab.handle)

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
    verify garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](tab.handle))
  newTimestampArray(cast[ptr GArrowTimestampArray](handle), tab.unit, tab.tz)

# Date32Array operations
proc append*(d32ab: Date32ArrayBuilder, val: int32) =
  verify garrow_date32_array_builder_append_value(d32ab.handle, val)

proc appendNull*(d32ab: Date32ArrayBuilder) =
  verify garrow_date32_array_builder_append_null(d32ab.handle)

proc append*(d32ab: Date32ArrayBuilder, val: Date32) =
  d32ab.append(val.value)

proc append*(d32ab: Date32ArrayBuilder, val: Option[Date32]) =
  if val.isSome:
    d32ab.append(val.get())
  else:
    d32ab.appendNull()

proc finish*(d32ab: Date32ArrayBuilder): Date32Array =
  let handle =
    verify garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](d32ab.handle))
  Date32Array(handle: cast[ptr GArrowDate32Array](handle))

# DurationArrayBuilder operations
proc append*(dab: DurationArrayBuilder, val: int64) =
  verify garrow_duration_array_builder_append_value(dab.handle, val)

proc appendNull*(dab: DurationArrayBuilder) =
  verify garrow_array_builder_append_null(cast[ptr GArrowArrayBuilder](dab.handle))

proc append*(dab: DurationArrayBuilder, val: Duration) =
  if val.unit != dab.unit:
    raise newException(ValueError, "Duration unit mismatch")
  dab.append(val.value)

proc append*(dab: DurationArrayBuilder, val: Option[Duration]) =
  if val.isSome:
    dab.append(val.get())
  else:
    dab.appendNull()

proc appendValues*(dab: DurationArrayBuilder, values: openArray[int64]) =
  if values.len == 0:
    return
  verify garrow_duration_array_builder_append_values(
    dab.handle, cast[ptr gint64](values[0].unsafeAddr), values.len.gint64,
    nil, 0
  )

proc finish*(dab: DurationArrayBuilder): DurationArray =
  let handle =
    verify garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](dab.handle))
  DurationArray(handle: cast[ptr GArrowDurationArray](handle), unit: dab.unit)

# MonthIntervalArrayBuilder operations
proc append*(miab: MonthIntervalArrayBuilder, val: int32) =
  verify garrow_month_interval_array_builder_append_value(miab.handle, val)

proc appendNull*(miab: MonthIntervalArrayBuilder) =
  verify garrow_array_builder_append_null(cast[ptr GArrowArrayBuilder](miab.handle))

proc append*(miab: MonthIntervalArrayBuilder, val: MonthInterval) =
  miab.append(val.months)

proc append*(miab: MonthIntervalArrayBuilder, val: Option[MonthInterval]) =
  if val.isSome:
    miab.append(val.get())
  else:
    miab.appendNull()

proc appendValues*(miab: MonthIntervalArrayBuilder, values: openArray[int32]) =
  if values.len == 0:
    return
  verify garrow_month_interval_array_builder_append_values(
    miab.handle, cast[ptr gint32](values[0].unsafeAddr), values.len.gint64,
    nil, 0
  )

proc finish*(miab: MonthIntervalArrayBuilder): MonthIntervalArray =
  let handle =
    verify garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](miab.handle))
  MonthIntervalArray(handle: cast[ptr GArrowMonthIntervalArray](handle))

# DayTimeIntervalArrayBuilder operations
proc append*(dtiab: DayTimeIntervalArrayBuilder, val: DayTimeInterval) =
  let dm = garrow_day_millisecond_new(val.days, val.millis)
  if dm.isNil:
    raise newException(OperationError, "Failed to create DayMillisecond")
  verify garrow_day_time_interval_array_builder_append_value(dtiab.handle, dm)
  g_object_unref(dm)

proc appendNull*(dtiab: DayTimeIntervalArrayBuilder) =
  verify garrow_array_builder_append_null(
    cast[ptr GArrowArrayBuilder](dtiab.handle))

proc append*(dtiab: DayTimeIntervalArrayBuilder, val: Option[DayTimeInterval]) =
  if val.isSome:
    dtiab.append(val.get())
  else:
    dtiab.appendNull()

proc finish*(dtiab: DayTimeIntervalArrayBuilder): DayTimeIntervalArray =
  let handle = verify garrow_array_builder_finish(
    cast[ptr GArrowArrayBuilder](dtiab.handle))
  DayTimeIntervalArray(handle: cast[ptr GArrowDayTimeIntervalArray](handle))

# MonthDayNanoIntervalArrayBuilder operations
proc append*(mdniab: MonthDayNanoIntervalArrayBuilder, val: MonthDayNanoInterval) =
  let mdn = garrow_month_day_nano_new(val.months, val.days, val.nanos)
  if mdn.isNil:
    raise newException(OperationError,
      "Failed to create MonthDayNano")
  verify garrow_month_day_nano_interval_array_builder_append_value(
    mdniab.handle, mdn)
  g_object_unref(mdn)

proc appendNull*(mdniab: MonthDayNanoIntervalArrayBuilder) =
  verify garrow_array_builder_append_null(
    cast[ptr GArrowArrayBuilder](mdniab.handle))

proc append*(mdniab: MonthDayNanoIntervalArrayBuilder,
             val: Option[MonthDayNanoInterval]) =
  if val.isSome:
    mdniab.append(val.get())
  else:
    mdniab.appendNull()

proc finish*(mdniab: MonthDayNanoIntervalArrayBuilder): MonthDayNanoIntervalArray =
  let handle = verify garrow_array_builder_finish(
    cast[ptr GArrowArrayBuilder](mdniab.handle))
  MonthDayNanoIntervalArray(
    handle: cast[ptr GArrowMonthDayNanoIntervalArray](handle))

# DayTimeIntervalArray operations
proc len*(dtia: DayTimeIntervalArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](dtia.handle))

proc `[]`*(dtia: DayTimeIntervalArray, idx: int): DayTimeInterval =
  if idx < 0 or idx >= dtia.len:
    raise newException(IndexDefect, "Index out of bounds")
  let dm = garrow_day_time_interval_array_get_value(dtia.handle, idx.gint64)
  var days: gint32
  var millis: gint32
  g_object_get(cast[gpointer](dm), "day".cstring, addr days,
               "millisecond".cstring, addr millis, nil)
  g_object_unref(dm)
  DayTimeInterval(days: days, millis: millis)

proc isNull*(dtia: DayTimeIntervalArray, idx: int): bool =
  if idx < 0 or idx >= dtia.len:
    raise newException(IndexDefect, "Index out of bounds")
  garrow_array_is_null(cast[ptr GArrowArray](dtia.handle), idx) != 0

proc `$`*(dtia: DayTimeIntervalArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](dtia.handle))
  result = $newGString(cStr, owned = true)

# MonthDayNanoIntervalArray operations
proc len*(mdnia: MonthDayNanoIntervalArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](mdnia.handle))

proc `[]`*(mdnia: MonthDayNanoIntervalArray, idx: int): MonthDayNanoInterval =
  if idx < 0 or idx >= mdnia.len:
    raise newException(IndexDefect, "Index out of bounds")
  let mdn = garrow_month_day_nano_interval_array_get_value(
    mdnia.handle, idx.gint64)
  var months: gint32
  var days: gint32
  var nanos: int64
  g_object_get(cast[gpointer](mdn), "month".cstring, addr months,
               "day".cstring, addr days, "nanosecond".cstring, addr nanos, nil)
  g_object_unref(mdn)
  MonthDayNanoInterval(months: months, days: days, nanos: nanos)

proc isNull*(mdnia: MonthDayNanoIntervalArray, idx: int): bool =
  if idx < 0 or idx >= mdnia.len:
    raise newException(IndexDefect, "Index out of bounds")
  garrow_array_is_null(cast[ptr GArrowArray](mdnia.handle), idx) != 0

proc `$`*(mdnia: MonthDayNanoIntervalArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](mdnia.handle))
  result = $newGString(cStr, owned = true)

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
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](ta.handle))
  result = $newGString(cStr, owned = true)

proc `$`*(d32a: Date32Array): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](d32a.handle))
  result = $newGString(cStr, owned = true)

# DurationArray operations
proc len*(da: DurationArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](da.handle))

proc `[]`*(da: DurationArray, idx: int): int64 =
  if idx < 0 or idx >= da.len:
    raise newException(IndexDefect, "Index out of bounds")
  garrow_duration_array_get_value(da.handle, idx.gint64)

proc isNull*(da: DurationArray, idx: int): bool =
  if idx < 0 or idx >= da.len:
    raise newException(IndexDefect, "Index out of bounds")
  garrow_array_is_null(cast[ptr GArrowArray](da.handle), idx) != 0

proc `$`*(da: DurationArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](da.handle))
  result = $newGString(cStr, owned = true)

# MonthIntervalArray operations
proc len*(mia: MonthIntervalArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](mia.handle))

proc `[]`*(mia: MonthIntervalArray, idx: int): int32 =
  if idx < 0 or idx >= mia.len:
    raise newException(IndexDefect, "Index out of bounds")
  garrow_month_interval_array_get_value(mia.handle, idx.gint64)

proc isNull*(mia: MonthIntervalArray, idx: int): bool =
  if idx < 0 or idx >= mia.len:
    raise newException(IndexDefect, "Index out of bounds")
  garrow_array_is_null(cast[ptr GArrowArray](mia.handle), idx) != 0

proc `$`*(mia: MonthIntervalArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](mia.handle))
  result = $newGString(cStr, owned = true)

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
  result = $hours
  result.add(":")
  result.add($mins)
  result.add(":")
  result.add($s)

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
  result = $millis
  result.add(" ms")

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

  Time32ArrayBuilder(handle: handle, unit: unit)

proc append*(t32ab: Time32ArrayBuilder, val: int32) =
  verify garrow_time32_array_builder_append_value(t32ab.handle, val)

proc appendNull*(t32ab: Time32ArrayBuilder) =
  verify garrow_time32_array_builder_append_null(t32ab.handle)

proc append*(t32ab: Time32ArrayBuilder, val: Time32) =
  t32ab.append(val.value)

proc append*(t32ab: Time32ArrayBuilder, val: Option[Time32]) =
  if val.isSome:
    t32ab.append(val.get())
  else:
    t32ab.appendNull()

proc finish*(t32ab: Time32ArrayBuilder): Time32Array =
  let handle =
    verify garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](t32ab.handle))
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

  Time64ArrayBuilder(handle: handle, unit: unit)

proc append*(t64ab: Time64ArrayBuilder, val: int64) =
  verify garrow_time64_array_builder_append_value(t64ab.handle, val)

proc appendNull*(t64ab: Time64ArrayBuilder) =
  verify garrow_time64_array_builder_append_null(t64ab.handle)

proc append*(t64ab: Time64ArrayBuilder, val: Time64) =
  t64ab.append(val.value)

proc append*(t64ab: Time64ArrayBuilder, val: Option[Time64]) =
  if val.isSome:
    t64ab.append(val.get())
  else:
    t64ab.appendNull()

proc finish*(t64ab: Time64ArrayBuilder): Time64Array =
  let handle =
    verify garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](t64ab.handle))
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
  result = $seconds
  result.add(".")
  result.add($millis)
  result.add("s")

proc `$`*(t64a: Time64Array): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](t64a.handle))
  result = $newGString(cStr, owned = true)

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
  result = $mi.months
  result.add(" months")

proc `$`*(dti: DayTimeInterval): string =
  result = $dti.days
  result.add(" days ")
  result.add($dti.millis)
  result.add(" ms")

proc `$`*(mdni: MonthDayNanoInterval): string =
  result = $mdni.months
  result.add(" months ")
  result.add($mdni.days)
  result.add(" days ")
  result.add($mdni.nanos)
  result.add(" ns")
