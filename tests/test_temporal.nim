import unittest2
import std/[times, options, strutils]
import ../src/[ffi, gtemporal]

suite "Date32 - Creation and Conversion":

  test "Create Date32 from days":
    let d = newDate32(18628'i32)  # ~51 years from epoch
    check d.toDays() == 18628'i32

  test "Create Date32 from DateTime":
    let dt = dateTime(2021, mJan, 1)
    let d = newDate32(dt)
    let dt2 = d.toDateTime()
    # Check year is approximately correct (may have timezone differences)
    check dt2.year in [2020, 2021]

  test "Date32 string representation":
    let d = newDate32(0'i32)
    let str = $d
    check str.contains("1970")

suite "Date64 - Creation and Conversion":

  test "Create Date64 from milliseconds":
    let d = newDate64(1609459200000'i64)  # 2021-01-01
    check d.toMs() == 1609459200000'i64

  test "Create Date64 from DateTime":
    let dt = dateTime(2021, mJan, 1)
    let d = newDate64(dt)
    let dt2 = d.toDateTime()
    check dt2.year in [2020, 2021]

  test "Date64 string representation":
    let d = newDate64(0'i64)
    let str = $d
    check str.contains("1970")

suite "Timestamp - Creation and Timezone":

  test "Create Timestamp from value":
    let ts = newTimestamp(1000'i64, GARROW_TIME_UNIT_MILLI, "UTC")
    check ts.unit == GARROW_TIME_UNIT_MILLI
    check ts.tz == "UTC"

  test "Create Timestamp from DateTime":
    let dt = dateTime(2021, mJan, 1)
    let ts = newTimestamp(dt, GARROW_TIME_UNIT_NANO, "UTC")
    let dt2 = ts.toDateTime()
    check dt2.year in [2020, 2021]

  test "Timestamp with different time units":
    let ts1 = newTimestamp(1000'i64, GARROW_TIME_UNIT_SECOND, "UTC")
    let ts2 = newTimestamp(1000000'i64, GARROW_TIME_UNIT_MILLI, "UTC")
    let ts3 = newTimestamp(1000000000'i64, GARROW_TIME_UNIT_MICRO, "UTC")
    check ts1.unit == GARROW_TIME_UNIT_SECOND
    check ts2.unit == GARROW_TIME_UNIT_MILLI
    check ts3.unit == GARROW_TIME_UNIT_MICRO

  test "Timestamp with different timezones":
    let ts1 = newTimestamp(1000'i64, GARROW_TIME_UNIT_NANO, "UTC")
    let ts2 = newTimestamp(1000'i64, GARROW_TIME_UNIT_NANO, "EST")
    check ts1.tz == "UTC"
    check ts2.tz == "EST"

  test "Timestamp string representation":
    let ts = newTimestamp(0'i64, GARROW_TIME_UNIT_NANO, "UTC")
    let str = $ts
    check str.contains("UTC")

suite "Duration - Creation and Operations":

  test "Create Duration from value":
    let d = newDuration(5000'i64, GARROW_TIME_UNIT_MILLI)
    check d.value == 5000'i64
    check d.unit == GARROW_TIME_UNIT_MILLI

  test "Duration toNanos conversion":
    let d1 = newDuration(1'i64, GARROW_TIME_UNIT_SECOND)
    let d2 = newDuration(1000'i64, GARROW_TIME_UNIT_MILLI)
    let d3 = newDuration(1000000'i64, GARROW_TIME_UNIT_MICRO)
    check d1.toNanos() == d2.toNanos()
    check d2.toNanos() == d3.toNanos()

  test "Duration string representation":
    let d = newDuration(1000000000'i64, GARROW_TIME_UNIT_NANO)
    let str = $d
    check str.contains("s")

  test "Duration with different units":
    let d1 = newDuration(1'i64, GARROW_TIME_UNIT_SECOND)
    let d2 = newDuration(1000'i64, GARROW_TIME_UNIT_MILLI)
    let d3 = newDuration(1000000'i64, GARROW_TIME_UNIT_MICRO)
    let d4 = newDuration(1000000000'i64, GARROW_TIME_UNIT_NANO)
    # All should represent approximately 1 second
    check d1.toNanos() == 1_000_000_000
    check d2.toNanos() == 1_000_000_000
    check d3.toNanos() == 1_000_000_000
    check d4.toNanos() == 1_000_000_000

suite "TimestampArray - Building and Operations":

  test "Create TimestampArrayBuilder":
    let builder = newTimestampArrayBuilder(GARROW_TIME_UNIT_NANO, "UTC")
    check builder.unit == GARROW_TIME_UNIT_NANO
    check builder.tz == "UTC"

  test "Append to TimestampArray":
    var builder = newTimestampArrayBuilder(GARROW_TIME_UNIT_MILLI, "UTC")
    let ts = newTimestamp(1000'i64, GARROW_TIME_UNIT_MILLI, "UTC")
    builder.append(ts)
    builder.append(ts)
    let arr = builder.finish()
    check arr.len == 2

  test "Append Option to TimestampArray":
    var builder = newTimestampArrayBuilder(GARROW_TIME_UNIT_NANO, "UTC")
    let ts = newTimestamp(1000'i64, GARROW_TIME_UNIT_NANO, "UTC")
    builder.append(some(ts))
    builder.append(none(Timestamp))
    builder.append(some(ts))
    let arr = builder.finish()
    check arr.len == 3

  test "TimestampArray nullability check":
    var builder = newTimestampArrayBuilder(GARROW_TIME_UNIT_MILLI, "UTC")
    let ts = newTimestamp(1000'i64, GARROW_TIME_UNIT_MILLI, "UTC")
    builder.append(some(ts))
    builder.append(none(Timestamp))
    let arr = builder.finish()
    check not arr.isNull(0)
    check arr.isNull(1)

  test "TimestampArray indexing":
    var builder = newTimestampArrayBuilder(GARROW_TIME_UNIT_MILLI, "UTC")
    builder.append(1000'i64)
    builder.append(2000'i64)
    builder.append(3000'i64)
    let arr = builder.finish()
    check arr[0] == 1000'i64
    check arr[1] == 2000'i64
    check arr[2] == 3000'i64

  test "TimestampArray out of bounds check":
    var builder = newTimestampArrayBuilder(GARROW_TIME_UNIT_MILLI, "UTC")
    builder.append(1000'i64)
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr[10]
    expect(IndexDefect):
      discard arr[-1]

  test "TimestampArray string representation":
    var builder = newTimestampArrayBuilder(GARROW_TIME_UNIT_MILLI, "UTC")
    builder.append(1000'i64)
    builder.append(2000'i64)
    let arr = builder.finish()
    let str = $arr
    check str.len > 0

  test "TimestampArray unit/timezone mismatch error":
    var builder = newTimestampArrayBuilder(GARROW_TIME_UNIT_MILLI, "UTC")
    let ts = newTimestamp(1000'i64, GARROW_TIME_UNIT_NANO, "UTC")
    expect(ValueError):
      builder.append(ts)

  test "TimestampArray timezone mismatch error":
    var builder = newTimestampArrayBuilder(GARROW_TIME_UNIT_MILLI, "UTC")
    let ts = newTimestamp(1000'i64, GARROW_TIME_UNIT_MILLI, "EST")
    expect(ValueError):
      builder.append(ts)

suite "Date32Array - Building and Operations":

  test "Create Date32ArrayBuilder":
    let builder = newDate32ArrayBuilder()
    # Builder created successfully if this doesn't raise
    check true

  test "Append to Date32Array":
    var builder = newDate32ArrayBuilder()
    let d1 = newDate32(18628'i32)
    let d2 = newDate32(18629'i32)
    builder.append(d1)
    builder.append(d2)
    builder.append(d1)
    let arr = builder.finish()
    # Array created successfully if this doesn't raise
    check true

  test "Append Option to Date32Array":
    var builder = newDate32ArrayBuilder()
    let d = newDate32(18628'i32)
    builder.append(some(d))
    builder.append(none(Date32))
    builder.append(some(d))
    let arr = builder.finish()
    check true

  test "Date32Array indexing":
    var builder = newDate32ArrayBuilder()
    builder.append(100'i32)
    builder.append(200'i32)
    builder.append(300'i32)
    let arr = builder.finish()
    check arr[0] == 100'i32
    check arr[1] == 200'i32
    check arr[2] == 300'i32

  test "Date32Array out of bounds check":
    var builder = newDate32ArrayBuilder()
    builder.append(100'i32)
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr[10]
    expect(IndexDefect):
      discard arr[-1]

suite "Memory Management - Temporal Types":

  test "Temporal types copy semantics":
    let d1 = newDate32(100'i32)
    var d2 = d1
    check d2.toDays() == 100'i32

  test "TimestampArray multiple copies":
    var builder = newTimestampArrayBuilder(GARROW_TIME_UNIT_MILLI, "UTC")
    builder.append(1000'i64)
    let arr1 = builder.finish()
    let arr2 = arr1
    let arr3 = arr2
    check arr1.len == 1
    check arr2.len == 1
    check arr3.len == 1

  test "Date32Array multiple copies":
    var builder = newDate32ArrayBuilder()
    builder.append(100'i32)
    let arr1 = builder.finish()
    let arr2 = arr1
    let arr3 = arr2
    # Arrays created successfully if this doesn't raise
    check true

  test "Rapid allocation and deallocation":
    for i in 0..100:
      var builder = newTimestampArrayBuilder(GARROW_TIME_UNIT_MILLI, "UTC")
      builder.append(int64(i * 1000))
      let arr = builder.finish()
      check arr.len == 1

  test "Temporal type in loops":
    var dates: seq[Date32]
    for i in 0..50:
      dates.add(newDate32(int32(i)))
    check dates.len == 51
    check dates[0].toDays() == 0
    check dates[50].toDays() == 50

suite "Error Handling - Temporal Types":

  test "Invalid array access raises IndexDefect":
    var builder = newTimestampArrayBuilder(GARROW_TIME_UNIT_MILLI, "UTC")
    builder.append(1000'i64)
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr[100]

  test "Null value access":
    var builder = newTimestampArrayBuilder(GARROW_TIME_UNIT_MILLI, "UTC")
    builder.append(some(newTimestamp(1000'i64, GARROW_TIME_UNIT_MILLI, "UTC")))
    builder.append(none(Timestamp))
    let arr = builder.finish()
    check arr.isNull(0) == false
    check arr.isNull(1) == true

  test "Empty array handling":
    var builder = newTimestampArrayBuilder(GARROW_TIME_UNIT_MILLI, "UTC")
    let arr = builder.finish()
    check arr.len == 0
