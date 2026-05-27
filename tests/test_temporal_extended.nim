import unittest2
import std/[options, strutils]
import ../src/narrow/[core/ffi, types/gtemporal]

suite "Time32 - Creation and Operations":

  test "Create Time32 from seconds":
    let t = newTime32(3661'i32)  # 1 hour, 1 minute, 1 second
    check t.toSeconds() == 3661.0

  test "Create Time32 from float seconds":
    let t = newTime32FromSeconds(3661.5)
    check t.toSeconds() == 3661.0  # truncated to int32

  test "Time32 string representation":
    let t = newTime32(3661'i32)  # 1:01:01
    let str = $t
    check str.contains("1")

suite "Time64 - Creation and Operations":

  test "Create Time64 from nanoseconds":
    let t = newTime64(1000000000'i64)  # 1 second in nanos
    check t.toMicros() == 1000000.0

  test "Create Time64 from microseconds":
    let t = newTime64FromMicros(1000000.0)  # 1 second
    check t.toMicros() == 1000000.0

  test "Time64 string representation":
    let t = newTime64(1000000000'i64)
    let str = $t
    check str.contains("ms")

suite "Time32Array - Building and Operations":

  test "Create Time32ArrayBuilder":
    let builder = newTime32ArrayBuilder(GARROW_TIME_UNIT_SECOND)
    check builder.unit == GARROW_TIME_UNIT_SECOND

  test "Append to Time32Array":
    var builder = newTime32ArrayBuilder(GARROW_TIME_UNIT_SECOND)
    builder.append(3661'i32)
    builder.append(7322'i32)
    let arr = builder.finish()
    check arr.len == 2

  test "Append Time32 objects":
    var builder = newTime32ArrayBuilder(GARROW_TIME_UNIT_SECOND)
    let t1 = newTime32(1000'i32)
    let t2 = newTime32(2000'i32)
    builder.append(t1)
    builder.append(t2)
    let arr = builder.finish()
    check arr.len == 2

  test "Append Option[Time32]":
    var builder = newTime32ArrayBuilder(GARROW_TIME_UNIT_SECOND)
    let t = newTime32(3661'i32)
    builder.append(some(t))
    builder.append(none(Time32))
    builder.append(some(t))
    let arr = builder.finish()
    check arr.len == 3

  test "Time32Array string representation":
    var builder = newTime32ArrayBuilder(GARROW_TIME_UNIT_SECOND)
    builder.append(1000'i32)
    builder.append(2000'i32)
    let arr = builder.finish()
    let str = $arr
    check str.len > 0

suite "Time64Array - Building and Operations":

  test "Create Time64ArrayBuilder":
    let builder = newTime64ArrayBuilder(GARROW_TIME_UNIT_MICRO)
    check builder.unit == GARROW_TIME_UNIT_MICRO

  test "Append to Time64Array":
    var builder = newTime64ArrayBuilder(GARROW_TIME_UNIT_MICRO)
    builder.append(1000000'i64)
    builder.append(2000000'i64)
    let arr = builder.finish()
    check arr.len == 2
    check arr[0] == 1000000'i64
    check arr[1] == 2000000'i64

  test "Append Time64 objects":
    var builder = newTime64ArrayBuilder(GARROW_TIME_UNIT_MICRO)
    let t1 = newTime64(1000000'i64)
    let t2 = newTime64(2000000'i64)
    builder.append(t1)
    builder.append(t2)
    let arr = builder.finish()
    check arr.len == 2
    check arr[0] == 1000000'i64

  test "Append Option[Time64]":
    var builder = newTime64ArrayBuilder(GARROW_TIME_UNIT_MICRO)
    let t = newTime64(1000000'i64)
    builder.append(some(t))
    builder.append(none(Time64))
    builder.append(some(t))
    let arr = builder.finish()
    check arr.len == 3

  test "Time64Array string representation":
    var builder = newTime64ArrayBuilder(GARROW_TIME_UNIT_MICRO)
    builder.append(1000000'i64)
    builder.append(2000000'i64)
    let arr = builder.finish()
    let str = $arr
    check str.len > 0

suite "Duration - Operations and Conversions":

  test "Duration creation and conversions":
    let d1 = newDuration(1'i64, GARROW_TIME_UNIT_SECOND)
    let d2 = newDuration(1000'i64, GARROW_TIME_UNIT_MILLI)
    let d3 = newDuration(1000000'i64, GARROW_TIME_UNIT_MICRO)
    check d1.toNanos() == d2.toNanos()
    check d2.toNanos() == d3.toNanos()

  test "Duration between time values":
    let d = newDuration(5000'i64, GARROW_TIME_UNIT_MILLI)  # 5 seconds
    check d.toNanos() == 5_000_000_000

  test "Duration string formatting":
    let d1 = newDuration(1000'i64, GARROW_TIME_UNIT_SECOND)
    let str1 = d1.toDuration()
    check str1.contains("s")

    let d2 = newDuration(500'i64, GARROW_TIME_UNIT_MILLI)
    let str2 = d2.toDuration()
    check str2.len > 0

suite "Interval Types - Creation and Operations":

  test "Create MonthInterval":
    let mi = newMonthInterval(3)
    check mi.months == 3
    let str = $mi
    check str.contains("months")

  test "Create DayTimeInterval":
    let dti = newDayTimeInterval(5, 3600000)  # 5 days, 1 hour in millis
    check dti.days == 5
    check dti.millis == 3600000
    let str = $dti
    check str.contains("days")

  test "Create MonthDayNanoInterval":
    let mdni = newMonthDayNanoInterval(2, 10, 500000000)  # 2 months, 10 days, 0.5 seconds
    check mdni.months == 2
    check mdni.days == 10
    check mdni.nanos == 500000000
    let str = $mdni
    check str.contains("months")
    check str.contains("days")

  test "String representations of intervals":
    let mi = newMonthInterval(6)
    let dti = newDayTimeInterval(1, 1000)
    let mdni = newMonthDayNanoInterval(1, 1, 1000000000)
    
    check ($mi).len > 0
    check ($dti).len > 0
    check ($mdni).len > 0

suite "Time Array Edge Cases":

  test "Empty Time32Array":
    var builder = newTime32ArrayBuilder(GARROW_TIME_UNIT_SECOND)
    let arr = builder.finish()
    check arr.len == 0

  test "Empty Time64Array":
    var builder = newTime64ArrayBuilder(GARROW_TIME_UNIT_MICRO)
    let arr = builder.finish()
    check arr.len == 0

  test "Time32Array with all nulls":
    var builder = newTime32ArrayBuilder(GARROW_TIME_UNIT_SECOND)
    builder.appendNull()
    builder.appendNull()
    builder.appendNull()
    let arr = builder.finish()
    check arr.len == 3

  test "Time64Array with all nulls":
    var builder = newTime64ArrayBuilder(GARROW_TIME_UNIT_MICRO)
    builder.appendNull()
    builder.appendNull()
    let arr = builder.finish()
    check arr.len == 2

  test "Time32Array out of bounds":
    var builder = newTime32ArrayBuilder(GARROW_TIME_UNIT_SECOND)
    builder.append(1000'i32)
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr[10]
    expect(IndexDefect):
      discard arr[-1]

  test "Time64Array out of bounds":
    var builder = newTime64ArrayBuilder(GARROW_TIME_UNIT_MICRO)
    builder.append(1000000'i64)
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr[10]
    expect(IndexDefect):
      discard arr[-1]

suite "Time Array Memory Management":

  test "Time32Array copies":
    var builder = newTime32ArrayBuilder(GARROW_TIME_UNIT_SECOND)
    builder.append(1000'i32)
    let arr1 = builder.finish()
    let arr2 = arr1
    let arr3 = arr2
    check arr1.len == 1
    check arr2.len == 1
    check arr3.len == 1

  test "Time64Array copies":
    var builder = newTime64ArrayBuilder(GARROW_TIME_UNIT_MICRO)
    builder.append(1000000'i64)
    let arr1 = builder.finish()
    let arr2 = arr1
    check arr1.len == 1
    check arr2.len == 1

  test "Rapid Time32Array allocation":
    for i in 0..100:
      var builder = newTime32ArrayBuilder(GARROW_TIME_UNIT_SECOND)
      builder.append(int32(i * 100))
      let arr = builder.finish()
      check arr.len == 1

  test "Rapid Time64Array allocation":
    for i in 0..100:
      var builder = newTime64ArrayBuilder(GARROW_TIME_UNIT_MICRO)
      builder.append(int64(i * 1000000))
      let arr = builder.finish()
      check arr.len == 1

suite "Time Unit Consistency":

  test "Time32 builder respects unit":
    let builder1 = newTime32ArrayBuilder(GARROW_TIME_UNIT_SECOND)
    let builder2 = newTime32ArrayBuilder(GARROW_TIME_UNIT_MILLI)
    check builder1.unit == GARROW_TIME_UNIT_SECOND
    check builder2.unit == GARROW_TIME_UNIT_MILLI

  test "Time64 builder respects unit":
    let builder1 = newTime64ArrayBuilder(GARROW_TIME_UNIT_MICRO)
    let builder2 = newTime64ArrayBuilder(GARROW_TIME_UNIT_NANO)
    check builder1.unit == GARROW_TIME_UNIT_MICRO
    check builder2.unit == GARROW_TIME_UNIT_NANO

  test "Time32 array preserves unit":
    var builder = newTime32ArrayBuilder(GARROW_TIME_UNIT_MILLI)
    builder.append(1000'i32)
    let arr = builder.finish()
    check arr.unit == GARROW_TIME_UNIT_MILLI

  test "Time64 array preserves unit":
    var builder = newTime64ArrayBuilder(GARROW_TIME_UNIT_NANO)
    builder.append(1000000000'i64)
    let arr = builder.finish()
    check arr.unit == GARROW_TIME_UNIT_NANO

suite "DurationArray - Building and Operations":

  test "Create DurationArrayBuilder with default unit":
    let builder = newDurationArrayBuilder()
    check builder.unit == GARROW_TIME_UNIT_NANO

  test "Create DurationArrayBuilder with explicit unit":
    let builder = newDurationArrayBuilder(GARROW_TIME_UNIT_MILLI)
    check builder.unit == GARROW_TIME_UNIT_MILLI

  test "Append raw int64 values and finish":
    var builder = newDurationArrayBuilder(GARROW_TIME_UNIT_MICRO)
    builder.append(100'i64)
    builder.append(200'i64)
    builder.appendNull()
    let arr = builder.finish()
    check arr.len == 3
    check arr.unit == GARROW_TIME_UNIT_MICRO
    check arr[0] == 100
    check arr[1] == 200
    check arr.isNull(2)
    check not arr.isNull(0)

  test "Append Duration objects":
    var builder = newDurationArrayBuilder(GARROW_TIME_UNIT_SECOND)
    let d1 = newDuration(60, GARROW_TIME_UNIT_SECOND)
    let d2 = newDuration(120, GARROW_TIME_UNIT_SECOND)
    builder.append(d1)
    builder.append(d2)
    let arr = builder.finish()
    check arr.len == 2
    check arr[0] == 60
    check arr[1] == 120

  test "Append Duration with unit mismatch raises":
    var builder = newDurationArrayBuilder(GARROW_TIME_UNIT_MILLI)
    let d = newDuration(1, GARROW_TIME_UNIT_SECOND)
    expect(ValueError):
      builder.append(d)

  test "Append Option[Duration]":
    var builder = newDurationArrayBuilder(GARROW_TIME_UNIT_NANO)
    builder.append(some(newDuration(500'i64, GARROW_TIME_UNIT_NANO)))
    builder.append(none(Duration))
    builder.append(some(newDuration(1000'i64, GARROW_TIME_UNIT_NANO)))
    let arr = builder.finish()
    check arr.len == 3
    check arr[0] == 500
    check arr.isNull(1)
    check arr[2] == 1000

  test "Bulk appendValues with raw int64":
    var builder = newDurationArrayBuilder(GARROW_TIME_UNIT_MICRO)
    builder.appendValues([1'i64, 2'i64, 3'i64])
    let arr = builder.finish()
    check arr.len == 3
    check arr[0] == 1
    check arr[1] == 2
    check arr[2] == 3

  test "appendValues with empty array":
    var builder = newDurationArrayBuilder()
    builder.appendValues(newSeq[int64](0))
    let arr = builder.finish()
    check arr.len == 0

  test "String representation":
    var builder = newDurationArrayBuilder(GARROW_TIME_UNIT_SECOND)
    builder.append(1'i64)
    builder.append(42'i64)
    builder.appendNull()
    let arr = builder.finish()
    let str = $arr
    check str.len > 0

  test "Bounds checking on index":
    var builder = newDurationArrayBuilder()
    builder.append(1'i64)
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr[-1]
    expect(IndexDefect):
      discard arr[5]

  test "Memory management - multiple arrays":
    for _ in 0 ..< 10:
      var builder = newDurationArrayBuilder()
      builder.append(1'i64)
      discard builder.finish()

suite "MonthIntervalArray - Building and Operations":

  test "Create MonthIntervalArrayBuilder":
    var builder = newMonthIntervalArrayBuilder()
    builder.append(1'i32)
    let arr = builder.finish()
    check arr.len == 1

  test "Append raw int32 values and finish":
    var builder = newMonthIntervalArrayBuilder()
    builder.append(1'i32)
    builder.append(6'i32)
    builder.appendNull()
    let arr = builder.finish()
    check arr.len == 3
    check arr[0] == 1
    check arr[1] == 6
    check arr.isNull(2)

  test "Append MonthInterval objects":
    var builder = newMonthIntervalArrayBuilder()
    let m1 = newMonthInterval(3'i32)
    let m2 = newMonthInterval(12'i32)
    builder.append(m1)
    builder.append(m2)
    let arr = builder.finish()
    check arr.len == 2
    check arr[0] == 3
    check arr[1] == 12

  test "Append Option[MonthInterval]":
    var builder = newMonthIntervalArrayBuilder()
    builder.append(some(newMonthInterval(1'i32)))
    builder.append(none(MonthInterval))
    builder.append(some(newMonthInterval(24'i32)))
    let arr = builder.finish()
    check arr.len == 3
    check arr[0] == 1
    check arr.isNull(1)
    check arr[2] == 24

  test "Bulk appendValues with raw int32":
    var builder = newMonthIntervalArrayBuilder()
    builder.appendValues([1'i32, 2'i32, 3'i32])
    let arr = builder.finish()
    check arr.len == 3
    check arr[0] == 1
    check arr[1] == 2
    check arr[2] == 3

  test "appendValues with empty array":
    var builder = newMonthIntervalArrayBuilder()
    builder.appendValues(newSeq[int32](0))
    let arr = builder.finish()
    check arr.len == 0

  test "String representation":
    var builder = newMonthIntervalArrayBuilder()
    builder.append(1'i32)
    builder.appendNull()
    builder.append(12'i32)
    let arr = builder.finish()
    let str = $arr
    check str.len > 0

  test "Bounds checking on index":
    var builder = newMonthIntervalArrayBuilder()
    builder.append(1'i32)
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr[-1]
    expect(IndexDefect):
      discard arr[5]

  test "isNull bounds checking":
    var builder = newMonthIntervalArrayBuilder()
    builder.append(1'i32)
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr.isNull(-1)

  test "Memory management - multiple arrays":
    for _ in 0 ..< 10:
      var builder = newMonthIntervalArrayBuilder()
      builder.append(1'i32)
      discard builder.finish()

suite "DayTimeIntervalArray - Building and Operations":

  test "Create builder and append values":
    var builder = newDayTimeIntervalArrayBuilder()
    let dti1 = newDayTimeInterval(1'i32, 1000'i32)
    let dti2 = newDayTimeInterval(2'i32, 2000'i32)
    builder.append(dti1)
    builder.append(dti2)
    let arr = builder.finish()
    check arr.len == 2

  test "Append with nulls":
    var builder = newDayTimeIntervalArrayBuilder()
    let dti1 = newDayTimeInterval(5'i32, 500'i32)
    builder.append(some(dti1))
    builder.append(none(DayTimeInterval))
    builder.append(some(newDayTimeInterval(10'i32, 1000'i32)))
    let arr = builder.finish()
    check arr.len == 3
    check arr.isNull(1)
    check not arr.isNull(0)

  test "Index access returns correct values":
    var builder = newDayTimeIntervalArrayBuilder()
    builder.append(newDayTimeInterval(1'i32, 100'i32))
    builder.append(newDayTimeInterval(7'i32, 3500'i32))
    let arr = builder.finish()
    check arr[0].days == 1
    check arr[0].millis == 100
    check arr[1].days == 7
    check arr[1].millis == 3500

  test "String representation":
    var builder = newDayTimeIntervalArrayBuilder()
    builder.append(newDayTimeInterval(1'i32, 100'i32))
    builder.appendNull()
    let arr = builder.finish()
    let str = $arr
    check str.len > 0

  test "Bounds checking":
    var builder = newDayTimeIntervalArrayBuilder()
    builder.append(newDayTimeInterval(1'i32, 0'i32))
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr[-1]
    expect(IndexDefect):
      discard arr[5]

  test "Memory management stress":
    for _ in 0 ..< 10:
      var builder = newDayTimeIntervalArrayBuilder()
      builder.append(newDayTimeInterval(1'i32, 100'i32))
      discard builder.finish()

suite "MonthDayNanoIntervalArray - Building and Operations":

  test "Create builder and append values":
    var builder = newMonthDayNanoIntervalArrayBuilder()
    let mdn1 = newMonthDayNanoInterval(1'i32, 15'i32, 1000'i64)
    let mdn2 = newMonthDayNanoInterval(3'i32, 0'i32, 500'i64)
    builder.append(mdn1)
    builder.append(mdn2)
    let arr = builder.finish()
    check arr.len == 2

  test "Append with nulls":
    var builder = newMonthDayNanoIntervalArrayBuilder()
    builder.append(some(newMonthDayNanoInterval(2'i32, 10'i32, 0'i64)))
    builder.append(none(MonthDayNanoInterval))
    builder.append(some(newMonthDayNanoInterval(6'i32, 0'i32, 999'i64)))
    let arr = builder.finish()
    check arr.len == 3
    check arr.isNull(1)
    check not arr.isNull(0)

  test "Index access returns correct values":
    var builder = newMonthDayNanoIntervalArrayBuilder()
    builder.append(newMonthDayNanoInterval(2'i32, 5'i32, 100'i64))
    builder.append(newMonthDayNanoInterval(12'i32, 30'i32, 9999'i64))
    let arr = builder.finish()
    check arr[0].months == 2
    check arr[0].days == 5
    check arr[0].nanos == 100
    check arr[1].months == 12
    check arr[1].days == 30
    check arr[1].nanos == 9999

  test "String representation":
    var builder = newMonthDayNanoIntervalArrayBuilder()
    builder.append(newMonthDayNanoInterval(1'i32, 1'i32, 1'i64))
    let arr = builder.finish()
    let str = $arr
    check str.len > 0

  test "Bounds checking":
    var builder = newMonthDayNanoIntervalArrayBuilder()
    builder.append(newMonthDayNanoInterval(1'i32, 0'i32, 0'i64))
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr[-1]
    expect(IndexDefect):
      discard arr[5]

  test "Memory management stress":
    for _ in 0 ..< 10:
      var builder = newMonthDayNanoIntervalArrayBuilder()
      builder.append(newMonthDayNanoInterval(1'i32, 0'i32, 0'i64))
      discard builder.finish()
