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
