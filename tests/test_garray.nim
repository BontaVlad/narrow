import std/options
import unittest2
import ../src/narrow/[core/ffi, column/primitive, types/gtypes]

## TODO: remove AI GENERATED TESTS THAT DON"T DO ANYTHING
 
suite "Array - Equality":
  
  test "Equal arrays":
    let arr1 = newArray(@[true, false])
    let arr2 = newArray(@[true, false])
    check arr1 == arr2
  
  test "Not equal arrays - different values":
    let arr1 = newArray(@[1'i32, 2, 3])
    let arr2 = newArray(@[1'i32, 2, 4])
    check arr1 != arr2
  
  test "Not equal arrays - different lengths":
    let arr1 = newArray(@[1'i32, 2, 3])
    let arr2 = newArray(@[1'i32, 2])
    check arr1 != arr2

suite "Array - Null Checking":
  
  test "is_null checks null elements":
    var builder = newArrayBuilder[bool]()
    builder.appendNull()
    builder.append(true)
    let arr = builder.finish()
    
    check arr.isNull(0) == true
    check arr.isNull(1) == false
  
  test "is_valid checks valid elements":
    var builder = newArrayBuilder[bool]()
    builder.appendNull()
    builder.append(true)
    let arr = builder.finish()
    
    check arr.isValid(0) == false
    check arr.isValid(1) == true
  
  test "Multiple nulls pattern":
    var builder = newArrayBuilder[bool]()
    builder.appendNull()
    builder.append(true)
    builder.append(false)
    builder.appendNull()
    builder.append(false)
    let arr = builder.finish()
    
    var nullFlags: seq[bool]
    for i in 0 ..< arr.len:
      nullFlags.add(arr.isNull(i))
    
    check nullFlags == @[true, false, false, true, false]

suite "Array - Basic Properties":
  
  test "Length of array":
    var builder = newArrayBuilder[bool]()
    builder.append(true)
    let arr = builder.finish()
    check arr.len == 1
  
  test "Length of empty array":
    let arr = newArray[int32](@[])
    check arr.len == 0
  
  test "Length after multiple appends":
    var builder = newArrayBuilder[int32]()
    builder.append(1'i32)
    builder.append(2'i32)
    builder.append(3'i32)
    let arr = builder.finish()
    check arr.len == 3

suite "Array - Slicing":
  
  test "Slice subset of array":
    var builder = newArrayBuilder[bool]()
    builder.append(true)
    builder.append(false)
    builder.append(true)
    let arr = builder.finish()
    
    let subArray = arr[1..2]
    check subArray.len == 2
    check subArray[0] == false
    check subArray[1] == true
  
  test "Slice with single element":
    let arr = newArray(@[1'i32, 2, 3, 4, 5])
    let subArray = arr[2..2]
    check subArray.len == 1
    check subArray[0] == 3
  
  test "Slice full array":
    let arr = newArray(@[1'i32, 2, 3])
    let subArray = arr[0..2]
    check subArray.len == 3
    check subArray[0] == 1
    check subArray[2] == 3

suite "Array - String Representation":
  
  test "to_s for boolean array":
    let arr = newArray(@[true, false, true])
    let str = $arr
    check str.len > 0
  
  test "to_s for int array":
    let arr = newArray(@[1'i32, 2, 3])
    let str = $arr
    check str.len > 0
  
  test "to_s for string array":
    let arr = newArray(@["Start", "Shutdown", "Reboot"])
    let str = $arr
    check str.len > 0
  
  test "to_s for empty array":
    let arr = newArray[int32](@[])
    let str = $arr
    check str.len > 0

suite "Array - Type Conversions":
  
  test "Array to seq conversion":
    let arr = newArray(@[1'i32, 2, 3])
    let s = @arr
    check s.len == 3
    check s == @[1'i32, 2, 3]
  
  test "Array to seq with nulls":
    var builder = newArrayBuilder[int32]()
    builder.append(1'i32)
    builder.appendNull()
    builder.append(3'i32)
    let arr = builder.finish()
    
    let s = @arr
    check s.len == 3

suite "Array - Builder Patterns":
  
  test "Create and read int32 array":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    check arr.len == 5
    check arr[0] == 1
    check arr[1] == 2
    check arr[4] == 5

  test "Create and read int32 array with infered type":
    let arr = newArray(@[1'i32, 2, 3, 4, 5])
    check arr.len == 5
    check arr[0] == 1
    check arr[1] == 2
    check arr[4] == 5
  
  test "Create and read int64 array":
    let arr = newArray[int64](@[10'i64, 20, 30])
    check arr.len == 3
    check arr[0] == 10
    check arr[1] == 20
    check arr[2] == 30
  
  test "Create and read float64 array":
    let arr = newArray[float64](@[1.5, 2.5, 3.5])
    check arr.len == 3
    check arr[0] == 1.5
    check arr[1] == 2.5
    check arr[2] == 3.5
  
  test "Create and read string array":
    let arr = newArray[string](@["hello", "world", "test"])
    check arr.len == 3
    check arr[0] == "hello"
    check arr[1] == "world"
    check arr[2] == "test"
  
  test "Create and read bool array":
    let arr = newArray[bool](@[true, false, true, false])
    check arr.len == 4
    check arr[0] == true
    check arr[1] == false
    check arr[2] == true
    check arr[3] == false

suite "Array - Builder Operations":
  
  test "Append individual values":
    var builder = newArrayBuilder[int32]()
    for i in 1..5:
      builder.append(i.int32)
    let arr = builder.finish()
    check arr.len == 5
    check arr[0] == 1
    check arr[4] == 5
  
  test "Append values batch":
    var builder = newArrayBuilder[int32]()
    builder.appendValues(@[1'i32, 2, 3, 4, 5])
    let arr = builder.finish()
    check arr.len == 5
  
  test "Mixed append operations":
    var builder = newArrayBuilder[int32]()
    builder.append(1'i32)
    builder.appendValues(@[2'i32, 3, 4])
    builder.append(5'i32)
    let arr = builder.finish()
    check arr.len == 5
  
  test "Append null values with builder":
    var builder = newArrayBuilder[int32]()
    builder.append(1'i32)
    builder.appendNull()
    builder.append(3'i32)
    let arr = builder.finish()
    
    check arr.len == 3
    check arr.isValid(0) == true
    check arr.isValid(1) == false
    check arr.isValid(2) == true
    check arr.isNull(1) == true
  
  test "Append Option values":
    let builder = newArrayBuilder[int32]()
    builder.append(some(1'i32))
    builder.append(none(int32))
    builder.append(some(3'i32))
    let arr = builder.finish()
    
    check arr.len == 3
    check arr.isNull(1) == true

suite "Array - Iteration":
  
  test "Array iteration":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    var sum = 0'i32
    for val in arr:
      sum += val
    check sum == 15
  
  test "Iteration over string array":
    let arr = newArray[string](@["a", "b", "c"])
    var concat = ""
    for val in arr:
      concat &= val
    check concat == "abc"
  
  test "Iteration over boolean array":
    let arr = newArray[bool](@[true, false, true, true])
    var trueCount = 0
    for val in arr:
      if val:
        trueCount += 1
    check trueCount == 3

suite "Array - Different Types":
  
  test "uint8 array":
    let arr = newArray[uint8](@[1'u8, 2, 3])
    check arr.len == 3
    check arr[0] == 1
  
  test "int8 array":
    let arr = newArray[int8](@[2'i8, 3, 6, 10])
    check arr.len == 4
    check arr[0] == 2
    check arr[3] == 10
  
  test "int16 array":
    let arr = newArray[int16](@[100'i16, 200, 300])
    check arr.len == 3
    check arr[1] == 200
  
  test "uint16 array":
    let arr = newArray[uint16](@[1000'u16, 2000, 3000])
    check arr.len == 3
    check arr[2] == 3000
  
  test "uint32 array":
    let arr = newArray[uint32](@[1000000'u32, 2000000])
    check arr.len == 2
  
  test "uint64 array":
    let arr = newArray[uint64](@[1000000000'u64, 2000000000])
    check arr.len == 2
  
  test "float32 array":
    let arr = newArray[float32](@[1.5'f32, 2.5, 3.5])
    check arr.len == 3
    check arr[1] == 2.5'f32

suite "Array - Edge Cases":
  
  test "Empty array":
    let arr = newArray[int32](@[])
    check arr.len == 0
  
  test "Single element array":
    let arr = newArray[int32](@[42'i32])
    check arr.len == 1
    check arr[0] == 42
  
  test "Large array":
    var values: seq[int32]
    for i in 0..<10000:
      values.add(i.int32)
    let arr = newArray[int32](values)
    check arr.len == 10000
    check arr[0] == 0
    check arr[9999] == 9999
  
  test "String array with empty strings":
    let arr = newArray[string](@["", "hello", "", "world"])
    check arr.len == 4
    check arr[0] == ""
    check arr[1] == "hello"
    check arr[2] == ""
  
  test "Slice at boundaries":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    let first = arr[0..0]
    check first.len == 1
    check first[0] == 1
    
    let last = arr[4..4]
    check last.len == 1
    check last[0] == 5
    
    let all = arr[0..4]
    check all.len == 5

suite "Array - tryGet with Options":
  
  test "tryGet with null values":
    let builder = newArrayBuilder[int32]()
    builder.append(1'i32)
    builder.appendNull()
    builder.append(3'i32)
    let arr = builder.finish()
    
    check arr.tryGet(0).isSome()
    check arr.tryGet(0).get() == 1
    check arr.tryGet(1).isNone()
    check arr.tryGet(2).isSome()
    check arr.tryGet(2).get() == 3
  
  test "tryGet with out of bounds":
    let arr = newArray[int32](@[1'i32, 2, 3])
    check arr.tryGet(-1).isNone()
    check arr.tryGet(10).isNone()
  
  test "tryGet with valid values":
    let arr = newArray[string](@["Start", "Shutdown", "Reboot"])
    check arr.tryGet(0).get() == "Start"
    check arr.tryGet(1).get() == "Shutdown"
    check arr.tryGet(2).get() == "Reboot"

suite "Array - Memory Management":
  
  test "Create and destroy many arrays":
    for i in 0..1000:
      let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
      check arr.len == 5
  
  test "Create and destroy many builders":
    for i in 0..1000:
      var builder = newArrayBuilder[int32]()
      builder.append(1'i32)
      builder.append(2'i32)
      let arr = builder.finish()
      check arr.len == 2
  
  test "Array copying":
    let original = newArray[int32](@[1'i32, 2, 3, 4, 5])
    for i in 0..1000:
      let copy1 = original
      let copy2 = copy1
      check copy2.len == 5
  
  test "Array slicing stress":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    for i in 0..1000:
      let sliced = arr[2..7]
      check sliced.len == 6
  
  test "Builder reuse pattern":
    for i in 0..100:
      var builder = newArrayBuilder[int32]()
      builder.appendValues(@[1'i32, 2, 3])
      let arr1 = builder.finish()
      
      builder = newArrayBuilder[int32]()
      builder.appendValues(@[4'i32, 5, 6])
      let arr2 = builder.finish()
      
      check arr1.len == 3
      check arr2.len == 3

suite "Array - Error Handling: Index Out of Bounds":
  
  test "Access negative index":
    let arr = newArray[int32](@[1'i32, 2, 3])
    expect(IndexDefect):
      discard arr[-1]
  
  test "Access index beyond array length":
    let arr = newArray[int32](@[1'i32, 2, 3])
    expect(IndexDefect):
      discard arr[10]
  
  test "Access index on empty array":
    let arr = newArray[int32](@[])
    expect(IndexDefect):
      discard arr[0]
  
  test "Slice with negative start":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    expect(IndexDefect):
      discard arr[-1..2]
  
  test "Slice with end beyond length":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    expect(IndexDefect):
      discard arr[2..10]
  
  test "Slice with start > end":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    expect(IndexDefect):
      discard arr[4..2]
  
  test "isNull with negative index":
    let arr = newArray[int32](@[1'i32, 2, 3])
    expect(IndexDefect):
      discard arr.isNull(-1)
  
  test "isNull with index beyond length":
    let arr = newArray[int32](@[1'i32, 2, 3])
    expect(IndexDefect):
      discard arr.isNull(10)
  
  test "isValid with negative index":
    let arr = newArray[int32](@[1'i32, 2, 3])
    expect(IndexDefect):
      discard arr.isValid(-1)
  
  test "isValid with index beyond length":
    let arr = newArray[int32](@[1'i32, 2, 3])
    expect(IndexDefect):
      discard arr.isValid(10)

suite "Array - Error Handling: Builder Errors":
  
  test "Append after finish - create new array":
    var builder = newArrayBuilder[int32]()
    builder.append(1'i32)
    let arr1 = builder.finish()
    
    builder.append(2'i32)
    let arr2 = builder.finish()
    check arr1 != arr2
    check arr1[0] == 1
    check arr2[0] == 2
  
  test "Finish empty builder":
    var builder = newArrayBuilder[int32]()
    let arr = builder.finish()
    check arr.len == 0
  
  test "Multiple finish calls on same builder":
    var builder = newArrayBuilder[int32]()
    builder.append(1'i32)
    let arr1 = builder.finish()
    
    let arr2 = builder.finish()
    check len(arr1) == 1
    check len(arr2) == 0
  
  test "AppendValues with empty sequence":
    var builder = newArrayBuilder[int32]()
    builder.appendValues(@[])
    let arr = builder.finish()
    check arr.len == 0

suite "Array - Error Handling: Type Mismatches":
  
  test "String array with null characters":
    let arr = newArray[string](@["hello\0world", "test"])
    # Should handle null characters in strings
    check arr.len == 2
  
  test "String array with unicode":
    let arr = newArray[string](@["Hello 👋", "World 🌍", "Nim 🎯"])
    check arr.len == 3
    check arr[0] == "Hello 👋"
  
  test "Boolean array edge values":
    let arr = newArray[bool](@[true, false, true, false])
    check arr[0] == true
    check arr[1] == false
  
  test "Float array with special values - Infinity":
    let arr = newArray[float64](@[1.0, Inf, -Inf])
    check arr.len == 3
    check arr[1] == Inf
    check arr[2] == -Inf
  
  test "Float array with special values - negative zero":
    let arr = newArray[float64](@[0.0, -0.0, 1.0])
    check arr.len == 3

suite "Array - Error Handling: Null Operations":
  
  test "All null array":
    var builder = newArrayBuilder[int32]()
    for i in 0..10:
      builder.appendNull()
    let arr = builder.finish()
    check arr.len == 11
    for i in 0..10:
      check arr.isNull(i)
  
  test "Access value at null position":
    var builder = newArrayBuilder[int32]()
    builder.appendNull()
    let arr = builder.finish()
    
    # Accessing null position may return undefined value
    # tryGet should return None
    check arr.tryGet(0).isNone()
  
  test "Iteration over array with nulls":
    var builder = newArrayBuilder[int32]()
    builder.append(1'i32)
    builder.appendNull()
    builder.append(3'i32)
    let arr = builder.finish()
    
    var count = 0
    for val in arr:
      count += 1
    check count == 3  # Should iterate over all elements including nulls
  
  test "String array with null values":
    var builder = newArrayBuilder[string]()
    builder.append("hello")
    builder.appendNull()
    builder.append("world")
    let arr = builder.finish()
    
    check arr.len == 3
    check arr.isNull(1)
    check arr.tryGet(1).isNone()

suite "Array - Error Handling: Memory Extremes":
  
  test "Create array with maximum safe size":
    var values: seq[int32]
    for i in 0..<100000:
      values.add(i.int32)
    let arr = newArray[int32](values)
    check arr.len == 100000
  
  test "Rapid allocation and deallocation":
    for cycle in 0..1000:
      var builder = newArrayBuilder[int32]()
      builder.append(1'i32)
      let arr = builder.finish()
      # Array and builder destroyed at end of iteration
  
  test "Deep copy chain":
    let original = newArray[int32](@[1'i32, 2, 3])
    var current = original
    for i in 0..1000:
      let next = current
      current = next
    check current.len == 3
  
  test "Nested iteration":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    var count = 0
    for i in arr:
      for j in arr:
        count += 1
    check count == 25
  
  test "Slice chain stress":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    var sliced = arr[1..8]
    sliced = sliced[1..6]
    sliced = sliced[1..4]
    check sliced.len == 4

suite "Array - Error Handling: Builder State":
  
  test "Builder with only nulls":
    var builder = newArrayBuilder[int32]()
    builder.appendNull()
    builder.appendNull()
    builder.appendNull()
    let arr = builder.finish()
    check arr.len == 3
    for i in 0..2:
      check arr.isNull(i)
  
  test "Builder with alternating nulls and values":
    var builder = newArrayBuilder[int32]()
    for i in 0..99:
      if i mod 2 == 0:
        builder.append(i.int32)
      else:
        builder.appendNull()
    let arr = builder.finish()
    check arr.len == 100
  
  test "Builder with batch append followed by nulls":
    var builder = newArrayBuilder[int32]()
    builder.appendValues(@[1'i32, 2, 3])
    builder.appendNull()
    builder.appendNull()
    let arr = builder.finish()
    check arr.len == 5
    check arr.isNull(3)
    check arr.isNull(4)
  
  test "Builder with nulls followed by batch append":
    var builder = newArrayBuilder[int32]()
    builder.appendNull()
    builder.appendNull()
    builder.appendValues(@[1'i32, 2, 3])
    let arr = builder.finish()
    check arr.len == 5
    check arr.isNull(0)
    check arr.isNull(1)

suite "Array - Error Handling: String Edge Cases":
  
  test "Empty string in array":
    let arr = newArray[string](@["", "", ""])
    check arr.len == 3
    check arr[0] == ""
    check arr[1] == ""
    check arr[2] == ""
  
  test "String with special characters":
    let arr = newArray[string](@["\n", "\t", "\r\n", "\\"])
    check arr.len == 4
    check arr[0] == "\n"
    check arr[1] == "\t"
  
  test "String with emoji sequences":
    let arr = newArray[string](@["👨‍👩‍👧‍👦", "🏳️‍🌈", "👍🏽"])
    check arr.len == 3

suite "Array - Error Handling: Numeric Edge Cases":
  
  test "Integer overflow values":
    let arr = newArray[int32](@[high(int32), low(int32), 0'i32])
    check arr.len == 3
    check arr[0] == high(int32)
    check arr[1] == low(int32)
  
  test "Unsigned integer edge values":
    let arr = newArray[uint32](@[high(uint32), low(uint32), 0'u32])
    check arr.len == 3
    check arr[0] == high(uint32)
    check arr[1] == low(uint32)
  
  test "Float denormalized numbers":
    let arr = newArray[float64](@[1e-308, 1e-309, 1e-310])
    check arr.len == 3
  
  test "Mixed sign integers":
    let arr = newArray[int32](@[-100'i32, 0, 100, -1, 1])
    check arr.len == 5
    var sum = 0'i32
    for val in arr:
      sum += val
    check sum == 0

suite "Array - Error Handling: Concurrent Operations":
  
  test "Multiple builders of different types":
    var builder1 = newArrayBuilder[int32]()
    var builder2 = newArrayBuilder[float64]()
    var builder3 = newArrayBuilder[string]()
    
    builder1.append(1'i32)
    builder2.append(1.5)
    builder3.append("test")
    
    let arr1 = builder1.finish()
    let arr2 = builder2.finish()
    let arr3 = builder3.finish()
    
    check arr1.len == 1
    check arr2.len == 1
    check arr3.len == 1
  
  test "Interleaved array operations":
    let arr1 = newArray[int32](@[1'i32, 2, 3])
    var builder = newArrayBuilder[int32]()
    let arr2 = newArray[int32](@[4'i32, 5, 6])
    
    builder.append(7'i32)
    let arr3 = builder.finish()
    
    check arr1.len == 3
    check arr2.len == 3
    check arr3.len == 1
  
  test "Multiple slices from same array":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5, 6, 7, 8])
    let slice1 = arr[0..3]
    let slice2 = arr[2..5]
    let slice3 = arr[4..7]
    
    check slice1.len == 4
    check slice2.len == 4
    check slice3.len == 4

suite "Array - Error Handling: Conversion Errors":
  
  test "Convert array with nulls to seq":
    var builder = newArrayBuilder[int32]()
    builder.append(1'i32)
    builder.appendNull()
    builder.append(3'i32)
    let arr = builder.finish()
    
    let s = @arr
    check s.len == 3
    # Note: null values may have undefined behavior in seq
  
  test "String representation of array with nulls":
    var builder = newArrayBuilder[int32]()
    builder.append(1'i32)
    builder.appendNull()
    builder.append(3'i32)
    let arr = builder.finish()
    
    let str = $arr
    check str.len > 0
  
  test "String representation of empty array":
    let arr = newArray[int32](@[])
    let str = $arr
    check str.len > 0
  
  test "String representation of large array":
    var values: seq[int32]
    for i in 0..1000:
      values.add(i.int32)
    let arr = newArray[int32](values)
    let str = $arr
    check str.len > 0

suite "Array - Error Handling: Edge Slicing":
  
  test "Zero-length slice":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    expect(Exception):
      discard arr[2..1]  # Invalid range
  
  test "Slice exactly at array end":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    let slice = arr[4..4]
    check slice.len == 1
    check slice[0] == 5
  
  test "Slice of slice":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5, 6, 7, 8])
    let slice1 = arr[2..6]
    let slice2 = slice1[1..3]
    check slice2.len == 3
  
  test "Slice on single element array":
    let arr = newArray[int32](@[42'i32])
    let slice = arr[0..0]
    check slice.len == 1
    check slice[0] == 42

suite "Array - Error Handling: Iterator Edge Cases":
  
  test "Iterate empty array":
    let arr = newArray[int32](@[])
    var count = 0
    for val in arr:
      count += 1
    check count == 0
  
  test "Iterate array with all nulls":
    var builder = newArrayBuilder[int32]()
    for i in 0..4:
      builder.appendNull()
    let arr = builder.finish()
    
    var count = 0
    for val in arr:
      count += 1
    check count == 5
  
  test "Nested iteration with early break":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    var count = 0
    for i in arr:
      for j in arr:
        count += 1
        if count == 10:
          break
      if count >= 10:
        break
    check count == 10
  
  test "Iteration with modification via tryGet":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    var sum = 0'i32
    for i in 0..<arr.len:
      let val = arr.tryGet(i)
      if val.isSome():
        sum += val.get()
    check sum == 15
