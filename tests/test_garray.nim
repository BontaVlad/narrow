import std/options
import unittest2
import ../src/[garray, gtypes]

suite "Array - Basic Functionality":
  
  test "Create and read int32 array":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
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
  
  test "Array iteration":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    var sum = 0'i32
    for val in arr:
      sum += val
    check sum == 15
  
  test "Array slicing":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    let sliced = arr[1..3]
    check sliced.len == 3
    check sliced[0] == 2
    check sliced[1] == 3
    check sliced[2] == 4
  
  test "Array to seq conversion":
    let arr = newArray[int32](@[1'i32, 2, 3])
    let s = @arr
    check s.len == 3
    check s == @[1'i32, 2, 3]
  
  test "Array string representation":
    let arr = newArray[int32](@[1'i32, 2, 3])
    let str = $arr
    check str.len > 0

suite "Array - Null Handling":
  
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
    var builder = newArrayBuilder[int32]()
    builder.append(some(1'i32))
    builder.append(none(int32))
    builder.append(some(3'i32))
    let arr = builder.finish()
    
    check arr.len == 3
    check arr.isNull(1) == true
  
  test "tryGet with null values":
    var builder = newArrayBuilder[int32]()
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

suite "Array - Different Types":
  
  test "uint8 array":
    let arr = newArray[uint8](@[1'u8, 2, 3])
    check arr.len == 3
    check arr[0] == 1
  
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

suite "Array - Memory Stress Tests":
  
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
  
  test "Nested array operations":
    for outer in 0..100:
      var builder = newArrayBuilder[int32]()
      for inner in 0..99:
        builder.append(inner.int32)
      let arr = builder.finish()
      check arr.len == 100
  
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
  
  test "Large array creation and destruction":
    for cycle in 0..10:
      var values: seq[int32]
      for i in 0..<10000:
        values.add(i.int32)
      let arr = newArray[int32](values)
      check arr.len == 10000
  
  test "String array memory":
    for i in 0..1000:
      let arr = newArray[string](@["test1", "test2", "test3"])
      check arr.len == 3
      discard $arr
  
  test "Multiple type arrays":
    for i in 0..100:
      block:
        let intArr = newArray[int32](@[1'i32, 2, 3])
        let floatArr = newArray[float64](@[1.0, 2.0, 3.0])
        let strArr = newArray[string](@["a", "b", "c"])
        let boolArr = newArray[bool](@[true, false, true])
        
        check intArr.len == 3
        check floatArr.len == 3
        check strArr.len == 3
        check boolArr.len == 3
  
  test "Array iteration stress":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    for i in 0..10000:
      var sum = 0'i32
      for val in arr:
        sum += val
      check sum == 15
  
  test "Array to seq conversion stress":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    for i in 0..1000:
      let s = @arr
      check s.len == 5
  
  test "Builder with nulls":
    for i in 0..1000:
      var builder = newArrayBuilder[int32]()
      builder.append(1'i32)
      builder.appendNull()
      builder.append(3'i32)
      let arr = builder.finish()
      check arr.isNull(1)
  
  test "Interleaved operations":
    for i in 0..100:
      var builder1 = newArrayBuilder[int32]()
      let arr1 = newArray[int32](@[1'i32, 2, 3])
      var builder2 = newArrayBuilder[float64]()
      let arr2 = newArray[float64](@[1.0, 2.0])
      
      builder1.append(10'i32)
      builder2.append(10.0)
      
      let result1 = builder1.finish()
      let result2 = builder2.finish()
      
      check result1.len == 1
      check result2.len == 1
  
  test "Copy chains":
    let original = newArray[int32](@[1'i32, 2, 3, 4, 5])
    for i in 0..100:
      let copy1 = original
      let copy2 = copy1
      let copy3 = copy2
      let copy4 = copy3
      
      check copy4.len == 5
      check copy4[0] == 1
  
  test "Slice chains":
    let original = newArray[int32](@[1'i32, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    for i in 0..100:
      let slice1 = original[2..8]
      let slice2 = slice1[1..5]
      check slice2.len == 5
  
  test "Mixed builders and arrays":
    for i in 0..100:
      var builder = newArrayBuilder[int32]()
      let arr = newArray[int32](@[1'i32, 2, 3])
      
      for val in arr:
        builder.append(val)
      
      builder.appendValues(@[4'i32, 5, 6])
      let res = builder.finish()
      check res.len == 6
  
  test "Array string conversion stress":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    for i in 0..1000:
      let str = $arr
      check str.len > 0
  
  test "Rapid allocation deallocation":
    for cycle in 0..10:
      var arrays: seq[Array[int32]]
      for i in 0..999:
        arrays.add(newArray[int32](@[i.int32]))
      # All destroyed when arrays goes out of scope
  
  test "Builder append patterns":
    for i in 0..1000:
      var builder = newArrayBuilder[int32]()
      
      # Individual appends
      builder.append(1'i32)
      builder.append(2'i32)
      
      # Batch append
      builder.appendValues(@[3'i32, 4, 5])
      
      # Null append
      builder.appendNull()
      
      # More individual
      builder.append(6'i32)
      
      let arr = builder.finish()
      check arr.len == 7
      check arr.isNull(5)
