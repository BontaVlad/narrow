import std/options
import unittest2
import ../src/narrow/[core/ffi, compute/filters, column/primitive, column/primitive]

suite "Filters - Creation":

  test "Set and get null selection behavior":
    var options = newFilterOptions()
    check options.nullSelectionBehavior == Drop
    options.nullSelectionBehavior = EmitNull
    check options.nullSelectionBehavior == EmitNull
    options.nullSelectionBehavior = Drop
    check options.nullSelectionBehavior == Drop

suite "BooleanArray - Creation":

  test "Create BooleanArray from sequence":
    let arr = newBooleanArray(@[true, false, true, false])
    check arr.len == 4
    check arr[0] == true
    check arr[1] == false
    check arr[2] == true
    check arr[3] == false

  test "Create BooleanArray from Array[bool]":
    let arr = newArray[bool](@[true, false, true])
    let boolArr = newBooleanArray(arr)
    check boolArr.len == 3
    check boolArr[0] == true
    check boolArr[1] == false
    check boolArr[2] == true

  test "Create BooleanArray with nulls":
    let arr = newBooleanArray(@[true, false, true, false], @[false, false, true, false])
    check arr.len == 4
    check arr[0] == true
    check arr[1] == false
    check arr.isNull(2) == true
    check arr[3] == false

  test "Create BooleanArray from Options":
    let arr = newBooleanArray(@[some(true), some(false), none(bool), some(true)])
    check arr.len == 4
    check arr[0] == true
    check arr[1] == false
    check arr.isNull(2) == true
    check arr[3] == true

  test "BooleanArray iteration":
    let arr = newBooleanArray(@[true, false, true])
    var results: seq[bool] = @[]
    for val in arr:
      results.add(val)
    check results == @[true, false, true]

  test "BooleanArray toSeq":
    let arr = newBooleanArray(@[true, false, true])
    check arr.toSeq == @[true, false, true]
    check @arr == @[true, false, true]

suite "ChunkedArray - Filter":

  test "Filter ChunkedArray with BooleanArray (Drop nulls)":
    # Create chunked array: [[2, 2, 4], [4, 5, 100]]
    let chunks = [
      newArray[int32](@[2'i32, 2, 4]),
      newArray[int32](@[4'i32, 5, 100])
    ]
    let nLegs = newChunkedArray(chunks)
    
    # Create mask: [True, False, None, True, False, True]
    let mask = newBooleanArray(@[true, false, true, true, false, true], @[false, false, true, false, false, false])
    
    # Filter with Drop behavior (default)
    var options = newFilterOptions()
    options.nullSelectionBehavior = Drop
    let filtered = nLegs.filter(mask, options)
    
    # Result should be: [[2], [4, 100]]
    check filtered.len == 3
    check filtered[0] == 2'i32
    check filtered[1] == 4'i32
    check filtered[2] == 100'i32

  test "Filter ChunkedArray with BooleanArray (EmitNull)":
    # Create chunked array: [[2, 2, 4], [4, 5, 100]]
    let chunks = [
      newArray[int32](@[2'i32, 2, 4]),
      newArray[int32](@[4'i32, 5, 100])
    ]
    let nLegs = newChunkedArray(chunks)
    
    # Create mask: [True, False, None, True, False, True]
    let mask = newBooleanArray(@[true, false, true, true, false, true], @[false, false, true, false, false, false])
    
    # Filter with EmitNull behavior
    var options = newFilterOptions()
    options.nullSelectionBehavior = EmitNull
    let filtered = nLegs.filter(mask, options)
    
    # Result should be: [[2, null], [4, 100]]
    check filtered.len == 4
    check filtered[0] == 2'i32
    check filtered.isNull(1) == true
    check filtered[2] == 4'i32
    check filtered[3] == 100'i32

  test "Filter ChunkedArray with BooleanArray (convenience overload)":
    let chunks = [
      newArray[int32](@[1'i32, 2, 3, 4, 5])
    ]
    let arr = newChunkedArray(chunks)
    let mask = newBooleanArray(@[true, false, true, false, true])
    
    let filtered = arr.filter(mask)
    check filtered.len == 3
    check filtered[0] == 1'i32
    check filtered[1] == 3'i32
    check filtered[2] == 5'i32

  test "Filter ChunkedArray with ChunkedArray mask":
    let chunks = [
      newArray[int32](@[1'i32, 2, 3]),
      newArray[int32](@[4'i32, 5, 6])
    ]
    let arr = newChunkedArray(chunks)
    
    let maskChunks = [
      newArray[bool](@[true, false, true]),
      newArray[bool](@[false, true, false])
    ]
    let mask = newChunkedArray(maskChunks)
    
    let filtered = arr.filter(mask)
    check filtered.len == 3
    check filtered[0] == 1'i32
    check filtered[1] == 3'i32
    check filtered[2] == 5'i32

suite "Array - Filter":

  test "Filter Array with BooleanArray":
    let arr = newArray[int32](@[1'i32, 2, 3, 4, 5])
    let mask = newBooleanArray(@[true, false, true, false, true])
    
    let filtered = arr.filter(mask)
    check filtered.len == 3
    check filtered[0] == 1'i32
    check filtered[1] == 3'i32
    check filtered[2] == 5'i32

  test "Filter Array with BooleanArray and options":
    let arr = newArray[int32](@[1'i32, 2, 3])
    let mask = newBooleanArray(@[true, false, true], @[false, true, false])
    
    var options = newFilterOptions()
    options.nullSelectionBehavior = Drop
    let filtered = arr.filter(mask, options)
    
    check filtered.len == 2
    check filtered[0] == 1'i32
    check filtered[1] == 3'i32

suite "Table - Filter":

  test "Filter Table with BooleanArray":
    # This test would require creating a table first
    # Skipping for now as it requires more setup
    skip()
