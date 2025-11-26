
import unittest2
import ../src/[ffi, gtypes]

suite "GArrow types":

  test "Test basic types":
    let gBoolType = newGType(bool)
    check $gBoolType == "bool"

    let gInt8Type = newGType(int8)
    check $gInt8Type == "int8"

    let gUint8Type = newGType(uint8)
    check $gUint8Type == "uint8"

    let gInt16Type = newGType(int16)
    check $gInt16Type == "int16"

    let gUint16Type = newGType(uint16)
    check $gUint16Type == "uint16"

    let gInt32Type = newGType(int32)
    check $gInt32Type == "int32"

    let gUint32Type = newGType(uint32)
    check $gUint32Type == "uint32"

    let gInt64Type = newGType(int64)
    check $gInt64Type == "int64"

    let gIntType = newGType(int)
    check $gIntType == "int64"

    let gUint64Type = newGType(uint64)
    check $gUint64Type == "uint64"

    let gFloat32Type = newGType(float32)
    check $gFloat32Type == "float"

    let gFloat64Type = newGType(float64)
    check $gFloat64Type == "double"

    let gStringType = newGType(string)
    check $gStringType == "utf8"

    let gBytesType = newGType(seq[byte])
    check $gBytesType == "binary"

    let gCstringType = newGType(cstring)
    check $gCstringType == "large_utf8"

suite "GArrow types - Memory Stress Tests":

  test "Multiple allocations and deallocations":
    # Create and destroy many instances
    for i in 0..1000:
      let gType = newGType(int32)
      check $gType == "int32"
    # All should be automatically destroyed

  test "Nested scope allocations":
    # Test destruction in nested scopes
    block:
      let outer = newGType(bool)
      block:
        let inner = newGType(int64)
        check $inner == "int64"
      # inner destroyed here
      check $outer == "bool"
    # outer destroyed here

  test "Assignment and copying":
    var gType1 = newGType(int32)
    check $gType1 == "int32"
    
    # Test copy
    var gType2 = gType1
    check $gType2 == "int32"
    
    # Both should be valid
    check $gType1 == "int32"
    check $gType2 == "int32"
    
    # Reassign gType1
    gType1 = newGType(int32)
    check $gType1 == "int32"
    check $gType2 == "int32"  # gType2 should still be valid

  test "Array of types":
    var types: array[10, GADType[int32]]
    for i in 0..9:
      types[i] = newGType(int32)
      check $types[i] == "int32"
    
    # All should be valid
    for i in 0..9:
      check $types[i] == "int32"

  test "Sequence of types":
    var types: seq[GADType[int64]]
    for i in 0..99:
      types.add(newGType(int64))
    
    # All should be valid
    for i in 0..99:
      check $types[i] == "int64"
    
    # Clear and check
    types.setLen(0)

  test "Self-assignment":
    var gType = newGType(string)
    let originalHandle = gType.handle
    gType = gType  # Self-assignment
    check $gType == "utf8"
    # Handle might change due to copy semantics, but object should be valid

  test "Reassignment loop":
    var gType = newGType(int8)
    for i in 0..100:
      gType = newGType(int8)
      check $gType == "int8"

  test "Mixed type allocations":
    var types: seq[GADType[int32]]
    for i in 0..50:
      if i mod 2 == 0:
        types.add(newGType(int32))
      else:
        let temp = newGType(int32)
        types.add(temp)
    
    for t in types:
      check $t == "int32"

  test "GString memory management":
    let tp = newGType(int32)
    for i in 0..1000:
      let myStr = garrow_data_type_get_name(tp.handle)
      let gStr = newGString(cstring(myStr))
      check $gStr == "int32"

  test "Interleaved GADType and GString allocations":
    for i in 0..100:
      let gType = newGType(bool)
      let gStr = newGString(garrow_data_type_get_name(gType.handle))
      check $gStr == "bool"
      check $gType == "bool"

  test "Stress test with all types":
    for i in 0..100:
      block:
        let gBool = newGType(bool)
        let gInt8 = newGType(int8)
        let gUint8 = newGType(uint8)
        let gInt16 = newGType(int16)
        let gUint16 = newGType(uint16)
        let gInt32 = newGType(int32)
        let gUint32 = newGType(uint32)
        let gInt64 = newGType(int64)
        let gUint64 = newGType(uint64)
        let gFloat32 = newGType(float32)
        let gFloat64 = newGType(float64)
        let gString = newGType(string)
        let gBytes = newGType(seq[byte])
        let gCstring = newGType(cstring)
        
        check $gBool == "bool"
        check $gInt8 == "int8"
        check $gUint8 == "uint8"
        check $gInt16 == "int16"
        check $gUint16 == "uint16"
        check $gInt32 == "int32"
        check $gUint32 == "uint32"
        check $gInt64 == "int64"
        check $gUint64 == "uint64"
        check $gFloat32 == "float"
        check $gFloat64 == "double"
        check $gString == "utf8"
        check $gBytes == "binary"
        check $gCstring == "large_utf8"

  test "Deep copy chain":
    let original = newGType(int32)
    var copy1 = original
    var copy2 = copy1
    var copy3 = copy2
    var copy4 = copy3
    
    check $original == "int32"
    check $copy1 == "int32"
    check $copy2 == "int32"
    check $copy3 == "int32"
    check $copy4 == "int32"


  test "Nil handle safety":
    var gType: GADType[int32]
    # Default initialized, handle should be nil
    # Destroying should not crash
    
  test "Copy from sequence":
    var types: seq[GADType[string]]
    for i in 0..9:
      types.add(newGType(string))
    
    # Copy from sequence
    let copied = types[5]
    check $copied == "utf8"
    check $types[5] == "utf8"

  test "Return value optimization":
    proc createType(): GADType[int32] =
      result = newGType(int32)
    
    for i in 0..100:
      let gType = createType()
      check $gType == "int32"

  test "Temporary object lifespan":
    for i in 0..1000:
      check $newGType(bool) == "bool"
      # Temporary should be destroyed immediately after use

  test "Complex nesting and copying":
    var outer: seq[seq[GADType[int32]]]
    for i in 0..9:
      var inner: seq[GADType[int32]]
      for j in 0..9:
        inner.add(newGType(int32))
      outer.add(inner)
    
    for i in 0..9:
      for j in 0..9:
        check $outer[i][j] == "int32"

suite "GArrow types - Edge Cases":

  test "Rapid creation and destruction":
    # Simulate rapid allocation/deallocation
    for cycle in 0..10:
      var temp: seq[GADType[int64]]
      for i in 0..999:
        temp.add(newGType(int64))
      # All destroyed when temp goes out of scope

  test "Copy during iteration":
    var original: seq[GADType[float64]]
    for i in 0..99:
      original.add(newGType(float64))
    
    var copied: seq[GADType[float64]]
    for item in original:
      copied.add(item)
    
    check original.len == copied.len
    for i in 0..99:
      check $original[i] == "double"
      check $copied[i] == "double"

  test "Overwrite in loop":
    var gType = newGType(uint32)
    for i in 0..1000:
      let newType = newGType(uint32)
      gType = newType  # Should properly cleanup old gType
      check $gType == "uint32"
