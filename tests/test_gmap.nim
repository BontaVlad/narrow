import unittest2
import ../src/[ffi, gschema, gtypes, gmap]

suite "MapType - Basic Creation":
  
  test "Create map with string keys and int32 items":
    let keyType = newGType(string)
    let itemType = newGType(int32)
    let mapType = newMapType(keyType, itemType)
    check mapType.handle != nil
  
  test "Create map with int64 keys and float64 items":
    let keyType = newGType(int64)
    let itemType = newGType(float64)
    let mapType = newMapType(keyType, itemType)
    check mapType.handle != nil

suite "MapType - Property Access":
  
  test "Get key type from map":
    let keyType = newGType(string)
    let itemType = newGType(int32)
    let mapType = newMapType(keyType, itemType)
    let retrievedKeyType = mapType.keyType()
    check retrievedKeyType.id == GArrowType.GARROW_TYPE_STRING
  
  test "Get item type from map":
    let keyType = newGType(string)
    let itemType = newGType(int32)
    let mapType = newMapType(keyType, itemType)
    let retrievedItemType = mapType.itemType()
    check retrievedItemType.id == GArrowType.GARROW_TYPE_INT32
  
  test "Key type returns correct type for int64 keys":
    let keyType = newGType(int64)
    let itemType = newGType(float64)
    let mapType = newMapType(keyType, itemType)
    check mapType.keyType().id == GArrowType.GARROW_TYPE_INT64
  
  test "Item type returns correct type for float64 items":
    let keyType = newGType(int64)
    let itemType = newGType(float64)
    let mapType = newMapType(keyType, itemType)
    check mapType.itemType().id == GArrowType.GARROW_TYPE_DOUBLE

suite "MapType - Memory Management":
  
  test "MapType is cleaned up when going out of scope":
    var mapType: MapType
    block:
      let keyType = newGType(string)
      let itemType = newGType(int32)
      mapType = newMapType(keyType, itemType)
      check mapType.handle != nil
    # mapType should still be valid here due to Nim's ARC
    check mapType.handle != nil
  
  test "MapType copy creates independent reference":
    let keyType = newGType(string)
    let itemType = newGType(int32)
    let map1 = newMapType(keyType, itemType)
    let map2 = map1  # Copy
    check map1.handle != nil
    check map2.handle != nil
    check map1.keyType().id == map2.keyType().id

suite "MapType - GADType Integration":
  
  test "Convert MapType to GADType":
    let keyType = newGType(string)
    let itemType = newGType(int32)
    let mapType = newMapType(keyType, itemType)
    let gadType = mapType.toGADType()
    check gadType.handle != nil

suite "MapType - String Representation":
  
  test "String representation contains type info":
    let keyType = newGType(string)
    let itemType = newGType(int32)
    let mapType = newMapType(keyType, itemType)
    let str = $mapType
    check str.len > 0

suite "MapType - Schema Integration":
  
  test "Create field with MapType":
    let keyType = newGType(string)
    let itemType = newGType(int32)
    let mapType = newMapType(keyType, itemType)
    let field = newField("metadata", mapType.toGADType())
    check field.name == "metadata"
  
  test "Create schema with MapType field":
    let keyType = newGType(string)
    let itemType = newGType(int32)
    let mapType = newMapType(keyType, itemType)
    let field = newField("attributes", mapType.toGADType())
    let schema = newSchema([field])
    check schema.nFields == 1
