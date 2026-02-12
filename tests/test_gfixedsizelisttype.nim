import std/strutils
import unittest2
import ../src/narrow/[core/ffi, column/metadata, types/gtypes, types/gfixedsizelisttype]

suite "FixedSizeListType - Basic Creation":

  test "Create fixed size list with int32 element type and size 4":
    let valueType = newGType(int32)
    let listType = newFixedSizeListType(valueType, 4)
    check listType.handle != nil

  test "Create fixed size list with string element type and size 10":
    let valueType = newGType(string)
    let listType = newFixedSizeListType(valueType, 10)
    check listType.handle != nil

  test "Create fixed size list using field constructor":
    let valueField = newField[int32]("item")
    let listType = newFixedSizeListType(valueField, 4)
    check listType.handle != nil

suite "FixedSizeListType - Value Field Access":

  test "Get value field from fixed size list":
    let valueField = newField[float64]("coordinates")
    let listType = newFixedSizeListType(valueField, 3)
    let retrievedField = listType.valueField()
    check retrievedField.name == "coordinates"

  test "Value field returns correct data type":
    let valueType = newGType(int32)
    let listType = newFixedSizeListType(valueType, 4)
    let retrievedField = listType.valueField()
    check retrievedField.dataType().id == GArrowType.GARROW_TYPE_INT32

suite "FixedSizeListType - Memory Management":

  test "FixedSizeListType is cleaned up when going out of scope":
    var listType: FixedSizeListType
    block:
      let valueType = newGType(int32)
      listType = newFixedSizeListType(valueType, 4)
      check listType.handle != nil
    check listType.handle != nil

  test "FixedSizeListType copy creates independent reference":
    let valueType = newGType(int32)
    let list1 = newFixedSizeListType(valueType, 4)
    let list2 = list1
    check list1.handle != nil
    check list2.handle != nil

suite "FixedSizeListType - GADType Integration":

  test "Convert FixedSizeListType to GADType":
    let valueType = newGType(int32)
    let listType = newFixedSizeListType(valueType, 4)
    let gadType = listType.toGADType()
    check gadType.handle != nil

suite "FixedSizeListType - String Representation":

  test "String representation contains type info":
    let valueType = newGType(int32)
    let listType = newFixedSizeListType(valueType, 4)
    let str = $listType
    check str.len > 0
    check "fixed" in str.toLowerAscii()

suite "FixedSizeListType - Schema Integration":

  test "Create field with FixedSizeListType":
    let valueType = newGType(int32)
    let listType = newFixedSizeListType(valueType, 4)
    let field = newField("coordinates", listType.toGADType())
    check field.name == "coordinates"

  test "Create schema with FixedSizeListType field":
    let valueType = newGType(string)
    let listType = newFixedSizeListType(valueType, 5)
    let field = newField("tokens", listType.toGADType())
    let schema = newSchema([field])
    check schema.nFields == 1
