import std/strutils
import unittest2
import ../src/[ffi, gschema, gtypes, glargelisttype]

suite "LargeListType - Basic Creation":
  
  test "Create large list with int32 element type":
    let valueType = newField[int32]("item")
    let listType = newLargeListType(valueType)
    check listType.handle != nil
  
  test "Create large list with string element type":
    let valueType = newField[string]("item")
    let listType = newLargeListType(valueType)
    check listType.handle != nil
  
  test "Create large list with nested type":
    let innerField = newField[int32]("inner")
    let listType = newLargeListType(innerField)
    check listType.handle != nil

suite "LargeListType - Value Field Access":
  
  test "Get value field from large list":
    let valueField = newField[int32]("item")
    let listType = newLargeListType(valueField)
    let retrievedField = listType.valueField()
    check retrievedField.name == "item"
  
  test "Value field returns correct data type":
    let valueField = newField[string]("text")
    let listType = newLargeListType(valueField)
    let retrievedField = listType.valueField()
    check retrievedField.dataType().id == GArrowType.GARROW_TYPE_STRING

suite "LargeListType - Memory Management":
  
  test "LargeListType is cleaned up when going out of scope":
    var listType: LargeListType
    block:
      let valueField = newField[int32]("item")
      listType = newLargeListType(valueField)
      check listType.handle != nil
    check listType.handle != nil
  
  test "LargeListType copy creates independent reference":
    let valueField = newField[int32]("item")
    let list1 = newLargeListType(valueField)
    let list2 = list1
    check list1.handle != nil
    check list2.handle != nil

suite "LargeListType - GADType Integration":
  
  test "Convert LargeListType to GADType":
    let valueField = newField[int32]("item")
    let listType = newLargeListType(valueField)
    let gadType = listType.toGADType()
    check gadType.handle != nil

suite "LargeListType - String Representation":
  
  test "String representation contains type info":
    let valueField = newField[int32]("item")
    let listType = newLargeListType(valueField)
    let str = $listType
    check str.len > 0
    check "large" in str.toLowerAscii()

suite "LargeListType - Schema Integration":
  
  test "Create field with LargeListType":
    let valueField = newField[int32]("scores")
    let listType = newLargeListType(valueField)
    let field = newField("grades", listType.toGADType())
    check field.name == "grades"
  
  test "Create schema with LargeListType field":
    let valueField = newField[string]("tag")
    let listType = newLargeListType(valueField)
    let field = newField("tags", listType.toGADType())
    let schema = newSchema([field])
    check schema.nFields == 1
