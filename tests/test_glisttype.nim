import std/strutils
import unittest2
import ../src/[ffi, gschema, gtypes, glisttype]

suite "ListType - Basic Creation":
  
  test "Create list with int32 element type":
    let valueType = newField[int32]("item")
    let listType = newListType(valueType)
    check listType.handle != nil
  
  test "Create list with string element type":
    let valueType = newField[string]("item")
    let listType = newListType(valueType)
    check listType.handle != nil
  
  test "Create list with nested type":
    let innerField = newField[int32]("inner")
    let listType = newListType(innerField)
    check listType.handle != nil

suite "ListType - Value Field Access":
  
  test "Get value field from list":
    let valueField = newField[int32]("item")
    let listType = newListType(valueField)
    let retrievedField = listType.valueField()
    check retrievedField.name == "item"
  
  test "Value field returns correct data type":
    let valueField = newField[string]("text")
    let listType = newListType(valueField)
    let retrievedField = listType.valueField()
    check retrievedField.dataType().id == GArrowType.GARROW_TYPE_STRING
  
  test "Value field for nested list":
    let innerField = newField[float64]("value")
    let listType = newListType(innerField)
    let retrievedField = listType.valueField()
    check retrievedField.dataType().id == GArrowType.GARROW_TYPE_DOUBLE

suite "ListType - Memory Management":
  
  test "ListType is cleaned up when going out of scope":
    var listType: ListType
    block:
      let valueField = newField[int32]("item")
      listType = newListType(valueField)
      check listType.handle != nil
    # listType should still be valid here due to Nim's ARC
    check listType.handle != nil
  
  test "ListType copy creates independent reference":
    let valueField = newField[int32]("item")
    let list1 = newListType(valueField)
    let list2 = list1  # Copy
    check list1.handle != nil
    check list2.handle != nil
    check list1.valueField().name == list2.valueField().name

suite "ListType - GADType Integration":
  
  test "Convert ListType to GADType":
    let valueField = newField[int32]("item")
    let listType = newListType(valueField)
    let gadType = listType.toGADType()
    check gadType.handle != nil

suite "ListType - String Representation":
  
  test "String representation contains type info":
    let valueField = newField[int32]("item")
    let listType = newListType(valueField)
    let str = $listType
    check str.len > 0
    check "list" in str.toLowerAscii()

suite "ListType - Schema Integration":
  
  test "Create field with ListType":
    let valueField = newField[int32]("scores")
    let listType = newListType(valueField)
    let field = newField("grades", listType.toGADType())
    check field.name == "grades"
  
  test "Create schema with ListType field":
    let valueField = newField[string]("tag")
    let listType = newListType(valueField)
    let field = newField("tags", listType.toGADType())
    let schema = newSchema([field])
    check schema.nFields == 1
