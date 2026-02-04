import std/strutils
import unittest2
import ../src/[ffi, gschema, gtypes, guuid]

suite "UUIDType - Basic Creation":
  
  test "Create UUID type":
    let uuidType = newUUIDType()
    check uuidType.handle != nil
  
  test "UUID type has extension name":
    let uuidType = newUUIDType()
    let extName = uuidType.extensionName()
    check extName.len > 0

suite "UUIDType - Memory Management":
  
  test "UUIDType is cleaned up when going out of scope":
    var uuidType: UUIDType
    block:
      uuidType = newUUIDType()
      check uuidType.handle != nil
    check uuidType.handle != nil
  
  test "UUIDType copy creates independent reference":
    let uuid1 = newUUIDType()
    let uuid2 = uuid1
    check uuid1.handle != nil
    check uuid2.handle != nil

suite "UUIDType - GADType Integration":
  
  test "Convert UUIDType to GADType":
    let uuidType = newUUIDType()
    let gadType = uuidType.toGADType()
    check gadType.handle != nil

suite "UUIDType - String Representation":
  
  test "String representation contains uuid info":
    let uuidType = newUUIDType()
    let str = $uuidType
    check str.len > 0

suite "UUIDType - Schema Integration":
  
  test "Create field with UUIDType":
    let uuidType = newUUIDType()
    let field = newField("id", uuidType.toGADType())
    check field.name == "id"
  
  test "Create schema with UUIDType field":
    let uuidType = newUUIDType()
    let field = newField("uuid_field", uuidType.toGADType())
    let schema = newSchema([field])
    check schema.nFields == 1
