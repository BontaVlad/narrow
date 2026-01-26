import unittest2
import std/[strutils]
import ../src/[ffi, gschema, gstruct, garray]

suite "Struct - Basic Creation and Field Access":
  
  test "Create struct with multiple fields":
    let fieldAge = newField[int32]("age")
    let fieldAlive = newField[bool]("alive")
    let fieldName = newField[string]("name")
    let struct = newStruct(@[fieldAge, fieldAlive, fieldName])
    check struct.hasField("age")
    check struct.hasField("alive")
    check struct.hasField("name")
    check not struct.hasField("nonexistent")

  test "Access struct fields by name":
    let fieldAge = newField[int32]("age")
    let fieldName = newField[string]("name")
    let struct = newStruct(@[fieldAge, fieldName])
    let f1 = struct["age"]
    let f2 = struct.age
    check f1.name == f2.name
    check f1 == f2

  test "Access struct fields by index":
    let fieldAge = newField[int32]("age")
    let fieldName = newField[string]("name")
    let fieldActive = newField[bool]("active")
    let struct = newStruct(@[fieldAge, fieldName, fieldActive])
    check struct[0].name == "age"
    check struct[1].name == "name"
    check struct[2].name == "active"

  test "Get field index":
    let fieldAge = newField[int32]("age")
    let fieldName = newField[string]("name")
    let struct = newStruct(@[fieldAge, fieldName])
    check struct.fieldIndex("age") == 0
    check struct.fieldIndex("name") == 1
    check struct.fieldIndex("nonexistent") == -1

  test "Get field count":
    let fields = @[
      newField[int32]("id"),
      newField[string]("name"),
      newField[bool]("active")
    ]
    let struct = newStruct(fields)
    check struct.fieldCount == 3

  test "Get all fields from struct":
    let fields = @[
      newField[int32]("id"),
      newField[string]("name"),
      newField[bool]("active")
    ]
    let struct = newStruct(fields)
    let structFields = struct.fields
    check structFields.len == 3
    check structFields[0].name == "id"
    check structFields[1].name == "name"
    check structFields[2].name == "active"

  test "String representation":
    let struct = newStruct(@[newField[int32]("age"), newField[string]("name")])
    let str = $struct
    check str.contains("age")
    check str.contains("name")

  test "Access nonexistent field by name raises error":
    let struct = newStruct(@[newField[int32]("age")])
    expect(KeyError):
      discard struct["nonexistent"]

  test "Access field by invalid index raises error":
    let struct = newStruct(@[newField[int32]("age")])
    expect(IndexDefect):
      discard struct[10]
    expect(IndexDefect):
      discard struct[-1]

suite "StructArray - Creation and Operations":

  test "Create struct array from fields":
    let structType = newStruct(@[newField[int32]("id"), newField[string]("name")])
    let idArray = newArray[int32](@[1'i32, 2, 3])
    let nameArray = newArray[string](@["Alice", "Bob", "Charlie"])
    let structArray = newStructArray(structType, idArray.toPtr, nameArray.toPtr)
    check structArray.len == 3

  test "Get field from struct array":
    let structType = newStruct(@[newField[int32]("id"), newField[string]("name")])
    let idArray = newArray[int32](@[1'i32, 2, 3])
    let nameArray = newArray[string](@["Alice", "Bob", "Charlie"])
    let structArray = newStructArray(structType, idArray.toPtr, nameArray.toPtr)
    let idFieldPtr = structArray.getField(0)
    let len = garrow_array_get_length(idFieldPtr)
    check len == 3

suite "StructBuilder - Building Struct Arrays":

  test "Create struct builder and build array":
    let structType = newStruct(@[newField[int32]("id"), newField[bool]("active")])
    var builder = newStructBuilder(structType)
    builder.append()
    builder.append()
    builder.appendNull()
    let res = builder.finish()
    check res.len == 3

  test "Builder with multiple appends":
    let structType = newStruct(@[newField[int32]("value")])
    var builder = newStructBuilder(structType)
    for i in 1..5:
      builder.append()
    let res = builder.finish()
    check res.len == 5

  test "Builder with nulls":
    let structType = newStruct(@[newField[int32]("value")])
    var builder = newStructBuilder(structType)
    builder.append()
    builder.appendNull()
    builder.append()
    let res = builder.finish()
    check res.len == 3

  test "String representation of struct array":
    let structType = newStruct(@[newField[int32]("id")])
    var builder = newStructBuilder(structType)
    builder.append()
    builder.append()
    let res = builder.finish()
    let str = $res
    check str.len > 0
