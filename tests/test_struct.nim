import unittest2
import ../src/[ffi, gschema, gstruct, glist]

suite "Struct - Basic Creation":
  
  test "Create int32 field":
    let fieldAge = newField[int32]("age")
    let fieldAlive = newField[bool]("alive")
    let fieldName = newField[string]("name")
    let fieldCount = newField[int8]("count")
    let struct = newStruct(@[fieldAge, fieldAlive, fieldName, fieldCount])
    check struct.age == struct["age"]
    check struct.alive == struct["alive"]
    echo struct
