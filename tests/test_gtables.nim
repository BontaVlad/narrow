import std/[strutils]
import unittest2
import ../src/[gtables, gchunkedarray, garray, gtypes]

suite "Field - Basic Operations":
  
  test "Create and destroy field":
    let field = newField[int32]("test_field")
    check field.isValid
    check field.name == "test_field"
  
  test "Field equality":
    let field1 = newField[int32]("col1")
    let field2 = newField[int32]("col1")
    let field3 = newField[float64]("col1")
    
    check field1 == field2
    check field1 != field3
  
  test "Field to string":
    let field = newField[int32]("age")
    let str = $field
    check str.len > 0
    check "age" in str
  
  test "Field copying":
    let original = newField[int32]("original")
    let copy = original
    check copy.isValid
    check copy.name == "original"
    check copy == original

suite "Schema - Basic Operations":
  
  test "Create schema from fields":
    let ffields = [
      newField[int32]("id"),
      newField[string]("name"),
      newField[float64]("value")
    ]
    
    let schema = newSchema(ffields)
    check schema.isValid
    check schema.nFields == 3
  
  test "Schema field access":
    let schema = newSchema([
      newField[int32]("a"),
      newField[int32]("b"),
      newField[int32]("c")
    ])
    
    check schema.getField(0).name == "a"
    check schema.getField(1).name == "b"
    check schema.getField(2).name == "c"
  
  test "Schema field by name":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name")
    ])
    
    let field = schema.getFieldByName("name")
    check field.isValid
    check field.name == "name"
  
  test "Schema field index":
    let schema = newSchema([
      newField[int32]("x"),
      newField[int32]("y"),
      newField[int32]("z")
    ])
    
    check schema.getFieldIndex("x") == 0
    check schema.getFieldIndex("y") == 1
    check schema.getFieldIndex("z") == 2
    check schema.getFieldIndex("missing") < 0
  
  test "Schema iteration":
    let schema = newSchema([
      newField[int32]("a"),
      newField[int32]("b"),
      newField[int32]("c")
    ])
    
    var names: seq[string]
    for field in schema:
      names.add(field.name)
    
    check names == @["a", "b", "c"]
  
  test "Schema to string":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name")
    ])
    
    let str = $schema
    check str.len > 0
  
  test "Schema equality":
    let schema1 = newSchema([newField[int32]("a"), newField[int32]("b")])
    let schema2 = newSchema([newField[int32]("a"), newField[int32]("b")])
    let schema3 = newSchema([newField[int32]("a"), newField[float64]("b")])
    
    check schema1 == schema2
    check schema1 != schema3
  
  test "Empty schema":
    let schema = newSchema([])
    check schema.isValid
    check schema.nFields == 0

suite "Field - Memory Tests":
  
  test "Create many fields":
    for i in 0..1000:
      let field = newField[int32]("field_" & $i)
      check field.isValid
  
  test "Field copy chains":
    let original = newField[int32]("original")
    for i in 0..100:
      let copy1 = original
      let copy2 = copy1
      let copy3 = copy2
      check copy3.name == "original"
  
  test "Field string conversion stress":
    let field = newField[int32]("test")
    for i in 0..1000:
      let str = $field
      check str.len > 0

suite "Schema - Memory Tests":
  
  test "Create many schemas":
    for i in 0..1000:
      let schema = newSchema([
        newField[int32]("a"),
        newField[int32]("b")
      ])
      check schema.nFields == 2
  
  test "Schema with many fields":
    var fields: seq[Field]
    for i in 0..99:
      fields.add(newField[int32]("col_" & $i))
    
    let schema = newSchema(fields)
    check schema.nFields == 100
  
  test "Schema field iteration stress":
    let schema = newSchema([
      newField[int32]("a"),
      newField[int32]("b"),
      newField[int32]("c")
    ])
    
    for cycle in 0..1000:
      var count = 0
      for field in schema:
        count.inc
      check count == 3
  
  test "Schema copying":
    let original = newSchema([
      newField[int32]("x"),
      newField[int32]("y")
    ])
    
    for i in 0..1000:
      let copy = original
      check copy.nFields == 2
  
  test "Schema field access stress":
    let schema = newSchema([
      newField[int32]("a"),
      newField[int32]("b"),
      newField[int32]("c"),
      newField[int32]("d"),
      newField[int32]("e")
    ])
    
    for i in 0..1000:
      for j in 0..4:
        let field = schema.getField(j)
        check field.isValid
