import std/[options, strutils]
import unittest2
import ../src/[ffi, gschema, gtypes]

suite "Field - Creation and Properties":
  test "Create field with primitive types":
    let intField = newField[int32]("id")
    let floatField = newField[float64]("value")
    let stringField = newField[string]("name")
    let boolField = newField[bool]("active")
    
    check intField.name == "id"
    check floatField.name == "value"
    check stringField.name == "name"
    check boolField.name == "active"

  test "Field data type retrieval":
    let intField = newField[int32]("count")
    let floatField = newField[float64]("price")
    let stringField = newField[string]("description")
    
    check $intField.dataType == "int32"
    check $floatField.dataType == "double"
    check $stringField.dataType == "utf8"

  test "Field equality":
    let field1 = newField[int32]("id")
    let field2 = newField[int32]("id")
    let field3 = newField[int64]("id")
    let field4 = newField[int32]("other")
    
    check field1 == field2
    check field1 != field3  # Different types
    check field1 != field4  # Different names

  test "Field string representation":
    let field = newField[int32]("age")
    let repr = $field
    check "age" in repr
    check "int32" in repr

  test "Field with GADType":
    let gtype = newGType(int64)
    let field = newField("timestamp", gtype)
    check field.name == "timestamp"
    check $field.dataType == "int64"

suite "Schema - Creation and Basic Operations":
  test "Create schema with multiple fields":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
      newField[float64]("score")
    ])
    check schema.nFields == 3
    check schema.len == 3

  test "Create empty schema":
    let schema = newSchema([])
    check schema.nFields == 0
    check schema.len == 0

  test "Duplicate field names rejected":
    expect(ValueError):
      discard newSchema([
        newField[int32]("id"),
        newField[string]("id")
      ])

  test "Schema string representation":
    let schema = newSchema([
      newField[int32]("x"),
      newField[float64]("y")
    ])
    let repr = $schema
    check "x" in repr
    check "y" in repr

  test "Schema equality":
    let schema1 = newSchema([
      newField[int32]("a"),
      newField[string]("b")
    ])
    let schema2 = newSchema([
      newField[int32]("a"),
      newField[string]("b")
    ])
    let schema3 = newSchema([
      newField[int64]("a"),
      newField[string]("b")
    ])
    
    check schema1 == schema2
    check schema1 != schema3

suite "Schema - Field Access":
  test "Get field by index":
    let schema = newSchema([
      newField[int32]("first"),
      newField[string]("second"),
      newField[bool]("third")
    ])
    
    check schema.getField(0).name == "first"
    check schema.getField(1).name == "second"
    check schema.getField(2).name == "third"
    check schema[0].name == "first"
    check schema[1].name == "second"

  test "Get field by name":
    let schema = newSchema([
      newField[int32]("user_id"),
      newField[string]("username"),
      newField[float64]("rating")
    ])
    
    check schema.getFieldByName("user_id").name == "user_id"
    check schema.getFieldByName("username").name == "username"
    check schema["user_id"].name == "user_id"
    check schema["rating"].name == "rating"

  test "Field not found raises KeyError":
    let schema = newSchema([newField[int32]("only_field")])
    expect(KeyError):
      discard schema.getFieldByName("missing")
    expect(KeyError):
      discard schema["missing"]

  test "Safe field access with Option":
    let schema = newSchema([
      newField[int32]("existing")
    ])
    
    let found = schema.tryGetField("existing")
    let notFound = schema.tryGetField("nonexistent")
    let outOfBounds = schema.tryGetField(5)
    
    check found.isSome
    check found.get().name == "existing"
    check notFound.isNone
    check outOfBounds.isNone

  test "Get field index":
    let schema = newSchema([
      newField[int32]("alpha"),
      newField[string]("beta"),
      newField[bool]("gamma")
    ])
    
    check schema.getFieldIndex("alpha") == 0
    check schema.getFieldIndex("beta") == 1
    check schema.getFieldIndex("gamma") == 2

  test "Get field index for nonexistent field raises":
    let schema = newSchema([newField[int32]("lonely")])
    expect(KeyError):
      discard schema.getFieldIndex("ghost")

suite "Schema - Collection Operations":
  test "Get all fields as sequence":
    let schema = newSchema([
      newField[int32]("one"),
      newField[int64]("two"),
      newField[float32]("three")
    ])
    
    let fields = schema.ffields
    check fields.len == 3
    check fields[0].name == "one"
    check fields[1].name == "two"
    check fields[2].name == "three"

  test "Iterate over schema fields":
    let schema = newSchema([
      newField[int32]("a"),
      newField[string]("b"),
      newField[bool]("c")
    ])
    
    var names: seq[string]
    for field in schema:
      names.add(field.name)
    
    check names == @["a", "b", "c"]

  test "Iterate over empty schema":
    let schema = newSchema([])
    var count = 0
    for _ in schema:
      count += 1
    check count == 0

suite "Schema - Complex Types":
  test "Schema with various primitive types":
    let schema = newSchema([
      newField[bool]("flag"),
      newField[int8]("tiny"),
      newField[int16]("small"),
      newField[int32]("medium"),
      newField[int64]("large"),
      newField[uint8]("utiny"),
      newField[uint16]("usmall"),
      newField[uint32]("umedium"),
      newField[uint64]("ularge"),
      newField[float32]("single"),
      newField[float64]("double"),
      newField[string]("text")
    ])
    
    check schema.nFields == 12
    check schema["flag"].dataType.isCompatible(bool)
    check schema["large"].dataType.isCompatible(int64)
    check schema["double"].dataType.isCompatible(float64)
    check schema["text"].dataType.isCompatible(string)

suite "Schema - Memory Management":
  test "Schema copy semantics":
    let original = newSchema([
      newField[int32]("x"),
      newField[string]("y")
    ])
    
    var copy = original
    check copy == original
    check copy.nFields == original.nFields

  test "Multiple schemas from same field definitions":
    let idField = newField[int32]("id")
    let nameField = newField[string]("name")
    
    let schema1 = newSchema([idField, nameField])
    let schema2 = newSchema([idField, nameField])
    
    check schema1 == schema2

  test "Create and destroy many schemas":
    for i in 0..<1000:
      let schema = newSchema([
        newField[int32]("id"),
        newField[string]("name"),
        newField[float64]("value")
      ])
      check schema.nFields == 3

suite "Schema - Error Handling":
  test "Access field at negative index":
    let schema = newSchema([newField[int32]("solo")])
    # Note: getField with negative index may cause segfault per FIXME comment
    check schema.tryGetField(-1).isNone

  test "Access field beyond bounds":
    let schema = newSchema([
      newField[int32]("first"),
      newField[string]("second")
    ])
    check schema.tryGetField(10).isNone

  test "Empty schema field access":
    let schema = newSchema([])
    check schema.tryGetField(0).isNone
    check schema.tryGetField("anything").isNone

  test "Schema with many fields":
    var fields: seq[Field]
    for i in 0..<100:
      fields.add(newField[int32]("field_" & $i))
    
    let schema = newSchema(fields)
    check schema.nFields == 100
    check schema["field_50"].name == "field_50"
    check schema.getFieldIndex("field_99") == 99

suite "Field - Edge Cases":
  test "Field with special characters in name":
    let field1 = newField[int32]("field_with_underscores")
    let field2 = newField[string]("Field.With.Dots")
    let field3 = newField[float64]("field-with-dashes")
    
    check field1.name == "field_with_underscores"
    check field2.name == "Field.With.Dots"
    check field3.name == "field-with-dashes"

  test "Field with unicode name":
    let field = newField[int32]("字段")
    check field.name == "字段"

  test "Field preservation through schema operations":
    let originalField = newField[int32]("preserved")
    let schema = newSchema([originalField])
    let retrievedField = schema["preserved"]
    
    check retrievedField.name == "preserved"
    check $retrievedField.dataType == "int32"
    check retrievedField == originalField
