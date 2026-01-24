import unittest2
import ../src/[ffi, gschema, gtypes]

suite "Field - Basic Creation":
  
  test "Create int32 field":
    let field = newField[int32]("age")
    check field.name == "age"
  
  test "Create int64 field":
    let field = newField[int64]("count")
    check field.name == "count"
  
  test "Create float64 field":
    let field = newField[float64]("price")
    check field.name == "price"
  
  test "Create string field":
    let field = newField[string]("name")
    check field.name == "name"
  
  test "Create boolean field":
    let field = newField[bool]("is_active")
    check field.name == "is_active"
  
  test "Create uint32 field":
    let field = newField[uint32]("id")
    check field.name == "id"
  
  test "Create float32 field":
    let field = newField[float32]("temperature")
    check field.name == "temperature"

suite "Field - Properties":
  
  test "Field name property":
    let field = newField[int32]("user_id")
    check field.name == "user_id"
  
  test "Field with empty name":
    let field = newField[int32]("")
    check field.name == ""
  
  test "Field with special characters in name":
    let field = newField[int32]("user-id_123")
    check field.name == "user-id_123"
  
  test "Field with unicode in name":
    let field = newField[int32]("用户ID")
    check field.name == "用户ID"
  
  test "Field data type property":
    let field = newField[int32]("count")
    let dataType = field.dataType()
    check dataType.id == GArrowType.GARROW_TYPE_INT32

# suite "Field - String Representation":
  
#   test "String representation of int32 field":
#     let field = newField[int32]("age")
#     let str = $field
#     check str.len > 0
#     check "age" in str
  
#   test "String representation of string field":
#     let field = newField[string]("name")
#     let str = $field
#     check str.len > 0
#     check "name" in str
  
#   test "String representation of float64 field":
#     let field = newField[float64]("price")
#     let str = $field
#     check str.len > 0
#     check "price" in str
  
#   test "String representation of boolean field":
#     let field = newField[bool]("active")
#     let str = $field
#     check str.len > 0
#     check "active" in str

# suite "Field - Equality":
  
#   test "Equal fields with same name and type":
#     let field1 = newField[int32]("age")
#     let field2 = newField[int32]("age")
#     check field1 == field2
  
#   test "Not equal fields with different names":
#     let field1 = newField[int32]("age")
#     let field2 = newField[int32]("count")
#     check field1 != field2
  
#   test "Not equal fields with different types":
#     let field1 = newField[int32]("value")
#     let field2 = newField[float64]("value")
#     check field1 != field2
  
#   test "Not equal fields with different names and types":
#     let field1 = newField[int32]("age")
#     let field2 = newField[string]("name")
#     check field1 != field2
  
#   test "Field equality with same instance":
#     let field = newField[int32]("age")
#     check field == field

# suite "Field - Memory Management":
  
#   test "Create and destroy many fields":
#     for i in 0..1000:
#       let field = newField[int32]("field_" & $i)
#       check field.name == "field_" & $i
  
#   test "Field copying":
#     let original = newField[int32]("original")
#     for i in 0..1000:
#       let copy1 = original
#       let copy2 = copy1
#       check copy2.name == "original"
  
#   test "Multiple fields of different types":
#     for i in 0..100:
#       let intField = newField[int32]("int_field")
#       let floatField = newField[float64]("float_field")
#       let strField = newField[string]("str_field")
#       let boolField = newField[bool]("bool_field")
      
#       check intField.name == "int_field"
#       check floatField.name == "float_field"
#       check strField.name == "str_field"
#       check boolField.name == "bool_field"

# suite "Schema - Basic Creation":
  
#   test "Create empty schema":
#     let schema = newSchema([])
#     check schema.nFields == 0
  
#   test "Create schema with single field":
#     let field = newField[int32]("age")
#     let schema = newSchema([field])
#     check schema.nFields == 1
  
#   test "Create schema with multiple fields":
#     let fields = [
#       newField[int32]("id"),
#       newField[string]("name"),
#       newField[float64]("price")
#     ]
#     let schema = newSchema(fields)
#     check schema.nFields == 3
  
#   test "Create schema with many fields":
#     var fields: seq[Field]
#     for i in 0..99:
#       fields.add(newField[int32]("field_" & $i))
#     let schema = newSchema(fields)
#     check schema.nFields == 100
  
#   test "Create schema with different types":
#     let fields = [
#       newField[int8]("int8_field"),
#       newField[int16]("int16_field"),
#       newField[int32]("int32_field"),
#       newField[int64]("int64_field"),
#       newField[uint8]("uint8_field"),
#       newField[uint16]("uint16_field"),
#       newField[uint32]("uint32_field"),
#       newField[uint64]("uint64_field"),
#       newField[float32]("float32_field"),
#       newField[float64]("float64_field"),
#       newField[bool]("bool_field"),
#       newField[string]("string_field")
#     ]
#     let schema = newSchema(fields)
#     check schema.nFields == 12

# suite "Schema - Field Access":
  
#   test "Get field by index":
#     let fields = [
#       newField[int32]("id"),
#       newField[string]("name"),
#       newField[float64]("price")
#     ]
#     let schema = newSchema(fields)
    
#     let field0 = schema.getField(0)
#     let field1 = schema.getField(1)
#     let field2 = schema.getField(2)
    
#     check field0.name == "id"
#     check field1.name == "name"
#     check field2.name == "price"
  
#   test "Get field by name":
#     let fields = [
#       newField[int32]("id"),
#       newField[string]("name"),
#       newField[float64]("price")
#     ]
#     let schema = newSchema(fields)
    
#     let idField = schema.getFieldByName("id")
#     let nameField = schema.getFieldByName("name")
#     let priceField = schema.getFieldByName("price")
    
#     check idField.name == "id"
#     check nameField.name == "name"
#     check priceField.name == "price"
  
#   test "Get field index by name":
#     let fields = [
#       newField[int32]("id"),
#       newField[string]("name"),
#       newField[float64]("price")
#     ]
#     let schema = newSchema(fields)
    
#     check schema.getFieldIndex("id") == 0
#     check schema.getFieldIndex("name") == 1
#     check schema.getFieldIndex("price") == 2
  
#   test "Get field by name - non-existent":
#     let fields = [
#       newField[int32]("id"),
#       newField[string]("name")
#     ]
#     let schema = newSchema(fields)
    

#     expect(IndexDefect):
#       discard schema.getFieldByName("nonexistent")
  
#   test "Get field index - non-existent":
#     let fields = [
#       newField[int32]("id"),
#       newField[string]("name")
#     ]
#     let schema = newSchema(fields)
    
#     expect(IndexDefect):
#       discard schema.getFieldIndex("nonexistent")

# suite "Schema - Field Collection":
  
#   test "Get all fields":
#     let fields = [
#       newField[int32]("id"),
#       newField[string]("name"),
#       newField[float64]("price")
#     ]
#     let schema = newSchema(fields)
    
#     let allFields = schema.ffields
#     check allFields.len == 3
#     check allFields[0].name == "id"
#     check allFields[1].name == "name"
#     check allFields[2].name == "price"
  
#   test "Get fields from empty schema":
#     let schema = newSchema([])
#     let allFields = schema.ffields
#     check allFields.len == 0
  
#   test "Get fields from large schema":
#     var fields: seq[Field]
#     for i in 0..99:
#       fields.add(newField[int32]("field_" & $i))
#     let schema = newSchema(fields)
    
#     let allFields = schema.ffields
#     check allFields.len == 100
#     for i in 0..99:
#       check allFields[i].name == "field_" & $i

# suite "Schema - Iteration":
  
#   test "Iterate over schema fields":
#     let fields = [
#       newField[int32]("id"),
#       newField[string]("name"),
#       newField[float64]("price")
#     ]
#     let schema = newSchema(fields)
    
#     var names: seq[string]
#     for field in schema:
#       names.add(field.name)
    
#     check names == @["id", "name", "price"]
  
#   test "Iterate over empty schema":
#     let schema = newSchema([])
    
#     var count = 0
#     for field in schema:
#       count += 1
    
#     check count == 0
  
# suite "Schema - String Representation":
  
#   test "String representation of simple schema":
#     let fields = [
#       newField[int32]("id"),
#       newField[string]("name")
#     ]
#     let schema = newSchema(fields)
#     let str = $schema
    
#     check str.len > 0
#     check "id" in str
#     check "name" in str
  
#   test "String representation of empty schema":
#     let schema = newSchema([])
#     let str = $schema
#     check str.len == 0
#     check str == ""

# suite "Schema - Equality":
  
#   test "Equal schemas with same fields":
#     let fields1 = [
#       newField[int32]("id"),
#       newField[string]("name")
#     ]
#     let fields2 = [
#       newField[int32]("id"),
#       newField[string]("name")
#     ]
    
#     let schema1 = newSchema(fields1)
#     let schema2 = newSchema(fields2)
    
#     check schema1 == schema2
  
#   test "Not equal schemas with different field count":
#     let fields1 = [
#       newField[int32]("id"),
#       newField[string]("name")
#     ]
#     let fields2 = [
#       newField[int32]("id")
#     ]
    
#     let schema1 = newSchema(fields1)
#     let schema2 = newSchema(fields2)
    
#     check schema1 != schema2
  
#   test "Not equal schemas with different field names":
#     let fields1 = [
#       newField[int32]("id"),
#       newField[string]("name")
#     ]
#     let fields2 = [
#       newField[int32]("id"),
#       newField[string]("title")
#     ]
    
#     let schema1 = newSchema(fields1)
#     let schema2 = newSchema(fields2)
    
#     check schema1 != schema2
  
#   test "Not equal schemas with different field types":
#     let fields1 = [
#       newField[int32]("id"),
#       newField[string]("name")
#     ]
#     let fields2 = [
#       newField[int32]("id"),
#       newField[int32]("name")
#     ]
    
#     let schema1 = newSchema(fields1)
#     let schema2 = newSchema(fields2)
    
#     check schema1 != schema2
  
#   test "Not equal schemas with different field order":
#     let fields1 = [
#       newField[int32]("id"),
#       newField[string]("name")
#     ]
#     let fields2 = [
#       newField[string]("name"),
#       newField[int32]("id")
#     ]
    
#     let schema1 = newSchema(fields1)
#     let schema2 = newSchema(fields2)
    
#     check schema1 != schema2
  
#   test "Schema equality with same instance":
#     let fields = [
#       newField[int32]("id"),
#       newField[string]("name")
#     ]
#     let schema = newSchema(fields)
    
#     check schema == schema
  
#   test "Equal empty schemas":
#     let schema1 = newSchema([])
#     let schema2 = newSchema([])
    
#     check schema1 == schema2

# suite "Schema - Memory Management":
  
#   test "Create and destroy many schemas":
#     for i in 0..1000:
#       let fields = [
#         newField[int32]("id"),
#         newField[string]("name")
#       ]
#       let schema = newSchema(fields)
#       check schema.nFields == 2
  
#   test "Schema copying":
#     let fields = [
#       newField[int32]("id"),
#       newField[string]("name")
#     ]
#     let original = newSchema(fields)
    
#     for i in 0..1000:
#       let copy1 = original
#       let copy2 = copy1
#       check copy2.nFields == 2
  
#   test "Field reuse across schemas":
#     let idField = newField[int32]("id")
#     let nameField = newField[string]("name")
    
#     let schema1 = newSchema([idField, nameField])
#     let schema2 = newSchema([idField])
#     let schema3 = newSchema([nameField])
    
#     check schema1.nFields == 2
#     check schema2.nFields == 1
#     check schema3.nFields == 1
  
# suite "Schema - Edge Cases":
  
#   test "Schema with single field":
#     let schema = newSchema([newField[int32]("id")])
#     check schema.nFields == 1
#     check schema.getField(0).name == "id"
  
#   test "Schema with duplicate field names":
#     let fields = [
#       newField[int32]("id"),
#       newField[string]("id"),  # Same name, different type
#     ]
#     expect(ValueError):
#       discard newSchema(fields)
  
#   test "Schema with empty field names":
#     let fields = [
#       newField[int32](""),
#       newField[string](""),
#     ]
#     expect(ValueError):
#       discard newSchema(fields)
  
# suite "Schema - Field Access Edge Cases":
  
#   test "Access first field":
#     let fields = [
#       newField[int32]("first"),
#       newField[string]("second"),
#       newField[float64]("third")
#     ]
#     let schema = newSchema(fields)
    
#     let first = schema.getField(0)
#     check first.name == "first"
  
#   test "Access last field":
#     let fields = [
#       newField[int32]("first"),
#       newField[string]("second"),
#       newField[float64]("third")
#     ]
#     let schema = newSchema(fields)
    
#     let last = schema.getField(2)
#     check last.name == "third"
  
#   test "Access middle field":
#     let fields = [
#       newField[int32]("first"),
#       newField[string]("second"),
#       newField[float64]("third")
#     ]
#     let schema = newSchema(fields)
    
#     let middle = schema.getField(1)
#     check middle.name == "second"
  
#   test "Get field by name case sensitivity":
#     let fields = [
#       newField[int32]("Name"),
#       newField[string]("name"),
#       newField[float64]("NAME")
#     ]
#     let schema = newSchema(fields)
    
#     # Each should be treated as different field
#     check schema.getFieldByName("Name").name == "Name"
#     check schema.getFieldByName("name").name == "name"
#     check schema.getFieldByName("NAME").name == "NAME"

# suite "Schema - Complex Scenarios":
  
#   test "User table schema":
#     let schema = newSchema([
#       newField[int32]("id"),
#       newField[string]("username"),
#       newField[string]("email"),
#       newField[int64]("created_at"),
#       newField[bool]("is_active")
#     ])
    
#     check schema.nFields == 5
#     check schema.getFieldByName("username").name == "username"
#     check schema.getFieldIndex("email") == 2
  
# suite "Schema - Integration with Fields":
  
#   test "Multiple schemas sharing same fields":
#     let field1 = newField[int32]("id")
#     let field2 = newField[string]("name")
    
#     let schema1 = newSchema([field1, field2])
#     let schema2 = newSchema([field1])
#     let schema3 = newSchema([field2])
    
#     check schema1.nFields == 2
#     check schema2.nFields == 1
#     check schema3.nFields == 1
    
#     check schema1.getField(0).name == "id"
#     check schema2.getField(0).name == "id"
#     check schema3.getField(0).name == "name"
