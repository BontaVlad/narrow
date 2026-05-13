import unittest2
import std/[options, strutils]
import ../src/narrow/[column/primitive, column/nested, column/metadata]

# ============================================================================
# ListArray
# ============================================================================

suite "ListArray - Creation and Basic Properties":

  test "Create list array with builder":
    var builder = newListArrayBuilder[int32]()
    var vb = builder.valueBuilder()
    builder.append()
    vb.append(1'i32)
    vb.append(2'i32)
    builder.append()
    vb.append(3'i32)
    builder.appendNull()
    let arr = builder.finish()
    check arr.len == 3

  test "valueAt returns correct arrays":
    var builder = newListArrayBuilder[int32]()
    var vb = builder.valueBuilder()
    builder.append()
    vb.append(1'i32)
    vb.append(2'i32)
    builder.append()
    vb.append(3'i32)
    builder.appendNull()
    let arr = builder.finish()

    let v0 = arr.valueAt(0)
    check v0.len == 2
    check v0[0] == 1'i32
    check v0[1] == 2'i32

    let v1 = arr.valueAt(1)
    check v1.len == 1
    check v1[0] == 3'i32

  test "valueLength and valueOffset":
    var builder = newListArrayBuilder[int32]()
    var vb = builder.valueBuilder()
    builder.append()
    vb.append(10'i32)
    vb.append(20'i32)
    vb.append(30'i32)
    builder.append()
    vb.append(40'i32)
    builder.appendNull()
    let arr = builder.finish()

    check arr.valueLength(0) == 3
    check arr.valueLength(1) == 1
    check arr.valueOffset(0) == 0
    check arr.valueOffset(1) == 3

  test "String list array":
    var builder = newListArrayBuilder[string]()
    var vb = builder.valueBuilder()
    builder.append()
    vb.append("hello")
    vb.append("world")
    builder.append()
    vb.append("foo")
    let arr = builder.finish()

    check arr.len == 2
    let v0 = arr.valueAt(0)
    check v0[0] == "hello"
    check v0[1] == "world"
    let v1 = arr.valueAt(1)
    check v1[0] == "foo"

  test "Empty list array":
    var builder = newListArrayBuilder[int32]()
    let arr = builder.finish()
    check arr.len == 0

  test "All-null list array":
    var builder = newListArrayBuilder[int32]()
    builder.appendNull()
    builder.appendNull()
    let arr = builder.finish()
    check arr.len == 2
    check arr.isNull(0)
    check arr.isNull(1)
    check arr.nNulls == 2

  test "Single-element lists":
    var builder = newListArrayBuilder[int32]()
    var vb = builder.valueBuilder()
    builder.append()
    vb.append(100'i32)
    builder.append()
    vb.append(200'i32)
    let arr = builder.finish()

    check arr.len == 2
    check arr.valueAt(0)[0] == 100'i32
    check arr.valueAt(1)[0] == 200'i32

suite "ListArray - Null Handling":

  test "isNull and isValid patterns":
    var builder = newListArrayBuilder[int32]()
    var vb = builder.valueBuilder()
    builder.append()
    vb.append(1'i32)
    builder.appendNull()
    builder.append()
    vb.append(2'i32)
    builder.appendNull()
    let arr = builder.finish()

    check arr.isValid(0)
    check arr.isNull(1)
    check arr.isValid(2)
    check arr.isNull(3)
    check arr.nNulls == 2

  test "tryGet with valid and null entries":
    var builder = newListArrayBuilder[int32]()
    var vb = builder.valueBuilder()
    builder.append()
    vb.append(1'i32)
    builder.appendNull()
    let arr = builder.finish()

    let opt0 = arr.tryGet(0)
    check opt0.isSome
    check opt0.get().len == 1
    check opt0.get()[0] == 1'i32

    let opt1 = arr.tryGet(1)
    check opt1.isNone

  test "tryGet out of bounds":
    var builder = newListArrayBuilder[int32]()
    builder.appendNull()
    let arr = builder.finish()
    check arr.tryGet(-1).isNone
    check arr.tryGet(99).isNone

suite "ListArray - Conversions and Iteration":

  test "toSeq converts to seq[seq[T]]":
    var builder = newListArrayBuilder[int32]()
    var vb = builder.valueBuilder()
    builder.append()
    vb.append(1'i32)
    vb.append(2'i32)
    builder.append()
    vb.append(3'i32)
    builder.appendNull()
    let arr = builder.finish()

    let s = arr.toSeq
    check s.len == 3
    check s[0] == @[1'i32, 2'i32]
    check s[1] == @[3'i32]
    check s[2].len == 0  # null becomes empty seq

  test "@ operator alias":
    var builder = newListArrayBuilder[int32]()
    var vb = builder.valueBuilder()
    builder.append()
    vb.append(7'i32)
    let arr = builder.finish()
    check @arr == @[@[7'i32]]

  test "items iterator skips nulls":
    var builder = newListArrayBuilder[int32]()
    var vb = builder.valueBuilder()
    builder.append()
    vb.append(1'i32)
    builder.appendNull()
    builder.append()
    vb.append(2'i32)
    let arr = builder.finish()

    var values: seq[int32]
    for lv in arr:
      values.add(lv[0])
    check values == @[1'i32, 2'i32]

  test "ListValue len and indexing":
    var builder = newListArrayBuilder[int32]()
    var vb = builder.valueBuilder()
    builder.append()
    vb.append(10'i32)
    vb.append(20'i32)
    vb.append(30'i32)
    let arr = builder.finish()

    let lv = arr.tryGet(0).get()
    check lv.len == 3
    check lv[0] == 10'i32
    check lv[1] == 20'i32
    check lv[2] == 30'i32

suite "ListArray - Equality and String":

  test "Equal list arrays":
    var b1 = newListArrayBuilder[int32]()
    var vb1 = b1.valueBuilder()
    b1.append()
    vb1.append(1'i32)
    vb1.append(2'i32)
    let arr1 = b1.finish()

    var b2 = newListArrayBuilder[int32]()
    var vb2 = b2.valueBuilder()
    b2.append()
    vb2.append(1'i32)
    vb2.append(2'i32)
    let arr2 = b2.finish()

    check arr1 == arr2

  test "Not equal list arrays":
    var b1 = newListArrayBuilder[int32]()
    var vb1 = b1.valueBuilder()
    b1.append()
    vb1.append(1'i32)
    let arr1 = b1.finish()

    var b2 = newListArrayBuilder[int32]()
    var vb2 = b2.valueBuilder()
    b2.append()
    vb2.append(2'i32)
    let arr2 = b2.finish()

    check arr1 != arr2

  test "String representation is non-empty":
    var builder = newListArrayBuilder[int32]()
    var vb = builder.valueBuilder()
    builder.append()
    vb.append(1'i32)
    let arr = builder.finish()
    let str = $arr
    check str.len > 0

suite "ListArray - Error Cases":

  test "valueAt out of bounds raises IndexDefect":
    var builder = newListArrayBuilder[int32]()
    builder.appendNull()
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr.valueAt(-1)
    expect(IndexDefect):
      discard arr.valueAt(99)

  test "isNull out of bounds raises IndexDefect":
    var builder = newListArrayBuilder[int32]()
    let arr = builder.finish()
    expect(IndexDefect):
      discard arr.isNull(0)

# ============================================================================
# MapArray
# ============================================================================

suite "MapArray - Creation and Basic Properties":

  test "Create map array from offsets keys and values":
    let offsets = newArray(@[0'i32, 2, 3])
    let keys = newArray(@["a", "b", "c"])
    let values = newArray(@[1'i32, 2, 3])
    let arr = newMapArray(offsets, keys, values)
    check arr.len == 2

  test "keys and items accessors":
    let offsets = newArray(@[0'i32, 2, 3])
    let keys = newArray(@["a", "b", "c"])
    let values = newArray(@[1'i32, 2, 3])
    let arr = newMapArray(offsets, keys, values)

    let allKeys = arr.keys
    let allItems = arr.items
    check allKeys.len == 3
    check allItems.len == 3
    check allKeys[0] == "a"
    check allItems[0] == 1'i32

  test "Empty map array":
    let offsets = newArray(@[0'i32])
    let keys = newArray(@[""])
    let values = newArray(@[0'i32])
    let arr = newMapArray(offsets, keys, values)
    check arr.len == 0

suite "MapArray - Null Handling":

  test "isNull and isValid":
    let offsets = newArray(@[0'i32, 2, 2])
    let keys = newArray(@["a", "b"])
    let values = newArray(@[1'i32, 2])
    let arr = newMapArray(offsets, keys, values)

    check arr.isValid(0)
    check arr.isValid(1)
    check arr.nNulls == 0

  test "tryGet valid and out of bounds":
    let offsets = newArray(@[0'i32, 1])
    let keys = newArray(@["x"])
    let values = newArray(@[42'i32])
    let arr = newMapArray(offsets, keys, values)

    let opt0 = arr.tryGet(0)
    check opt0.isSome
    check opt0.get().keys[0] == "x"
    check opt0.get().values[0] == 42'i32

    check arr.tryGet(-1).isNone
    check arr.tryGet(99).isNone

suite "MapArray - Conversions and Iteration":

  test "items iterator":
    let offsets = newArray(@[0'i32, 2, 3])
    let keys = newArray(@["a", "b", "c"])
    let values = newArray(@[1'i32, 2, 3])
    let arr = newMapArray(offsets, keys, values)

    var count = 0
    for entry in arr:
      count += 1
      check entry.keys.len > 0
    check count == 2

  test "toSeq":
    let offsets = newArray(@[0'i32, 2, 3])
    let keys = newArray(@["a", "b", "c"])
    let values = newArray(@[1'i32, 2, 3])
    let arr = newMapArray(offsets, keys, values)

    let s = arr.toSeq
    check s.len == 2
    check s[0].keys[0] == "a"
    check s[0].values[0] == 1'i32

  test "@ operator":
    let offsets = newArray(@[0'i32, 1])
    let keys = newArray(@["k"])
    let values = newArray(@[99'i32])
    let arr = newMapArray(offsets, keys, values)
    check @arr == arr.toSeq

suite "MapArray - Equality and String":

  test "Equal map arrays":
    let offsets = newArray(@[0'i32, 1])
    let keys = newArray(@["k"])
    let values = newArray(@[1'i32])
    let arr1 = newMapArray(offsets, keys, values)
    let arr2 = newMapArray(offsets, keys, values)
    check arr1 == arr2

  test "String representation":
    let offsets = newArray(@[0'i32, 1])
    let keys = newArray(@["k"])
    let values = newArray(@[1'i32])
    let arr = newMapArray(offsets, keys, values)
    check ($arr).len > 0

suite "MapArray - Error Cases":

  test "isNull out of bounds raises IndexDefect":
    let offsets = newArray(@[0'i32])
    let keys = newArray(@["k"])
    let values = newArray(@[1'i32])
    let arr = newMapArray(offsets, keys, values)
    expect(IndexDefect):
      discard arr.isNull(0)

# ============================================================================
# MapArrayBuilder
# ============================================================================

suite "MapArrayBuilder - Creation and Basic Operations":

  test "Create map array with builder":
    var builder = newMapArrayBuilder[string, int32]()
    var kb = builder.keyBuilder()
    var ib = builder.itemBuilder()

    builder.append()
    kb.append("a")
    ib.append(1'i32)
    kb.append("b")
    ib.append(2'i32)

    builder.append()
    kb.append("c")
    ib.append(3'i32)

    builder.appendNull()

    let arr = builder.finish()
    check arr.len == 3

  test "MapArrayBuilder values are correct":
    var builder = newMapArrayBuilder[string, int32]()
    var kb = builder.keyBuilder()
    var ib = builder.itemBuilder()

    builder.append()
    kb.append("x")
    ib.append(10'i32)
    kb.append("y")
    ib.append(20'i32)

    builder.append()
    kb.append("z")
    ib.append(30'i32)

    let arr = builder.finish()
    check arr.len == 2

    let allKeys = arr.keys
    let allItems = arr.items
    check allKeys.len == 3
    check allItems.len == 3
    check allKeys[0] == "x"
    check allItems[0] == 10'i32
    check allKeys[1] == "y"
    check allItems[1] == 20'i32
    check allKeys[2] == "z"
    check allItems[2] == 30'i32

  test "MapArrayBuilder with nulls":
    var builder = newMapArrayBuilder[string, int32]()
    var kb = builder.keyBuilder()
    var ib = builder.itemBuilder()

    builder.append()
    kb.append("a")
    ib.append(1'i32)

    builder.appendNull()

    builder.append()
    kb.append("b")
    ib.append(2'i32)

    let arr = builder.finish()
    check arr.len == 3
    check arr.isValid(0)
    check arr.isNull(1)
    check arr.isValid(2)
    check arr.nNulls == 1

  test "MapArrayBuilder int64 to float64":
    var builder = newMapArrayBuilder[int64, float64]()
    var kb = builder.keyBuilder()
    var ib = builder.itemBuilder()

    builder.append()
    kb.append(100'i64)
    ib.append(1.5'f64)

    builder.append()
    kb.append(200'i64)
    ib.append(2.5'f64)

    let arr = builder.finish()
    check arr.len == 2
    let allKeys = arr.keys
    let allItems = arr.items
    check allKeys[0] == 100'i64
    check allItems[0] == 1.5'f64

  test "Empty map array with builder":
    var builder = newMapArrayBuilder[string, int32]()
    let arr = builder.finish()
    check arr.len == 0

# ============================================================================
# Struct
# ============================================================================

suite "Struct - Field Access":

  test "Create struct with multiple fields":
    let fieldAge = newField[int32]("age")
    let fieldName = newField[string]("name")
    let structType = newStruct(@[fieldAge, fieldName])
    check structType.hasField("age")
    check structType.hasField("name")
    check not structType.hasField("nonexistent")

  test "Access fields by name and index":
    let structType = newStruct(@[newField[int32]("id"), newField[string]("name")])
    check structType["id"].name == "id"
    check structType[0].name == "id"
    check structType[1].name == "name"
    check structType.id.name == "id"

  test "fieldIndex and fieldCount":
    let structType = newStruct(@[newField[int32]("a"), newField[string]("b")])
    check structType.fieldIndex("a") == 0
    check structType.fieldIndex("b") == 1
    check structType.fieldIndex("missing") == -1
    check structType.fieldCount == 2

  test "Get all fields":
    let structType = newStruct(@[newField[int32]("x"), newField[bool]("y")])
    let flds = structType.fields
    check flds.len == 2
    check flds[0].name == "x"
    check flds[1].name == "y"

  test "Access nonexistent field raises KeyError":
    let structType = newStruct(@[newField[int32]("a")])
    expect(KeyError):
      discard structType["missing"]

  test "Access invalid index raises IndexDefect":
    let structType = newStruct(@[newField[int32]("a")])
    expect(IndexDefect):
      discard structType[10]
    expect(IndexDefect):
      discard structType[-1]

  test "String representation":
    let structType = newStruct(@[newField[int32]("age"), newField[string]("name")])
    let str = $structType
    check str.contains("age")
    check str.contains("name")

# ============================================================================
# StructArray
# ============================================================================

suite "StructArray - Creation and Field Access":

  test "Create struct array with real data":
    let structType = newStruct(@[newField[int32]("id"), newField[string]("name")])
    let idArray = newArray(@[1'i32, 2, 3])
    let nameArray = newArray(@["Alice", "Bob", "Charlie"])
    let sa = newStructArray(structType, idArray.toPtr, nameArray.toPtr)
    check sa.len == 3

  test "getField returns correct values":
    let structType = newStruct(@[newField[int32]("id"), newField[string]("name")])
    let idArray = newArray(@[10'i32, 20, 30])
    let nameArray = newArray(@["A", "B", "C"])
    let sa = newStructArray(structType, idArray.toPtr, nameArray.toPtr)

    let ids = sa.getField[:int32](0)
    check ids.len == 3
    check ids[0] == 10'i32
    check ids[1] == 20'i32
    check ids[2] == 30'i32

    let names = sa.getField[:string](1)
    check names[0] == "A"
    check names[1] == "B"
    check names[2] == "C"

  test "structType matches creation type":
    let structType = newStruct(@[newField[int32]("x")])
    let arr = newArray(@[1'i32, 2])
    let sa = newStructArray(structType, arr.toPtr)
    check sa.structType.fieldCount == 1
    check sa.structType.hasField("x")

  test "fields and fieldCount":
    let structType = newStruct(@[newField[int32]("a"), newField[string]("b"), newField[bool]("c")])
    let arr = newArray(@[1'i32, 2])
    let sa = newStructArray(structType, arr.toPtr, newArray(@["x", "y"]).toPtr, newArray(@[true, false]).toPtr)
    check sa.fieldCount == 3
    check sa.fieldIndex("b") == 1
    check sa.fieldIndex("missing") == -1

  test "Row slice with []":
    let structType = newStruct(@[newField[int32]("id")])
    let arr = newArray(@[100'i32, 200, 300])
    let sa = newStructArray(structType, arr.toPtr)

    let row0 = sa[0]
    check row0.len == 1
    let row0Ids = row0.getField[:int32](0)
    check row0Ids[0] == 100'i32

    let row2 = sa[2]
    let row2Ids = row2.getField[:int32](0)
    check row2Ids[0] == 300'i32

suite "StructArray - Null Handling":

  test "isNull and isValid with null rows":
    let structType = newStruct(@[newField[int32]("v")])
    let arr = newArray(@[1'i32, 2, 3])
    let sa = newStructArray(structType, arr.toPtr)

    check sa.isValid(0)
    check sa.isValid(1)
    check sa.isValid(2)
    check not sa.isNull(0)
    check sa.nNulls == 0

  test "tryGet valid and out of bounds":
    let structType = newStruct(@[newField[int32]("v")])
    let arr = newArray(@[42'i32])
    let sa = newStructArray(structType, arr.toPtr)

    let opt0 = sa.tryGet(0)
    check opt0.isSome
    check opt0.get().getField[:int32](0) == 42'i32

    check sa.tryGet(-1).isNone
    check sa.tryGet(99).isNone

suite "StructArray - StructRow":

  test "StructRow getField by index and name":
    let structType = newStruct(@[newField[int32]("id"), newField[string]("name")])
    let idArray = newArray(@[1'i32, 2])
    let nameArray = newArray(@["Alice", "Bob"])
    let sa = newStructArray(structType, idArray.toPtr, nameArray.toPtr)

    let row = sa.tryGet(0).get()
    check row.len == 2
    check row.getField[:int32](0) == 1'i32
    check row.getField[:string]("name") == "Alice"

  test "StructRow getField missing name raises KeyError":
    let structType = newStruct(@[newField[int32]("id")])
    let arr = newArray(@[1'i32])
    let sa = newStructArray(structType, arr.toPtr)
    let row = sa.tryGet(0).get()
    expect(KeyError):
      discard row.getField[:int32]("missing")

  test "StructRow string representation":
    let structType = newStruct(@[newField[int32]("id")])
    let arr = newArray(@[1'i32])
    let sa = newStructArray(structType, arr.toPtr)
    let row = sa.tryGet(0).get()
    let str = $row
    check str.len > 0
    check str.contains("StructRow")

suite "StructArray - Iteration and Conversion":

  test "items iterator":
    let structType = newStruct(@[newField[int32]("v")])
    let arr = newArray(@[10'i32, 20, 30])
    let sa = newStructArray(structType, arr.toPtr)

    var sum = 0'i32
    for row in sa:
      sum += row.getField[:int32](0)
    check sum == 60'i32

  test "toSeq":
    let structType = newStruct(@[newField[int32]("v")])
    let arr = newArray(@[5'i32, 10])
    let sa = newStructArray(structType, arr.toPtr)

    let s = sa.toSeq
    check s.len == 2
    check s[0].getField[:int32](0) == 5'i32
    check s[1].getField[:int32](0) == 10'i32

  test "@ operator":
    let structType = newStruct(@[newField[int32]("v")])
    let arr = newArray(@[7'i32])
    let sa = newStructArray(structType, arr.toPtr)
    check @sa == sa.toSeq

suite "StructArray - Equality and String":

  test "Equal struct arrays":
    let structType = newStruct(@[newField[int32]("v")])
    let arr = newArray(@[1'i32, 2])
    let sa1 = newStructArray(structType, arr.toPtr)
    let sa2 = newStructArray(structType, arr.toPtr)
    check sa1 == sa2

  test "Not equal struct arrays":
    let structType = newStruct(@[newField[int32]("v")])
    let arr1 = newArray(@[1'i32, 2])
    let arr2 = newArray(@[1'i32, 3])
    let sa1 = newStructArray(structType, arr1.toPtr)
    let sa2 = newStructArray(structType, arr2.toPtr)
    check sa1 != sa2

  test "String representation":
    let structType = newStruct(@[newField[int32]("v")])
    let arr = newArray(@[1'i32])
    let sa = newStructArray(structType, arr.toPtr)
    check ($sa).len > 0

suite "StructArray - Error Cases":

  test "getField negative index raises IndexDefect":
    let structType = newStruct(@[newField[int32]("v")])
    let arr = newArray(@[1'i32])
    let sa = newStructArray(structType, arr.toPtr)
    expect(IndexDefect):
      discard sa.getField[:int32](-1)

  test "[] out of bounds raises IndexDefect":
    let structType = newStruct(@[newField[int32]("v")])
    let arr = newArray(@[1'i32])
    let sa = newStructArray(structType, arr.toPtr)
    expect(IndexDefect):
      discard sa[-1]
    expect(IndexDefect):
      discard sa[99]

# ============================================================================
# StructBuilder
# ============================================================================

suite "StructBuilder - Creation and Basic Operations":

  test "Create struct builder and build empty array":
    let structType = newStruct(@[newField[int32]("id"), newField[bool]("active")])
    var builder = newStructBuilder(structType)
    builder.append()
    builder.append()
    builder.appendNull()
    let res = builder.finish()
    check res.len == 3

  test "fieldBuilder returns working child builder":
    let structType = newStruct(@[newField[int32]("id"), newField[string]("name")])
    var builder = newStructBuilder(structType)
    var idBuilder = builder.fieldBuilder[:int32](0)
    var nameBuilder = builder.fieldBuilder[:string](1)

    builder.append()
    idBuilder.append(1'i32)
    nameBuilder.append("Alice")

    builder.append()
    idBuilder.append(2'i32)
    nameBuilder.append("Bob")

    builder.appendNull()

    let arr = builder.finish()
    check arr.len == 3

    let ids = arr.getField[:int32](0)
    check ids[0] == 1'i32
    check ids[1] == 2'i32

    let names = arr.getField[:string](1)
    check names[0] == "Alice"
    check names[1] == "Bob"

  test "StructBuilder round-trip with mixed types":
    let structType = newStruct(@[
      newField[int32]("id"),
      newField[string]("name"),
      newField[bool]("active")
    ])
    var builder = newStructBuilder(structType)
    var idB = builder.fieldBuilder[:int32](0)
    var nameB = builder.fieldBuilder[:string](1)
    var activeB = builder.fieldBuilder[:bool](2)

    builder.append()
    idB.append(10'i32)
    nameB.append("X")
    activeB.append(true)

    builder.append()
    idB.append(20'i32)
    nameB.append("Y")
    activeB.append(false)

    builder.appendNull()

    builder.append()
    idB.append(30'i32)
    nameB.append("Z")
    activeB.append(true)

    let arr = builder.finish()
    check arr.len == 4
    check arr.nNulls == 1

    let ids = arr.getField[:int32](0)
    check ids[0] == 10'i32
    check ids[1] == 20'i32
    check ids[2] == 0'i32  # null row may have default value
    check ids[3] == 30'i32

    let active = arr.getField[:bool](2)
    check active[0] == true
    check active[1] == false
    check active[3] == true

  test "fieldBuilder negative index raises IndexDefect":
    let structType = newStruct(@[newField[int32]("v")])
    var builder = newStructBuilder(structType)
    expect(IndexDefect):
      discard builder.fieldBuilder[:int32](-1)

  test "fieldBuilder out of bounds raises KeyError":
    let structType = newStruct(@[newField[int32]("v")])
    var builder = newStructBuilder(structType)
    expect(KeyError):
      discard builder.fieldBuilder[:int32](99)
