import std/[sequtils, strutils]
import unittest2
import ../src/narrow

suite "Test Arrow Functions - Discovery":
  test "listFunctions returns available functions":
    let functions = listFunctions()
    unittest2.check functions.len > 0
    # Check that we have some basic arithmetic functions
    let names = functions.mapIt(it.name)
    unittest2.check "add" in names
    unittest2.check "subtract" in names
    unittest2.check "multiply" in names

  test "find returns function by name":
    let addFn = find("add")
    unittest2.check addFn.name == "add"

  test "find raises ValueError for unknown function":
    expect ValueError:
      discard find("nonexistent_function_xyz")

  test "function equality":
    let add1 = find("add")
    let add2 = find("add")
    let sub = find("subtract")
    unittest2.check add1 == add2
    unittest2.check add1 != sub

  test "function toString":
    let addFn = find("add")
    let s = $addFn
    unittest2.check strutils.contains(s, "add")

suite "Test Arrow Functions - Execution":
  test "execute add on two int32 arrays returns Datum":
    let a = newDatum(newArray(@[1'i32, 2, 3]))
    let b = newDatum(newArray(@[10'i32, 20, 30]))

    let addFn = find("add")
    let output = addFn.execute([a, b])
    unittest2.check output == newDatum(newArray(@[11'i32, 22, 33]))

    unittest2.check output.isArray
    unittest2.check output.kind == DatumKind.dkArray

  test "call convenience function returns Datum":
    let a = newDatum(newArray(@[5'i32, 10, 15]))
    let b = newDatum(newArray(@[2'i32, 3, 4]))

    let output = call("multiply", [a, b], FunctionOptions(), nil)

    unittest2.check output.isArray
    unittest2.check output.kind == DatumKind.dkArray
    unittest2.check output == newDatum(newArray(@[10'i32, 30, 60]))

  test "call with single argument returns Datum":
    let arr = newDatum(newArray(@[1.0'f64, 4.0, 9.0]))

    let res = call("sqrt", arr)

    unittest2.check res == newDatum(newArray(@[1.0'f64, 2.0, 3.0]))
    unittest2.check res.isArray
    unittest2.check res.kind == DatumKind.dkArray

  test "execute sum aggregation returns scalar Datum":
    let arr = newDatum(newArray(@[1'i32, 2, 3, 4, 5]))

    let sumFn = find("sum")
    let res = sumFn.execute([arr])

    unittest2.check res.isScalar
    unittest2.check res.kind == DatumKind.dkScalar

    let scalar = res.toScalar()
    # Sum of int32 returns int64 to avoid overflow
    unittest2.check scalar.getInt64() == 15
