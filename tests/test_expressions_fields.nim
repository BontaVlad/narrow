import unittest2
import std/[sets, sequtils, algorithm]
import ../src/narrow

suite "Expression - Field Reference Extraction":
  test "extract fields from simple comparison":
    let expr = col("age") >= 18'i32
    let fields = extractFieldReferences(expr)
    unittest2.check fields == @["age"]

  test "extract fields from AND expression":
    let expr = (col("age") >= 18'i32) and (col("name") == "Alice")
    let fields = extractFieldReferences(expr).sorted
    unittest2.check fields == @["age", "name"]

  test "extract fields deduplicates":
    let expr = (col("x") > 1'i32) and (col("x") < 10'i32)
    let fields = extractFieldReferences(expr)
    unittest2.check fields == @["x"]

  test "extract fields from literal-only returns empty":
    let expr = newLiteralExpression(42'i32)
    let fields = extractFieldReferences(expr)
    unittest2.check fields.len == 0

  test "complex nested expression":
    let expr = ((col("a") > 1'i32) or (col("b") < 5'i32)) and (col("c") == 3'i32)
    let fields = extractFieldReferences(expr).sorted
    unittest2.check fields == @["a", "b", "c"]

  test "fieldName extraction from FieldExpression":
    let expr = col("age")
    unittest2.check fieldName(expr) == "age"

  test "fieldName on comparison expression returns the field":
    # Comparison expressions have one referenced field
    let expr = col("age") > 18'i32
    # This works because there's exactly one field referenced
    unittest2.check fieldName(expr) == "age"
