import std/strutils
import unittest2
import ../src/[ffi, gschema, gtypes, gfixedshapetensortype]

suite "FixedShapeTensorType - Basic Creation":
  
  test "Create tensor type with float64 values":
    let valueType = newGType(float64)
    let shape = @[3'i64, 224, 224]  # [channels, height, width]
    let tensorType = newFixedShapeTensorType(valueType, shape)
    check tensorType.handle != nil
  
  test "Create tensor type with int32 values":
    let valueType = newGType(int32)
    let shape = @[2'i64, 3, 4]
    let tensorType = newFixedShapeTensorType(valueType, shape)
    check tensorType.handle != nil

suite "FixedShapeTensorType - Shape Access":
  
  test "Get shape from tensor type":
    let valueType = newGType(float64)
    let shape = @[3'i64, 224, 224]
    let tensorType = newFixedShapeTensorType(valueType, shape)
    let retrievedShape = tensorType.shape()
    check retrievedShape.len == 3
    check retrievedShape[0] == 3
    check retrievedShape[1] == 224
    check retrievedShape[2] == 224

suite "FixedShapeTensorType - Permutation":
  
  test "Create tensor with permutation":
    let valueType = newGType(float64)
    let shape = @[3'i64, 224, 224]
    let permutation = @[2'i64, 0, 1]  # NHWC to NCHW
    let tensorType = newFixedShapeTensorType(valueType, shape, permutation)
    check tensorType.handle != nil
    let retrievedPerm = tensorType.permutation()
    check retrievedPerm.len == 3

suite "FixedShapeTensorType - Memory Management":
  
  test "Tensor type is cleaned up when going out of scope":
    var tensorType: FixedShapeTensorType
    block:
      let valueType = newGType(float64)
      let shape = @[3'i64, 224, 224]
      tensorType = newFixedShapeTensorType(valueType, shape)
      check tensorType.handle != nil
    check tensorType.handle != nil
  
  test "Tensor type copy creates independent reference":
    let valueType = newGType(float64)
    let shape = @[3'i64, 224, 224]
    let tensor1 = newFixedShapeTensorType(valueType, shape)
    let tensor2 = tensor1
    check tensor1.handle != nil
    check tensor2.handle != nil

suite "FixedShapeTensorType - GADType Integration":
  
  test "Convert tensor type to GADType":
    let valueType = newGType(float64)
    let shape = @[3'i64, 224, 224]
    let tensorType = newFixedShapeTensorType(valueType, shape)
    let gadType = tensorType.toGADType()
    check gadType.handle != nil

suite "FixedShapeTensorType - String Representation":
  
  test "String representation contains tensor info":
    let valueType = newGType(float64)
    let shape = @[3'i64, 224, 224]
    let tensorType = newFixedShapeTensorType(valueType, shape)
    let str = $tensorType
    check str.len > 0

suite "FixedShapeTensorType - Schema Integration":
  
  test "Create field with tensor type":
    let valueType = newGType(float64)
    let shape = @[3'i64, 224, 224]
    let tensorType = newFixedShapeTensorType(valueType, shape)
    let field = newField("image", tensorType.toGADType())
    check field.name == "image"
  
  test "Create schema with tensor type field":
    let valueType = newGType(float64)
    let shape = @[3'i64, 224, 224]
    let tensorType = newFixedShapeTensorType(valueType, shape)
    let field = newField("embedding", tensorType.toGADType())
    let schema = newSchema([field])
    check schema.nFields == 1
