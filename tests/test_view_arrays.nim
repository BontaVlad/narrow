import unittest2
import ../src/narrow

suite "StringViewArray - Type Checks":
  test "arcGObject generates proper hooks":
    var sv1 = StringViewArray(handle: nil)
    check isNil(sv1.handle)
    var sv2 = sv1
    check isNil(sv2.handle)
    var sv3 = StringViewArray(handle: nil)
    sv3 = sv1
    check isNil(sv3.handle)

  test "data type can be created":
    let dt = garrow_string_view_data_type_new()
    check not isNil(dt)
    g_object_unref(dt)

suite "BinaryViewArray - Type Checks":
  test "arcGObject generates proper hooks":
    var bv1 = BinaryViewArray(handle: nil)
    check isNil(bv1.handle)
    var bv2 = bv1
    check isNil(bv2.handle)
    var bv3 = BinaryViewArray(handle: nil)
    bv3 = bv1
    check isNil(bv3.handle)

  test "data type can be created":
    let dt = garrow_binary_view_data_type_new()
    check not isNil(dt)
    g_object_unref(dt)
