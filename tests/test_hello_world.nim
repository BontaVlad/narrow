import unittest2
import ../src/[ffi]

suite "Test Suite configuration":

  test "Test say hello":
    let builder = garrow_string_array_builder_new()

    echo repr builder
