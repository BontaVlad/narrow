import ./ffi, error

when isMainModule:
  let builder = garrow_string_array_builder_new()

  check(
    garrow_boolean_array_builder_append_value(
      cast[ptr GArrowBooleanArrayBuilder](builder), 0.gboolean
    )
  )
