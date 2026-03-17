proc getRefCount(handle: pointer): uint32 =
  type GObj = object
    g_type_instance: pointer
    ref_count: uint32
  result = cast[ptr GObj](handle).ref_count
