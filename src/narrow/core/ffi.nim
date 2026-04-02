when defined(useFuthark):
  import os
  import futhark

  importc:
    compilerArg gorge(
      "pkg-config --cflags-only-I arrow-glib parquet-glib gobject-2.0 glib-2.0"
    )
    # we need it for glong definitionts and other types
    path gorge("pkg-config --variable=includedir glib-2.0") & "/glib-2.0" & "/glib"
    "gtypes.h"
    "gobject/gobject.h"
    path gorge("pkg-config --variable=includedir arrow-glib") & "/arrow-glib"
    "arrow-glib.h"
    path gorge("pkg-config --variable=includedir parquet-glib") & "/parquet-glib"
    "parquet-glib.h"

    path gorge("pkg-config --variable=includedir arrow-dataset-glib") &
      "/arrow-dataset-glib"
    "arrow-dataset-glib.h"

    outputPath currentSourcePath.parentDir / "generated.nim"
else:
  include "generated.nim"

  proc g_type_name*(gtype: GType): cstring {.cdecl, importc: "g_type_name".}

  proc g_type_check_instance_is_a*(
    instance: ptr GTypeInstance, iface_type: GType
  ): gboolean {.cdecl, importc: "g_type_check_instance_is_a".}
