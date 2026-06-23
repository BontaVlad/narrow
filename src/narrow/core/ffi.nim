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

  # Link Arrow/GLib libraries so downstream consumers don't need to manage
  # pkg-config flags themselves. These pragmas are processed when any module
  # imports narrow/core/ffi (which is imported by the main narrow module).
  when not defined(nimsuggest):
    {.passC: gorge("pkg-config --cflags arrow-glib parquet-glib gobject-2.0 glib-2.0 2>/dev/null || true").}
    {.passL: gorge("pkg-config --libs arrow-glib parquet-glib gobject-2.0 glib-2.0 2>/dev/null || true").}

  proc g_type_name*(gtype: GType): cstring {.cdecl, importc: "g_type_name".}

  proc g_type_check_instance_is_a*(
    instance: ptr GTypeInstance, iface_type: GType
  ): gboolean {.cdecl, importc: "g_type_check_instance_is_a".}
