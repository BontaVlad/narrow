when defined(useFuthark):
  import os
  import futhark

  importc:
    compilerArg gorge("pkg-config --cflags-only-I arrow-glib gobject-2.0 glib-2.0")
    # we need it for glong definitionts and other types
    path gorge("pkg-config --variable=includedir glib-2.0") & "/glib-2.0" & "/glib"
    "gtypes.h"
    "gobject/gobject.h"
    path gorge("pkg-config --variable=includedir arrow-glib") & "/arrow-glib"
    "arrow-glib.h"

    outputPath currentSourcePath.parentDir / "generated.nim"
else:
  include "generated.nim"
