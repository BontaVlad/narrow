when defined(useFuthark):
  import os
  import futhark
  
  # Use pkg-config to automatically get all necessary flags
  {.passC: gorge("pkg-config --cflags arrow-glib glib-2.0").}
  {.passL: gorge("pkg-config --libs arrow-glib glib-2.0").}
  
  importc:
    outputPath currentSourcePath.parentDir / "generated.nim"
else:
  include "generated.nim"
