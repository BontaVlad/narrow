when defined(useFuthark):
  import os, futhark

  importc:
    outputPath currentSourcePath.parentDir / "wrapper.nim"
else:
  include "wrapper.nim"
