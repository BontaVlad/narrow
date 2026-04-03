import std/[os, strformat, strutils, sequtils, parseutils]

version       = "0.1.0"
author        = "Sergiu Vlad Bonta"
description   = "A Nim wrapper around the Apache Arrow C API."
license       = "MIT License"
srcDir        = "src"
installExt    = @["nim"]
bin           = @["narrow"]

# requires "nim >= 2.2.6"
requires "unittest2 >= 0.2.3"

task generate, "Generate bindings":
  # Futhark is required only for binding generation, not for normal use
  exec "nimble install -y futhark"
  exec "nim c --maxLoopIterationsVM=10000000000 -d:useFuthark -d:nodeclguards:true -d:exportall:true -r src/narrow.nim"

task format, "Recursively format all Nim files in a specific directory":
  let directory = "src"
  for file in walkDirRec(directory):
    if file.endsWith(".nim"):
      let output = gorge(fmt"nph {file}")
      if len(output) > 0:
        echo output

task docs, "Generate documentation with runnable examples":
  let libs = gorge("pkg-config --libs arrow-dataset-glib parquet-glib")
  exec "nim doc --docCmd:\"--passL:'" & libs & "'\" --project --outdir:docs src/narrow.nim"
