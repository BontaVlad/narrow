import std/[os, strformat]
version       = "0.1.0"
author        = "Sergiu Vlad Bonta"
description   = "A Nim wrapper around the Apache Arrow C API."
license       = "MIT License"
srcDir        = "src"
installExt    = @["nim"]
bin           = @["narrow"]

requires "nim >= 2.0.0"
requires "futhark"
requires "unittest2 >= 0.2.3"

task test, "Run testament":
  echo staticExec("testament p \"./tests/test_*.nim\"")
  discard staticExec("find tests/ -type f ! -name \"*.*\" -delete 2> /dev/null")

task generate, "Generate bindings":
  exec "nim c --maxLoopIterationsVM=10000000000 -d:useFuthark -d:nodeclguards:true -d:exportall:true -r src/narrow.nim"

task format, "Recursively format all Nim files in a specific directory":
  let directory = "src"
  
  echo fmt"Formatting Nim files in: {directory}"
  
  for file in walkDirRec(directory):
    if file.endsWith(".nim"):
      echo fmt"Formatting {file}..."
      discard gorge(fmt"nph {file}")
