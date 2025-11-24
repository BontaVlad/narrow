version       = "0.1.0"
author        = "Sergiu Vlad Bonta"
description   = "A Nim wrapper around the Apache Arrow C API."
license       = "MIT License"
srcDir        = "src"
installExt    = @["nim"]
bin           = @["narrow"]

requires "nim >= 2.0.0"
requires "futhark"

task test, "Run testament":
  echo staticExec("testament p \"./tests/test_*.nim\"")
  discard staticExec("find tests/ -type f ! -name \"*.*\" -delete 2> /dev/null")

task generate, "Generate bindings":
  exec "nim c --maxLoopIterationsVM=10000000000 -d:useFuthark -d:nodeclguards:true -d:exportall:true -r src/narrow.nim"
