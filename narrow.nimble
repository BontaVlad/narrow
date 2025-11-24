version       = "0.1.0"
author        = "Sergiu Vlad Bonta"
description   = "A Nim wrapper around the Apache Arrow C API."
license       = "MIT License"
srcDir        = "src"
installExt    = @["nim"]
bin           = @["narrow"]


# switch("passL", "-larrow-glib")
# switch("passL", "-lgobject-2.0")
# switch("passL", "-lglib-2.0")
# switch("passL", "-larrow")
# switch("passL", "-lasan")

requires "nim >= 2.0.0"
requires "futhark"

task test, "Run testament":
  echo staticExec("testament p \"./tests/test_*.nim\"")
  discard staticExec("find tests/ -type f ! -name \"*.*\" -delete 2> /dev/null")

task generate, "Generate bindings":
  exec "nim c -d:useFuthark src/your_arrow_wrapper.nim"
