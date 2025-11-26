import std/[os, strformat, strutils, sequtils, parseutils]

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

# task test, "Generate test coverage":
#   var isParallel = false
#   var cores = 4
#   var pattern = ""

#   for i in 0..paramCount():
#     let parts = paramStr(i).split(":")
#     if parts.len == 2:
#       let (name, value) = (parts[0], parts[1])
#       if name == "parallel":
#         if value == "true":
#           isParallel = true
#         else:
#           isParallel = false
#       if name == "cores":
#         cores = parseInt(value)
#       if name == "pattern":
#         pattern = value

#   const nimcacheDir = "nimcache"
#   let testsDir = nimcacheDir / "tests"
  
#   # Create nimcache/tests directory if it doesn't exist
#   mkDir(testsDir)
  
#   # Compile flags
#   const compileFlags = [
#     "-d:debug",
#     "-d:nimDebugDlOpen",
#     "--verbosity:0",
#     "--hints:off",
#     "--opt:none",
#     "--debugger:native",
#     "--stacktrace:on",
#     "--passc:-fsanitize=address",
#     "--passl:-fsanitize=address",
#     "-d:useMalloc",
#     "--mm:orc",
#     "--passC:-O0",
#     "--passC:-g3",
#     "--passC:-ggdb3",
#     "--passC:-gdwarf-4",
#     "--lineDir:on",
#     "--debuginfo:on",
#     "--excessiveStackTrace:on"
#   ]
  
#   proc runTest(file: string) =
#     echo "Processing file: ", file
#     let filename = file.extractFilename()
#     let filenameNoExt = filename.changeFileExt("")
#     let outputPath = testsDir / filenameNoExt
    
#     # Step 1: Compile with debug flags
#     var compileCmd = @["nim", "c"]
#     compileCmd.add(compileFlags)
#     compileCmd.add(["-o:" & outputPath, file, pattern])
    
#     let compileResult = gorge(compileCmd.join(" "))
#     echo compileResult
#     # if compileResult != "0":
#     #   echo "Compilation failed for: ", file
#     #   quit(1)
    
#     # Step 2: Run the compiled test
#     echo gorge(outputPath)
#     # if runResult != "0":
#     #   echo "Test failed for: ", file
#     #   quit(1)
  
#   # Find test files
#   var testFiles: seq[string] = @[]
#   for file in walkDirRec("tests"):
#     if file.endsWith(".nim") and file.extractFilename().startsWith("test_"):
#       testFiles.add(file)
  
#   if testFiles.len == 0:
#     echo "No test files found"
#     quit(0)
  
#   if isParallel:
#     echo "Running tests in parallel..."
#     # Note: Nim doesn't have built-in parallel execution like xargs -P
#     # For true parallel execution, you'd need to use threads or the threadpool module
#     # This implementation runs sequentially but can be extended with threading
#     echo "Warning: Parallel execution not yet implemented, running sequentially"
#     for file in testFiles:
#       runTest(file)
#   else:
#     echo "Running tests sequentially..."
#     for file in testFiles:
#       runTest(file)

task generate, "Generate bindings":
  exec "nim c --maxLoopIterationsVM=10000000000 -d:useFuthark -d:nodeclguards:true -d:exportall:true -r src/narrow.nim"

task format, "Recursively format all Nim files in a specific directory":
  let directory = "src"
  for file in walkDirRec(directory):
    if file.endsWith(".nim"):
      let output = gorge(fmt"nph {file}")
      if len(output) > 0:
        echo output
