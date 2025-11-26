import std/[os, tempfiles, strformat]
import unittest2
import ../src/[ffi, filesystem]

const
  TestRoot = "/tmp/arrow_nim_test"

proc setupTestDir() =
  ## Ensure clean test directory exists
  if dirExists(TestRoot):
    removeDir(TestRoot)
  createDir(TestRoot)

proc teardownTestDir() =
  ## Clean up test directory
  if dirExists(TestRoot):
    removeDir(TestRoot)

template withTestDir(body: untyped) =
  ## Run test body with clean test directory
  setupTestDir()
  try:
    body
  finally:
    teardownTestDir()


suite "FileInfo retrieval":

  var dir: string
  var file: string

  setup:
    # create a fresh temp directory for each test
    dir = createTempDir("tmpprefix_", "_end")
    let (f, path) = createTempFile("mytest_", "_end.tmp")
    file = path
    close(f)

  teardown:
    removeFile(file)
    removeDir(dir)

  test "getFileInfo - existing directory":
    let fs = newLocalFileSystem()
    let info = fs.getFileInfo(dir)

    check fs.typeName == "local"
    check info.isDir
    check info.fileType == GARROW_FILE_TYPE_DIR
    check info.exists
    check info.path == $dir

  test "getFileInfo - existing file":
    let fs = newLocalFileSystem()
    let info = fs.getFileInfo(file)

    check info.fileType == GARROW_FILE_TYPE_FILE
    check info.isFile
    check info.exists
    check info.path == $file
    check info.extension == "tmp"

  test "getFileInfo - non-existent path":
    let fs = newLocalFileSystem()
    let info = fs.getFileInfo("/tmp/this_path_should_not_exist_12345")
    
    check info.isValid
    check not info.exists
    check info.fileType == GARROW_FILE_TYPE_NOT_FOUND

suite "Directory Creation":

  test "createDir - simple directory":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "simple_dir"
      
      fs.createDir(path, recursive = false)
      
      let info = fs.getFileInfo(path)
      check info.exists
      check info.isDir

  test "createDir - recursive directory":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "a" / "b" / "c"
      
      fs.createDir(path, recursive = true)
      
      let info = fs.getFileInfo(path)
      check info.exists
      check info.isDir

  test "createDir - default is recursive":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "x" / "y" / "z"
      
      fs.createDir(path)  # recursive = true by default
      
      check fs.getFileInfo(path).exists

suite "Output Streams":

  test "openOutputStream - create new file":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "output.txt"
      
      let stream = fs.openOutputStream(path)
      check stream.isValid
      
      check fs.getFileInfo(path).exists

  test "OutputStream - write string":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "write_string.txt"
      
      let stream = fs.openOutputStream(path)
      stream.write("Hello, Arrow!")
      stream.close()
      
      let info = fs.getFileInfo(path)
      check info.exists
      check info.size == 13

  test "OutputStream - write bytes":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "write_bytes.bin"
      
      let stream = fs.openOutputStream(path)
      stream.write([0x00'u8, 0x01, 0x02, 0x03, 0xFF])
      stream.close()
      
      let info = fs.getFileInfo(path)
      check info.size == 5

  test "OutputStream - multiple writes":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "multi_write.txt"
      
      let stream = fs.openOutputStream(path)
      stream.write("Hello, ")
      stream.write("World!")
      stream.close()
      
      check fs.getFileInfo(path).size == 13

  test "OutputStream - tell position":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "tell_test.txt"
      
      let stream = fs.openOutputStream(path)
      check stream.tell() == 0
      stream.write("12345")
      check stream.tell() == 5
      stream.write("67890")
      check stream.tell() == 10
      stream.close()

  test "OutputStream - flush":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "flush_test.txt"
      
      let stream = fs.openOutputStream(path)
      stream.write("test data")
      stream.flush()  # Should not raise
      stream.close()

suite "Streams - Corrected":

  # FIXME: we skip this because destroy -> close problem
  # test "OutputStream - write and close":
  #   withTestDir:
  #     let fs = newLocalFileSystem()
  #     let path = TestRoot / "write_test.txt"
      
  #     var stream = fs.openOutputStream(path)
  #     check stream.isValid
      
  #     stream.write("Hello, Arrow!")
  #     stream.close()
      
  #     check not stream.isValid
  #     check fs.getFileInfo(path).size == 13

  test "OutputStream - flush":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "flush_test.txt"
      
      var stream = fs.openOutputStream(path)
      stream.write("buffered data")
      stream.flush()  # Explicit flush
      
      # Data should be written even before close
      check fs.getFileInfo(path).size == 13
      
      stream.close()

  test "OutputStream - tell position":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "tell_test.txt"
      
      var stream = fs.openOutputStream(path)
      check stream.tell() == 0
      
      stream.write("12345")
      check stream.tell() == 5
      
      stream.write("67890")
      check stream.tell() == 10
      
      stream.close()

  test "OutputStream - automatic flush on close":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "auto_flush.txt"
      
      var stream = fs.openOutputStream(path)
      stream.write("will be flushed")
      stream.close()  # Should flush automatically
      
      check fs.readText(path) == "will be flushed"

  test "InputStream - read bytes":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "read_test.txt"
      
      # Write test data
      var outStream = fs.openOutputStream(path)
      outStream.write("Hello, World!")
      outStream.close()
      
      # Read it back
      var inStream = fs.openInputStream(path)
      let data = inStream.read(5)
      inStream.close()
      
      check data.len == 5
      check data == @[72'u8, 101, 108, 108, 111]  # "Hello"

  test "InputStream - readAll":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "readall_test.txt"
      
      var outStream = fs.openOutputStream(path)
      outStream.write("Complete content here")
      outStream.close()
      
      var inStream = fs.openInputStream(path)
      let data = inStream.readAll()
      inStream.close()
      
      check data.len == 21

  test "InputStream - readString":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "readstring.txt"
      
      var outStream = fs.openOutputStream(path)
      outStream.write("Nim is great!")
      outStream.close()
      
      var inStream = fs.openInputStream(path)
      let text = inStream.readAllString()
      inStream.close()
      
      check text == "Nim is great!"

  test "SeekableInputStream - size":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "size_test.txt"
      
      var outStream = fs.openOutputStream(path)
      outStream.write("0123456789")
      outStream.close()
      
      var inStream = fs.openInputFile(path)
      check inStream.size == 10
      inStream.close()

  test "SeekableInputStream - readAt":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "readat_test.txt"
      
      var outStream = fs.openOutputStream(path)
      outStream.write("ABCDEFGHIJ")
      outStream.close()
      
      var inStream = fs.openInputFile(path)
      
      # Read from middle
      let middle = inStream.readAt(position = 3, nBytes = 4)
      check middle == @[68'u8, 69, 70, 71]  # "DEFG"
      
      # Read from start
      let start = inStream.readAt(position = 0, nBytes = 3)
      check start == @[65'u8, 66, 67]  # "ABC"
      
      # Read from end
      let ending = inStream.readAt(position = 7, nBytes = 3)
      check ending == @[72'u8, 73, 74]  # "HIJ"
      
      inStream.close()

  test "SeekableInputStream - readAtString":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "readatstring.txt"
      
      var outStream = fs.openOutputStream(path)
      outStream.write("Hello World")
      outStream.close()
      
      var inStream = fs.openInputFile(path)
      
      let word1 = inStream.readAtString(0, 5)
      check word1 == "Hello"
      
      let word2 = inStream.readAtString(6, 5)
      check word2 == "World"
      
      inStream.close()

  test "withOutputStream macro style":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "callback_out.txt"
      
      with fs.openOutputStream(path), stream:
        stream.write("Callback style")
      
      check fs.readText(path) == "Callback style"

  test "Stream - write empty":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "empty_write.txt"
      
      var stream = fs.openOutputStream(path)
      stream.write("")  # Should not fail
      stream.write(newSeq[byte]())  # Should not fail
      stream.close()
      
      check fs.getFileInfo(path).size == 0

  test "Stream - read empty file":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "empty_read.txt"
      
      var outStream = fs.openOutputStream(path)
      outStream.close()
      
      var inStream = fs.openInputStream(path)
      let data = inStream.readAll()
      inStream.close()
      
      check data.len == 0

  # # test "Stream - error on closed stream":
  # #   withTestDir:
  # #     let fs = newLocalFileSystem()
  # #     let path = TestRoot / "closed_error.txt"
      
  # #     var stream = fs.openOutputStream(path)
  # #     stream.close()
      
  # #     expect StreamError:
  # #       stream.write("This should fail")

  test "Stream - writeLine":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "lines.txt"
      
      var stream = fs.openOutputStream(path)
      stream.writeLine("Line 1")
      stream.writeLine("Line 2")
      stream.write("Line 3")  # No newline
      stream.close()
      
      check fs.readText(path) == "Line 1\nLine 2\nLine 3"

suite "FileSystem high level":

  test "New FileSystem from uri":
    let filename = "mystring.txt"
    let uri = "file://" / TestRoot / filename

    let fp = newFileSystem(uri)
    var outStream = fp.openOutputStream(filename)
    outStream.write("Hello world")
    outStream.close()

    let fs = fp.openInputStream(filename)
    echo fs.readAllString()
suite "FileSystem - Memory Stress Tests":
  
  test "Create and destroy many FileInfo objects":
    withTestDir:
      let fs = newLocalFileSystem()
      for i in 0..1000:
        let info = fs.getFileInfo(TestRoot)
        check info.isValid
        check info.isDir
  
  test "Create and destroy many FileSystems":
    for i in 0..1000:
      let fs = newLocalFileSystem()
      check fs.isValid
  
  test "Repeated file creation and deletion":
    withTestDir:
      let fs = newLocalFileSystem()
      for i in 0..100:
        let path = TestRoot / fmt"file_{i}.txt"
        var stream = fs.openOutputStream(path)
        stream.write("test data")
        stream.close()
        
        let info = fs.getFileInfo(path)
        check info.exists
        
        fs.deleteFile(path)
  
  test "Multiple stream operations":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "stream_test.txt"
      
      for i in 0..1000:
        var outStream = fs.openOutputStream(path)
        outStream.write(fmt"iteration {i}")
        outStream.close()
        
        var inStream = fs.openInputStream(path)
        let data = inStream.readAllString()
        inStream.close()
        check data.len > 0
  
  test "Nested directory creation and deletion":
    withTestDir:
      let fs = newLocalFileSystem()
      for i in 0..100:
        let path = TestRoot / fmt"dir_{i}" / "subdir" / "nested"
        fs.createDir(path)
        check fs.getFileInfo(path).isDir
        fs.deleteDir(TestRoot / fmt"dir_{i}")
  
  test "Large file write and read":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "large_file.bin"
      
      # Create 1MB of data
      var largeData = newSeq[byte](1024 * 1024)
      for i in 0..<largeData.len:
        largeData[i] = (i mod 256).byte
      
      for cycle in 0..10:
        var outStream = fs.openOutputStream(path)
        outStream.write(largeData)
        outStream.close()
        
        var inStream = fs.openInputFile(path)
        let readData = inStream.readAll()
        inStream.close()
        
        check readData.len == largeData.len
  
  test "Multiple FileInfo from paths":
    withTestDir:
      let fs = newLocalFileSystem()
      
      # Create test files
      var paths: seq[string]
      for i in 0..99:
        let path = TestRoot / fmt"multi_{i}.txt"
        paths.add(path)
        var stream = fs.openOutputStream(path)
        stream.write("test")
        stream.close()
      
      for cycle in 0..10:
        let infos = fs.getFileInfos(paths)
        check infos.len == 100
        for info in infos:
          check info.exists
          check info.isFile
  
  test "Stream read/write cycles":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "cycle_test.txt"
      
      for i in 0..1000:
        var outStream = fs.openOutputStream(path)
        outStream.write(fmt"{i}")
        outStream.flush()
        outStream.close()
        
        var inStream = fs.openInputStream(path)
        discard inStream.readAll()
        inStream.close()
  
  test "FileInfo property access":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "props.txt"
      
      var stream = fs.openOutputStream(path)
      stream.write("test content")
      stream.close()
      
      for i in 0..1000:
        let info = fs.getFileInfo(path)
        discard info.path()
        discard info.baseName()
        discard info.dirName()
        discard info.extension()
        discard info.size()
        discard info.mtime()
        discard info.fileType()
  
  test "SeekableInputStream random access":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "seekable.bin"
      
      # Write test data
      var data = newSeq[byte](1000)
      for i in 0..<data.len:
        data[i] = (i mod 256).byte
      
      var outStream = fs.openOutputStream(path)
      outStream.write(data)
      outStream.close()
      
      # Random access reads
      for cycle in 0..1000:
        var inStream = fs.openInputFile(path)
        discard inStream.readAt(100, 50)
        discard inStream.readAt(500, 100)
        discard inStream.readAt(0, 10)
        inStream.close()
  
  test "OutputStream tell position":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "tell_test.txt"
      
      for i in 0..1000:
        var stream = fs.openOutputStream(path)
        for j in 0..99:
          stream.write("x")
          discard stream.tell()
        stream.close()
  
  test "Multiple filesystem instances":
    withTestDir:
      for i in 0..100:
        let fs1 = newLocalFileSystem()
        let fs2 = newLocalFileSystem()
        let fs3 = newFileSystem("file://" & TestRoot)
        
        check fs1.isValid
        check fs2.isValid
        check fs3.isValid
  
  test "FileInfo copying":
    withTestDir:
      let fs = newLocalFileSystem()
      let original = fs.getFileInfo(TestRoot)
      
      for i in 0..1000:
        let copy1 = original
        let copy2 = copy1
        let copy3 = copy2
        check copy3.isValid
        check copy3.isDir
  
  test "Stream copying":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "copy_stream.txt"
      
      var outStream = fs.openOutputStream(path)
      outStream.write("test")
      outStream.close()
      
      for i in 0..100:
        let stream1 = fs.openInputStream(path)
        let stream2 = stream1
        let stream3 = stream2
        discard stream3.readAll()
  
  test "Rapid file creation and getFileInfo":
    withTestDir:
      let fs = newLocalFileSystem()
      
      for i in 0..1000:
        let path = TestRoot / fmt"rapid_{i}.txt"
        var stream = fs.openOutputStream(path)
        stream.write("x")
        stream.close()
        
        let info = fs.getFileInfo(path)
        check info.exists
        
        fs.deleteFile(path)
  
  test "WriteText and ReadText cycles":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "text_cycles.txt"
      
      for i in 0..1000:
        fs.writeText(path, fmt"iteration {i}")
        let content = fs.readText(path)
        check content == fmt"iteration {i}"
  
  test "AppendText operations":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "append.txt"
      
      for i in 0..100:
        fs.appendText(path, fmt"{i}\n")
      
      let content = fs.readText(path)
      check content.len > 0
  
  test "Directory listing stress":
    withTestDir:
      let fs = newLocalFileSystem()
      
      # Create many files
      for i in 0..99:
        let path = TestRoot / fmt"list_{i}.txt"
        var stream = fs.openOutputStream(path)
        stream.write("x")
        stream.close()
      
      # List repeatedly
      for cycle in 0..10:
        var paths: seq[string]
        for i in 0..99:
          paths.add(TestRoot / fmt"list_{i}.txt")
        
        let infos = fs.getFileInfos(paths)
        check infos.len == 100
  
  test "FileInfo string conversion":
    withTestDir:
      let fs = newLocalFileSystem()
      let info = fs.getFileInfo(TestRoot)
      
      for i in 0..1000:
        let str = $info
        check str.len > 0
  
  test "Mixed stream types":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "mixed.txt"
      
      for i in 0..100:
        var outStream = fs.openOutputStream(path)
        outStream.write(fmt"data {i}")
        outStream.close()
        
        var inStream = fs.openInputStream(path)
        discard inStream.readAll()
        inStream.close()
        
        var seekableStream = fs.openInputFile(path)
        discard seekableStream.readAll()
        seekableStream.close()
  
  test "Interleaved operations":
    withTestDir:
      let fs = newLocalFileSystem()
      
      for i in 0..100:
        let path1 = TestRoot / fmt"inter1_{i}.txt"
        let path2 = TestRoot / fmt"inter2_{i}.txt"
        
        var stream1 = fs.openOutputStream(path1)
        let info1 = fs.getFileInfo(path1)
        var stream2 = fs.openOutputStream(path2)
        let info2 = fs.getFileInfo(path2)
        
        stream1.write("a")
        stream2.write("b")
        
        stream1.close()
        stream2.close()
        
        check info1.exists
        check info2.exists

suite "FileSystem - Error Recovery":
  
  test "Handle missing files gracefully":
    withTestDir:
      let fs = newLocalFileSystem()
      for i in 0..100:
        let path = TestRoot / fmt"missing_{i}.txt"
        let info = fs.getFileInfo(path)
        check not info.exists
  
  test "Close streams multiple times":
    withTestDir:
      let fs = newLocalFileSystem()
      let path = TestRoot / "multi_close.txt"
      
      var outStream = fs.openOutputStream(path)
      outStream.write("test")
      outStream.close()
      # Second close should be safe due to nil check
      outStream.close()
  
  # test "Write to closed stream detection":
  #   withTestDir:
  #     let fs = newLocalFileSystem()
  #     let path = TestRoot / "closed_write.txt"
      
  #     var stream = fs.openOutputStream(path)
  #     stream.close()
      
  #     try:
  #       stream.write("should fail")
  #       check false # Should not reach here
  #     except StreamError:
  #       check true
