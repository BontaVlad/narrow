import unittest2
import testfixture
import ../src/narrow

suite "Compressed I/O Streams":
  var fixture: TestFixture
  var fs: LocalFileSystem

  setup:
    fixture = newTestFixture("test_compressed")
    fs = newLocalFileSystem()

  teardown:
    fixture.cleanup()

  test "newCodec - name, type, level for gzip":
    let codec = newCodec(GARROW_COMPRESSION_TYPE_GZIP)
    check codec.compressionType == GARROW_COMPRESSION_TYPE_GZIP
    check codec.name.len > 0

  test "newCodec - name, type, level for zstd":
    let codec = newCodec(GARROW_COMPRESSION_TYPE_ZSTD)
    check codec.compressionType == GARROW_COMPRESSION_TYPE_ZSTD
    check codec.name.len > 0

  test "newCodec - level is a valid integer":
    let codec = newCodec(GARROW_COMPRESSION_TYPE_GZIP)
    check codec.level >= -1

  test "codecFromExtension - known extensions":
    check codecFromExtension(".gz") == GARROW_COMPRESSION_TYPE_GZIP
    check codecFromExtension(".zst") == GARROW_COMPRESSION_TYPE_ZSTD
    check codecFromExtension(".zstd") == GARROW_COMPRESSION_TYPE_ZSTD
    check codecFromExtension(".lz4") == GARROW_COMPRESSION_TYPE_LZ4
    check codecFromExtension(".bz2") == GARROW_COMPRESSION_TYPE_BZ2
    check codecFromExtension(".snappy") == GARROW_COMPRESSION_TYPE_SNAPPY
    check codecFromExtension(".br") == GARROW_COMPRESSION_TYPE_BROTLI
    check codecFromExtension(".gzip") == GARROW_COMPRESSION_TYPE_GZIP

  test "codecFromExtension - case insensitive":
    check codecFromExtension(".GZ") == GARROW_COMPRESSION_TYPE_GZIP
    check codecFromExtension(".Zst") == GARROW_COMPRESSION_TYPE_ZSTD

  test "codecFromExtension - unknown extension raises":
    expect(ValueError):
      discard codecFromExtension(".xyz")

  test "write and read compressed text (gzip, explicit)":
    let path = fixture / "explicit_gzip.gz"
    let codec = newCodec(GARROW_COMPRESSION_TYPE_GZIP)

    block:
      let outStream = fs.openCompressedOutputStream(path, codec)
      outStream.write("hello from gzip")
    block:
      let inStream = fs.openCompressedInputStream(path, codec)
      let data = inStream.readString(100)
      check data == "hello from gzip"

  test "write and read compressed text (zstd, explicit)":
    let path = fixture / "explicit_zstd.zst"
    let codec = newCodec(GARROW_COMPRESSION_TYPE_ZSTD)

    block:
      let outStream = fs.openCompressedOutputStream(path, codec)
      outStream.write("hello from zstd")
    block:
      let inStream = fs.openCompressedInputStream(path, codec)
      let data = inStream.readString(100)
      check data == "hello from zstd"

  test "write and read compressed bytes (bz2, explicit)":
    let path = fixture / "explicit_bz2.bz2"
    let codec = newCodec(GARROW_COMPRESSION_TYPE_BZ2)
    let original = @[byte 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    block:
      let outStream = fs.openCompressedOutputStream(path, codec)
      outStream.write(original)
    block:
      let inStream = fs.openCompressedInputStream(path, codec)
      let data = inStream.readAll()
      check data == original

  test "snappy and lz4 codecs create but streaming not supported":
    for ct in [GARROW_COMPRESSION_TYPE_SNAPPY, GARROW_COMPRESSION_TYPE_LZ4]:
      let codec = newCodec(ct)
      check codec.name.len > 0

      let path = fixture / "unsupported" & $ct
      expect(OperationError):
        discard fs.openCompressedOutputStream(path, codec)

  test "write and read via inferred extension (gzip)":
    let path = fixture / "inferred.gz"

    block:
      let outStream = fs.openCompressedOutputStream(path)
      outStream.write("inferred gzip")
    block:
      let inStream = fs.openCompressedInputStream(path)
      let data = inStream.readString(100)
      check data == "inferred gzip"

  test "write and read via inferred extension (zstd)":
    let path = fixture / "inferred.zst"

    block:
      let outStream = fs.openCompressedOutputStream(path)
      outStream.write("inferred zstd")
    block:
      let inStream = fs.openCompressedInputStream(path)
      let data = inStream.readString(100)
      check data == "inferred zstd"

  test "inferred extension fails for unknown suffix":
    let path = fixture / "data.unknown"
    expect(ValueError):
      discard fs.openCompressedOutputStream(path)

  test "round-trip empty data":
    let path = fixture / "empty.gz"

    block:
      let outStream = fs.openCompressedOutputStream(path)
      outStream.write("")
    block:
      let inStream = fs.openCompressedInputStream(path)
      let data = inStream.readAll()
      check data.len == 0

  test "round-trip larger text data":
    let path = fixture / "larger.gz"
    let msg = "The quick brown fox jumps over the lazy dog. " &
              "1234567890!@#$%^&*()"

    block:
      let outStream = fs.openCompressedOutputStream(path)
      outStream.write(msg)
    block:
      let inStream = fs.openCompressedInputStream(path)
      let data = inStream.readString(int64 msg.len + 100)
      check data == msg

  test "newCompressedInputStream with explicit codec and raw stream":
    let path = fixture / "raw_stream.gz"
    let codec = newCodec(GARROW_COMPRESSION_TYPE_GZIP)

    block:
      let rawOut = fs.openOutputStream(path)
      var compOut = newCompressedOutputStream(codec, rawOut)
      compOut.write("via raw stream")
    block:
      let rawIn = fs.openInputStream(path)
      var compIn = newCompressedInputStream(codec, rawIn)
      let data = compIn.readString(100)
      check data == "via raw stream"

  test "readAll reads full compressed content":
    let path = fixture / "readall.gz"
    let lines = @["line one\n", "line two\n", "line three"]

    block:
      let outStream = fs.openCompressedOutputStream(path)
      for line in lines:
        outStream.write(line)
    block:
      let inStream = fs.openCompressedInputStream(path)
      let data = inStream.readAll()
      let text = newString(data.len)
      if data.len > 0:
        copyMem(addr text[0], unsafeAddr data[0], data.len)
      check text == "line one\nline two\nline three"

  test "multiple writes produce correct compressed content":
    let path = fixture / "multi.gz"

    block:
      let outStream = fs.openCompressedOutputStream(path)
      outStream.write("hello")
      outStream.write(" ")
      outStream.write("world")
    block:
      let inStream = fs.openCompressedInputStream(path)
      let data = inStream.readString(100)
      check data == "hello world"
