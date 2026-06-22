## Compression and decompression streams.
##
## `Codec` wraps Arrow's compression support (SNAPPY, GZIP, ZSTD, LZ4, BROTLI).
## `CompressedInputStream`/`CompressedOutputStream` transparently compress or
## decompress data flowing through a stream.
import std/[os, strutils]
import ../core/[ffi, error, utils]
import ./filesystem

arcGObject:
  type Codec* = object ## Compression codec for a specific algorithm.
    handle*: ptr GArrowCodec

proc newCodec*(compressionType: GArrowCompressionType): Codec =
  ## Creates a `Codec` for the given compression type.
  result.handle = verify garrow_codec_new(compressionType)

func name*(codec: Codec): string =
  let s = garrow_codec_get_name(codec.toPtr)
  if s != nil:
    $s
  else:
    ""

func compressionType*(codec: Codec): GArrowCompressionType =
  garrow_codec_get_compression_type(codec.toPtr)

func level*(codec: Codec): int =
  garrow_codec_get_compression_level(codec.toPtr).int

func codecFromExtension*(ext: string): GArrowCompressionType =
  let ext = ext.toLowerAscii()
  case ext
  of ".gz", ".gzip":
    GARROW_COMPRESSION_TYPE_GZIP
  of ".zst", ".zstd":
    GARROW_COMPRESSION_TYPE_ZSTD
  of ".lz4":
    GARROW_COMPRESSION_TYPE_LZ4
  of ".bz2":
    GARROW_COMPRESSION_TYPE_BZ2
  of ".snappy":
    GARROW_COMPRESSION_TYPE_SNAPPY
  of ".br":
    GARROW_COMPRESSION_TYPE_BROTLI
  else:
    raise newException(ValueError, "Unknown compression extension: " & ext)

proc newCompressedInputStream*(codec: Codec, raw: InputStream): InputStream =
  ## Wraps a raw input stream with decompression using the given codec.
  let handle = verify garrow_compressed_input_stream_new(codec.toPtr, raw.toPtr)
  result.handle = cast[ptr GArrowInputStream](handle)

proc newCompressedOutputStream*(codec: Codec, raw: OutputStream): OutputStream =
  ## Wraps a raw output stream with compression using the given codec.
  let handle = verify garrow_compressed_output_stream_new(codec.toPtr, raw.toPtr)
  result.handle = cast[ptr GArrowOutputStream](handle)

proc openCompressedInputStream*(fs: FileSystem, path: string): InputStream =
  let ct = codecFromExtension(splitFile(path).ext)
  newCompressedInputStream(newCodec(ct), fs.openInputStream(path))

proc openCompressedInputStream*(
    fs: FileSystem, path: string, codec: Codec
): InputStream =
  newCompressedInputStream(codec, fs.openInputStream(path))

proc openCompressedOutputStream*(fs: FileSystem, path: string): OutputStream =
  let ct = codecFromExtension(splitFile(path).ext)
  newCompressedOutputStream(newCodec(ct), fs.openOutputStream(path))

proc openCompressedOutputStream*(
    fs: FileSystem, path: string, codec: Codec
): OutputStream =
  newCompressedOutputStream(codec, fs.openOutputStream(path))
