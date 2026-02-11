import ./[ffi, error, glist, gtypes]

const
  PropPath = "path"
  PropBaseName = "base-name"
  PropDirName = "dir-name"
  PropExtension = "extension"
  PropSize = "size"
  PropMtime = "mtime"
  PropFileType = "type"

type
  FileInfo* = object ## Information about a filesystem entry
    handle*: ptr GArrowFileInfo

  FileSelector* = object ## A selector for discovering files in a directory
    handle*: ptr GArrowFileSelector

  FileSystemObj = object of RootObj ## Base filesystem object
    handle*: ptr GArrowFileSystem

  FileSystem* = ref FileSystemObj
    ## Abstract filesystem interface
    ##
    ## FileSystem provides a unified interface for interacting with different
    ## storage backends, including local filesystems, cloud storage, and HDFS.

  LocalFileSystemOptions* = object ## Options for creating a local filesystem
    handle*: ptr GArrowLocalFileSystemOptions

  LocalFileSystem* = ref object of FileSystemObj
    ## Filesystem implementation for local disk access
    ##
    ## Provides access to files on the local machine. Symlinks are automatically
    ## followed except when deleting entries.

  SubTreeFileSystem* = ref object of FileSystemObj
    ## A filesystem rooted at a particular subdirectory
    ##
    ## Useful for sandboxing filesystem access to a specific directory tree.
    basePath*: string

  SlowFileSystem* = ref object of FileSystemObj
    ## A filesystem wrapper that adds artificial latency (for testing)
    averageLatency*: float64

  InputStream* = object ## A readable stream
    handle*: ptr GArrowInputStream

  SeekableInputStream* = object
    ## A readable stream that supports random access (seeking)
    handle*: ptr GArrowSeekableInputStream

  OutputStream* = object ## A writable stream
    handle*: ptr GArrowOutputStream

  StreamError* = object of CatchableError

template with*(resource, name, body: untyped): untyped =
  block:
    var name = resource
    defer:
      name.close()
    body

# =============================================================================
# LocalFileSystemOptions Implementation
# =============================================================================

proc `=destroy`*(opts: LocalFileSystemOptions) =
  if opts.handle != nil:
    g_object_unref(opts.handle)

proc `=sink`*(dest: var LocalFileSystemOptions, src: LocalFileSystemOptions) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var LocalFileSystemOptions, src: LocalFileSystemOptions) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc newLocalFileSystemOptions*(): LocalFileSystemOptions =
  var handle = garrow_local_file_system_options_new()

  # Sink floating reference if present
  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)

  result.handle = handle

# =============================================================================
# FileInfo Implementation
# =============================================================================

proc newFileInfo*(): FileInfo =
  var handle = garrow_file_info_new()

  # Sink floating reference if present
  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)

  result.handle = handle

proc `=destroy`*(info: FileInfo) =
  if info.handle != nil:
    g_object_unref(info.handle)

proc `=sink`*(dest: var FileInfo, src: FileInfo) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var FileInfo, src: FileInfo) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc isValid*(info: FileInfo): bool {.inline.} =
  ## Check if the FileInfo handle is valid
  info.handle != nil

proc `==`*(a, b: FileInfo): bool =
  ## Compare two FileInfo objects for equality
  if not a.isValid or not b.isValid:
    return false
  return garrow_file_info_equal(a.handle, b.handle) != 0

proc isDir*(info: FileInfo): bool =
  garrow_file_info_is_dir(info.handle).bool

proc isFile*(info: FileInfo): bool =
  garrow_file_info_is_file(info.handle).bool

proc fileType*(info: FileInfo): GArrowFileType =
  var fileType: cint = 0
  g_object_get(info.handle, PropFileType.cstring, addr fileType, nil)
  result = GArrowFileType(fileType)

proc path*(info: FileInfo): string =
  var cstr: cstring = nil
  g_object_get(info.handle, PropPath.cstring, addr cstr, nil)
  if cstr != nil:
    result = $cstr
    g_free(cstr)

proc baseName*(info: FileInfo): string =
  var cstr: cstring = nil
  g_object_get(info.handle, PropBaseName.cstring, addr cstr, nil)
  if cstr != nil:
    result = $cstr
    g_free(cstr)

proc dirName*(info: FileInfo): string =
  var cstr: cstring = nil
  g_object_get(info.handle, PropDirName.cstring, addr cstr, nil)
  if cstr != nil:
    result = $cstr
    g_free(cstr)

proc extension*(info: FileInfo): string =
  var cstr: cstring = nil
  g_object_get(info.handle, PropExtension.cstring, addr cstr, nil)
  if cstr != nil:
    result = $cstr
    g_free(cstr)

proc size*(info: FileInfo): int64 =
  var size: int64 = -1
  g_object_get(info.handle, PropSize.cstring, addr size, nil)
  result = size

proc mtime*(info: FileInfo): int64 =
  var mtime: int64 = 0
  g_object_get(info.handle, PropMtime.cstring, addr mtime, nil)
  result = mtime

proc exists*(info: FileInfo): bool =
  ## Check if the entry exists
  info.fileType != GARROW_FILE_TYPE_NOT_FOUND and
    info.fileType != GARROW_FILE_TYPE_UNKNOWN

proc `$`*(info: FileInfo): string =
  ## Get a string representation of the FileInfo
  if info.handle == nil:
    return "<invalid FileInfo>"
  let cstr = garrow_file_info_to_string(info.handle)
  if cstr == nil:
    return "<FileInfo>"
  result = $cstr
  g_free(cstr)

# =============================================================================
# FileSelector Implementation
# =============================================================================

proc `=destroy`*(sel: FileSelector) =
  if sel.handle != nil:
    g_object_unref(sel.handle)

proc `=sink`*(dest: var FileSelector, src: FileSelector) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var FileSelector, src: FileSelector) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

# =============================================================================
# Helper: Convert GBytes to seq[byte]
# =============================================================================

proc toBytesSeq(gbytes: ptr GBytes): seq[byte] =
  ## Convert GBytes to Nim seq[byte] and free the GBytes
  if gbytes == nil:
    return @[]

  let size = g_bytes_get_size(gbytes)
  if size == 0:
    g_bytes_unref(gbytes)
    return @[]

  var sizeOut: gsize
  let data = g_bytes_get_data(gbytes, addr sizeOut)

  result = newSeq[byte](sizeOut)
  if sizeOut > 0:
    copyMem(addr result[0], data, sizeOut)

  g_bytes_unref(gbytes)

proc toBytesSeq(buffer: ptr GArrowBuffer): seq[byte] =
  ## Convert GArrowBuffer to Nim seq[byte] and unref the buffer
  if buffer == nil:
    return @[]

  let gbytes = garrow_buffer_get_data(buffer)
  result = toBytesSeq(gbytes)
  g_object_unref(buffer)

# =============================================================================
# Stream Implementations
# =============================================================================

# TODO: maybe converters? this is more explicit
proc asWritable(stream: OutputStream): ptr GArrowWritable {.inline.} =
  cast[ptr GArrowWritable](stream.handle)

proc asReadable(stream: InputStream): ptr GArrowReadable {.inline.} =
  cast[ptr GArrowReadable](stream.handle)

proc asReadable(stream: SeekableInputStream): ptr GArrowReadable {.inline.} =
  ## Cast to GArrowReadable interface
  cast[ptr GArrowReadable](stream.handle)

proc toPtr*(stream: OutputStream): ptr GArrowOutputStream {.inline.} =
  stream.handle
# =============================================================================
# InputStream Implementation
# =============================================================================

proc `=destroy`*(stream: InputStream) =
  if stream.handle != nil:
    g_object_unref(stream.handle)

proc `=sink`*(dest: var InputStream, src: InputStream) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var InputStream, src: InputStream) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc isValid*(stream: InputStream): bool {.inline.} =
  stream.handle != nil

proc close*(stream: InputStream) =
  ## Close the input stream (releases the underlying resource)
  discard
  # if stream.handle != nil:
  #   g_object_unref(stream.handle)

proc read*(stream: InputStream, nBytes: int64): seq[byte] =
  ## Read up to nBytes from the stream
  ## 
  ## Returns the bytes read. May return fewer bytes than requested
  ## if end of stream is reached.
  if stream.handle == nil:
    raise newException(StreamError, "Cannot read from closed stream")

  let gbytes = check garrow_readable_read_bytes(stream.asReadable, nBytes.gint64)
  result = toBytesSeq(gbytes)

proc readBuffer*(stream: InputStream, nBytes: int64): seq[byte] =
  ## Read using GArrowBuffer (alternative implementation)
  if stream.handle == nil:
    raise newException(StreamError, "Cannot read from closed stream")

  let buffer = check garrow_readable_read(stream.asReadable, nBytes.gint64)
  result = toBytesSeq(buffer)

proc readAll*(stream: InputStream, chunkSize: int64 = 65536): seq[byte] =
  ## Read all remaining data from the stream
  result = @[]
  while true:
    let chunk = stream.read(chunkSize)
    if chunk.len == 0:
      break
    result.add(chunk)

proc readString*(stream: InputStream, nBytes: int64): string =
  ## Read nBytes as a string
  let data = stream.read(nBytes)
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc readAllString*(stream: InputStream): string =
  ## Read all remaining data as a string
  let data = stream.readAll()
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

# =============================================================================
# SeekableInputStream Implementation
# =============================================================================

proc `=destroy`*(stream: SeekableInputStream) =
  if stream.handle != nil:
    g_object_unref(stream.handle)

proc `=sink`*(dest: var SeekableInputStream, src: SeekableInputStream) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var SeekableInputStream, src: SeekableInputStream) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc isValid*(stream: SeekableInputStream): bool {.inline.} =
  stream.handle != nil

proc close*(stream: SeekableInputStream) =
  ## Close the seekable input stream
  discard
  # if stream.handle != nil:
  #   g_object_unref(stream.handle)

proc toPtr*(sis: SeekableInputStream): ptr GArrowSeekableInputStream {.inline.} =
  sis.handle

proc size*(stream: SeekableInputStream): uint64 =
  ## Get the total size of the stream in bytes
  if stream.handle == nil:
    raise newException(StreamError, "Cannot get size of closed stream")
  check garrow_seekable_input_stream_get_size(stream.handle)

proc read*(stream: SeekableInputStream, nBytes: int64): seq[byte] =
  ## Read up to nBytes from current position
  if stream.handle == nil:
    raise newException(StreamError, "Cannot read from closed stream")

  let gbytes = check garrow_readable_read_bytes(stream.asReadable, nBytes.gint64)
  result = toBytesSeq(gbytes)

proc readAt*(stream: SeekableInputStream, position: int64, nBytes: int64): seq[byte] =
  ## Read nBytes starting at position (random access)
  ## 
  ## Does not change the stream's current position for sequential reads.
  if stream.handle == nil:
    raise newException(StreamError, "Cannot read from closed stream")

  let gbytes = check garrow_seekable_input_stream_read_at_bytes(
    stream.handle, position.gint64, nBytes.gint64
  )
  result = toBytesSeq(gbytes)

proc readAtBuffer*(
    stream: SeekableInputStream, position: int64, nBytes: int64
): seq[byte] =
  ## Read using GArrowBuffer (alternative)
  if stream.handle == nil:
    raise newException(StreamError, "Cannot read from closed stream")

  let buffer = check garrow_seekable_input_stream_read_at(
    stream.handle, position.gint64, nBytes.gint64
  )
  result = toBytesSeq(buffer)

proc readAll*(stream: SeekableInputStream): seq[byte] =
  ## Read entire stream contents
  let totalSize = stream.size
  if totalSize == 0:
    return @[]
  stream.readAt(0, totalSize.int64)

proc readString*(stream: SeekableInputStream, nBytes: int64): string =
  ## Read nBytes as a string
  let data = stream.read(nBytes)
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc readAtString*(
    stream: SeekableInputStream, position: int64, nBytes: int64
): string =
  ## Read nBytes at position as a string
  let data = stream.readAt(position, nBytes)
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc readAllString*(stream: SeekableInputStream): string =
  ## Read entire stream as a string
  let data = stream.readAll()
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

# =============================================================================
# OutputStream Implementation
# =============================================================================

proc `=destroy`*(stream: OutputStream) =
  if stream.handle != nil:
    # Flush before destroying
    var error: ptr GError = nil
    discard garrow_writable_flush(stream.asWritable, addr error)
    if error != nil:
      g_error_free(error)

    g_object_unref(stream.handle)

proc `=sink`*(dest: var OutputStream, src: OutputStream) =
  if dest.handle != nil and dest.handle != src.handle:
    # Flush before releasing
    var error: ptr GError = nil
    discard garrow_writable_flush(dest.asWritable, addr error)
    if error != nil:
      g_error_free(error)
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var OutputStream, src: OutputStream) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      # Flush before releasing
      var error: ptr GError = nil
      discard garrow_writable_flush(dest.asWritable, addr error)
      if error != nil:
        g_error_free(error)
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc isValid*(stream: OutputStream): bool {.inline.} =
  stream.handle != nil

proc flush*(stream: OutputStream) =
  ## Flush any buffered data to the underlying storage
  if stream.handle == nil:
    raise newException(StreamError, "Cannot flush closed stream")
  check garrow_writable_flush(stream.asWritable)

proc close*(stream: OutputStream) =
  ## Flush and close the output stream
  if stream.handle != nil:
    # Flush before closing
    var error: ptr GError = nil
    discard garrow_writable_flush(stream.asWritable, addr error)
    if error != nil:
      let msg =
        if error.message != nil:
          $error.message
        else:
          "Flush failed"
      g_error_free(error)
      # g_object_unref(stream.handle)
      raise newException(StreamError, msg)

    # g_object_unref(stream.handle)

proc write*(stream: OutputStream, data: openArray[byte]) =
  ## Write bytes to the stream
  if stream.handle == nil:
    raise newException(StreamError, "Cannot write to closed stream")

  if data.len == 0:
    return

  check garrow_writable_write(
    stream.asWritable, cast[ptr guint8](unsafeAddr data[0]), data.len.gint64
  )

proc write*(stream: OutputStream, data: string) =
  ## Write a string to the stream
  if stream.handle == nil:
    raise newException(StreamError, "Cannot write to closed stream")

  if data.len == 0:
    return

  check garrow_writable_write(
    stream.asWritable, cast[ptr guint8](unsafeAddr data[0]), data.len.gint64
  )

proc writeLine*(stream: OutputStream, data: string) =
  ## Write a string followed by a newline
  stream.write(data)
  stream.write("\n")

proc tell*(stream: OutputStream): int64 =
  ## Get the current stream position (bytes written so far)
  if stream.handle == nil:
    raise newException(StreamError, "Cannot tell position of closed stream")
  result = check garrow_file_tell(cast[ptr GArrowFile](stream.handle))

# =============================================================================
# FileSystem Implementation
# =============================================================================

proc `=destroy`*(fs: FileSystemObj) =
  if fs.handle != nil:
    g_object_unref(fs.handle)

proc isValid*(fs: FileSystem): bool {.inline.} =
  ## Check if the filesystem handle is valid
  fs != nil and fs.handle != nil

proc newFileSystem*(uri: string): FileSystem =
  ## Create a filesystem from a URI
  ##
  ## The URI scheme determines the filesystem type:
  ## - `file://` or no scheme: Local filesystem
  ## - `s3://`: Amazon S3 (requires S3 initialization)
  ## - `gs://`: Google Cloud Storage
  ## - `abfs://` or `az://`: Azure Blob Storage
  ## - `hdfs://`: Hadoop Distributed File System
  ##
  ## Example:
  ## ```nim
  ## let localFs = newFileSystem("file:///home/user")
  ## let s3Fs = newFileSystem("s3://my-bucket/prefix")
  ## ```
  new(result)
  let handle = check garrow_file_system_create(uri.cstring)

  # Sink floating reference if present
  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)

  result.handle = handle

proc typeName*(fs: FileSystem): string =
  ## Get the filesystem type name (e.g., "local", "s3", "gcs")
  let cstr = garrow_file_system_get_type_name(fs.handle)
  if cstr != nil:
    result = $newGString(cstr)

proc getFileInfo*(fs: FileSystem, path: string): FileInfo =
  ## Get information about a single path
  ##
  ## Returns FileInfo even if the path doesn't exist (with type = ftNotFound)
  result.handle = check garrow_file_system_get_file_info(fs.handle, path.cstring)

proc getFileInfos*(fs: FileSystem, paths: openArray[string]): seq[FileInfo] =
  ## Get information about multiple paths
  ##
  ## More efficient than calling getFileInfo multiple times.
  if paths.len == 0:
    return @[]

  # Convert paths to C strings array
  var cPaths = newSeq[cstring](paths.len)
  for i, p in paths:
    cPaths[i] = p.cstring

  let glistPtr = check garrow_file_system_get_file_infos_paths(
    fs.handle, addr cPaths[0], paths.len.gsize
  )

  let gList = newGlist[ptr GArrowFileInfo](glistPtr)
  result = newSeq[FileInfo]()
  for p in gList:
    result.add(FileInfo(handle: cast[ptr GArrowFileInfo](p)))

proc getFileInfos*(fs: FileSystem, selector: FileSelector): seq[FileInfo] =
  ## Get information about files matching a selector
  ##
  ## Example:
  ## ```nim
  ## let selector = newFileSelector("/data", recursive = true)
  ## for info in fs.getFileInfos(selector):
  ##   echo info.path
  ## ```
  let glistPtr =
    check garrow_file_system_get_file_infos_selector(fs.handle, selector.handle)
  let gList = newGlist[ptr GArrowFileInfo](glistPtr)
  result = newSeq[FileInfo]()
  for p in gList:
    result.add(FileInfo(handle: cast[ptr GArrowFileInfo](p)))

proc createDir*(fs: FileSystem, path: string, recursive = true) =
  ## Create a directory
  ##
  ## Parameters:
  ## - `path`: Path to the directory to create
  ## - `recursive`: If true, create parent directories as needed
  check garrow_file_system_create_dir(fs.handle, path.cstring, recursive.gboolean)

proc deleteDir*(fs: FileSystem, path: string) =
  ## Delete a directory and all its contents
  ##
  ## Fails if the path is not a directory.
  check garrow_file_system_delete_dir(fs.handle, path.cstring)

proc deleteDirContents*(fs: FileSystem, path: string) =
  ## Delete the contents of a directory, but not the directory itself
  check garrow_file_system_delete_dir_contents(fs.handle, path.cstring)

proc deleteFile*(fs: FileSystem, path: string) =
  ## Delete a file
  ##
  ## Fails if the path is not a regular file.
  check garrow_file_system_delete_file(fs.handle, path.cstring)

proc deleteFiles*(fs: FileSystem, paths: openArray[string]) =
  ## Delete multiple files
  ##
  ## More efficient than calling deleteFile multiple times.
  ## All paths must be regular files.
  if paths.len == 0:
    return

  var cPaths = newSeq[cstring](paths.len)
  for i, p in paths:
    cPaths[i] = p.cstring

  check garrow_file_system_delete_files(fs.handle, addr cPaths[0], paths.len.gsize)

proc move*(fs: FileSystem, src, dest: string) =
  ## Move/rename a file or directory
  ##
  ## If `dest` already exists and is a directory, and `src` is also a directory,
  ## `src` is moved into `dest`.
  check garrow_file_system_move(fs.handle, src.cstring, dest.cstring)

proc rename*(fs: FileSystem, src, dest: string) {.inline.} =
  ## Alias for move
  fs.move(src, dest)

proc copyFile*(fs: FileSystem, src, dest: string) =
  ## Copy a file
  ##
  ## If `dest` already exists, it is overwritten.
  check garrow_file_system_copy_file(fs.handle, src.cstring, dest.cstring)

proc openInputStream*(fs: FileSystem, path: string): InputStream =
  ## Open a file for reading (sequential access)
  result.handle = check garrow_file_system_open_input_stream(fs.handle, path.cstring)

proc openInputFile*(fs: FileSystem, path: string): SeekableInputStream =
  ## Open a file for reading with random access (seeking)
  result.handle = check garrow_file_system_open_input_file(fs.handle, path.cstring)

proc openOutputStream*(fs: FileSystem, path: string): OutputStream =
  ## Open a file for writing (creates or truncates)
  result.handle = check garrow_file_system_open_output_stream(fs.handle, path.cstring)

proc openAppendStream*(fs: FileSystem, path: string): OutputStream =
  ## Open a file for appending
  result.handle = check garrow_file_system_open_append_stream(fs.handle, path.cstring)

# FIXME: not sure about these
proc readFile*(fs: FileSystem, path: string): seq[byte] =
  ## Read entire file contents as bytes
  let stream = fs.openInputFile(path)
  try:
    result = stream.readAll()
  finally:
    stream.close()

proc readText*(fs: FileSystem, path: string): string =
  ## Read entire file contents as a string
  let data = fs.readFile(path)
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc writeFile*(fs: FileSystem, path: string, data: openArray[byte]) =
  ## Write bytes to a file (creates or overwrites)
  let stream = fs.openOutputStream(path)
  try:
    stream.write(data)
  finally:
    stream.close()

proc writeText*(fs: FileSystem, path: string, text: string) =
  ## Write a string to a file (creates or overwrites)
  let stream = fs.openOutputStream(path)
  try:
    stream.write(text)
  finally:
    stream.close()

proc appendFile*(fs: FileSystem, path: string, data: openArray[byte]) =
  ## Append bytes to a file
  let stream = fs.openAppendStream(path)
  try:
    stream.write(data)
  finally:
    stream.close()

proc appendText*(fs: FileSystem, path: string, text: string) =
  ## Append a string to a file
  let stream = fs.openAppendStream(path)
  try:
    stream.write(text)
  finally:
    stream.close()

# =============================================================================
# LocalFileSystem Implementation
# =============================================================================

proc newLocalFileSystem*(options: LocalFileSystemOptions): LocalFileSystem =
  ## Create a local filesystem with custom options
  new(result)
  let handle = garrow_local_file_system_new(options.handle)

  # Sink floating reference if present
  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)

  result.handle = cast[ptr GArrowFileSystem](handle)

proc newLocalFileSystem*(): LocalFileSystem =
  ## Create a local filesystem with default options
  ##
  ## Provides access to files on the local machine. Symlinks are automatically
  ## followed except when deleting entries.
  let options = newLocalFileSystemOptions()
  newLocalFileSystem(options)
