import std/os
import unittest2
import ../src/narrow/io/filesystem

suite "Uri parsing and access":

  test "parse simple file URI":
    let uri = newUri("file:///home/user/test.txt")
    check uri.scheme == "file"
    check uri.host == ""
    check uri.path == "/home/user/test.txt"
    check uri.port == -1

  test "parse file URI with no path":
    let uri = newUri("file:///")
    check uri.scheme == "file"
    check uri.path == "/"

  test "parse HTTP URI":
    let uri = newUri("http://example.com:8080/path/to/resource?query=1#fragment")
    check uri.scheme == "http"
    check uri.host == "example.com"
    check uri.port == 8080
    check uri.path == "/path/to/resource"
    check uri.query == "query=1"
    check uri.fragment == "fragment"

  test "parse HTTPS URI":
    let uri = newUri("https://secure.example.com/api/data")
    check uri.scheme == "https"
    check uri.host == "secure.example.com"
    check uri.port == -1
    check uri.path == "/api/data"

  test "parse S3 URI":
    let uri = newUri("s3://my-bucket/path/to/file.parquet")
    check uri.scheme == "s3"
    check uri.host == "my-bucket"
    check uri.path == "/path/to/file.parquet"

  test "parse URI with userinfo":
    let uri = newUri("ftp://user:password@example.com/file.txt")
    check uri.scheme == "ftp"
    check uri.user == "user"
    check uri.password == "password"
    check uri.host == "example.com"

  test "toString roundtrip - file URI":
    let original = "file:///home/user/test.txt"
    let uri = newUri(original)
    let str = uri.toString
    check str == original

  test "toString roundtrip - HTTP URI with port":
    let original = "http://example.com:8080/path?query=1"
    let uri = newUri(original)
    let str = uri.toString
    check str == original

  test "toString roundtrip - S3 URI":
    let original = "s3://bucket/key/file.parquet"
    let uri = newUri(original)
    let str = uri.toString
    check str == original

  test "isValid - valid URIs":
    check isValidUri("file:///tmp")
    check isValidUri("http://example.com")
    check isValidUri("s3://bucket/path")

  test "isValid - invalid URIs":
    check not isValidUri("not-a-uri")
    check not isValidUri("")
    check not isValidUri("ht tp://bad.com")

  test "scheme extraction only":
    let scheme = getScheme("http://example.com/path")
    check scheme == "http"

  test "URI from absolute path":
    let uri = newUri("/absolute/path/file.txt")
    check uri.scheme == "file"
    check uri.path == "/absolute/path/file.txt"

  test "URI copying":
    let uri1 = newUri("file:///tmp/test.txt")
    let uri2 = uri1
    check uri2.path == "/tmp/test.txt"
    check uri1.path == "/tmp/test.txt"

  test "URI building from components":
    let uri = newUri(scheme = "s3", host = "my-bucket", path = "/data/file.parquet")
    check uri.scheme == "s3"
    check uri.host == "my-bucket"
    check uri.path == "/data/file.parquet"
    check uri.toString == "s3://my-bucket/data/file.parquet"

  test "URI building with port":
    let uri = newUri(scheme = "http", host = "localhost", port = 9000, path = "/api")
    check uri.scheme == "http"
    check uri.host == "localhost"
    check uri.port == 9000
    check uri.path == "/api"
    check uri.toString == "http://localhost:9000/api"

suite "Uri with FileSystem":

  test "newFileSystem from Uri object":
    let uri = newUri("file://" & getCurrentDir())
    let fs = newFileSystem(uri)
    check fs.isValid

  test "newFileSystem from string (backward compat)":
    let fs = newFileSystem("file://" & getCurrentDir())
    check fs.isValid

  test "FileSystem with Uri roundtrip":
    let uri = newUri("file://" & getCurrentDir())
    let fs = newFileSystem(uri)
    let info = fs.getFileInfo(".")
    check info.isDir
