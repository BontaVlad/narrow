switch("path", "$projectDir/../src")
switch("passL", gorge("pkg-config --libs arrow-dataset-glib parquet-glib"))
switch("passC", gorge("pkg-config --cflags arrow-dataset-glib parquet-glib"))

when defined(useSanitizers):
  switch("passL", "-fsanitize=address")
  switch("passc", "-fsanitize=address")
  switch("define", "useMalloc")
  switch("stacktrace", "on")
  switch("excessiveStackTrace", "on")
  switch("debuginfo", "on")
# begin Nimble config (version 2)
when withDir(thisDir(), system.fileExists("nimble.paths")):
  include "nimble.paths"
# end Nimble config
