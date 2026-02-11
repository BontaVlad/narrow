switch("path", "$projectDir/../src")
switch("passL", "-larrow-glib")
switch("passL", "-lgobject-2.0")
switch("passL", "-lglib-2.0")
switch("passL", "-larrow")
switch("passL", "-lparquet-glib")

when defined(useSanitizers):
  switch("passL", "-fsanitize=address")
  switch("passc", "-fsanitize=address")
  switch("define", "useMalloc")
  switch("stacktrace", "on")
  switch("excessiveStackTrace", "on")
  switch("debuginfo", "on")
