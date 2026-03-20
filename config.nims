# --- Project paths ---
switch("path", "$projectDir/../src")

# --- pkg-config flags (safe for LSP) ---
when not defined(nimsuggest):
  let libs = gorge("pkg-config --libs arrow-dataset-glib parquet-glib")
  if libs.len > 0:
    switch("passL", libs)

  let cflags = gorge("pkg-config --cflags arrow-dataset-glib parquet-glib")
  if cflags.len > 0:
    switch("passC", cflags)

# --- Sanitizers ---
when defined(useSanitizers):
  switch("passL", "-fsanitize=address")
  switch("passC", "-fsanitize=address")
  switch("define", "useMalloc")
  switch("stacktrace", "on")
  switch("excessiveStackTrace", "on")
  switch("debuginfo", "on")

# --- Nimble integration ---
# begin Nimble config (version 2)
when withDir(thisDir(), system.fileExists("nimble.paths")):
  include "nimble.paths"
# end Nimble config
