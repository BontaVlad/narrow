# # --- Global Settings ---
# switch("backend", "cpp")
# # --- Silence Output ---
# switch("warnings", "off")   # Hides Nim compiler warnings
# switch("hints", "off")      # Hides Nim "Hint:" messages
# switch("passC", "-w")       # Hides all C++ compiler warnings

# Project paths ---
switch("path", "$projectDir/../src")

# --- pkg-config flags (safe for LSP) ---
when not defined(nimsuggest):
  let libs = gorge("pkg-config --libs arrow-dataset-glib parquet-glib")
  if libs.len > 0:
    switch("passL", libs)

  let cflags = gorge("pkg-config --cflags arrow-dataset-glib parquet-glib")
  if cflags.len > 0:
    switch("passC", cflags)

  # --- Arrow version check ---
  import std/[strutils, parseutils]
  let arrowVer = gorge("pkg-config --modversion arrow-glib")
  if arrowVer.len > 0:
    let majorDot = arrowVer.find('.')
    if majorDot > 0:
      let majorStr = arrowVer[0 ..< majorDot]
      var major: int
      if parseInt(majorStr, major) == majorStr.len and major < 24:
        echo "ERROR: Arrow GLib >= 24.0.0 is required. Found: " & arrowVer
        quit(1)

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
