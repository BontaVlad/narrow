import std/[os, osproc, strutils, strformat, re]

const DefaultCookbookDir = "docs/cookbook"

# Matches: ```nim test\n<code>```\n\n```\n<o>```
# Group 1 = code block (with fences), group 2 = existing output (may be empty)
let nimTestRe = re("(```nim test\n.*?```)\n\n```\n(.*?)```", {reDotAll})

proc getProjectFlags(): string =
  let srcPath = getCurrentDir() / "src"
  let cflags = execProcess("pkg-config --cflags arrow-dataset-glib parquet-glib").strip()
  let lflags = execProcess("pkg-config --libs arrow-dataset-glib parquet-glib").strip()
  result = fmt"--path:{srcPath.quoteShell} --passC:{cflags.quoteShell} --passL:{lflags.quoteShell}"

proc runSnippet(code: string, flags: string): string =
  let tmpFile = getTempDir() / "snippet_tmp.nim"
  writeFile(tmpFile, code)
  let cmd = fmt"nim r --hints:off --warnings:off {flags} {tmpFile.quoteShell}"
  let (output, exitCode) = execCmdEx(cmd)
  result = if exitCode == 0: output.strip()
           else: "Error during execution:\n" & output.strip()

proc processFile(path: string, flags: string) =
  echo "Processing: ", path
  let content = readFile(path)

  var pairs: seq[(string, string, string)] = @[]  # (fullMatch, codeBlock, oldOutput)
  var captures: array[2, string]
  var pos = 0
  while true:
    let (matchStart, matchEnd) = findBounds(content, nimTestRe, captures, pos)
    if matchStart < 0: break
    pairs.add((content[matchStart .. matchEnd], captures[0], captures[1]))
    pos = matchEnd + 1

  if pairs.len == 0:
    echo "  (no runnable snippets, skipping)"
    return

  var output = content
  for idx, (fullMatch, codeBlock, _) in pairs:
    echo fmt"  [{idx + 1}/{pairs.len}] running snippet..."
    let snippetOutput = runSnippet(
      codeBlock.replace("```nim test\n", "").replace("\n```", ""),
      flags
    )
    let replacement = codeBlock & "\n\n```\n" & snippetOutput & "\n```"
    output = output.replace(fullMatch, replacement)

  writeFile(path, output)
  echo "  done"

proc processDir(dir: string, flags: string) =
  for path in walkDirRec(dir):
    if path.endsWith(".md"):
      processFile(path, flags)

if isMainModule:
  let args      = commandLineParams()
  let targetDir = if args.len >= 1: args[0] else: DefaultCookbookDir

  if not dirExists(targetDir):
    quit("Directory not found: " & targetDir, 1)

  echo "narrow cookbook runner -- ", targetDir
  let flags = getProjectFlags()
  processDir(targetDir, flags)
  echo "Done."
