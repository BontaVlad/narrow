## Test Fixture System
##
## Provides isolated test directories with automatic cleanup.
##
## Usage:
##   import testfixture
##   
##   suite "My Tests":
##     var fixture: TestFixture
##     
##     setup:
##       fixture = newTestFixture("test_csv")
##     
##     teardown:
##       fixture.cleanup()
##     
##     test "something":
##       let path = fixture / "data.csv"

import std/[os, times, strformat, strutils, random, algorithm]

when defined(posix):
  import std/posix

const
  BaseDir* = "/tmp/narrow"
  RunsDir = BaseDir / "runs"
  CurrentSymlink = BaseDir / "current"
  LockFile = BaseDir / ".fixture.lock"
  MaxRunsToKeep = 2

# Module-level state for session management
var
  sessionRunId {.global.}: string = ""  # Shared across all tests in same session
  sessionInitialized {.global.}: bool = false
  rng {.global.}: Rand
  testCounter {.global.}: int = 0

proc initSessionIfNeeded() =
  ## Initialize the RNG if not already done
  if rng == default(Rand):
    rng = initRand()

proc generateRunId(): string =
  ## Generate timestamp-runId: YYYYMMDD-HHMMSS-<8_hex_chars>
  initSessionIfNeeded()
  let now = now()
  let timestamp = now.format("yyyyMMdd-HHmmss")
  let randomSuffix = rng.rand(high(int32)).toHex(8)
  result = fmt"{timestamp}-{randomSuffix}"

proc acquireLock(): File =
  ## Acquire exclusive lock for filesystem operations
  createDir(BaseDir)
  result = open(LockFile, fmReadWrite)
  
  when defined(posix):
    var lock: Tflock
    lock.l_type = cshort(F_WRLCK)
    lock.l_whence = cshort(SEEK_SET)
    lock.l_start = 0
    lock.l_len = 0
    
    if fcntl(getFileHandle(result), F_SETLKW, addr lock) == -1:
      raise newException(IOError, "Failed to acquire lock: " & $strerror(errno))

proc releaseLock(f: File) =
  ## Release the exclusive lock
  when defined(posix):
    var lock: Tflock
    lock.l_type = cshort(F_UNLCK)
    lock.l_whence = cshort(SEEK_SET)
    lock.l_start = 0
    lock.l_len = 0
    discard fcntl(getFileHandle(f), F_SETLK, addr lock)
  f.close()

proc cleanupOldRuns() =
  ## Remove old runs, keeping only the most recent MaxRunsToKeep
  if not dirExists(RunsDir):
    return
  
  var runs: seq[string]
  for kind, path in walkDir(RunsDir):
    if kind == pcDir:
      runs.add(path)
  
  # Sort in reverse order (newest first based on timestamp in name)
  runs.sort(system.cmp, SortOrder.Descending)
  
  # Remove excess runs
  if runs.len > MaxRunsToKeep:
    for i in MaxRunsToKeep ..< runs.len:
      try:
        removeDir(runs[i])
      except:
        stderr.writeLine("Warning: Failed to cleanup old run: " & runs[i])

proc initializeSession(): string =
  ## Initialize the session run ID (thread-safe)
  let lock = acquireLock()
  defer: releaseLock(lock)
  
  if not sessionInitialized:
    sessionRunId = generateRunId()
    sessionInitialized = true
    
    # Create the run directory
    createDir(RunsDir / sessionRunId)
    
    # Update current symlink
    try:
      if symlinkExists(CurrentSymlink):
        removeFile(CurrentSymlink)
      createSymlink("runs" / sessionRunId, CurrentSymlink)
      cleanupOldRuns()
    except:
      stderr.writeLine("Warning: Failed to update current symlink: " & getCurrentExceptionMsg())
  
  result = sessionRunId

type
  TestFixture* = object
    basePath*: string      ## Full path to test directory
    runPath*: string       ## Path to the run session directory
    runId*: string         ## Timestamp-runId identifier
    suiteName*: string     ## Name of the test suite
    testName*: string      ## Name of the current test

proc generateTestName(): string =
  ## Generate a unique test name using counter
  initSessionIfNeeded()
  inc(testCounter)
  result = fmt"test_{testCounter:04d}"

proc newTestFixture*(suiteName: string): TestFixture =
  ## Creates a new test fixture for the current test.
  ## Uses auto-generated test names for isolation.
  ## All tests in the same process share the same run session.
  ##
  ## Example:
  ##   var fixture = newTestFixture("test_csv")
  ##   let path = fixture / "output.csv"
  let runId = initializeSession()
  let testName = generateTestName()
  
  result.suiteName = suiteName
  result.testName = testName
  result.runId = runId
  result.runPath = RunsDir / runId
  result.basePath = result.runPath / suiteName / testName
  
  # Create directory structure
  createDir(result.basePath)

proc newTestFixture*(suiteName, testName: string): TestFixture =
  ## Creates a new test fixture with explicit test name.
  ## Use this for more descriptive test directory names.
  let runId = initializeSession()
  
  result.suiteName = suiteName
  result.testName = testName
  result.runId = runId
  result.runPath = RunsDir / runId
  result.basePath = result.runPath / suiteName / testName
  
  # Create directory structure
  createDir(result.basePath)

proc `/`*(fixture: TestFixture, subpath: string): string {.inline.} =
  ## Path concatenation operator for fixture.
  ## Returns fixture.basePath / subpath
  result = fixture.basePath / subpath

proc cleanup*(fixture: TestFixture) =
  ## Remove the test directory and all its contents.
  ## Call this in teardown to clean up after each test.
  try:
    if dirExists(fixture.basePath):
      removeDir(fixture.basePath)
  except:
    stderr.writeLine("Warning: Failed to cleanup test directory: " & fixture.basePath)

proc `=destroy`*(fixture: TestFixture) =
  ## RAII cleanup - no-op by default.
  ## Tests must explicitly call cleanup() if they want cleanup.
  discard

proc listRuns*(): seq[string] =
  ## List all run directories sorted by timestamp (newest first).
  ## Useful for debugging and inspection.
  if not dirExists(RunsDir):
    return @[]
  
  result = @[]
  for kind, path in walkDir(RunsDir):
    if kind == pcDir:
      result.add(path)
  
  result.sort(system.cmp, SortOrder.Descending)

proc getCurrentRun*(): string =
  ## Get the path to the current run directory (via symlink).
  if symlinkExists(CurrentSymlink):
    result = CurrentSymlink
  else:
    result = ""

proc getSessionRunId*(): string =
  ## Get the current session run ID (empty if not initialized).
  result = sessionRunId
