import criterion
import std/os
import ../src/narrow
import ./config

let cfg = narrowConfig()

# ---------------------------------------------------------------------------
# IPC read/write benchmarks — inspired by arrow/cpp/src/arrow/ipc/read_write_benchmark.cc
# ---------------------------------------------------------------------------

let ipcSchema = newSchema(@[
  newField[int64]("f0"),
  newField[int64]("f1"),
  newField[int64]("f2"),
  newField[int64]("f3"),
])

proc makeIpcTable(nRows: int): ArrowTable =
  var f0 = newArrayBuilder[int64]()
  var f1 = newArrayBuilder[int64]()
  var f2 = newArrayBuilder[int64]()
  var f3 = newArrayBuilder[int64]()
  for i in 0 ..< nRows:
    f0.append(i.int64)
    f1.append((i + 1).int64)
    f2.append((i + 2).int64)
    f3.append((i + 3).int64)
  newArrowTable(ipcSchema, f0.finish(), f1.finish(), f2.finish(), f3.finish())

const ipcTmpPath = "/tmp/narrow_bench_ipc.arrow"

# Pre-create test file
let ipcSetupTable = makeIpcTable(1_000_000)
writeTable(ipcSetupTable, ipcTmpPath)

benchmark cfg:

  proc benchIpcWriteTable1M {.measure.} =
    let table = makeIpcTable(1_000_000)
    let path = ipcTmpPath & ".write"
    writeTable(table, path)
    removeFile(path)

  proc benchIpcReadTable1M {.measure.} =
    discard readTable(ipcTmpPath)
