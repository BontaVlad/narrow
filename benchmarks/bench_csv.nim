import criterion
import std/os
import ../src/narrow
import ./config

let cfg = narrowConfig()

# ---------------------------------------------------------------------------
# CSV benchmarks — inspired by arrow/cpp/src/arrow/csv/converter_benchmark.cc
# ---------------------------------------------------------------------------

const csvTmpPath = "/tmp/narrow_bench.csv"

proc makeCsvData(nRows: int): string =
  result = "id,name,value,active\n"
  for i in 0 ..< nRows:
    result.add($i)
    result.add(",name_")
    result.add($i)
    result.add(",")
    result.add($(i.float64))
    result.add(",")
    result.add($(i mod 2 == 0))
    result.add("\n")

# Pre-create test file
let csvData = makeCsvData(100_000)
writeFile(csvTmpPath, csvData)

benchmark cfg:

  proc benchCsvWriteData100K {.measure.} =
    let data = makeCsvData(100_000)
    let path = csvTmpPath & ".write"
    writeFile(path, data)
    removeFile(path)

  proc benchCsvReadTable100K {.measure.} =
    discard readCsv(csvTmpPath)

