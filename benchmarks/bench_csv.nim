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
    var result = readCsv(csvTmpPath)
    blackBox(result)

  proc benchCsvWriteTable100K {.measure.} =
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
      newField[float64]("value"),
      newField[bool]("active")
    ])
    var ids = newSeq[int32](100_000)
    var names = newSeq[string](100_000)
    var values = newSeq[float64](100_000)
    var actives = newSeq[bool](100_000)
    for i in 0 ..< 100_000:
      ids[i] = i.int32
      names[i] = "name_" & $i
      values[i] = i.float64
      actives[i] = i mod 2 == 0
    let table = newArrowTable(schema, newArray(ids), newArray(names), newArray(values), newArray(actives))
    let path = csvTmpPath & ".write_table"
    writeCsv(path, table, newWriteOptions())
    removeFile(path)
