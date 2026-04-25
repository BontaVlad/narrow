import criterion
import std/os
import ../src/narrow
import ./config

let cfg = narrowConfig()

let testSchema = newSchema(@[
  newField[int64]("id"),
  newField[string]("name"),
  newField[float64]("value"),
  newField[bool]("active"),
])

proc makeTable(nRows: int): ArrowTable =
  var ids = newArrayBuilder[int64]()
  var names = newArrayBuilder[string]()
  var values = newArrayBuilder[float64]()
  var actives = newArrayBuilder[bool]()
  for i in 0 ..< nRows:
    ids.append(i.int64)
    names.append("name_" & $i)
    values.append(i.float64)
    actives.append(i mod 2 == 0)
  newArrowTable(testSchema, ids.finish(), names.finish(), values.finish(), actives.finish())

const tmpPath = "/tmp/narrow_bench.parquet"

# One-time setup: create the file so read benchmarks have data
removeFile(tmpPath)
writeTable(makeTable(1_000_000), tmpPath)

benchmark cfg:

  proc benchWriteParquet1M {.measure.} =
    let path = tmpPath & ".write"
    let table = makeTable(1_000_000)
    writeTable(table, path)
    removeFile(path)

  proc benchReadParquet1M {.measure.} =
    discard readTable(tmpPath)

  proc benchReadParquetColumns1M {.measure.} =
    discard readTable(tmpPath, @["id", "value"])
