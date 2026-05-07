import criterion
import ../src/narrow
import ./config

let cfg = narrowConfig()

# Reusable schema and data
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

# Pre-build tables at module scope so operation benchmarks measure
# only the operation, not construction.
let table100K = makeTable(100_000)
let table1M = makeTable(1_000_000)

benchmark cfg:

  proc benchBuildTable100K {.measure.} =
    var result = makeTable(100_000)
    blackBox(result)

  proc benchBuildTable1M {.measure.} =
    var result = makeTable(1_000_000)
    blackBox(result)

  proc benchTableConcatenate {.measure.} =
    let t2 = makeTable(100_000)
    var result = table100K.concatenate([t2])
    blackBox(result)

  proc benchTableSlice {.measure.} =
    var result = table1M.slice(100_000, 500_000)
    blackBox(result)

  proc benchTableValidate {.measure.} =
    var result = table100K.validate()
    blackBox(result)
