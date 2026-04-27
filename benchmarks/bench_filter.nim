import criterion
import ../src/narrow
import ./config

let cfg = narrowConfig()

# ---------------------------------------------------------------------------
# Filter benchmarks — inspired by arrow/cpp/src/arrow/acero/filter_benchmark.cc
# ---------------------------------------------------------------------------

let filterSchema = newSchema(@[
  newField[int64]("id"),
  newField[float64]("value"),
  newField[bool]("active"),
])

proc makeFilterTable(nRows: int): ArrowTable =
  var ids = newArrayBuilder[int64]()
  var values = newArrayBuilder[float64]()
  var actives = newArrayBuilder[bool]()
  for i in 0 ..< nRows:
    ids.append(i.int64)
    values.append(i.float64)
    actives.append(i mod 3 == 0)
  newArrowTable(filterSchema, ids.finish(), values.finish(), actives.finish())

benchmark cfg:

  proc benchFilterTableInt64Equal1M {.measure.} =
    let table = makeFilterTable(1_000_000)
    let filter = col("id") == 500_000'i64
    var result = filterTable(table, filter)
    blackBox(result)

  proc benchFilterTableFloat64Greater1M {.measure.} =
    let table = makeFilterTable(1_000_000)
    let filter = col("value") > 500_000.0
    var result = filterTable(table, filter)
    blackBox(result)

  proc benchFilterTableBoolEqual1M {.measure.} =
    let table = makeFilterTable(1_000_000)
    let filter = col("active") == true
    var result = filterTable(table, filter)
    blackBox(result)

  proc benchFilterTableCompound1M {.measure.} =
    let table = makeFilterTable(1_000_000)
    let filter = (col("id") >= 100_000'i64) and (col("value") < 900_000.0)
    var result = filterTable(table, filter)
    blackBox(result)
