import criterion
import ../src/narrow
import ./config

let cfg = narrowConfig()

# ---------------------------------------------------------------------------
# Array builder benchmarks — inspired by arrow/cpp/src/arrow/builder_benchmark.cc
# ---------------------------------------------------------------------------

benchmark cfg:

  proc benchBuildInt64Array1K {.measure.} =
    var builder = newArrayBuilder[int64]()
    for i in 0 ..< 1_000:
      builder.append(i.int64)
    var result = builder.finish()
    blackBox(result)

  proc benchBuildInt64Array100K {.measure.} =
    var builder = newArrayBuilder[int64]()
    for i in 0 ..< 100_000:
      builder.append(i.int64)
    var result = builder.finish()
    blackBox(result)

  proc benchBuildInt64Array1M {.measure.} =
    var builder = newArrayBuilder[int64]()
    for i in 0 ..< 1_000_000:
      builder.append(i.int64)
    var result = builder.finish()
    blackBox(result)

  proc benchBuildInt64Array1MWithNulls {.measure.} =
    var builder = newArrayBuilder[int64]()
    for i in 0 ..< 1_000_000:
      if i mod 10 == 0:
        builder.appendNull()
      else:
        builder.append(i.int64)
    var result = builder.finish()
    blackBox(result)

  proc benchBuildInt64Array1MBulk {.measure.} =
    var values = newSeq[int64](1_000_000)
    for i in 0 ..< 1_000_000:
      values[i] = i.int64
    var builder = newArrayBuilder[int64]()
    builder.appendValues(values)
    var result = builder.finish()
    blackBox(result)

  proc benchBuildFloat64Array1M {.measure.} =
    var builder = newArrayBuilder[float64]()
    for i in 0 ..< 1_000_000:
      builder.append(i.float64)
    var result = builder.finish()
    blackBox(result)

  proc benchBuildBoolArray1M {.measure.} =
    var builder = newArrayBuilder[bool]()
    for i in 0 ..< 1_000_000:
      builder.append(i mod 2 == 0)
    var result = builder.finish()
    blackBox(result)

  proc benchBuildStringArray100K {.measure.} =
    var builder = newArrayBuilder[string]()
    for i in 0 ..< 100_000:
      builder.append("value_" & $i)
    var result = builder.finish()
    blackBox(result)

  proc benchArrayToSeqInt64Array1M {.measure.} =
    var values = newSeq[int64](1_000_000)
    for i in 0 ..< 1_000_000:
      values[i] = i.int64
    let arr = newArray(values)
    var result = arr.toSeq()
    blackBox(result)

  proc benchArrayEqualityInt64Array1M {.measure.} =
    var values = newSeq[int64](1_000_000)
    for i in 0 ..< 1_000_000:
      values[i] = i.int64
    let a = newArray(values)
    let b = newArray(values)
    var result = a == b
    blackBox(result)
