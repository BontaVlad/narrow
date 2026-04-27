import criterion
import ../src/narrow
import ../src/narrow/core/utils
import ./config
import std/random

let cfg = narrowConfig()

# ============================================================================
# Test data
# ============================================================================

proc makeTable(nRows: int): ArrowTable =
  var rng = initRand(42)
  var ids = newSeqOfCap[int64](nRows)
  var values = newSeqOfCap[float64](nRows)
  var actives = newSeqOfCap[bool](nRows)
  for i in 0 ..< nRows:
    ids.add i.int64
    values.add rng.rand(0.0..1_000_000.0)
    actives.add rng.rand(1.0) < 0.3
  let schema = newSchema([
    newField[int64]("id"),
    newField[float64]("value"),
    newField[bool]("active"),
  ])
  newArrowTable(schema, newArray(ids), newArray(values), newArray(actives))

let table1K = makeTable(1_000)
let table10K = makeTable(10_000)
let table100K = makeTable(100_000)
let table1M = makeTable(1_000_000)

let exprSimple = col("id") >= 500'i64
let exprComplex = (col("id") >= 100'i64) and (col("value") < 900_000.0) and (col("active") == true)

benchmark cfg:

  # --------------------------------------------------------------------------
  # Scaling: how does filterTable behave with table size?
  # --------------------------------------------------------------------------
  proc benchFilter1K {.measure.} =
    let result = filterTable(table1K, exprSimple)
    blackBox(result.nRows)

  proc benchFilter10K {.measure.} =
    let result = filterTable(table10K, exprSimple)
    blackBox(result.nRows)

  proc benchFilter100K {.measure.} =
    let result = filterTable(table100K, exprSimple)
    blackBox(result.nRows)

  proc benchFilter1M {.measure.} =
    let result = filterTable(table1M, exprSimple)
    blackBox(result.nRows)

  # --------------------------------------------------------------------------
  # Expression complexity: simple vs complex on same data
  # --------------------------------------------------------------------------
  proc benchFilterSimple1M {.measure.} =
    let result = filterTable(table1M, exprSimple)
    blackBox(result.nRows)

  proc benchFilterComplex1M {.measure.} =
    let result = filterTable(table1M, exprComplex)
    blackBox(result.nRows)

  # --------------------------------------------------------------------------
  # Overhead isolation: repeated calls on same table
  # If plan creation dominates, repeated calls won't amortize.
  # If data scanning dominates, repeated calls should be similar.
  # --------------------------------------------------------------------------
  proc benchFilterRepeated10x10K {.measure.} =
    var total: int64 = 0
    for i in 0 ..< 10:
      let result = filterTable(table10K, exprSimple)
      total += result.nRows
    blackBox(total)

  # --------------------------------------------------------------------------
  # Low-level: plan creation + validation only (no execution)
  # This isolates the Acero setup overhead.
  # --------------------------------------------------------------------------
  proc benchPlanBuildOnly10K {.measure.} =
    ensureComputeInitialized()
    let executor = newThreadPool().toExecutor
    let ctx = newExecuteContext(executor)
    let plan = newExecutePlan(ctx)
    let sourceOpts = newSourceNodeOptions(table10K)
    let sourceNode = plan.buildSourceNode(sourceOpts)
    let filterOpts = newFilterNodeOptions(exprSimple)
    let filterNode = plan.buildFilterNode(sourceNode, filterOpts)
    let sinkOpts = newSinkNodeOptions()
    discard plan.buildSinkNode(filterNode, sinkOpts)
    plan.validate()
    blackBox(plan)

  # --------------------------------------------------------------------------
  # Low-level: execution with pre-built context/plan
  # This isolates the actual data scanning + filtering time.
  # --------------------------------------------------------------------------
  proc benchPlanExecuteOnly10K {.measure.} =
    ensureComputeInitialized()
    let executor = newThreadPool().toExecutor
    let ctx = newExecuteContext(executor)
    let plan = newExecutePlan(ctx)
    let sourceOpts = newSourceNodeOptions(table10K)
    let sourceNode = plan.buildSourceNode(sourceOpts)
    let filterOpts = newFilterNodeOptions(exprSimple)
    let filterNode = plan.buildFilterNode(sourceNode, filterOpts)
    let sinkOpts = newSinkNodeOptions()
    discard plan.buildSinkNode(filterNode, sinkOpts)
    plan.validate()
    let outputSchema = filterNode.outputSchema
    let reader = sinkOpts.getReader(outputSchema)
    plan.start()
    let result = reader.readAll()
    plan.wait()
    blackBox(result.nRows)
