import std/[cpuinfo, strutils]
import ../core/[ffi, error, utils]
import ../types/glist
import ../column/metadata
import ../tabular/[table, batch]
import ../compute/expressions

# ============================================================================
# Type Definitions
# ============================================================================

arcGObject:
  type
    ExecuteContext* = object
      handle*: ptr GArrowExecuteContext

    ExecutePlan* = object
      handle*: ptr GArrowExecutePlan

    ExecuteNode* = object
      handle*: ptr GArrowExecuteNode

    SourceNodeOptions* = object
      handle*: ptr GArrowSourceNodeOptions

    FilterNodeOptions* = object
      handle*: ptr GArrowFilterNodeOptions

    SinkNodeOptions* = object
      handle*: ptr GArrowSinkNodeOptions

    ThreadPool* = object
      handle*: ptr GArrowThreadPool

func toExecutor*(pool: ThreadPool): ptr GArrowExecutor {.inline.} =
  cast[ptr GArrowExecutor](pool.handle)

# ============================================================================
# Constructors
# ============================================================================

proc newExecuteContext*(executor: ptr GArrowExecutor): ExecuteContext =
  ## Creates a new execution context using the default executor.
  result.handle = garrow_execute_context_new(executor)

proc newExecutePlan*(ctx: ExecuteContext): ExecutePlan =
  ## Creates a new execution plan.
  result.handle = verify garrow_execute_plan_new(ctx.toPtr)

proc newSourceNodeOptions*(table: ArrowTable): SourceNodeOptions =
  ## Creates source node options from a table.
  result.handle = garrow_source_node_options_new_table(table.toPtr)

proc newSourceNodeOptions*(batch: RecordBatch): SourceNodeOptions =
  ## Creates source node options from a record batch.
  result.handle = garrow_source_node_options_new_record_batch(batch.toPtr)

proc newFilterNodeOptions*(expr: Expression): FilterNodeOptions =
  ## Creates filter node options from an expression.
  result.handle = garrow_filter_node_options_new(expr.toPtr)

proc newSinkNodeOptions*(): SinkNodeOptions =
  ## Creates new sink node options for capturing output.
  result.handle = garrow_sink_node_options_new()

proc newThreadPool*(n_threads: int = countProcessors()): ThreadPool =
  result.handle = verify garrow_thread_pool_new(n_threads = n_threads.guint)

# ============================================================================
# Plan Building
# ============================================================================

proc buildSourceNode*(plan: ExecutePlan, options: SourceNodeOptions): ExecuteNode =
  ## Adds a source node to the plan.
  let handle = verify garrow_execute_plan_build_source_node(plan.toPtr, options.toPtr)
  result = ExecuteNode(handle: handle)

proc buildFilterNode*(
    plan: ExecutePlan, input: ExecuteNode, options: FilterNodeOptions
): ExecuteNode =
  ## Adds a filter node after `input`.
  let handle =
    verify garrow_execute_plan_build_filter_node(plan.toPtr, input.toPtr, options.toPtr)
  result = ExecuteNode(handle: handle)

proc buildSinkNode*(
    plan: ExecutePlan, input: ExecuteNode, options: SinkNodeOptions
): ExecuteNode =
  ## Adds a sink node after `input`. Results are read from SinkNodeOptions.
  let handle =
    verify garrow_execute_plan_build_sink_node(plan.toPtr, input.toPtr, options.toPtr)
  result = ExecuteNode(handle: handle)

# ============================================================================
# Node Properties
# ============================================================================

proc outputSchema*(node: ExecuteNode): Schema =
  ## Returns the output schema of a node.
  let handle = garrow_execute_node_get_output_schema(node.toPtr)
  result = newSchema(handle)

# ============================================================================
# Plan Lifecycle
# ============================================================================

proc validate*(plan: ExecutePlan) =
  ## Validates the plan. Raises OperationError if invalid.
  verify garrow_execute_plan_validate(plan.toPtr)

proc start*(plan: ExecutePlan) =
  ## Starts the plan execution.
  garrow_execute_plan_start(plan.toPtr)

proc wait*(plan: ExecutePlan) =
  ## Blocks until the plan finishes. Raises OperationError on failure.
  verify garrow_execute_plan_wait(plan.toPtr)

proc stop*(plan: ExecutePlan) =
  ## Stops the plan execution.
  garrow_execute_plan_stop(plan.toPtr)

# ============================================================================
# Sink Reader
# ============================================================================

proc getReader*(options: SinkNodeOptions, schema: Schema): RecordBatchReader =
  ## Gets a RecordBatchReader from the sink. Call AFTER plan.start().
  ## The reader streams results while the plan runs.
  let handle = garrow_sink_node_options_get_reader(options.toPtr, schema.toPtr)
  result = RecordBatchReader(handle: handle)

# ============================================================================
# Aggregation Types
# ============================================================================

arcGObject:
  type
    Aggregation* = object
      handle*: ptr GArrowAggregation

    AggregateNodeOptions* = object
      handle*: ptr GArrowAggregateNodeOptions

proc newAggregation*(function, input, output: string): Aggregation =
  ## Creates an aggregation descriptor for use with Acero aggregate nodes.
  ##
  ## Parameters:
  ##   function: Arrow compute function name (e.g., "sum", "count", "mean")
  ##   input:    Input field/column name
  ##   output:   Desired output field/column name
  result.handle = garrow_aggregation_new(
    function.cstring, nil, input.cstring, output.cstring
  )
  if result.handle.isNil:
    raise newException(OperationError, "Failed to create Aggregation")

proc newAggregateNodeOptions*(
    aggregations: openArray[Aggregation],
    keys: openArray[string] = [],
): AggregateNodeOptions =
  ## Creates options for an Acero aggregate node.
  var aggList = newGList[ptr GArrowAggregation]()
  for agg in aggregations:
    aggList.append(agg.handle)

  var keyPtrs: seq[cstring]
  for k in keys:
    keyPtrs.add(k.cstring)

  let keysPtr =
    if keyPtrs.len == 0: nil
    else: addr keyPtrs[0]

  result.handle = verify garrow_aggregate_node_options_new(
    aggList.toPtr, keysPtr, keyPtrs.len.gsize
  )

proc buildAggregateNode*(
    plan: ExecutePlan, input: ExecuteNode, options: AggregateNodeOptions
): ExecuteNode =
  ## Adds an aggregate node after `input`.
  let handle = verify garrow_execute_plan_build_aggregate_node(
    plan.toPtr, input.toPtr, options.toPtr
  )
  result = ExecuteNode(handle: handle)

# ============================================================================
# GroupBy / Aggregate Convenience
# ============================================================================

type GroupBy* = object
  ## Fluent group-by descriptor. Created via `table.groupBy(keys)`.
  table*: ArrowTable
  keys*: seq[string]

proc groupBy*(table: ArrowTable, keys: openArray[string]): GroupBy =
  ## Start a group-by operation on a table.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let result = table.groupBy("category").aggregate([
  ##       (field: "amount", fn: "sum", output: "total_amount")
  ##     ])
  result.table = table
  result.keys = @keys

proc groupBy*(table: ArrowTable, key: string): GroupBy =
  ## Start a group-by operation on a table with a single key.
  result.table = table
  result.keys = @[key]

proc aggregateTable*(
    table: ArrowTable,
    groupBy: openArray[string] = [],
    aggregations: openArray[tuple[field, fn, output: string]],
): ArrowTable =
  ## Run aggregations on a table, optionally grouping by one or more columns.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let result = aggregateTable(table, @["category"], @[
  ##       (field: "amount", fn: "sum", output: "total_amount"),
  ##       (field: "amount", fn: "count", output: "n_transactions"),
  ##     ])
  if aggregations.len == 0:
    raise newException(ValueError, "aggregateTable requires at least one aggregation")

  ensureComputeInitialized()

  let executor = newThreadPool().toExecutor
  let ctx = newExecuteContext(executor)
  let plan = newExecutePlan(ctx)

  let sourceOpts = newSourceNodeOptions(table)
  let sourceNode = plan.buildSourceNode(sourceOpts)

  var aggs = newSeq[Aggregation](aggregations.len)
  for i, spec in aggregations:
    let fnName =
      if groupBy.len > 0 and not spec.fn.startsWith("hash_"):
        "hash_" & spec.fn
      else:
        spec.fn
    aggs[i] = newAggregation(fnName, spec.field, spec.output)

  let aggOpts = newAggregateNodeOptions(aggs, groupBy)
  let aggNode = plan.buildAggregateNode(sourceNode, aggOpts)

  let sinkOpts = newSinkNodeOptions()
  discard plan.buildSinkNode(aggNode, sinkOpts)

  plan.validate()

  let outputSchema = aggNode.outputSchema
  let reader = sinkOpts.getReader(outputSchema)

  plan.start()
  result = reader.readAll()
  plan.wait()

proc aggregate*(
    gb: GroupBy,
    aggregations: openArray[tuple[field, fn, output: string]],
): ArrowTable =
  ## Execute aggregations on the group-by descriptor.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let result = table.groupBy("category").aggregate([
  ##       (field: "amount", fn: "sum", output: "total_amount")
  ##     ])
  result = aggregateTable(gb.table, gb.keys, aggregations)

# ============================================================================
# High-level Convenience
# ============================================================================

proc filterTable*(table: ArrowTable, filter: Expression): ArrowTable =
  ## Applies a filter expression to a table using the Acero engine.
  ## Returns a new table with only rows matching the filter.
  ##
  ## This delegates all filtering logic to Arrow C++ — no custom
  ## predicate evaluation in Nim.
  ensureComputeInitialized()

  let executor = newThreadPool().toExecutor

  let ctx = newExecuteContext(executor)
  let plan = newExecutePlan(ctx)

  let sourceOpts = newSourceNodeOptions(table)
  let sourceNode = plan.buildSourceNode(sourceOpts)

  let filterOpts = newFilterNodeOptions(filter)
  let filterNode = plan.buildFilterNode(sourceNode, filterOpts)

  let sinkOpts = newSinkNodeOptions()
  discard plan.buildSinkNode(filterNode, sinkOpts)

  plan.validate()

  let outputSchema = filterNode.outputSchema
  let reader = sinkOpts.getReader(outputSchema)

  plan.start()
  result = reader.readAll()
  plan.wait()
