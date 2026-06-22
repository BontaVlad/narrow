## Acero execution engine — in-memory data processing pipelines.
##
## Build a plan with Source → Filter → Project → Aggregate → Sink nodes, then
## execute it. `aggregateTable` provides a convenient group-by + aggregate API
## on top of the engine.
import std/[cpuinfo, strutils]
import ../core/[ffi, error, utils]
import ../types/glist
import ../column/metadata
import ../tabular/[table, batch]
import ../compute/expressions

# ============================================================================
# Join Type Enum
# ============================================================================

type JoinType* = enum ## Hash join type. Maps to `GArrowJoinType`.
  jtLeftSemi = 0
  jtRightSemi = 1
  jtLeftAnti = 2
  jtRightAnti = 3
  jtInner = 4
  jtLeftOuter = 5
  jtRightOuter = 6
  jtFullOuter = 7

# ============================================================================
# Type Definitions
# ============================================================================

arcGObject:
  type
    ExecuteContext* = object
      ## Execution context for an Acero plan (holds executor and options).
      handle*: ptr GArrowExecuteContext

    ExecutePlan* = object ## A complete Acero execution plan (DAG of nodes).
      handle*: ptr GArrowExecutePlan

    ExecuteNode* = object
      ## A single node in an execution plan (source, filter, sink, ...).
      handle*: ptr GArrowExecuteNode

    SourceNodeOptions* = object
      ## Options for constructing a source node from a table or record batch.
      handle*: ptr GArrowSourceNodeOptions

    FilterNodeOptions* = object
      ## Options for constructing a filter node from a boolean expression.
      handle*: ptr GArrowFilterNodeOptions

    SinkNodeOptions* = object
      ## Options for constructing a sink node that captures plan output.
      handle*: ptr GArrowSinkNodeOptions

    ThreadPool* = object ## A thread pool used as the executor for an execution context.
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

proc getKindName*(node: ExecuteNode): string =
  ## Returns the kind name of an execution node (e.g. "source", "filter").
  result = $garrow_execute_node_get_kind_name(node.toPtr)

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

proc getNodes*(plan: ExecutePlan): ptr GList =
  ## Returns the list of nodes in the execution plan.
  ## Useful for introspection and debugging.
  result = garrow_execute_plan_get_nodes(plan.toPtr)

# ============================================================================
# Sink Reader
# ============================================================================

proc getReader*(options: SinkNodeOptions, schema: Schema): RecordBatchReader =
  ## Gets a RecordBatchReader from the sink. Call AFTER plan.start().
  ## The reader streams results while the plan runs.
  let handle = garrow_sink_node_options_get_reader(options.toPtr, schema.toPtr)
  result.handle = handle

# ============================================================================
# Aggregation Types
# ============================================================================

arcGObject:
  type
    Aggregation* = object
      ## Descriptor of one aggregation (function, input, output) for an aggregate node.
      handle*: ptr GArrowAggregation

    AggregateNodeOptions* = object
      ## Options for constructing an aggregate node (aggregations + group keys).
      handle*: ptr GArrowAggregateNodeOptions

arcGObject:
  type HashJoinNodeOptions* = object
    ## Options for constructing a hash join node (join type + key columns).
    handle*: ptr GArrowHashJoinNodeOptions

arcGObject:
  type ProjectNodeOptions* = object
    ## Options for constructing a project node (output expressions + names).
    handle*: ptr GArrowProjectNodeOptions

proc newAggregation*(function, input, output: string): Aggregation =
  ## Creates an aggregation descriptor for use with Acero aggregate nodes.
  ##
  ## Parameters:
  ##   function: Arrow compute function name (e.g., "sum", "count", "mean")
  ##   input:    Input field/column name
  ##   output:   Desired output field/column name
  result.handle =
    garrow_aggregation_new(function.cstring, nil, input.cstring, output.cstring)
  if result.handle.isNil:
    raise newException(OperationError, "Failed to create Aggregation")

proc newAggregateNodeOptions*(
    aggregations: openArray[Aggregation], keys: openArray[string] = []
): AggregateNodeOptions =
  ## Creates options for an Acero aggregate node.
  var aggList = newGList[ptr GArrowAggregation]()
  for agg in aggregations:
    aggList.append(agg.handle)

  var keyPtrs: seq[cstring]
  for k in keys:
    keyPtrs.add(k.cstring)

  let keysPtr =
    if keyPtrs.len == 0:
      nil
    else:
      addr keyPtrs[0]

  result.handle =
    verify garrow_aggregate_node_options_new(aggList.toPtr, keysPtr, keyPtrs.len.gsize)

proc buildAggregateNode*(
    plan: ExecutePlan, input: ExecuteNode, options: AggregateNodeOptions
): ExecuteNode =
  ## Adds an aggregate node after `input`.
  let handle = verify garrow_execute_plan_build_aggregate_node(
    plan.toPtr, input.toPtr, options.toPtr
  )
  result = ExecuteNode(handle: handle)

proc newHashJoinNodeOptions*(
    joinType: JoinType, leftKeys, rightKeys: openArray[string]
): HashJoinNodeOptions =
  ## Creates options for an Acero hash join node.
  ##
  ## Parameters:
  ##   joinType:  Type of join (jtInner, jtLeftOuter, jtRightOuter, etc.)
  ##   leftKeys:  Column names from the left table to join on
  ##   rightKeys: Column names from the right table to join on
  var lp: seq[cstring]
  for k in leftKeys:
    lp.add(k.cstring)
  var rp: seq[cstring]
  for k in rightKeys:
    rp.add(k.cstring)

  let lptr =
    if lp.len == 0:
      nil
    else:
      addr lp[0]
  let rptr =
    if rp.len == 0:
      nil
    else:
      addr rp[0]

  result.handle = verify garrow_hash_join_node_options_new(
    joinType.GArrowJoinType, lptr, lp.len.gsize, rptr, rp.len.gsize
  )

proc setLeftOutputs*(opts: HashJoinNodeOptions, outputs: openArray[string]) =
  ## Restrict which columns from the left table appear in the join result.
  ## If not called, all left columns are included.
  var ptrs: seq[cstring]
  for o in outputs:
    ptrs.add(o.cstring)
  let p =
    if ptrs.len == 0:
      nil
    else:
      addr ptrs[0]
  verify garrow_hash_join_node_options_set_left_outputs(opts.toPtr, p, ptrs.len.gsize)

proc setRightOutputs*(opts: HashJoinNodeOptions, outputs: openArray[string]) =
  ## Restrict which columns from the right table appear in the join result.
  ## If not called, all right columns are included (key columns deduplicated).
  var ptrs: seq[cstring]
  for o in outputs:
    ptrs.add(o.cstring)
  let p =
    if ptrs.len == 0:
      nil
    else:
      addr ptrs[0]
  verify garrow_hash_join_node_options_set_right_outputs(opts.toPtr, p, ptrs.len.gsize)

proc buildHashJoinNode*(
    plan: ExecutePlan, left, right: ExecuteNode, options: HashJoinNodeOptions
): ExecuteNode =
  ## Adds a hash join node with `left` and `right` as inputs.
  let handle = verify garrow_execute_plan_build_hash_join_node(
    plan.toPtr, left.toPtr, right.toPtr, options.toPtr
  )
  result = ExecuteNode(handle: handle)

# ============================================================================
# GroupBy / Aggregate Convenience
# ============================================================================

type GroupBy* = object ## Fluent group-by descriptor. Created via `table.groupBy(keys)`.
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

  let pool = newThreadPool()
  let executor = pool.toExecutor
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
    gb: GroupBy, aggregations: openArray[tuple[field, fn, output: string]]
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

  let pool = newThreadPool()
  let executor = pool.toExecutor

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

proc joinTables*(
    left, right: ArrowTable, joinType: JoinType, leftKeys, rightKeys: openArray[string]
): ArrowTable =
  ## Join two tables on common key columns using Acero's hash join engine.
  ##
  ## By default, all columns from both tables appear in the result (right-side
  ## key columns are deduplicated). Use `setLeftOutputs` / `setRightOutputs`
  ## on a HashJoinNodeOptions to restrict output columns.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let result = joinTables(
  ##       leftTable, rightTable,
  ##       jtInner, ["id"], ["id"]
  ##     )
  ensureComputeInitialized()

  let pool = newThreadPool()
  let executor = pool.toExecutor
  let ctx = newExecuteContext(executor)
  let plan = newExecutePlan(ctx)

  let leftSourceOpts = newSourceNodeOptions(left)
  let leftNode = plan.buildSourceNode(leftSourceOpts)

  let rightSourceOpts = newSourceNodeOptions(right)
  let rightNode = plan.buildSourceNode(rightSourceOpts)

  let joinOpts = newHashJoinNodeOptions(joinType, leftKeys, rightKeys)
  let joinNode = plan.buildHashJoinNode(leftNode, rightNode, joinOpts)

  let sinkOpts = newSinkNodeOptions()
  discard plan.buildSinkNode(joinNode, sinkOpts)

  plan.validate()

  let outputSchema = joinNode.outputSchema
  let reader = sinkOpts.getReader(outputSchema)

  plan.start()
  result = reader.readAll()
  plan.wait()

# ============================================================================
# Project Node
# ============================================================================

proc newProjectNodeOptions*(
    expressions: openArray[Expression], names: openArray[string] = []
): ProjectNodeOptions =
  var exprList = newGList[ptr GArrowExpression]()
  for e in expressions:
    exprList.append(e.toPtr)

  var namePtrs: seq[cstring]
  for n in names:
    namePtrs.add(n.cstring)

  let namesPtr =
    if namePtrs.len == 0:
      nil
    else:
      addr namePtrs[0]

  result.handle =
    garrow_project_node_options_new(exprList.toPtr, namesPtr, namePtrs.len.gsize)
  if result.handle.isNil:
    raise newException(OperationError, "Failed to create project node options")

proc buildProjectNode*(
    plan: ExecutePlan, input: ExecuteNode, options: ProjectNodeOptions
): ExecuteNode =
  let handle = verify garrow_execute_plan_build_project_node(
    plan.toPtr, input.toPtr, options.toPtr
  )
  result = ExecuteNode(handle: handle)

proc projectTable*(
    table: ArrowTable, expressions: openArray[Expression], names: openArray[string] = []
): ArrowTable =
  ensureComputeInitialized()

  let pool = newThreadPool()
  let executor = pool.toExecutor
  let ctx = newExecuteContext(executor)
  let plan = newExecutePlan(ctx)

  let sourceOpts = newSourceNodeOptions(table)
  let sourceNode = plan.buildSourceNode(sourceOpts)

  let projOpts = newProjectNodeOptions(expressions, names)
  let projNode = plan.buildProjectNode(sourceNode, projOpts)

  let sinkOpts = newSinkNodeOptions()
  discard plan.buildSinkNode(projNode, sinkOpts)

  plan.validate()

  let outputSchema = projNode.outputSchema
  let reader = sinkOpts.getReader(outputSchema)

  plan.start()
  result = reader.readAll()
  plan.wait()
