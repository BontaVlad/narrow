import std/cpuinfo
import ../core/[ffi, error, utils]
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
  result.handle = check garrow_execute_plan_new(ctx.toPtr)

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
  result.handle = check garrow_thread_pool_new(n_threads = n_threads.guint)

# ============================================================================
# Plan Building
# ============================================================================

proc buildSourceNode*(plan: ExecutePlan, options: SourceNodeOptions): ExecuteNode =
  ## Adds a source node to the plan.
  let handle = check garrow_execute_plan_build_source_node(plan.toPtr, options.toPtr)
  result = ExecuteNode(handle: handle)

proc buildFilterNode*(
    plan: ExecutePlan, input: ExecuteNode, options: FilterNodeOptions
): ExecuteNode =
  ## Adds a filter node after `input`.
  let handle =
    check garrow_execute_plan_build_filter_node(plan.toPtr, input.toPtr, options.toPtr)
  result = ExecuteNode(handle: handle)

proc buildSinkNode*(
    plan: ExecutePlan, input: ExecuteNode, options: SinkNodeOptions
): ExecuteNode =
  ## Adds a sink node after `input`. Results are read from SinkNodeOptions.
  let handle =
    check garrow_execute_plan_build_sink_node(plan.toPtr, input.toPtr, options.toPtr)
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
  check garrow_execute_plan_validate(plan.toPtr)

proc start*(plan: ExecutePlan) =
  ## Starts the plan execution.
  garrow_execute_plan_start(plan.toPtr)

proc wait*(plan: ExecutePlan) =
  ## Blocks until the plan finishes. Raises OperationError on failure.
  check garrow_execute_plan_wait(plan.toPtr)

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
# High-level Convenience
# ============================================================================

var computeInitialized {.global.} = false

proc ensureComputeInitialized() =
  ## Ensures compute functions are registered. Thread-safe one-time initialization.
  once:
    var err = newError()
    if not garrow_compute_initialize(err.toPtr).bool or err:
      raise newException(OperationError, "Failed to initialize compute: " & $err)

    computeInitialized = true

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
