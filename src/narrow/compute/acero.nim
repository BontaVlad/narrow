import std/cpuinfo
import ../core/[ffi, error]
import ../column/metadata
import ../tabular/[table, batch]
import ../compute/expressions

# ============================================================================
# Type Definitions
# ============================================================================

type
  ExecuteContext* = object
    handle: ptr GArrowExecuteContext

  ExecutePlan* = object
    handle: ptr GArrowExecutePlan

  ExecuteNode* = object
    handle: ptr GArrowExecuteNode

  SourceNodeOptions* = object
    handle: ptr GArrowSourceNodeOptions

  FilterNodeOptions* = object
    handle: ptr GArrowFilterNodeOptions

  SinkNodeOptions* = object
    handle: ptr GArrowSinkNodeOptions

  ThreadPool* = object
    handle: ptr GArrowThreadPool

# ============================================================================
# ARC Hooks — ExecuteContext
# ============================================================================

proc `=destroy`*(ctx: ExecuteContext) =
  if ctx.handle != nil:
    g_object_unref(ctx.handle)

proc `=sink`*(dest: var ExecuteContext, src: ExecuteContext) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var ExecuteContext, src: ExecuteContext) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

# ============================================================================
# ARC Hooks — ExecutePlan
# ============================================================================

proc `=destroy`*(plan: ExecutePlan) =
  if plan.handle != nil:
    g_object_unref(plan.handle)

proc `=sink`*(dest: var ExecutePlan, src: ExecutePlan) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var ExecutePlan, src: ExecutePlan) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

# ============================================================================
# ARC Hooks — ExecuteNode
# NOTE: ExecuteNodes are owned by the ExecutePlan. We do NOT call g_object_unref
# in destroy because the plan manages their lifetime. We still need sink/copy
# for when ExecuteNode values are moved/copied around.
# ============================================================================

proc `=destroy`*(node: ExecuteNode) =
  # No-op: nodes are owned by the plan
  discard

proc `=sink`*(dest: var ExecuteNode, src: ExecuteNode) =
  dest.handle = src.handle

proc `=copy`*(dest: var ExecuteNode, src: ExecuteNode) =
  dest.handle = src.handle

# ============================================================================
# ARC Hooks — ThreadPool
# ============================================================================

proc `=destroy`*(pool: ThreadPool) =
  if pool.handle != nil:
    g_object_unref(pool.handle)

proc `=sink`*(dest: var ThreadPool, src: ThreadPool) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var ThreadPool, src: ThreadPool) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

# ============================================================================
# ARC Hooks — Node Options
# ============================================================================

proc `=destroy`*(opts: SourceNodeOptions) =
  if opts.handle != nil:
    g_object_unref(opts.handle)

proc `=sink`*(dest: var SourceNodeOptions, src: SourceNodeOptions) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var SourceNodeOptions, src: SourceNodeOptions) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(opts: FilterNodeOptions) =
  if opts.handle != nil:
    g_object_unref(opts.handle)

proc `=sink`*(dest: var FilterNodeOptions, src: FilterNodeOptions) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var FilterNodeOptions, src: FilterNodeOptions) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(opts: SinkNodeOptions) =
  if opts.handle != nil:
    g_object_unref(opts.handle)

proc `=sink`*(dest: var SinkNodeOptions, src: SinkNodeOptions) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var SinkNodeOptions, src: SinkNodeOptions) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

# ============================================================================
# Pointer Converters
# ============================================================================

proc toPtr*(ctx: ExecuteContext): ptr GArrowExecuteContext {.inline.} =
  ctx.handle

proc toPtr*(plan: ExecutePlan): ptr GArrowExecutePlan {.inline.} =
  plan.handle

proc toPtr*(node: ExecuteNode): ptr GArrowExecuteNode {.inline.} =
  node.handle

proc toPtr*(opts: SourceNodeOptions): ptr GArrowSourceNodeOptions {.inline.} =
  opts.handle

proc toPtr*(opts: FilterNodeOptions): ptr GArrowFilterNodeOptions {.inline.} =
  opts.handle

proc toPtr*(opts: SinkNodeOptions): ptr GArrowSinkNodeOptions {.inline.} =
  opts.handle

proc toPtr*(pool: ThreadPool): ptr GArrowThreadPool {.inline.} =
  pool.handle

proc toExecutor*(pool: ThreadPool): ptr GArrowExecutor {.inline.} =
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

proc newFilterNodeOptions*(expr: ExpressionObj): FilterNodeOptions =
  ## Creates filter node options from an expression.
  result.handle = garrow_filter_node_options_new(expr.toPtr)

proc newSinkNodeOptions*(): SinkNodeOptions =
  ## Creates new sink node options for capturing output.
  result.handle = garrow_sink_node_options_new()

proc newThreadPool*(n_threads: int = countProcessors()): ThreadPool =
  var err = newError()
  let handle = garrow_thread_pool_new(n_threads = n_threads.guint, err.toPtr)
  if err:
    raise newException(OperationError, "Failed to create a threadPool: " & $err)
  result.handle = handle

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

proc filterTable*(table: ArrowTable, filter: ExpressionObj): ArrowTable =
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
