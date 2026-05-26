import ../core/[ffi, error, utils]
import ../types/[gtypes, glist]
import ../column/primitive
import ../tabular/table

# ============================================================================
# Type Definitions
# ============================================================================

type SortOrder* = enum
  Ascending = GARROW_SORT_ORDER_ASCENDING
  Descending = GARROW_SORT_ORDER_DESCENDING

arcGObject:
  type
    SortKey* = object
      handle*: ptr GArrowSortKey

    SortOptions* = object
      handle*: ptr GArrowSortOptions

    TakeOptions* = object
      handle*: ptr GArrowTakeOptions

# ============================================================================
# Constructors
# ============================================================================

proc newSortKey*(name: string, order: SortOrder = Ascending): SortKey =
  let handle = verify garrow_sort_key_new(name.cstring, order.GArrowSortOrder)
  result.handle = handle

proc newSortOptions*(keys: openArray[SortKey]): SortOptions =
  var keyList = newGList[ptr GArrowSortKey]()
  for key in keys:
    keyList.append(key.handle)
  let handle = garrow_sort_options_new(keyList.toPtr)
  if isNil(handle):
    raise newException(IOError, "Failed to create SortOptions")
  result.handle = handle

proc newTakeOptions*(): TakeOptions =
  let handle = garrow_take_options_new()
  if isNil(handle):
    raise newException(IOError, "Failed to create TakeOptions")
  result.handle = handle

# ============================================================================
# sortIndices
# ============================================================================

proc sortIndices*[T](arr: Array[T], order: SortOrder = Ascending): Array[uint64] =
  ## Return indices that would sort the array in the given order.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let arr = newArray(@[3'i32, 1, 2])
  ##     let idx = sortIndices(arr)        # [1, 2, 0]
  ##     let sorted = take(arr, idx)       # [1, 2, 3]
  let handle = verify garrow_array_sort_indices(arr.toPtr, order.GArrowSortOrder)
  result = newArray[uint64](cast[ptr GArrowArray](handle))

proc sortIndices*(table: ArrowTable, keys: openArray[SortKey]): Array[uint64] =
  ## Return indices that would sort the table by the given sort keys.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let idx = sortIndices(table, @[newSortKey("age", Ascending)])
  ##     let sorted = take(table, idx)
  let options = newSortOptions(keys)
  let handle = verify garrow_table_sort_indices(table.toPtr, options.toPtr)
  result = newArray[uint64](cast[ptr GArrowArray](handle))

# ============================================================================
# take
# ============================================================================

proc take*[T](arr: Array[T], indices: Array[uint64]): Array[T] =
  ## Return a new array with elements selected by the given indices.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let arr = newArray(@[10'i32, 20, 30])
  ##     let result = take(arr, newArray(@[2'u64, 0, 1]))
  ##     # result == [30, 10, 20]
  let options = newTakeOptions()
  let handle = verify garrow_array_take(arr.toPtr, indices.toPtr, options.toPtr)
  result = newArray[T](handle)

proc take*(table: ArrowTable, indices: Array[uint64]): ArrowTable =
  ## Return a new table with rows selected by the given indices.
  let options = newTakeOptions()
  let handle = verify garrow_table_take(table.toPtr, indices.toPtr, options.toPtr)
  result = newArrowTable(handle)

# ============================================================================
# Convenience: sortBy
# ============================================================================

proc sortBy*(table: ArrowTable, keys: openArray[(string, SortOrder)]): ArrowTable =
  ## Sort a table by the given column names and orders.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let sorted = sortBy(table, @[("age", Ascending)])
  var sortKeys = newSeq[SortKey](keys.len)
  for i, (name, order) in keys:
    sortKeys[i] = newSortKey(name, order)
  let indices = sortIndices(table, sortKeys)
  result = take(table, indices)
