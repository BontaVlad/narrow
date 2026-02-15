## Statistics-Based Row Group Pruning
##
## This module provides a thin, conservative layer for pruning row groups
## based on Parquet statistics. When in doubt, it returns sMaybe (read the row group).
## All correctness is guaranteed by the post-read Acero filter.

import ../core/[error, generated]
import ../column/metadata
import ../compute/expressions
import ../io/parquet

type Satisfiability* = enum
  ## Whether a row group can possibly satisfy a predicate.
  ## Conservative: when in doubt, return sMaybe.
  sNever ## Provably cannot satisfy -> skip row group
  sMaybe ## Cannot determine -> must read

proc canRowGroupSatisfy*(
    filter: ExpressionObj, rgMeta: RowGroupMetadata, schema: Schema
): Satisfiability =
  ## Conservative row group pruning. Only handles simple patterns:
  ##   col <cmp> literal
  ## combined with AND/OR. Everything else returns sMaybe.
  ##
  ## IMPORTANT: This is a thin optimization layer. When in doubt, return sMaybe.
  ## All correctness is guaranteed by the post-read Acero filter in readTable.

  result = sMaybe # default: read the row group

  # Only handle CallExpressions (comparisons and logical ops)
  when filter is CallExpression:
    let callExpr = CallExpression(filter)
    let fn = callExpr.functionName

    # Handle logical operators
    if fn == "and":
      # AND: if either child is sNever, result is sNever
      # For simplicity with multiple args, we need to handle them all
      # Since we track fields, we can't easily access children
      # Return sMaybe for now - conservative
      return sMaybe
    elif fn == "or":
      # OR: only sNever if both children are sNever
      return sMaybe

    # Handle comparison operators
    elif fn in ["equal", "not_equal", "less", "less_equal", "greater", "greater_equal"]:
      # Need one field and one literal
      if filter.referencedFields.len != 1:
        return sMaybe

      let fieldName = filter.referencedFields[0]
      let fieldIdx = schema.getFieldIndex(fieldName)
      if fieldIdx < 0:
        return sMaybe

      # Get column chunk metadata for this field in this row group
      if fieldIdx >= rgMeta.nColumns:
        return sMaybe

      let colChunk = rgMeta.columnChunk(fieldIdx)
      let stats = colChunk.statistics

      if not stats.hasMinMax:
        return sMaybe

      # Get field type to cast statistics correctly
      let field = schema.getField(fieldIdx)
      let dataType = field.dataType

      # Try to evaluate based on type
      # For now, handle Int32 and Int64 (most common)
      case dataType.kind
      of Int32:
        let int32Stats =
          Int32Statistics(handle: cast[ptr GParquetInt32Statistics](stats.handle))
        let minVal = int32Stats.min
        let maxVal = int32Stats.max

        # For comparisons, we need the literal value
        # Since we don't have easy access to the literal from the expression,
        # we'll parse it from the string representation
        let exprStr = $filter
        # This is fragile - better approach would be to store literal value
        # For now, return sMaybe to be safe
        return sMaybe
      of Int64:
        let int64Stats =
          Int64Statistics(handle: cast[ptr GParquetInt64Statistics](stats.handle))
        let minVal = int64Stats.min
        let maxVal = int64Stats.max
        return sMaybe
      else:
        # Other types not yet supported for statistics evaluation
        return sMaybe
    else:
      # Unknown function
      return sMaybe
  else:
    # Not a call expression (field or literal)
    return sMaybe
