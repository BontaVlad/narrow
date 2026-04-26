import criterion
import ../src/narrow
import ./config

let cfg = narrowConfig()

# ============================================================================
# Reusable schemas and data
# ============================================================================

proc makeWideSchema(nCols: int): Schema =
  var fields = newSeq[Field](nCols)
  for i in 0 ..< nCols:
    fields[i] = newField[int32]("col_" & $i)
  newSchema(fields)

proc makeWideTable(nCols, nRows: int): ArrowTable =
  let schema = makeWideSchema(nCols)
  var builders = newSeq[ArrayBuilder[int32]](nCols)
  for i in 0 ..< nCols:
    builders[i] = newArrayBuilder[int32]()
  for row in 0 ..< nRows:
    for col in 0 ..< nCols:
      builders[col].append(row.int32 + col.int32)
  var arrays = newSeq[Array[int32]](nCols)
  for i in 0 ..< nCols:
    arrays[i] = builders[i].finish()
  newArrowTable(schema, arrays)

proc makeMultiChunkTable(nCols, nChunks, rowsPerChunk: int): ArrowTable =
  let schema = makeWideSchema(nCols)
  var chunkedArrays = newSeq[ChunkedArray[int32]](nCols)
  for col in 0 ..< nCols:
    var chunks = newSeq[Array[int32]](nChunks)
    for chunk in 0 ..< nChunks:
      var builder = newArrayBuilder[int32]()
      for row in 0 ..< rowsPerChunk:
        builder.append((chunk * rowsPerChunk + row).int32 + col.int32)
      chunks[chunk] = builder.finish()
    chunkedArrays[col] = newChunkedArray(chunks)
  newArrowTable(schema, chunkedArrays)

var arr1MValues = newSeq[int32](1_000_000)
for i in 0 ..< 1_000_000:
  arr1MValues[i] = i.int32
let arr1M = newArray(arr1MValues)
let wideTable20 = makeWideTable(20, 100_000)
let wideTable10 = makeWideTable(10, 100_000)
let multiChunkTable = makeMultiChunkTable(5, 10, 10_000)

# ============================================================================
# Benchmarks
# ============================================================================

benchmark cfg:

  let castMapFew = @[
    ("col_0", newGType(int64)),
    ("col_1", newGType(int64)),
  ]

  let castMapAll = @[
    ("col_0", newGType(int64)), ("col_1", newGType(int64)),
    ("col_2", newGType(int64)), ("col_3", newGType(int64)),
    ("col_4", newGType(int64)), ("col_5", newGType(int64)),
    ("col_6", newGType(int64)), ("col_7", newGType(int64)),
    ("col_8", newGType(int64)), ("col_9", newGType(int64)),
  ]

  let castMapIdentity = @[
    ("col_0", newGType(int32)), ("col_1", newGType(int32)),
    ("col_2", newGType(int32)), ("col_3", newGType(int32)),
    ("col_4", newGType(int32)), ("col_5", newGType(int32)),
    ("col_6", newGType(int32)), ("col_7", newGType(int32)),
    ("col_8", newGType(int32)), ("col_9", newGType(int32)),
  ]

  let castMapMulti = @[
    ("col_0", newGType(int64)), ("col_1", newGType(int64)),
    ("col_2", newGType(int64)), ("col_3", newGType(int64)),
    ("col_4", newGType(int64)),
  ]

  let schemaAll = newSchema(@[
    newField[int64]("col_0"), newField[int64]("col_1"), newField[int64]("col_2"),
    newField[int64]("col_3"), newField[int64]("col_4"), newField[int64]("col_5"),
    newField[int64]("col_6"), newField[int64]("col_7"), newField[int64]("col_8"),
    newField[int64]("col_9"),
  ])

  proc benchArrayCastInt32ToInt64 {.measure.} =
    discard castTo[int64](arr1M)

  proc benchTableCastHashmapFewColumns {.measure.} =
    # 20 columns, cast 2 → exercises pass-through wrapper churn
    discard castTable(wideTable20, castMapFew)

  proc benchTableCastHashmapAllColumns {.measure.} =
    # 10 columns, cast all → exercises full rebuild
    discard castTable(wideTable10, castMapAll)

  proc benchTableCastSchemaAllColumns {.measure.} =
    # Schema-driven cast of all columns
    discard castTable(wideTable10, schemaAll)

  proc benchTableCastIdentityPassThrough {.measure.} =
    # Cast map requests types that already match → should be near-instant with fast path
    discard castTable(wideTable10, castMapIdentity)

  proc benchTableCastMultiChunk {.measure.} =
    # 5 columns, 10 chunks each → exercises chunk iteration overhead
    discard castTable(multiChunkTable, castMapMulti)
