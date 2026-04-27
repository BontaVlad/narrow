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

proc makeChunkedInt32(totalRows, nChunks: int): ChunkedArray[int32] =
  let rowsPerChunk = totalRows div nChunks
  var chunks = newSeq[Array[int32]](nChunks)
  for i in 0 ..< nChunks:
    var values = newSeq[int32](rowsPerChunk)
    for j in 0 ..< rowsPerChunk:
      values[j] = (i * rowsPerChunk + j).int32
    chunks[i] = newArray(values)
  newChunkedArray(chunks)

var arr1MValues = newSeq[int32](1_000_000)
for i in 0 ..< 1_000_000:
  arr1MValues[i] = i.int32
let arr1M = newArray(arr1MValues)

var arr10MValues = newSeq[int32](10_000_000)
for i in 0 ..< 10_000_000:
  arr10MValues[i] = i.int32
let arr10M = newArray(arr10MValues)

let wideTable20 = makeWideTable(20, 100_000)
let wideTable10 = makeWideTable(10, 100_000)
let multiChunkTable = makeMultiChunkTable(5, 10, 10_000)

# Scaling chunked arrays
let chunked1M_1 = newChunkedArray(@[arr1M])
let chunked1M_10 = makeChunkedInt32(1_000_000, 10)
let chunked1M_100 = makeChunkedInt32(1_000_000, 100)
let chunked10M_1 = newChunkedArray(@[arr10M])
let chunked10M_10 = makeChunkedInt32(10_000_000, 10)
let chunked10M_100 = makeChunkedInt32(10_000_000, 100)

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

  proc benchArrayCastInt32ToInt64_1M {.measure.} =
    var result = castTo[int64](arr1M)
    blackBox(result)

  proc benchArrayCastInt32ToInt64_10M {.measure.} =
    var result = castTo[int64](arr10M)
    blackBox(result)

  proc benchChunkedArrayCastInt32ToInt64_1M_1chunk {.measure.} =
    var result = castChunks(chunked1M_1, newGType(int64))
    blackBox(result)

  proc benchChunkedArrayCastInt32ToInt64_1M_10chunks {.measure.} =
    var result = castChunks(chunked1M_10, newGType(int64))
    blackBox(result)

  proc benchChunkedArrayCastInt32ToInt64_1M_100chunks {.measure.} =
    var result = castChunks(chunked1M_100, newGType(int64))
    blackBox(result)

  proc benchChunkedArrayCastInt32ToInt64_10M_1chunk {.measure.} =
    var result = castChunks(chunked10M_1, newGType(int64))
    blackBox(result)

  proc benchChunkedArrayCastInt32ToInt64_10M_10chunks {.measure.} =
    var result = castChunks(chunked10M_10, newGType(int64))
    blackBox(result)

  proc benchChunkedArrayCastInt32ToInt64_10M_100chunks {.measure.} =
    var result = castChunks(chunked10M_100, newGType(int64))
    blackBox(result)

  proc benchTableCastHashmapFewColumns {.measure.} =
    # 20 columns, cast 2 → exercises pass-through wrapper churn
    var result = castTable(wideTable20, castMapFew)
    blackBox(result)

  proc benchTableCastHashmapAllColumns {.measure.} =
    # 10 columns, cast all → exercises full rebuild
    var result = castTable(wideTable10, castMapAll)
    blackBox(result)

  proc benchTableCastIdentityPassThrough {.measure.} =
    # Cast map requests types that already match → should be near-instant with fast path
    var result = castTable(wideTable10, castMapIdentity)
    blackBox(result)

  proc benchTableCastMultiChunk {.measure.} =
    # 5 columns, 10 chunks each → exercises chunk iteration overhead
    var result = castTable(multiChunkTable, castMapMulti)
    blackBox(result)
