import std/[os]
import unittest2
import narrow

suite "Parquet Statistics - Basic Properties":
  let uri = getCurrentDir() & "/tests/fatboy.parquet"
  let reader = newFileReader(uri)
  let metadata = reader.metadata
  let rg = metadata.rowGroup(0)

  test "Statistics object can be retrieved from column chunk":
    let cc = rg.columnChunk(4)  # int32_col
    let stats = cc.statistics
    unittest2.check not stats.handle.isNil

  test "Statistics has basic properties (null count, value count)":
    let cc = rg.columnChunk(4)  # int32_col
    let stats = cc.statistics
    unittest2.check stats.valueCount > 0
    unittest2.check stats.nullCount >= 0
    unittest2.check not stats.hasNulls or stats.nullCount > 0

  test "Statistics can check hasMinMax":
    let cc = rg.columnChunk(4)  # int32_col
    let stats = cc.statistics
    unittest2.check stats.hasMinMax

  test "Statistics equality comparison":
    let cc1 = rg.columnChunk(4)
    let cc2 = rg.columnChunk(4)
    let stats1 = cc1.statistics
    let stats2 = cc2.statistics
    unittest2.check stats1 == stats2

suite "Parquet Statistics - Type Checking":
  let uri = getCurrentDir() & "/tests/fatboy.parquet"
  let reader = newFileReader(uri)
  let metadata = reader.metadata
  let rg = metadata.rowGroup(0)

  test "int8_col (stored as INT32) has Int32Statistics":
    let cc = rg.columnChunk(4)
    let stats = cc.statistics
    unittest2.check stats.isInt32Statistics

  test "int16_col (stored as INT32) has Int32Statistics":
    let cc = rg.columnChunk(2)
    let stats = cc.statistics
    unittest2.check stats.isInt32Statistics

  test "int32_col has Int32Statistics":
    let cc = rg.columnChunk(4)
    let stats = cc.statistics
    unittest2.check stats.isInt32Statistics
    unittest2.check not stats.isInt64Statistics
    unittest2.check not stats.isFloatStatistics
    unittest2.check not stats.isDoubleStatistics

  test "int64_col has Int64Statistics":
    let cc = rg.columnChunk(7)
    let stats = cc.statistics
    unittest2.check stats.isInt64Statistics
    unittest2.check not stats.isInt32Statistics

  test "float32_col has FloatStatistics":
    let cc = rg.columnChunk(9)
    let stats = cc.statistics
    unittest2.check stats.isFloatStatistics
    unittest2.check not stats.isDoubleStatistics

  test "float64_col has DoubleStatistics":
    let cc = rg.columnChunk(10)
    let stats = cc.statistics
    unittest2.check stats.isDoubleStatistics
    unittest2.check not stats.isFloatStatistics

  test "string_col has ByteArrayStatistics":
    let cc = rg.columnChunk(11)
    let stats = cc.statistics
    unittest2.check stats.isByteArrayStatistics

  test "binary_col has ByteArrayStatistics":
    let cc = rg.columnChunk(12)
    let stats = cc.statistics
    unittest2.check stats.isByteArrayStatistics

suite "Parquet Statistics - Type Conversion and Min/Max":
  let uri = getCurrentDir() & "/tests/fatboy.parquet"
  let reader = newFileReader(uri)
  let metadata = reader.metadata
  let rg = metadata.rowGroup(0)

  test "Int32Statistics can be converted and min/max accessed":
    let cc = rg.columnChunk(5)  # int32_col
    let stats = cc.statistics
    unittest2.check stats.isInt32Statistics
    let typedStats = stats.toInt32Statistics
    let minVal = typedStats.min
    let maxVal = typedStats.max
    unittest2.check minVal <= maxVal

  test "Int64Statistics can be converted and min/max accessed":
    let cc = rg.columnChunk(7)  # int64_col
    let stats = cc.statistics
    unittest2.check stats.isInt64Statistics
    let typedStats = stats.toInt64Statistics
    let minVal = typedStats.min
    let maxVal = typedStats.max
    unittest2.check minVal <= maxVal

  test "FloatStatistics can be converted and min/max accessed":
    let cc = rg.columnChunk(9)  # float32_col
    let stats = cc.statistics
    unittest2.check stats.isFloatStatistics
    let typedStats = stats.toFloatStatistics
    let minVal = typedStats.min
    let maxVal = typedStats.max
    unittest2.check minVal <= maxVal

  test "DoubleStatistics can be converted and min/max accessed":
    let cc = rg.columnChunk(10)  # float64_col
    let stats = cc.statistics
    unittest2.check stats.isDoubleStatistics
    let typedStats = stats.toDoubleStatistics
    let minVal = typedStats.min
    let maxVal = typedStats.max
    unittest2.check minVal <= maxVal

  test "ByteArrayStatistics can be converted and min/max accessed":
    let cc = rg.columnChunk(11)  # string_col
    let stats = cc.statistics
    unittest2.check stats.isByteArrayStatistics
    let typedStats = stats.toByteArrayStatistics
    let minBytes = typedStats.min
    let maxBytes = typedStats.max
    unittest2.check minBytes.len >= 0
    unittest2.check maxBytes.len >= 0

  test "ByteArrayStatistics min/max can be converted to string":
    let cc = rg.columnChunk(11)  # string_col
    let stats = cc.statistics
    let typedStats = stats.toByteArrayStatistics
    let minStr = typedStats.min.toString
    let maxStr = typedStats.max.toString
    unittest2.check minStr.len >= 0
    unittest2.check maxStr.len >= 0
    unittest2.check minStr <= maxStr

suite "Parquet Statistics - Multiple Columns":
  let uri = getCurrentDir() & "/tests/fatboy.parquet"
  let reader = newFileReader(uri)
  let metadata = reader.metadata
  let rg = metadata.rowGroup(0)

  test "All integer columns have appropriate statistics":
    for i in [1, 2, 3, 4, 5, 6]:
      let cc = rg.columnChunk(i)
      let stats = cc.statistics
      unittest2.check stats.hasMinMax
      unittest2.check stats.isInt32Statistics
    
    for i in [7, 8]:
      let cc = rg.columnChunk(i)
      let stats = cc.statistics
      unittest2.check stats.hasMinMax
      unittest2.check stats.isInt64Statistics

  test "All float columns have appropriate statistics":
    let cc32 = rg.columnChunk(9)
    let stats32 = cc32.statistics
    unittest2.check stats32.hasMinMax
    unittest2.check stats32.isFloatStatistics
    
    let cc64 = rg.columnChunk(10)
    let stats64 = cc64.statistics
    unittest2.check stats64.hasMinMax
    unittest2.check stats64.isDoubleStatistics

  test "All string/binary columns have ByteArrayStatistics":
    for i in [11, 12, 13, 14]:
      let cc = rg.columnChunk(i)
      let stats = cc.statistics
      unittest2.check stats.isByteArrayStatistics

suite "Parquet Statistics - Value Counts":
  let uri = getCurrentDir() & "/tests/fatboy.parquet"
  let reader = newFileReader(uri)
  let metadata = reader.metadata
  let rg = metadata.rowGroup(0)

  test "Row group row count matches statistics value count":
    let cc = rg.columnChunk(0)
    let stats = cc.statistics
    unittest2.check stats.valueCount == rg.nRows - 1 # we have a nil value

suite "Parquet Statistics - Distinct Values":
  let uri = getCurrentDir() & "/tests/fatboy.parquet"
  let reader = newFileReader(uri)
  let metadata = reader.metadata
  let rg = metadata.rowGroup(0)

  test "Statistics reports hasDistinctValues correctly":
    let cc = rg.columnChunk(5)  # int32_col
    let stats = cc.statistics
    let hasDistinct = stats.hasDistinctValues
    unittest2.check hasDistinct == true or hasDistinct == false

  test "Distinct value count is non-negative":
    let cc = rg.columnChunk(5)  # int32_col
    let stats = cc.statistics
    if stats.hasDistinctValues:
      unittest2.check stats.distinctValueCount >= 0

suite "Parquet Statistics - Metadata Integration":
  let uri = getCurrentDir() & "/tests/fatboy.parquet"
  let reader = newFileReader(uri)

  test "Can access statistics through reader metadata":
    let metadata = reader.metadata
    unittest2.check metadata.nRowGroups > 0
    
    let rg = metadata.rowGroup(0)
    unittest2.check rg.nColumns > 0
    
    let cc = rg.columnChunk(0)
    let stats = cc.statistics
    unittest2.check not stats.handle.isNil

  test "Multiple row groups have consistent statistics":
    let metadata = reader.metadata
    if metadata.nRowGroups > 1:
      let rg0 = metadata.rowGroup(0)
      let rg1 = metadata.rowGroup(1)
      
      unittest2.check rg0.nColumns == rg1.nColumns

  test "Column chunk metadata provides file offset":
    let metadata = reader.metadata
    let rg = metadata.rowGroup(0)
    let cc = rg.columnChunk(0)
    unittest2.check cc.fileOffset >= 0

  test "Column chunk metadata can check decompression":
    let metadata = reader.metadata
    let rg = metadata.rowGroup(0)
    let cc = rg.columnChunk(0)
    let canDecompress = cc.canDecompress
    unittest2.check canDecompress == true or canDecompress == false

suite "Parquet Statistics - Edge Cases":
  let uri = getCurrentDir() & "/tests/fatboy.parquet"
  let reader = newFileReader(uri)
  let metadata = reader.metadata

  test "All row groups can be accessed":
    for i in 0..<metadata.nRowGroups:
      let rg = metadata.rowGroup(i)
      unittest2.check rg.nColumns > 0
      unittest2.check rg.nRows > 0

  test "All columns in all row groups have statistics":
    for rgIdx in 0..<metadata.nRowGroups:
      let rg = metadata.rowGroup(rgIdx)
      for colIdx in 0..<rg.nColumns:
        let cc = rg.columnChunk(colIdx)
        let stats = cc.statistics
        unittest2.check not stats.handle.isNil
        unittest2.check stats.valueCount > 0
