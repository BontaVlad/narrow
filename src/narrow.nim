# Narrow - Apache Arrow C API Wrapper for Nim
#
# This module provides a high-level, memory-safe wrapper around Apache Arrow's C API.

import narrow/core/[ffi, error]
import
  narrow/types/[
    gtypes, gtemporal, gdecimal, glisttype, glargelisttype, gfixedsizelisttype,
    gfixedshapetensortype, guuid, gmap, glist,
  ]
import narrow/column/[primitive, nested, metadata, buffer]
import narrow/tabular/[table, batch, dataset]
import
  narrow/compute/
    [filters, expressions, acero, functions, statistics, sorting, casting, aggregations]
import narrow/io/[parquet, csv, compressed, filesystem, json, ipc, feather]

export
  ffi, error, gtypes, gtemporal, gdecimal, glisttype, glargelisttype, gfixedsizelisttype,
  gfixedshapetensortype, guuid, gmap, glist, primitive, nested, metadata, buffer, table,
  batch, filters, expressions, acero, parquet, csv, compressed, filesystem, json, ipc, feather,
  dataset, functions, statistics, sorting, casting, aggregations
