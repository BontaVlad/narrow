# Narrow - Apache Arrow C API Wrapper for Nim
#
# This module provides a high-level, memory-safe wrapper around Apache Arrow's C API.

import narrow/core/[ffi, error, concepts, generated]
import
  narrow/types/[
    gtypes, gtemporal, glisttype, glargelisttype, gfixedsizelisttype,
    gfixedshapetensortype, guuid, gmap, glist,
  ]
import narrow/column/[primitive, nested, metadata]
import narrow/tabular/[table, batch]
import narrow/compute/[filters, expressions]
import narrow/io/[parquet, parquet_statistics, csv, filesystem]

export
  ffi, error, concepts, generated, gtypes, gtemporal, glisttype, glargelisttype,
  gfixedsizelisttype, gfixedshapetensortype, guuid, gmap, glist, primitive, nested,
  metadata, table, batch, filters, expressions, parquet, parquet_statistics, csv,
  filesystem
