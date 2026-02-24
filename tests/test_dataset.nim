import std/[os]
import unittest2
import ../src/narrow/[tabular/dataset, tabular/batch, column/primitive, column/metadata, io/filesystem]

# ============================================================================
# Phase 2 Tests (End-to-End with Factories)
# ============================================================================

suite "Dataset - Phase 2 (End-to-End)":
  
  test "Create FileSystemDatasetFactory":
    let format = newCSVFileFormat()
    let factory = newFileSystemDatasetFactory(format)
    check not factory.isNil
  
  test "Factory method chaining works":
    let format = newCSVFileFormat()
    let factory = newFileSystemDatasetFactory(format)
    # Set filesystem first, then add path
    let localFs = newLocalFileSystem()
    discard factory.setFileSystem(localFs)
    # Test that chaining methods work (they return factory for chaining)
    let factory2 = factory.addPath(getTempDir())
    check not factory2.isNil
  
  test "FinishOptions can be used with factory":
    let format = newCSVFileFormat()
    let factory = newFileSystemDatasetFactory(format)
    let opts = newFinishOptions()
    let localFs = newLocalFileSystem()
    discard factory.setFileSystem(localFs)
    # Even without adding paths, we should be able to create an empty dataset
    # (though it will have no fragments)
    let dataset = factory.finish(opts)
    check not dataset.isNil

# ============================================================================
# Phase 2 Tests (InMemoryFragment)
# ============================================================================

suite "Dataset - Phase 2 (InMemoryFragment)":
  
  test "Create InMemoryFragment from record batches":
    # Create a simple schema
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name")
    ])
    
    # Create record batches
    let batch1 = newRecordBatch(schema, @[1'i32, 2'i32, 3'i32], @["Alice", "Bob", "Charlie"])
    let batch2 = newRecordBatch(schema, @[4'i32, 5'i32], @["Diana", "Eve"])
    
    # Create in-memory fragment
    let fragment = newInMemoryFragment(schema, [batch1, batch2])
    check not fragment.isNil
    check $fragment == "InMemoryFragment()"
  
  test "InMemoryFragment requires non-empty batches":
    let schema = newSchema([newField[int32]("id")])
    expect ValueError:
      discard newInMemoryFragment(schema, [])
