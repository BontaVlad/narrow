import unittest2
import ../src/narrow

suite "Acero - Filter Table":
  test "filter table with simple comparison":
    let schema = newSchema([newField[int32]("age"), newField[string]("name")])
    let ages = newArray(@[10'i32, 25, 30, 15, 40])
    let names = newArray(@["child", "young", "adult", "teen", "senior"])
    let table = newArrowTable(schema, ages, names)

    let filter = col("age") >= 18'i32
    let filtered = filterTable(table, filter)

    unittest2.check filtered.nRows == 3  # 25, 30, 40

  test "filter with AND expression":
    let schema = newSchema([newField[int32]("x")])
    let xs = newArray(@[1'i32, 5, 10, 15, 20])
    let table = newArrowTable(schema, xs)

    let filter = (col("x") > 3'i32) and (col("x") < 16'i32)
    let filtered = filterTable(table, filter)

    unittest2.check filtered.nRows == 3  # 5, 10, 15

  test "filter that matches nothing returns empty table":
    let schema = newSchema([newField[int32]("v")])
    let vs = newArray(@[1'i32, 2, 3])
    let table = newArrowTable(schema, vs)

    let filter = col("v") > 100'i32
    let filtered = filterTable(table, filter)

    unittest2.check filtered.nRows == 0
    unittest2.check filtered.nColumns == 1  # schema preserved

suite "Acero - Project Table":
  test "select specific columns":
    let schema = newSchema([newField[int32]("a"), newField[int32]("b"),
                            newField[int32]("c")])
    let table = newArrowTable(schema,
      newArray(@[1'i32, 2, 3]),
      newArray(@[10'i32, 20, 30]),
      newArray(@[100'i32, 200, 300]))

    let projected = projectTable(table, [col("a"), col("c")], ["x", "y"])
    check projected.nRows == 3
    check projected.nColumns == 2

  test "compute derived expression column":
    let schema = newSchema([newField[int32]("price"),
                            newField[int32]("qty")])
    let table = newArrowTable(schema,
      newArray(@[10'i32, 20, 30]),
      newArray(@[1'i32, 2, 3]))

    let projected = projectTable(table,
      [col("price") * col("qty")], ["revenue"])
    check projected.nRows == 3
    check projected.nColumns == 1

  test "rename columns via project":
    let schema = newSchema([newField[string]("first"),
                            newField[string]("last")])
    let table = newArrowTable(schema,
      newArray(@["alice", "bob"]),
      newArray(@["smith", "jones"]))

    let projected = projectTable(table,
      [col("first"), col("last")], ["given_name", "family_name"])
    check projected.nRows == 2
    check projected.nColumns == 2

  test "project combined with filter via pipeline":
    let schema = newSchema([newField[int32]("id"),
                            newField[string]("name")])
    let table = newArrowTable(schema,
      newArray(@[1'i32, 2, 3, 4, 5]),
      newArray(@["a", "b", "c", "d", "e"]))
    let ctx = newExecuteContext(newThreadPool().toExecutor)
    let plan = newExecutePlan(ctx)
    let source = plan.buildSourceNode(newSourceNodeOptions(table))
    let filterNode = plan.buildFilterNode(source,
      newFilterNodeOptions(col("id") > 2'i32))
    let projNode = plan.buildProjectNode(filterNode,
      newProjectNodeOptions([col("id")], ["id"]))
    let sinkOpts = newSinkNodeOptions()
    discard plan.buildSinkNode(projNode, sinkOpts)
    plan.validate()
    let reader = sinkOpts.getReader(projNode.outputSchema)
    plan.start()
    let projected = reader.readAll()
    plan.wait()
    check projected.nRows == 3   # ids 3, 4, 5
    check projected.nColumns == 1

suite "Acero - Plan Introspection":
  test "getNodes returns node list":
    let schema = newSchema([newField[int32]("id"), newField[string]("name")])
    let ids = newArray(@[1'i32, 2, 3, 4, 5])
    let names = newArray(@["a", "b", "c", "d", "e"])
    let table = newArrowTable(schema, ids, names)

    let pool = newThreadPool()
    let ctx = newExecuteContext(pool.toExecutor)
    let plan = newExecutePlan(ctx)
    let source = plan.buildSourceNode(newSourceNodeOptions(table))
    let filterNode = plan.buildFilterNode(source,
      newFilterNodeOptions(col("id") > 2'i32))
    let sinkOpts = newSinkNodeOptions()
    discard plan.buildSinkNode(filterNode, sinkOpts)

    let nodes = plan.getNodes()
    check nodes != nil

  test "getKindName returns human-readable node type":
    let pool = newThreadPool()
    let ctx = newExecuteContext(pool.toExecutor)
    let plan = newExecutePlan(ctx)
    let table = newArrowTable(
      newSchema([newField[int32]("x")]),
      newArray(@[1'i32, 2, 3]),
    )
    let source = plan.buildSourceNode(newSourceNodeOptions(table))
    let sinkOpts = newSinkNodeOptions()
    discard plan.buildSinkNode(source, sinkOpts)

    let nodes = plan.getNodes()
    check nodes != nil

    var current = nodes
    var kindNames: seq[string]
    var nodeCount = 0
    while current != nil:
      let rawNode = cast[ptr GArrowExecuteNode](current.data)
      discard g_object_ref(rawNode)
      let kind = getKindName(ExecuteNode(handle: rawNode))
      kindNames.add(kind)
      current = current.next
      inc nodeCount

    check nodeCount >= 2
    check "SourceNode" in kindNames

  test "outputSchema on a plan node":
    let pool = newThreadPool()
    let ctx = newExecuteContext(pool.toExecutor)
    let plan = newExecutePlan(ctx)
    let table = newArrowTable(
      newSchema([newField[int32]("x"), newField[string]("y")]),
      newArray(@[1'i32, 2, 3]),
      newArray(@["a", "b", "c"]),
    )
    let source = plan.buildSourceNode(newSourceNodeOptions(table))
    let schema = source.outputSchema
    check schema.nFields == 2
    check schema.getField(0).name == "x"
    check schema.getField(1).name == "y"
