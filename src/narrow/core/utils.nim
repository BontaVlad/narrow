import std/macros

proc findHandleType(recList: NimNode): NimNode =
  if recList.kind != nnkRecList:
    return nil
  for field in recList:
    if field.kind == nnkIdentDefs:
      let name =
        if field[0].kind == nnkPostfix:
          field[0][1]
        else:
          field[0]
      if name.strVal == "handle":
        return field[1]

proc dot(a: NimNode, b: string): NimNode =
  newDotExpr(a, ident(b))

proc hookProc(
    name: string,
    params: openArray[NimNode],
    body: NimNode,
    pragmas: NimNode = newEmptyNode(),
): NimNode =
  newProc(
    name =
      newNimNode(nnkPostfix).add(ident"*", newNimNode(nnkAccQuoted).add(ident(name))),
    params = @[newEmptyNode()] & @params,
    body = body,
    pragmas = pragmas,
  )

proc genHooks(
    typeName: NimNode, recList: NimNode, unrefProc, refProc: NimNode
): NimNode =
  result = newStmtList()
  let h = "handle"
  let x = ident"x"
  let d = ident"dest"
  let s = ident"src"
  let varT = newNimNode(nnkVarTy).add(typeName)

  result.add hookProc(
    "=destroy",
    [newIdentDefs(x, typeName)],
    newStmtList(
      newIfStmt(
        (infix(x.dot(h), "!=", newNilLit()), newStmtList(newCall(unrefProc, x.dot(h))))
      )
    ),
  )

  result.add hookProc(
    "=sink",
    [newIdentDefs(d, varT), newIdentDefs(s, typeName)],
    newStmtList(
      newIfStmt(
        (
          infix(
            infix(d.dot(h), "!=", newNilLit()), "and", infix(d.dot(h), "!=", s.dot(h))
          ),
          newStmtList(newCall(unrefProc, d.dot(h))),
        )
      ),
      newAssignment(d.dot(h), s.dot(h)),
    ),
  )

  result.add hookProc(
    "=copy",
    [newIdentDefs(d, varT), newIdentDefs(s, typeName)],
    newStmtList(
      newIfStmt(
        (
          infix(d.dot(h), "!=", s.dot(h)),
          newStmtList(
            newIfStmt(
              (
                infix(d.dot(h), "!=", newNilLit()),
                newStmtList(newCall(unrefProc, d.dot(h))),
              )
            ),
            newAssignment(d.dot(h), s.dot(h)),
            newIfStmt(
              (
                infix(s.dot(h), "!=", newNilLit()),
                newStmtList(newNimNode(nnkDiscardStmt).add(newCall(refProc, d.dot(h)))),
              )
            ),
          ),
        )
      )
    ),
  )

  let handleType = findHandleType(recList)
  let retType =
    if handleType != nil:
      handleType
    else:
      ident"auto"
  result.add newProc(
    name = newNimNode(nnkPostfix).add(ident"*", ident"toPtr"),
    params = [retType, newIdentDefs(x, typeName)],
    body = newStmtList(x.dot(h)),
    pragmas = newNimNode(nnkPragma).add(ident"inline"),
  )

macro arcGObject*(body: untyped): untyped =
  ## Statement macro — annotate a type section to auto-generate ARC hooks.
  ## Every object type in the block gets `=destroy`, `=sink`, `=copy`, and `toPtr`.
  ##
  ## ```nim
  ## arcGObject:
  ##   type
  ##     FileInfo* = object
  ##       handle*: ptr GArrowFileInfo
  ##     FileSelector* = object
  ##       handle*: ptr GArrowFileSelector
  ## ```
  result = newStmtList()
  result.add(body)

  for node in body:
    if node.kind != nnkTypeSection:
      continue
    for typeDef in node:
      if typeDef.kind != nnkTypeDef:
        continue
      let impl = typeDef[2]
      if impl.kind != nnkObjectTy:
        continue
      if impl[1].kind != nnkEmpty:
        continue # skip `of` inheritance

      let nameNode = typeDef[0]
      let typeName =
        if nameNode.kind == nnkPostfix:
          nameNode[1]
        else:
          nameNode
      result.add genHooks(typeName, impl[2], ident"g_object_unref", ident"g_object_ref")

macro arcRef*(unrefName, refName: static[string], body: untyped): untyped =
  ## Statement macro with custom ref/unref functions.
  ##
  ## ```nim
  ## arcRef("g_uri_unref", "g_uri_ref"):
  ##   type
  ##     Uri* = object
  ##       handle*: ptr GUri
  ## ```
  result = newStmtList()
  result.add(body)

  for node in body:
    if node.kind != nnkTypeSection:
      continue
    for typeDef in node:
      if typeDef.kind != nnkTypeDef:
        continue
      let impl = typeDef[2]
      if impl.kind != nnkObjectTy:
        continue
      if impl[1].kind != nnkEmpty:
        continue

      let nameNode = typeDef[0]
      let typeName =
        if nameNode.kind == nnkPostfix:
          nameNode[1]
        else:
          nameNode
      result.add genHooks(typeName, impl[2], ident(unrefName), ident(refName))
