import std/macros

import ./[ffi, error]

var computeInitialized {.global.} = false

proc ensureComputeInitialized*() =
  ## Ensures compute functions are registered. Thread-safe one-time initialization.
  once:
    var err = newError()
    if not garrow_compute_initialize(err.toPtr).bool or err:
      raise newException(OperationError, "Failed to initialize compute: " & $err)
    computeInitialized = true

func findHandleType(recList: NimNode): NimNode =
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

func dot(a: NimNode, b: string): NimNode =
  newDotExpr(a, ident(b))

func genericParamNames(gp: NimNode): seq[NimNode] =
  ## Extract just the identifier nodes from a nnkGenericParams block.
  result = @[]
  if gp.kind != nnkGenericParams:
    return
  for identDefs in gp:
    if identDefs.kind != nnkIdentDefs:
      continue
    # All children except the last two are names being defined.
    let nameCount = identDefs.len - 2
    for i in 0 ..< nameCount:
      let n = identDefs[i]
      if n.kind == nnkIdent:
        result.add(n)
      elif n.kind == nnkPostfix and n[1].kind == nnkIdent:
        result.add(n[1])

func freshGenericParams(gp: NimNode): NimNode =
  ## Create a fresh nnkGenericParams with new ident nodes.
  ## Reusing generic param nodes from a type definition in macro-generated
  ## procs can cause "cannot instantiate" errors; fresh idents avoid this.
  if gp.kind != nnkGenericParams:
    return newEmptyNode()
  result = newNimNode(nnkGenericParams)
  for identDefs in gp:
    if identDefs.kind != nnkIdentDefs:
      continue
    let nameCount = identDefs.len - 2
    for i in 0 ..< nameCount:
      let n = identDefs[i]
      if n.kind == nnkIdent:
        result.add(newIdentDefs(ident(n.strVal), newEmptyNode()))
      elif n.kind == nnkPostfix and n[1].kind == nnkIdent:
        result.add(newIdentDefs(newNimNode(nnkPostfix).add(ident"*", ident(n[1].strVal)), newEmptyNode()))

func makeParamType(typeName: NimNode, genericParams: NimNode): NimNode =
  ## Build TypeName[T, U] from the base type name and generic params.
  if genericParams.kind == nnkGenericParams:
    var bracket = newNimNode(nnkBracketExpr).add(typeName)
    for name in genericParamNames(genericParams):
      bracket.add(name)
    bracket
  else:
    typeName

func makeProcDef(
    name: NimNode,
    params: openArray[NimNode],
    body: NimNode,
    pragmas: NimNode = newEmptyNode(),
    genericParams: NimNode = newEmptyNode(),
): NimNode =
  ## Build a nnkProcDef manually because newProc lacks a genericParams parameter.
  ## Mimics newProc's 7-child structure exactly.
  result = newNimNode(nnkProcDef)
  result.add(name)
  result.add(newEmptyNode()) # placeholder (matches newProc/parsed AST)
  result.add(genericParams)
  var formalParams = newNimNode(nnkFormalParams)
  for p in params:
    formalParams.add(p)
  result.add(formalParams)
  result.add(pragmas)
  result.add(newEmptyNode()) # reserved
  result.add(body)

func makeAccQuoted(name: string): NimNode =
  ## Build nnkAccQuoted from a name. Names starting with '=' (e.g. '=destroy')
  ## are split into '=' and the rest so the AST matches the parser.
  result = newNimNode(nnkAccQuoted)
  if name.len > 1 and name[0] == '=':
    result.add(ident"=")
    result.add(ident(name.substr(1)))
  else:
    result.add(ident(name))

func hookProc(
    name: string,
    params: openArray[NimNode],
    body: NimNode,
    pragmas: NimNode = newEmptyNode(),
    genericParams: NimNode = newEmptyNode(),
): NimNode =
  let procName =
    newNimNode(nnkPostfix).add(ident"*", makeAccQuoted(name))
  makeProcDef(procName, params, body, pragmas, genericParams)

func genHooks(
    typeName: NimNode,
    recList: NimNode,
    unrefProc, refProc: NimNode,
    genericParams: NimNode = newEmptyNode(),
): NimNode =
  result = newStmtList()
  let h = "handle"
  let x = ident"x"
  let d = ident"dest"
  let s = ident"src"

  let paramType = makeParamType(typeName, genericParams)
  let varParamType = newNimNode(nnkVarTy).add(paramType)

  # =destroy
  result.add hookProc(
    "=destroy",
    [newEmptyNode(), newIdentDefs(x, paramType)],
    newStmtList(
      newIfStmt(
        (infix(x.dot(h), "!=", newNilLit()), newStmtList(newCall(unrefProc, x.dot(h))))
      )
    ),
    genericParams = genericParams,
  )

  # =wasMoved
  result.add hookProc(
    "=wasMoved",
    [newEmptyNode(), newIdentDefs(x, varParamType)],
    newStmtList(
      newAssignment(x.dot(h), newNilLit())
    ),
    genericParams = genericParams,
  )

  # =dup
  result.add hookProc(
    "=dup",
    [paramType, newIdentDefs(x, paramType)],
    newStmtList(
      newAssignment(newDotExpr(ident"result", ident(h)), x.dot(h)),
      newIfStmt(
        (
          infix(x.dot(h), "!=", newNilLit()),
          newStmtList(
            newNimNode(nnkDiscardStmt).add(newCall(refProc, newDotExpr(ident"result", ident(h))))
          ),
        )
      )
    ),
    genericParams = genericParams,
  )

  # =copy
  result.add hookProc(
    "=copy",
    [newEmptyNode(), newIdentDefs(d, varParamType), newIdentDefs(s, paramType)],
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
    genericParams = genericParams,
  )

  let handleType = findHandleType(recList)
  let retType =
    if handleType != nil:
      handleType
    else:
      ident"auto"
  result.add makeProcDef(
    name = newNimNode(nnkPostfix).add(ident"*", ident"toPtr"),
    params = [retType, newIdentDefs(x, paramType)],
    body = newStmtList(x.dot(h)),
    pragmas = newNimNode(nnkPragma).add(ident"inline"),
    genericParams = genericParams,
  )

macro arcGObject*(body: untyped): untyped =
  ## Statement macro — annotate a type section to auto-generate ARC hooks.
  ## Every object type in the block gets `=destroy`, `=wasMoved`, `=dup`,
  ## `=copy`, and `toPtr`.
  ##
  ## Generic types are supported: `type Foo*[T] = object ...` produces
  ## hooks with the proper generic parameter list.
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
    if node.kind == nnkTypeSection:
      for typeDef in node:
        if typeDef.kind == nnkTypeDef:
          let impl = typeDef[2]
          if impl.kind == nnkObjectTy and impl[1].kind == nnkEmpty:
            let nameNode = typeDef[0]
            let typeName =
              if nameNode.kind == nnkPostfix:
                nameNode[1]
              else:
                nameNode
            let genericParams = freshGenericParams(typeDef[1])
            result.add genHooks(typeName, impl[2], ident"g_object_unref", ident"g_object_ref", genericParams)

macro arcRef*(unrefName, refName: static[string], body: untyped): untyped =
  ## Statement macro with custom ref/unref functions.
  ## Supports generic types the same way as `arcGObject`.
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
    if node.kind == nnkTypeSection:
      for typeDef in node:
        if typeDef.kind == nnkTypeDef:
          let impl = typeDef[2]
          if impl.kind == nnkObjectTy and impl[1].kind == nnkEmpty:
            let nameNode = typeDef[0]
            let typeName =
              if nameNode.kind == nnkPostfix:
                nameNode[1]
              else:
                nameNode
            let genericParams = freshGenericParams(typeDef[1])
            result.add genHooks(typeName, impl[2], ident(unrefName), ident(refName), genericParams)
