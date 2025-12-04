import ./[ffi]

type GListWrapper*[T] = object
  list*: ptr GList
  owned: bool # Whether this wrapper owns the list and should free it

proc toPtr*(g: GListWrapper): ptr GList =
  g.list

proc `=destroy`*[T](wrapper: GListWrapper[T]) =
  if wrapper.owned and wrapper.list != nil:
    g_list_free(wrapper.list)

proc `=copy`*[T](dest: var GListWrapper[T], source: GListWrapper[T]) =
  dest.list = source.list
  dest.owned = false # Copies are never owning to avoid double-free

proc `=sink`*[T](dest: var GListWrapper[T], source: GListWrapper[T]) =
  dest.list = source.list
  dest.owned = source.owned

proc newGList*[T](list: ptr GList, owned: bool = true): GListWrapper[T] =
  GListWrapper[T](list: list, owned: owned)

proc newGList*[T](): GListWrapper[T] =
  GListWrapper[T](list: nil, owned: true)

proc len*[T](wrapper: GListWrapper[T]): int =
  if wrapper.list == nil:
    result = 0
  else:
    result = int(g_list_length(wrapper.list))

proc append*[T](wrapper: var GListWrapper[T], data: T) =
  wrapper.list = g_list_append(wrapper.list, cast[gpointer](data))

proc prepend*[T](wrapper: var GListWrapper[T], data: T) =
  wrapper.list = g_list_prepend(wrapper.list, cast[gpointer](data))

proc `[]`*[T](wrapper: GListWrapper[T], index: int): T =
  let node = g_list_nth(wrapper.list, cuint(index))
  if node == nil:
    raise newException(IndexDefect, "Index out of bounds")
  cast[T](node.data)

iterator items*[T](wrapper: GListWrapper[T]): T =
  var current = wrapper.list
  while current != nil:
    yield cast[T](current.data)
    current = current.next

proc newGList*[T](items: openArray[T]): GListWrapper[T] =
  var lst = newGList[T]()
  for i in items:
    lst.append(i)
  result = lst
