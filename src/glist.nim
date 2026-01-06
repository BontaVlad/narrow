import ./[ffi]

type GAList*[T] = object
  list*: ptr GList
  owned: bool # Whether this wrapper owns the list and should free it

proc toPtr*(g: GAList): ptr GList =
  g.list

proc `=destroy`*[T](wrapper: GAList[T]) =
  if wrapper.owned and wrapper.list != nil:
    g_list_free(wrapper.list)

proc `=copy`*[T](dest: var GAList[T], source: GAList[T]) =
  dest.list = source.list
  dest.owned = false # Copies are never owning to avoid double-free

proc `=sink`*[T](dest: var GAList[T], source: GAList[T]) =
  dest.list = source.list
  dest.owned = source.owned

proc newGList*[T](list: ptr GList, owned: bool = true): GAList[T] =
  GAList[T](list: list, owned: owned)

proc newGList*[T](): GAList[T] =
  GAList[T](list: nil, owned: true)

proc len*[T](wrapper: GAList[T]): int =
  if wrapper.list == nil:
    result = 0
  else:
    result = int(g_list_length(wrapper.list))

proc append*[T](wrapper: var GAList[T], data: T) =
  wrapper.list = g_list_append(wrapper.list, cast[gpointer](data))

proc prepend*[T](wrapper: var GAList[T], data: T) =
  wrapper.list = g_list_prepend(wrapper.list, cast[gpointer](data))

proc `[]`*[T](wrapper: GAList[T], index: int): T =
  let node = g_list_nth(wrapper.list, cuint(index))
  if node == nil:
    raise newException(IndexDefect, "Index out of bounds")
  cast[T](node.data)

iterator items*[T](wrapper: GAList[T]): T =
  var current = wrapper.list
  while current != nil:
    yield cast[T](current.data)
    current = current.next

proc newGList*[T](items: openArray[T]): GAList[T] =
  var lst = newGList[T]()
  for i in items:
    lst.append(i)
  result = lst
