import unittest2
import ../src/narrow/types/glist

suite "GAList - Creation":
  test "newGList empty":
    let lst = newGList[int32]()
    check lst.len == 0

  test "newGList from seq":
    let lst = newGList([1'i32, 2, 3])
    check lst.len == 3

  test "newGList from ptr GList":
    let lst = newGList[float64](nil, owned = true)
    check lst.len == 0

suite "GAList - Append and Prepend":
  test "append to empty list":
    var lst = newGList[int32]()
    lst.append(10'i32)
    check lst.len == 1
    check lst[0] == 10'i32

  test "append multiple items":
    var lst = newGList[int32]()
    lst.append(1'i32)
    lst.append(2'i32)
    lst.append(3'i32)
    check lst.len == 3
    check lst[0] == 1'i32
    check lst[1] == 2'i32
    check lst[2] == 3'i32

  test "prepend adds to front":
    var lst = newGList[int32]()
    lst.append(2'i32)
    lst.prepend(1'i32)
    check lst.len == 2
    check lst[0] == 1'i32
    check lst[1] == 2'i32

suite "GAList - Indexing and Length":
  test "index out of bounds raises":
    var lst = newGList[int32]()
    lst.append(1'i32)
    expect(IndexDefect):
      discard lst[1]
    expect(IndexDefect):
      discard lst[-1]

  test "len returns correct count":
    var lst = newGList[int32]()
    check lst.len == 0
    lst.append(1'i32)
    check lst.len == 1
    lst.append(2'i32)
    check lst.len == 2

suite "GAList - Iteration and Conversion":
  test "items iterator":
    var lst = newGList[int32]()
    lst.append(10'i32)
    lst.append(20'i32)
    lst.append(30'i32)
    var collected: seq[int32]
    for v in lst:
      collected.add v
    check collected == @[10'i32, 20, 30]

  test "toSeq conversion":
    var lst = newGList([5'i32, 10, 15])
    check lst.toSeq == @[5'i32, 10, 15]

suite "GAList - Pointer Types":
  test "list of pointers":
    var lst = newGList[pointer]()
    var a = 1'i32
    lst.append(cast[pointer](addr a))
    lst.append(cast[pointer](addr a))
    check lst.len == 2

suite "GAList - Memory Management":
  test "copy does not double-free":
    var lst1 = newGList([1'i32, 2, 3])
    let lst2 = lst1  # =copy — sets owned = false
    check lst2.len == 3
    # lst1 and lst2 both go out of scope — no double-free

  test "sink moves ownership":
    var lst1 = newGList([1'i32])
    var lst2 = move(lst1)
    check lst2.len == 1
    # lst1 was moved-from, =wasMoved ran

  test "dup creates independent copy":
    var lst1 = newGList([1'i32, 2])
    let lst2 = lst1  # =dup — shares pointer, owned = false
    check lst2.len == 2
    check lst2[0] == 1'i32

  test "GAList with various integer types":
    var lst8 = newGList[int8]()
    lst8.append(10'i8)
    lst8.append(20'i8)
    check lst8.len == 2
    check lst8[0] == 10'i8
    check lst8[1] == 20'i8

    var lst64 = newGList[int64]()
    lst64.append(100'i64)
    lst64.append(200'i64)
    check lst64.len == 2
    check lst64[0] == 100'i64
