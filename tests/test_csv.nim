import std/[os]
import unittest2
import ../src/[gtables, csv]

suite "CSV":

  test "read csv file":
    let path = getCurrentDir() & "/tests/customers-100.csv"
    let table = readCSV(path)
    check table.nRows == 100
