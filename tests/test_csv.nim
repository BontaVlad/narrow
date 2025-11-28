import std/[os, options, sets, sequtils]
import unittest2
import ../src/[ffi, filesystem, gtables, csv, gtypes]

suite "Reading CSV":

  test "read csv file localFileSystem":
    let uri = getCurrentDir() & "/tests/customers-100.csv"
    let table = readCSV(uri)
    check table.nRows == 100

  test "read csv file with full uri":
    let uri = "file://" & getCurrentDir() & "/tests/customers-100.csv"
    let table = readCSV(uri)
    check table.nRows == 100

  test "read csv file with custom delimiter":
    let uri = getCurrentDir() & "/tests/email.csv"
    var options = newCsvReadOptions(delimiter=some(';'))

    let table = readCSV(uri, options)
    check table.nRows == 4

  test "read csv file with custom delimiter and column filtering":
    let uri = getCurrentDir() & "/tests/email.csv"
    var options = newCsvReadOptions(delimiter=some(';'))

    let schema = newSchema(@[newField[string]("First name"), newField[string]("Last name")])
    options.addSchema(schema)

    let table = readCSV(uri, options)
    check table.nRows == 4
    let tblKeys = toHashSet(table.keys.toSeq)
    check len(tblKeys) == 2
    check "First name" in tblKeys
    check "Last name" in tblKeys

suite "Writing CSV":

  test "writet table to csv file localFileSystem":
    let uri = getCurrentDir() & "/tests/customers-100.csv"
    # let table = 
    # let table = readCSV(uri)
    # check table.nRows == 100
