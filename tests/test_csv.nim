import std/[os, options]
import unittest2
import ../src/[ffi, filesystem, gtables, csv, gtypes]

suite "CSV":

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

    let schema = newSchema(@[newField[string]("First name"), newField[string]("Last name")])
    options.addSchema(schema)

    let table = readCSV(uri, options)
    check table.nRows == 4
