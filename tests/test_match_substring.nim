import std/options
import unittest2
import ../src/narrow/[core/ffi, compute/expressions, compute/match_substring_options, column/primitive, tabular/table, column/metadata, compute/filters]

suite "MatchSubstringOptions - Creation and Properties":
  test "Create MatchSubstringOptions with pattern":
    let opts = newMatchSubstringOptions("test")
    check opts.pattern == "test"
    check opts.ignoreCase == false
    
  test "Create MatchSubstringOptions with pattern and ignoreCase":
    let opts = newMatchSubstringOptions("pattern", true)
    check opts.pattern == "pattern"
    check opts.ignoreCase == true
    
  test "Set and get pattern":
    var opts = newMatchSubstringOptions("initial")
    opts.pattern = "changed"
    check opts.pattern == "changed"
    
  test "Set and get ignoreCase":
    var opts = newMatchSubstringOptions("test")
    opts.ignoreCase = true
    check opts.ignoreCase == true
    opts.ignoreCase = false
    check opts.ignoreCase == false
    
  test "MatchSubstringOptions memory management (copy)":
    let opts1 = newMatchSubstringOptions("test")
    let opts2 = opts1
    check opts1.pattern == opts2.pattern
    check opts1.ignoreCase == opts2.ignoreCase
    
  test "MatchSubstringOptions memory management (move)":
    var opts: MatchSubstringOptions
    block:
      let temp = newMatchSubstringOptions("moved")
      opts = temp
    check opts.pattern == "moved"

suite "String Expressions - match_substring":
  test "Create match_substring expression":
    let nameField = newFieldExpression("name")
    let expr = strContains(nameField, "li")
    check expr.toPtr != nil
    
  test "Create match_substring expression with ignore case":
    let nameField = newFieldExpression("name")
    let expr = strContains(nameField, "LI", true)
    check expr.toPtr != nil

suite "String Expressions - starts_with":
  test "Create starts_with expression":
    let nameField = newFieldExpression("name")
    let expr = startsWith(nameField, "Al")
    check expr.toPtr != nil
    
  test "Create starts_with expression with ignore case":
    let nameField = newFieldExpression("name")
    let expr = startsWith(nameField, "al", true)
    check expr.toPtr != nil

suite "String Expressions - ends_with":
  test "Create ends_with expression":
    let nameField = newFieldExpression("name")
    let expr = endsWith(nameField, "ce")
    check expr.toPtr != nil
    
  test "Create ends_with expression with ignore case":
    let nameField = newFieldExpression("name")
    let expr = endsWith(nameField, "CE", true)
    check expr.toPtr != nil

suite "String Expressions - match_substring_regex":
  test "Create match_substring_regex expression":
    let nameField = newFieldExpression("name")
    let expr = matchSubstringRegex(nameField, "^A.*e$")
    check expr.toPtr != nil
    
  test "Create match_substring_regex expression with ignore case":
    let nameField = newFieldExpression("name")
    let expr = matchSubstringRegex(nameField, "^a.*e$", true)
    check expr.toPtr != nil

suite "Filter Parser - contains operator":
  test "Parse contains filter clause":
    let clause: FilterClause = ("name", "contains", "Alice")
    let expr = parseFilter(clause)
    check expr.toPtr != nil
    
  test "Parse multiple filters with contains":
    let filters: seq[FilterClause] = @[
      ("name", "contains", "Alice"),
      ("email", "contains", "@gmail.com")
    ]
    let expr = parse(filters)
    check expr.toPtr != nil
