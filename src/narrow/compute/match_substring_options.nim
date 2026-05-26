import ../core/[ffi, utils]

arcGObject:
  type MatchSubstringOptions* = object
    handle*: ptr GArrowMatchSubstringOptions

proc newMatchSubstringOptions*(
    pattern: string, ignoreCase: bool = false
): MatchSubstringOptions =
  ## Creates match substring options with the given pattern
  result.handle = garrow_match_substring_options_new()
  g_object_set(result.handle, "pattern", pattern.cstring, nil)
  g_object_set(result.handle, "ignore-case", ignoreCase.gboolean, nil)

proc pattern*(options: MatchSubstringOptions): string {.inline.} =
  ## Gets the pattern to match
  var ptrn: cstring
  g_object_get(options.handle, "pattern", addr ptrn, nil)
  result = $ptrn
  g_free(ptrn)

proc `pattern=`*(options: MatchSubstringOptions, pattern: string) {.inline.} =
  ## Sets the pattern to match
  g_object_set(options.handle, "pattern", pattern.cstring, nil)

proc ignoreCase*(options: MatchSubstringOptions): bool {.inline.} =
  ## Gets whether to ignore case when matching
  var ignore: gboolean
  g_object_get(options.handle, "ignore-case", addr ignore, nil)
  result = ignore != 0

proc `ignoreCase=`*(options: MatchSubstringOptions, ignoreCase: bool) {.inline.} =
  ## Sets whether to ignore case when matching
  g_object_set(options.handle, "ignore-case", ignoreCase.gboolean, nil)
