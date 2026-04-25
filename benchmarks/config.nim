import criterion
import criterion/config
import std/os

proc narrowConfig*(): config.Config =
  ## Default benchmark configuration for Narrow.
  ## Tuned for I/O-heavy C API wrappers where warmup matters.
  result = newDefaultConfig()
  result.warmupBudget = 0.5
  result.budget = 0.1
  result.minSamples = 20
  result.verbose = true
  let outputPath = getEnv("NARROW_BENCH_OUTPUT", "")
  if outputPath != "":
    result.outputPath = outputPath
