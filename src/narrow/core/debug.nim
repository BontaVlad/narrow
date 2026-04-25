## gobject_debug.nim
## Comprehensive GObject debugging toolset for Nim
## Helps diagnose refcount issues, double frees, type mismatches, and lifecycle bugs.

# # ── 1. Quick inspection ───────────────────────────────────────────────────────
# dumpObject(someGtkWidget)
# # ┌─ GObject Dump ──────────────────────────────────
# # │  address   : 0x00007f8b2c001a80
# # │  type name : GtkButton
# # │  GType     : 94234567
# # │  ref_count : 1
# # └─────────────────────────────────────────────────

# # ── 2. Lifecycle tracking ─────────────────────────────────────────────────────
# withTracking:
#   let btn = gtk_button_new()
#   trackerRecord(btn, evAlloc)
#   discard trackedRef(btn)        # rc → 2
#   trackedUnref(btn)              # rc → 1
#   trackedUnref(btn)              # rc → 0 → evFree recorded
#   # reportLeaks() called automatically

# # ── 3. Refcount assertions ────────────────────────────────────────────────────
# assertRefCount(widget, 1)        # aborts if rc ≠ 1

# # ── 4. Snapshot diff across a code region ────────────────────────────────────
# snapshotScope("after dialog creation"):
#   let dlg = createMyDialog()
#   show(dlg)

# # ── 5. Manual validation ──────────────────────────────────────────────────────
# validate(suspiciousPtr, "mystery widget")

# # ── 6. Full event history ────────────────────────────────────────────────────
# dumpHistory(btn)

import std/[tables, strformat, strutils, locks]
when defined(windows):
  import std/winlean
else:
  import std/posix

import ../core/ffi

# ─── Core inspection helpers (your draft, hardened) ───────────────────────────

func getGType*(obj: pointer): GType {.inline.} =
  ## Reads the GType from a GObject instance.
  ## Layout: GObject → GTypeInstance → g_class (ptr GTypeClass) → g_type
  doAssert obj != nil, "getGType: null pointer"
  let classPtr = cast[ptr pointer](obj)[]       # GTypeInstance.g_class
  result = cast[ptr GType](classPtr)[]           # GTypeClass.g_type

func typeName*(obj: pointer): string {.inline.} =
  if obj == nil: return "<nil>"
  let n = g_type_name(getGType(obj))
  if n == nil: "<unknown>" else: $n

func getRefCount*(obj: pointer): uint32 {.inline.} =
  if obj == nil: return 0
  cast[ptr GObject](obj).ref_count

# ─── Poison / canary constants ────────────────────────────────────────────────

const
  FREED_POISON*  = 0xDEAD_BEEF'u32   ## refcount written on fake-free detection
  CANARY_MAGIC*  = 0xCAFE_F00D'u64   ## sentinel stored in tracker entries

# ─── Object lifecycle tracker ─────────────────────────────────────────────────

type
  LifecycleEvent* = enum
    evAlloc, evRef, evUnref, evFree, evDoubleFree, evRefAfterFree

  TrackEntry* = object
    address*:    pointer
    typeName*:   string
    refCount*:   uint32          ## snapshot at event time
    event*:      LifecycleEvent
    stackTrace*: string
    canary*:     uint64

  ObjectRecord* = object
    address*:    pointer
    typeName*:   string
    peakRef*:    uint32
    events*:     seq[TrackEntry]
    freed*:      bool
    canary*:     uint64

var
  gTracker*:   Table[pointer, ObjectRecord]
  gTrackerLock*: Lock
  gTrackerEnabled* = false

initLock(gTrackerLock)

func captureStack(): string =
  ## Lightweight stack capture – lists Nim source locations.
  result = ""
  for entry in getStackTraceEntries():
    if entry.filename != "":
      result.add &"  {entry.filename}:{entry.line} {entry.procname}\n"

proc trackerRecord*(obj: pointer, ev: LifecycleEvent) =
  if not gTrackerEnabled or obj == nil: return
  withLock gTrackerLock:
    let rc  = getRefCount(obj)
    let tn  = typeName(obj)
    let stk = captureStack()
    let entry = TrackEntry(
      address:    obj,
      typeName:   tn,
      refCount:   rc,
      event:      ev,
      stackTrace: stk,
      canary:     CANARY_MAGIC
    )
    if obj notin gTracker:
      gTracker[obj] = ObjectRecord(
        address:  obj,
        typeName: tn,
        peakRef:  rc,
        freed:    false,
        canary:   CANARY_MAGIC
      )
    var rec = addr gTracker[obj]
    rec.events.add entry
    if rc > rec.peakRef: rec.peakRef = rc
    case ev
    of evFree:
      if rec.freed:
        rec.events[^1].event = evDoubleFree
        stderr.writeLine &"[GObj] ⚠  DOUBLE FREE detected: {tn} @ {cast[uint](obj):#x}  refcount={rc}"
        stderr.writeLine stk
      rec.freed = true
    of evUnref:
      if rec.freed:
        rec.events[^1].event = evRefAfterFree
        stderr.writeLine &"[GObj] ⚠  UNREF AFTER FREE: {tn} @ {cast[uint](obj):#x}"
        stderr.writeLine stk
    else: discard

# ─── Safe wrappers that auto-track ────────────────────────────────────────────

proc trackedRef*(obj: pointer): pointer {.discardable.} =
  trackerRecord(obj, evRef)
  result = g_object_ref(obj)

proc trackedUnref*(obj: pointer) =
  let rc = getRefCount(obj)
  trackerRecord(obj, if rc <= 1: evFree else: evUnref)
  g_object_unref(obj)

# ─── Refcount snapshot & diff ─────────────────────────────────────────────────

type RefSnapshot* = Table[pointer, uint32]

proc takeSnapshot*(): RefSnapshot =
  ## Snapshot current refcounts for all tracked objects.
  withLock gTrackerLock:
    for k, v in gTracker:
      if not v.freed:
        result[k] = getRefCount(k)

proc diffSnapshots*(before, after: RefSnapshot): seq[string] =
  ## Returns human-readable lines for objects whose refcount changed.
  for k, rc in after:
    let prev = before.getOrDefault(k, 0)
    if rc != prev:
      result.add &"{typeName(k)} @ {cast[uint](k):#x}  {prev} → {rc}"
  for k, rc in before:
    if k notin after:
      result.add &"{typeName(k)} @ {cast[uint](k):#x}  {rc} → FREED"

# ─── Leak detector ────────────────────────────────────────────────────────────

proc reportLeaks*() =
  ## Print all tracked objects that were never freed.
  withLock gTrackerLock:
    var leaked = 0
    for _, rec in gTracker:
      if not rec.freed:
        inc leaked
        let rc = getRefCount(rec.address)
        echo &"[LEAK] {rec.typeName} @ {cast[uint](rec.address):#x}  refcount={rc}  peak={rec.peakRef}"
        for ev in rec.events:
          echo &"       {ev.event} rc={ev.refCount}"
          echo ev.stackTrace
    if leaked == 0:
      echo "[GObj] No leaks detected ✓"
    else:
      echo &"[GObj] {leaked} leaked object(s)"

# ─── Structural validator ─────────────────────────────────────────────────────

type ValidationResult* = object
  ok*:      bool
  issues*:  seq[string]

proc validateObject*(obj: pointer): ValidationResult =
  ## Sanity-check a live GObject pointer.
  result.ok = true

  if obj == nil:
    result.ok = false
    result.issues.add "Pointer is nil"
    return

  # Check refcount looks sane
  let rc = getRefCount(obj)
  if rc == 0:
    result.ok = false
    result.issues.add &"refcount is 0 – object may already be finalized"
  elif rc == FREED_POISON:
    result.ok = false
    result.issues.add "refcount matches FREED_POISON (0xDEADBEEF) – likely use-after-free"
  elif rc > 10_000:
    result.issues.add &"Suspiciously high refcount: {rc} – possible refcount leak"
    result.ok = false

  # Check g_class pointer is non-null
  let classPtr = cast[ptr pointer](obj)[]
  if classPtr == nil:
    result.ok = false
    result.issues.add "g_class pointer is nil – corrupt or freed GTypeInstance"
    return                         # can't safely dereference further

  # Verify type name is readable
  let gtype = cast[ptr GType](classPtr)[]
  let name  = g_type_name(gtype)
  if name == nil:
    result.ok = false
    result.issues.add &"g_type_name returned nil for GType {cast[uint](gtype)}"
  else:
    let ns = $name
    if ns.len == 0 or ns.len > 256:
      result.issues.add &"Suspicious type name length ({ns.len}): '{ns}'"

proc validate*(obj: pointer, label = "") =
  ## Validate and print a report.  Aborts on critical failures when
  ## compileOption("assertions") is on.
  let r = validateObject(obj)
  let tag = if label.len > 0: label else: typeName(obj)
  if r.ok:
    echo &"[GObj] ✓  {tag} @ {cast[uint](obj):#x}  rc={getRefCount(obj)}"
  else:
    echo &"[GObj] ✗  {tag} @ {cast[uint](obj):#x}"
    for iss in r.issues:
      echo &"         • {iss}"
    when compileOption("assertions"):
      doAssert false, "GObject validation failed: " & r.issues[0]

# ─── Pretty printer ───────────────────────────────────────────────────────────

proc dumpObject*(obj: pointer) =
  ## One-shot summary of an object's current state.
  if obj == nil:
    echo "[GObj] dumpObject: <nil>"
    return
  let rc    = getRefCount(obj)
  let tn    = typeName(obj)
  let gtype = getGType(obj)
  echo &"""
┌─ GObject Dump ──────────────────────────────────
│  address   : {cast[uint](obj):#018x}
│  type name : {tn}
│  GType     : {cast[uint](gtype)}
│  ref_count : {rc}
└─────────────────────────────────────────────────"""
  validate(obj)

proc dumpHistory*(obj: pointer) =
  ## Print the full lifecycle event history for obj.
  withLock gTrackerLock:
    if obj notin gTracker:
      echo &"[GObj] No history for {cast[uint](obj):#x} (tracking enabled? {gTrackerEnabled})"
      return
    let rec = gTracker[obj]
    echo &"History for {rec.typeName} @ {cast[uint](obj):#x}  (peak rc={rec.peakRef})"
    for i, ev in rec.events:
      echo &"  [{i:>3}] {ev.event:<14} rc={ev.refCount}"
      if ev.stackTrace.len > 0:
        for ln in ev.stackTrace.splitLines:
          if ln.len > 0: echo &"         {ln}"

# ─── Convenience macros ───────────────────────────────────────────────────────

template withTracking*(body: untyped) =
  ## Enable tracker, run body, then report leaks.
  gTrackerEnabled = true
  try:
    body
  finally:
    reportLeaks()
    gTrackerEnabled = false

template assertRefCount*(obj: pointer, expected: uint32) =
  let got = getRefCount(obj)
  if got != expected:
    let tn = typeName(obj)
    raiseAssert &"assertRefCount failed for {tn} @ {cast[uint](obj):#x}: " &
                &"expected {expected}, got {got}"

template snapshotScope*(label: string, body: untyped) =
  ## Print any refcount changes that occur inside `body`.
  let snapBefore = takeSnapshot()
  body
  let snapAfter  = takeSnapshot()
  let snapDiffs  = diffSnapshots(snapBefore, snapAfter)
  if snapDiffs.len > 0:
    echo &"[Snapshot '{label}'] changes:"
    for d in snapDiffs: echo "  ", d
  else:
    echo &"[Snapshot '{label}'] no changes"
