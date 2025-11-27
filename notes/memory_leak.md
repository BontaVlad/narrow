**Title**: "Memory leak in garrow_csv_read_options with ASAN"

**Description**:
```markdown
## Description
Memory leaks detected in Arrow GLib CSV read options when using AddressSanitizer.

## Reproduce
```c
#include <arrow-glib/arrow-glib.h>

int main() {
    GArrowStringDataType *string_type1 = garrow_string_data_type_new();
    GArrowStringDataType *string_type2 = garrow_string_data_type_new();
    
    GArrowField *field1 = garrow_field_new("col1", GARROW_DATA_TYPE(string_type1));
    GArrowField *field2 = garrow_field_new("col2", GARROW_DATA_TYPE(string_type2));
    
    GList *fields = NULL;
    fields = g_list_append(fields, field1);
    fields = g_list_append(fields, field2);
    
    GArrowSchema *schema = garrow_schema_new(fields);
    g_list_free(fields);
    
    GArrowCSVReadOptions *options = garrow_csv_read_options_new();
    garrow_csv_read_options_add_schema(options, schema);
    
    g_object_unref(schema);
    g_object_unref(options);
    g_object_unref(field1);
    g_object_unref(field2);
    g_object_unref(string_type1);
    g_object_unref(string_type2);
    
    return 0;
}
```

Compile: `gcc -o test test.c $(pkg-config --cflags --libs arrow-glib) -fsanitize=address -g`

## ASAN Output
==738964==ERROR: LeakSanitizer: detected memory leaks

Direct leak of 544 byte(s) in 1 object(s) allocated from:
    #0 0x7fc8141218cd in operator new(unsigned long) /usr/src/debug/gcc/gcc/libsanitizer/asan/asan_new_delete.cpp:86
    #1 0x7fc813616e23  (/usr/lib/libarrow.so.2200+0x816e23) (BuildId: 1cc0305502fc617971275ffc0630d0452fa87ab5)
    #2 0x7fc813617089 in arrow::csv::ConvertOptions::Defaults() (/usr/lib/libarrow.so.2200+0x817089) (BuildId: 1cc0305502fc617971275ffc0630d0452fa87ab5)
    #3 0x7fc813f77483 in garrow_csv_read_options_init (/usr/lib/libarrow-glib.so.2200+0x132483) (BuildId: 8797654954713a7ea7d7238c0ba43aa91fc37331)

Direct leak of 128 byte(s) in 1 object(s) allocated from:
    #0 0x7fc8141218cd in operator new(unsigned long) /usr/src/debug/gcc/gcc/libsanitizer/asan/asan_new_delete.cpp:86
    #1 0x7fc813616e23  (/usr/lib/libarrow.so.2200+0x816e23) (BuildId: 1cc0305502fc617971275ffc0630d0452fa87ab5)
    #2 0x7fc81361715a in arrow::csv::ConvertOptions::Defaults() (/usr/lib/libarrow.so.2200+0x81715a) (BuildId: 1cc0305502fc617971275ffc0630d0452fa87ab5)
    #3 0x7fc813f77483 in garrow_csv_read_options_init (/usr/lib/libarrow-glib.so.2200+0x132483) (BuildId: 8797654954713a7ea7d7238c0ba43aa91fc37331)

Direct leak of 128 byte(s) in 1 object(s) allocated from:
    #0 0x7fc8141218cd in operator new(unsigned long) /usr/src/debug/gcc/gcc/libsanitizer/asan/asan_new_delete.cpp:86
    #1 0x7fc813616e23  (/usr/lib/libarrow.so.2200+0x816e23) (BuildId: 1cc0305502fc617971275ffc0630d0452fa87ab5)
    #2 0x7fc813617227 in arrow::csv::ConvertOptions::Defaults() (/usr/lib/libarrow.so.2200+0x817227) (BuildId: 1cc0305502fc617971275ffc0630d0452fa87ab5)
    #3 0x7fc813f77483 in garrow_csv_read_options_init (/usr/lib/libarrow-glib.so.2200+0x132483) (BuildId: 8797654954713a7ea7d7238c0ba43aa91fc37331)

Direct leak of 104 byte(s) in 1 object(s) allocated from:
    #0 0x7fc8141218cd in operator new(unsigned long) /usr/src/debug/gcc/gcc/libsanitizer/asan/asan_new_delete.cpp:86
    #1 0x7fc813f80d4e in std::_Hashtable<std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> >, std::pair<std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> > const, std::shared_ptr<arrow::DataType> >, std::allocator<std::pair<std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> > const, std::shared_ptr<arrow::DataType> > >, std::__detail::_Select1st, std::equal_to<std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> > >, std::hash<std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> > >, std::__detail::_Mod_range_hashing, std::__detail::_Default_ranged_hash, std::__detail::_Prime_rehash_policy, std::__detail::_Hashtable_traits<true, false, true> >::_M_insert_unique_node(unsigned long, unsigned long, std::__detail::_Hash_node<std::pair<std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> > const, std::shared_ptr<arrow::DataType> >, true>*, unsigned long) (/usr/lib/libarrow-glib.so.2200+0x13bd4e) (BuildId: 8797654954713a7ea7d7238c0ba43aa91fc37331)

Indirect leak of 128 byte(s) in 2 object(s) allocated from:
    #0 0x7fc8141218cd in operator new(unsigned long) /usr/src/debug/gcc/gcc/libsanitizer/asan/asan_new_delete.cpp:86
    #1 0x7fc813f7d853 in garrow_csv_read_options_add_schema (/usr/lib/libarrow-glib.so.2200+0x138853) (BuildId: 8797654954713a7ea7d7238c0ba43aa91fc37331)

SUMMARY: AddressSanitizer: 1032 byte(s) leaked in 6 allocation(s).

## Environment
- Arrow version: [your version - likely 22.0.0 based on .so.2200]
- OS: arch
- Compiler: [gcc version]
```

### 2. Suppress the Leak in Your Tests

Create `lsan.supp`:
```
# Known Arrow GLib leaks
leak:arrow::csv::ConvertOptions::Defaults
leak:garrow_csv_read_options_add_schema
leak:garrow_csv_read_options_init
```

Then run your tests with:
```bash
LSAN_OPTIONS=suppressions=lsan.supp nim c -r --gc:orc -d:useMalloc -t:"-fsanitize=address" -l:"-fsanitize=address" tests/test_csv.nim
```

Or add to your config.nims:
```nim
when defined(useSanitizers):
  --passC: "-fsanitize=address"
  --passL: "-fsanitize=address"
  --define: useMalloc
```

And create a wrapper script:
```bash
#!/bin/bash
export LSAN_OPTIONS=suppressions=lsan.supp
nim c -r -d:useSanitizers tests/test_csv.nim
```

### 3. Document It

Add a comment in your code:
```nim
proc addSchema*(options: var CsvReadOptions, schema: Schema) =
  ## Adds a schema to CSV read options for column filtering.
  ## 
  ## Note: Arrow GLib has a known memory leak in this function
  ## (see https://github.com/apache/arrow/issues/XXXXX).
  ## The leak is small and bounded (happens once per options object).
  garrow_csv_read_options_add_schema(options.handle, schema.handle)
```

### 4. Continue Development

This is a **small, bounded leak** (only happens when you create CSV options with schema). It's not ideal, but it won't cause issues in practice unless you're creating thousands of options objects.

Your Nim wrapper code is **correct** - the leak is in the underlying C++ library. You can safely continue development and wait for the Arrow team to fix it in a future release.

### Summary

✅ **Your Nim code is correct**  
✅ **Your memory management is correct**  
❌ **Arrow GLib has a bug**  
✅ **Suppress the leak and move on**

The fact that the pure C version leaks proves this isn't a Nim issue. Good debugging work isolating it down to the C layer!
