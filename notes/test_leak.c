// gcc -o test_leak test_leak.c $(pkg-config --cflags --libs arrow-glib) -fsanitize=address -g ./test_leak
#include <arrow-glib/arrow-glib.h>
#include <stdio.h>

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
    
    // Cleanup
    g_object_unref(schema);
    g_object_unref(options);
    g_object_unref(field1);
    g_object_unref(field2);
    g_object_unref(string_type1);
    g_object_unref(string_type2);

    return 0;
}
