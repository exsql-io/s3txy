package io.exsql.s3xty;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

public final class SchemaHelper {

    private SchemaHelper() {}

    public static Object2ObjectOpenHashMap<UTF8String, DataType> convert(final StructType schema) {
        var fieldTypes = new Object2ObjectOpenHashMap<UTF8String, DataType>();
        for (var field: schema.fields()) {
            fieldTypes.put(UTF8String.fromString(field.name()), field.dataType());
        }

        return fieldTypes;
    }

}
