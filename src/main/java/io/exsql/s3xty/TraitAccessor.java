package io.exsql.s3xty;

import io.exsql.s3xty.value.Value;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.OptionalDouble;
import java.util.OptionalLong;

public interface TraitAccessor {
    Value get(final UTF8String key);
    OptionalLong getLong(final UTF8String key);
    OptionalDouble getDouble(final UTF8String key);
    boolean getBoolean(final UTF8String key);
    UTF8String[] getStrings(final UTF8String key);
    long[] getLongs(final UTF8String key);
    double[] getDoubles(final UTF8String key);
    boolean[] getBooleans(final UTF8String key);

    static TraitAccessor forArrayData(final Object2ObjectOpenHashMap<UTF8String, DataType> fieldTypes, final ArrayData entries) {
        return new CachedArrayDataAccessor(fieldTypes, entries);
    }

    static TraitAccessor forParquetFiles(final Object2ObjectOpenHashMap<UTF8String, DataType> fieldTypes, final FileSystem fs, final String path) {
        return null;
    }
}
