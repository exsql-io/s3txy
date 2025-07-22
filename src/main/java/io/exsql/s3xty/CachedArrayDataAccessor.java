package io.exsql.s3xty;

import io.exsql.s3xty.value.Value;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Iterator;
import java.util.OptionalDouble;
import java.util.OptionalLong;

public class CachedArrayDataAccessor {

    private static final DataType STRING_ARRAY_TYPE = DataTypes.createArrayType(DataTypes.StringType);
    private static final DataType LONG_ARRAY_TYPE = DataTypes.createArrayType(DataTypes.LongType);
    private static final DataType DOUBLE_ARRAY_TYPE = DataTypes.createArrayType(DataTypes.DoubleType);
    private static final DataType BOOLEAN_ARRAY_TYPE = DataTypes.createArrayType(DataTypes.BooleanType);
    private static final UTF8String DEFAULT_ARRAY_VALUE_DELIMITER = UTF8String.fromString(",");

    private final Object2ObjectOpenHashMap<UTF8String, Value> cache;

    private final ArrayDataIterator iterator;

    public CachedArrayDataAccessor(final Object2ObjectOpenHashMap<UTF8String, DataType> fieldTypes, final ArrayData entries) {
        this.cache = new Object2ObjectOpenHashMap<>();
        this.iterator = new ArrayDataIterator(fieldTypes, entries);
    }

    public Value get(final UTF8String key) {
        if (!this.iterator.hasNext() || this.cache.containsKey(key)) {
            return this.cache.get(key);
        }

        while (this.iterator.hasNext()) {
            var entry = this.iterator.next();
            if (entry.key.equals(key)) {
                this.cache.put(entry.key, entry.value);
                return entry.value;
            }
        }

        return null;
    }

    public OptionalLong getLong(final UTF8String key) {
        var value = this.get(key);
        if (value != null && !value.isNull()) {
            return OptionalLong.of(value.toLong());
        }

        return OptionalLong.empty();
    }

    public OptionalDouble getDouble(final UTF8String key) {
        var value = this.get(key);
        if (value != null && !value.isNull()) {
            return OptionalDouble.of(value.toDouble());
        }

        return OptionalDouble.empty();
    }

    public boolean getBoolean(final UTF8String key) {
        var value = this.get(key);
        if (value != null && !value.isNull()) {
            return value.toBoolean();
        }

        return false;
    }

    private record ArrayDataEntry(UTF8String key, Value value) {}
    private static final class ArrayDataIterator implements Iterator<ArrayDataEntry> {
        private final Object2ObjectOpenHashMap<UTF8String, DataType> fieldTypes;
        private final ArrayData arrayData;
        private final int length;
        private int index = 0;

        public ArrayDataIterator(final Object2ObjectOpenHashMap<UTF8String, DataType> fieldTypes, final ArrayData arrayData) {
            this.fieldTypes = fieldTypes;
            this.arrayData = arrayData;
            this.length = arrayData.numElements();
        }

        @Override
        public boolean hasNext() {
            return this.index < this.length;
        }

        @Override
        public ArrayDataEntry next() {
            var entry = this.arrayData.getStruct(this.index++, 2);
            var key = entry.getUTF8String(0);
            var value = entry.getUTF8String(1);

            var valueType = this.fieldTypes.getOrDefault(key, DataTypes.StringType);
            Value v = Value.nullValue();
            if (value != null) {
                if (valueType == DataTypes.BooleanType) {
                    v = Value.booleanValue(value.toString());
                } else if (valueType == DataTypes.LongType) {
                    v = Value.longValue(value.toLongExact());
                } else if (valueType == DataTypes.DoubleType) {
                    v = Value.doubleValue(Double.parseDouble(value.toString()));
                } else if (valueType.sameType(STRING_ARRAY_TYPE)) {
                    v = Value.stringArrayValue(DEFAULT_ARRAY_VALUE_DELIMITER, value);
                } else if (valueType.sameType(LONG_ARRAY_TYPE)) {
                    v = Value.longArrayValue(DEFAULT_ARRAY_VALUE_DELIMITER, value);
                } else if (valueType.sameType(DOUBLE_ARRAY_TYPE)) {
                    v = Value.doubleArrayValue(DEFAULT_ARRAY_VALUE_DELIMITER, value);
                } else if (valueType.sameType(BOOLEAN_ARRAY_TYPE)) {
                    v = Value.booleanArrayValue(DEFAULT_ARRAY_VALUE_DELIMITER, value);
                } else {
                    v = Value.stringValue(value);
                }
            }

            return new ArrayDataEntry(entry.getUTF8String(0), v);
        }
    }

}
