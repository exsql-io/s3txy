package io.exsql.s3xty;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Iterator;
import java.util.OptionalDouble;
import java.util.OptionalLong;

public class CachedValueBag {

    private final Object2ObjectOpenHashMap<UTF8String, Value> cache;

    private final ArrayDataIterator iterator;

    public CachedValueBag(final ArrayData entries) {
        this.cache = new Object2ObjectOpenHashMap<>();
        this.iterator = new ArrayDataIterator(entries);
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
        if (value != null) {
            return OptionalLong.of(value.toLong());
        }

        return OptionalLong.empty();
    }

    public OptionalDouble getDouble(final UTF8String key) {
        var value = this.get(key);
        if (value != null) {
            return OptionalDouble.of(value.toDouble());
        }

        return OptionalDouble.empty();
    }

    public boolean getBoolean(final UTF8String key) {
        var value = this.get(key);
        if (value != null) {
            return value.toBoolean();
        }

        return false;
    }

    private record ArrayDataEntry(UTF8String key, StringValue value) {}
    private static final class ArrayDataIterator implements Iterator<ArrayDataEntry> {
        private final ArrayData arrayData;
        private final int length;
        private int index = 0;

        public ArrayDataIterator(final ArrayData arrayData) {
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
            return new ArrayDataEntry(entry.getUTF8String(0), Value.stringValue(entry.getUTF8String(1)));
        }
    }

}
