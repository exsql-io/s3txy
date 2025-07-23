package io.exsql.s3xty;

import io.exsql.s3xty.value.*;
import org.apache.spark.unsafe.types.UTF8String;

public final class Operation {
    private Operation() {}

    public static boolean nullSafeLongEq(final Value right, final Value left) {
        return (left instanceof LongValue) &&
               (right instanceof LongValue) &&
               ((LongValue) left).wrapped() == ((LongValue) right).wrapped();
    }

    public static boolean nullSafeDoubleEq(final Value right, final Value left) {
        return (left instanceof DoubleValue) &&
               (right instanceof DoubleValue) &&
               ((DoubleValue) left).wrapped() == ((DoubleValue) right).wrapped();
    }

    public static boolean nullSafeBooleanEq(final Value right, final Value left) {
        return (left instanceof BooleanValue) &&
               (right instanceof BooleanValue) &&
               ((BooleanValue) left).wrapped() == ((BooleanValue) right).wrapped();
    }

    public static boolean nullSafeStringEq(final Value right, final Value left, final boolean useVectorAPI) {
        if (!(left instanceof StringValue)) return false;
        if (!(right instanceof StringValue)) return false;
        return useVectorAPI ?
                VectorOperation.nullSafeUTF8StringEq(((StringValue) right).wrapped(), ((StringValue) left).wrapped()):
                nullSafeUTF8StringEq(((StringValue) right).wrapped(), ((StringValue) left).wrapped());
    }

    public static boolean nullSafeStringCiEq(final Value right, final Value left, final boolean useVectorAPI) {
        if (!(left instanceof StringValue)) return false;
        if (!(right instanceof StringValue)) return false;

        var l = ((StringValue) left).lowercase() ? ((StringValue) left).wrapped(): ((StringValue) left).wrapped().toLowerCase();
        var r = ((StringValue) right).lowercase() ? ((StringValue) right).wrapped(): ((StringValue) right).wrapped().toLowerCase();
        return useVectorAPI ? VectorOperation.nullSafeUTF8StringEq(r, l): nullSafeUTF8StringEq(r, l);
    }

    private static boolean nullSafeUTF8StringEq(final UTF8String right, final UTF8String left) {
        return left != null && left.equals(right);
    }

    public static boolean nullSafeLongLt(final Value right, final Value left) {
        return (left instanceof LongValue) &&
               (right instanceof LongValue) &&
               ((LongValue) left).wrapped() < ((LongValue) right).wrapped();
    }

    public static boolean nullSafeDoubleLt(final Value right, final Value left) {
        return (left instanceof DoubleValue) &&
               (right instanceof DoubleValue) &&
               ((DoubleValue) left).wrapped() < ((DoubleValue) right).wrapped();
    }

    public static boolean nullSafeStringLt(final Value right, final Value left, final boolean useVectorAPI) {
        if (!(left instanceof StringValue)) return false;
        if (!(right instanceof StringValue)) return false;
        return useVectorAPI ?
                VectorOperation.nullSafeUTF8StringCompare(((StringValue) right).wrapped(), ((StringValue) left).wrapped()) < 0 :
                nullSafeUTF8StringCompare(((StringValue) right).wrapped(), ((StringValue) left).wrapped()) < 0;
    }

    public static boolean nullSafeLongLe(final Value right, final Value left) {
        return (left instanceof LongValue) &&
               (right instanceof LongValue) &&
               ((LongValue) left).wrapped() <= ((LongValue) right).wrapped();
    }

    public static boolean nullSafeDoubleLe(final Value right, final Value left) {
        return (left instanceof DoubleValue) &&
                (right instanceof DoubleValue) &&
                ((DoubleValue) left).wrapped() <= ((DoubleValue) right).wrapped();
    }

    public static boolean nullSafeStringLe(final Value right, final Value left, final boolean useVectorAPI) {
        if (!(left instanceof StringValue)) return false;
        if (!(right instanceof StringValue)) return false;
        return useVectorAPI ?
                VectorOperation.nullSafeUTF8StringCompare(((StringValue) right).wrapped(), ((StringValue) left).wrapped()) <= 0 :
                nullSafeUTF8StringCompare(((StringValue) right).wrapped(), ((StringValue) left).wrapped()) <= 0;
    }

    public static boolean nullSafeLongGt(final Value right, final Value left) {
        return (left instanceof LongValue) &&
               (right instanceof LongValue) &&
               ((LongValue) left).wrapped() > ((LongValue) right).wrapped();
    }

    public static boolean nullSafeDoubleGt(final Value right, final Value left) {
        return (left instanceof DoubleValue) &&
               (right instanceof DoubleValue) &&
               ((DoubleValue) left).wrapped() > ((DoubleValue) right).wrapped();
    }

    public static boolean nullSafeStringGt(final Value right, final Value left, final boolean useVectorAPI) {
        if (!(left instanceof StringValue)) return false;
        if (!(right instanceof StringValue)) return false;
        return useVectorAPI ?
                VectorOperation.nullSafeUTF8StringCompare(((StringValue) right).wrapped(), ((StringValue) left).wrapped()) > 0 :
                nullSafeUTF8StringCompare(((StringValue) right).wrapped(), ((StringValue) left).wrapped()) > 0;
    }

    public static boolean nullSafeLongGe(final Value right, final Value left) {
        return (left instanceof LongValue) &&
               (right instanceof LongValue) &&
               ((LongValue) left).wrapped() >= ((LongValue) right).wrapped();
    }

    public static boolean nullSafeDoubleGe(final Value right, final Value left) {
        return (left instanceof DoubleValue) &&
               (right instanceof DoubleValue) &&
               ((DoubleValue) left).wrapped() >= ((DoubleValue) right).wrapped();
    }

    public static boolean nullSafeStringGe(final Value right, final Value left, final boolean useVectorAPI) {
        if (!(left instanceof StringValue)) return false;
        if (!(right instanceof StringValue)) return false;
        return useVectorAPI ?
                VectorOperation.nullSafeUTF8StringCompare(((StringValue) right).wrapped(), ((StringValue) left).wrapped()) >= 0 :
                nullSafeUTF8StringCompare(((StringValue) right).wrapped(), ((StringValue) left).wrapped()) >= 0;
    }

    public static boolean isNotNull(final Value value) {
        return value != Value.NULL_VALUE;
    }

    public static boolean stringRegexMatch(final Value right, final Value left) {
        if (right instanceof RegexpValue) return ((RegexpValue) right).matches(left);
        return ((RegexpValue) left).matches(right);
    }

    private static int nullSafeUTF8StringCompare(final UTF8String right, final UTF8String left) {
        if (right == null) return -1;
        if (left == null) return 1;
        return left.compare(right);
    }

}
