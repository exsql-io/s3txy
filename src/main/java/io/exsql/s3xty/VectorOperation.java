package io.exsql.s3xty;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;
import org.apache.spark.unsafe.types.UTF8String;

import static jdk.incubator.vector.VectorOperators.NE;

public final class VectorOperation {

    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;

    private VectorOperation() {}

    static boolean nullSafeUTF8StringEq(final UTF8String right, final UTF8String left) {
        return left != null && right != null && equal(left.getBytes(), right.getBytes());
    }

    static int nullSafeUTF8StringCompare(final UTF8String right, final UTF8String left) {
        if (left == null) return 1;
        if (right == null) return -1;
        return compare(left.getBytes(), right.getBytes());
    }

    private static boolean equal(final byte[] left, final byte[] right) {
        final int bound = BYTE_SPECIES.loopBound(left.length);
        if (left.length != right.length) {
            return false;
        }

        if (left.length < bound) {
            return equal(left, right, 0);
        }

        var i = 0;
        for (; i < bound; i += BYTE_SPECIES.length()) {
            if (!equal(left, right, i)) {
                return false;
            }
        }

        for (; i < left.length; i++) {
            if (left[i] != right[i]) {
                return false;
            }
        }

        return true;
    }

    private static boolean equal(final byte[] left, final byte[] right, final int offset) {
        var leftVec = ByteVector.fromArray(BYTE_SPECIES, left, offset);
        var rightVec = ByteVector.fromArray(BYTE_SPECIES, right, offset);
        return leftVec.eq(rightVec).allTrue();
    }

    private static int compare(final byte[] left, final byte[] right) {
        // Handle different lengths
        final int minLength = Math.min(left.length, right.length);
        final int bound = BYTE_SPECIES.loopBound(minLength);

        // Process chunks using SIMD
        int i = 0;
        for (; i < bound; i += BYTE_SPECIES.length()) {
            int result = compareChunk(left, right, i);
            if (result != 0) {
                return result;
            }
        }

        // Process remaining bytes
        for (; i < minLength; i++) {
            int result = Byte.compare(left[i], right[i]);
            if (result != 0) {
                return result;
            }
        }

        // If we've reached here, the common prefix is identical
        // Return the length difference
        return Integer.compare(left.length, right.length);
    }

    private static int compareChunk(final byte[] left, final byte[] right, final int offset) {
        var leftVec = ByteVector.fromArray(BYTE_SPECIES, left, offset);
        var rightVec = ByteVector.fromArray(BYTE_SPECIES, right, offset);

        // Check for equality first (fast path)
        if (leftVec.eq(rightVec).allTrue()) {
            return 0;
        }

        // Find the first differing byte
        VectorMask<Byte> neqMask = leftVec.compare(NE, rightVec);
        int idx = neqMask.firstTrue();

        // Compare the differing bytes
        return Byte.compare(left[offset + idx], right[offset + idx]);
    }

}