package io.exsql.s3xty;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;
import org.apache.spark.unsafe.types.UTF8String;

public final class VectorOperation {

    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;

    private VectorOperation() {}

    static boolean nullSafeUTF8StringEq(final UTF8String right, final UTF8String left) {
        return left != null && right != null && equal(left.getBytes(), right.getBytes());
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

}
