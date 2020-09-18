package com.jpro.failover.impl;

public class BytesUtil {
    public static byte[] getBytes(byte[] buf, int start, int len) {
        if (start < 0 || start >= buf.length || (start + len) > buf.length) {
            throw new RuntimeException(String.format(
                "get bytes args error, buf length: %d, start: %d, length: %d",
                buf.length, start, len));
        }
        byte[] res = new byte[len];
        System.arraycopy(buf, start, res, 0, len);
        return res;
    }

    public static void putBytes(byte[] dst, int start, byte[] src) {
        putBytes(dst, start, src, 0, src.length);
    }

    public static void putBytes(byte[] dst, int start0, byte[] src, int start1, int len) {
        if (start0 < 0 || start0 >= dst.length
            || start1 < 0 || start1 >= src.length
            || (start0 + len) > dst.length
            || (start1 + len) > src.length) {
            throw new RuntimeException(String.format(
                "put bytes args error, dst len: %d, start: %d, src len: %d, start: %d, len: %d",
                dst.length, start0, src.length, start1, len));
        }
        if (len >= 0) System.arraycopy(src, start1, dst, start0, len);
    }
}
