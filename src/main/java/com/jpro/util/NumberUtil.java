package com.jpro.util;

public class NumberUtil {
    public static byte[] long2Byte(long v) {
        byte[] buf = new byte[8];
        for (int i = 0; i < 8; i++) {
            int offset = 64 - (i + 1) * 8;
            buf[i] = (byte) ((v >> offset) & 0xff);
        }
        return buf;
    }

    public static long byte2Long(byte[] b) {
        long values = 0;
        for (int i = 0; i < 8; i++) {
            values <<= 8; values|= (b[i] & 0xff);
        }
        return values;
    }

    public static String long2hex(long n) {
        return long2hex(n, false);
    }

    public static String long2hex(long n, boolean uppercase) {
        StringBuilder s = new StringBuilder();
        String r;
        char []a = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
        char []b = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
        while(n != 0){
            if (uppercase)
                s.append(a[(int) Math.floorMod(n, 16L)]);
            else
                s.append(b[(int) Math.floorMod(n, 16L)]);
            n = n / 16;
        }
        r = s.reverse().toString();
        return r;
    }
}
