package com.jpro;

import org.junit.Test;
import org.xerial.snappy.Snappy;

import java.io.IOException;

public class SnappyTest {
    @Test
    public void run() {
        tryIt("4e1b");
        tryIt("4e1c");
        tryIt("4e1d");
    }

    public static void tryIt(String data) {
        byte[] buf = new byte[0];
        try {
            buf = Snappy.compress(data.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(new String(buf));
        System.out.println("-------------------");
        try {
            System.out.println(new String(Snappy.uncompress(buf)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
