package com.jpro.failover.implv2;

import java.util.List;

public class Fragment {
    private int volume;
    private List<Long> parts;
//    private int prt1def;
//    private int prt2def;
//    private int prt3def;
//    private int prt4def;

    public Fragment(int size) {
        if (size % 8 != 0) {
            throw new RuntimeException("[Incompatible] Fragment (size % 8 != 0).");
        }
        volume = size;
        allocate();
    }

    public void allocate() {
//        prt1def = 1;
//        prt2def = volume / 8;
//        prt3def = volume * 16;
    }

//    public int getPart2Offset() {
//        return prt1def;
//    }
//
//    public int getPart3Offset() {
//        return prt1def + prt2def;
//    }
}
