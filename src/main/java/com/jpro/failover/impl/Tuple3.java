package com.jpro.failover.impl;

public class Tuple3<T1, T2, T3> {
    private T1 v1;
    private T2 v2;
    private T3 v3;

    public Tuple3(T1 v1, T2 v2, T3 v3) {
        this.v1 = v1;
        this.v2 = v2;
        this.v3 = v3;
    }

    public T1 _1() {
        return v1;
    }

    public T2 _2() {
        return v2;
    }

    public T3 _3() {
        return v3;
    }
}
