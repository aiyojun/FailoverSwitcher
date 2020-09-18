package com.jpro.failover.impl;

public class Pair<T1, T2> {
    private T1 key;
    private T2 value;

    public Pair(T1 key, T2 value) {
        this.key = key;
        this.value = value;
    }

    public void setKey(T1 k) {
        this.key = k;
    }

    public void setValue(T2 v) {
        this.value = v;
    }

    public T1 getKey() {
        return key;
    }

    public T2 getValue() {
        return value;
    }
}
