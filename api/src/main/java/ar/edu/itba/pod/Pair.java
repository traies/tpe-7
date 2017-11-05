package ar.edu.itba.pod;

import java.io.Serializable;

public class Pair<K, V> implements Serializable{
    private K k;
    private V v;

    public Pair(K k, V v) {
        this.k = k;
        this.v = v;
    }

    public K getFirstValue() {
        return k;
    }

    public V getSecondValue() {
        return v;
    }

    public void setFirstValue(K k) {
        this.k = k;
    }

    public void setSecondValue(V v) {
        this.v = v;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair<?, ?> pair = (Pair<?, ?>) o;

        if (k != null ? !k.equals(pair.k) : pair.k != null) return false;
        return v != null ? v.equals(pair.v) : pair.v == null;
    }

    @Override
    public int hashCode() {
        int result = k != null ? k.hashCode() : 0;
        result = 31 * result + (v != null ? v.hashCode() : 0);
        return result;
    }
}
