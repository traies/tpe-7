package ar.edu.itba.pod.model;

import java.io.Serializable;

public class ProvincePair implements Serializable{

    

    private Province p1,p2;

    public ProvincePair(Province p1,Province p2){
        if(p1.equals(p2)){
            throw new IllegalArgumentException("Cannot build pair of the same province");
        }
        this.p1 = p1.getName().compareTo(p2.getName()) < 0 ? p1:p2;
        this.p2 = p1.getName().compareTo(p2.getName()) > 0 ? p1:p2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProvincePair that = (ProvincePair) o;

        if (p1 != that.p1) return false;
        return p2 == that.p2;
    }

    @Override
    public int hashCode() {
        int result = p1 != null ? p1.hashCode() : 0;
        result = 31 * result + (p2 != null ? p2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return p1.getName() + " + " + p2.getName();
    }
}
