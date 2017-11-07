package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.model.Province;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class DepartmentCounterReducerFactory implements ReducerFactory<String, Set<Province>, Integer> {

    @Override
    public Reducer<Set<Province>, Integer> newReducer(String s) {
        return new DepartmentCounterReducer();
    }

    private class DepartmentCounterReducer extends Reducer<Set<Province>, Integer> {

        Set<Province> s = new HashSet<>();

        @Override
        public void reduce(Set<Province> value) {
            s.addAll(value);
        }

        @Override
        public Integer finalizeReduce() {
            return s.size();
        }
    }
}
