package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class DepartmentCounterReducerFactory implements ReducerFactory<String, Province , Long> {

    @Override
    public Reducer<Province, Long> newReducer(String s) {
        return new EchoReducer();
    }

    private class EchoReducer extends Reducer<Province, Long> {

        Set<Province> s = new HashSet<>();

        private Long appearances = 0L;

        @Override
        public void reduce(Province value) {
            if(!s.contains(value)) {
                s.add(value);
                appearances++;
            }
        }

        @Override
        public Long finalizeReduce() {
            return appearances;
        }
    }
}
