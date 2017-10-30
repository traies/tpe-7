package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class DepartmentCounterFactory implements ReducerFactory<String, Integer , Long> {

    @Override
    public Reducer<Integer, Long> newReducer(String s) {
        return new EchoReducer();
    }

    private class EchoReducer extends Reducer<Integer, Long> {

        private Long departments = 0L;

        @Override
        public void reduce(Integer value) {
//            departments += value;
            departments++;
        }

        @Override
        public Long finalizeReduce() {
            return departments;
        }
    }
}
