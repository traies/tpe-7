package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashMap;

public class InhabitantsPerDepartmentReducerFactory implements ReducerFactory<String, Long , Long >{

    @Override
    public Reducer<Long, Long> newReducer(String dep) {
        return new InhabitantPerDepartmentReducer();
    }


    private class InhabitantPerDepartmentReducer extends Reducer<Long, Long> {

        private Long count = 0L;

        @Override
        public void reduce(Long c) {
            count++;
        }

        @Override
        public Long finalizeReduce() {
            return count;
        }
    }
}
