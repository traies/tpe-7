package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;


/**
 * El InhabitantPerDepartmentReducer simplemente cuenta las ocurrencias de InhabitantRecord para un nombre de
 * departamento en particular, asi sacamos su poblacion.
 *
 * @Author nicolas marcantonio
 */
public class InhabitantsPerDepartmentReducerFactory implements ReducerFactory<String, Long , Long >{

    @Override
    public Reducer<Long, Long> newReducer(String dep) {
        return new InhabitantPerDepartmentReducer();
    }


    private class InhabitantPerDepartmentReducer extends Reducer<Long, Long> {

        private Long count = 0L;

        @Override
        public void reduce(Long c) {
            count+=c;
        }

        @Override
        public Long finalizeReduce() {
            return count;
        }
    }
}
