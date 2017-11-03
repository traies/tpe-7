package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

/**
 * Created by traie_000 on 11/3/2017.
 */
public class EmploymentReducerFactory implements ReducerFactory<Region, InhabitantRecord, Double> {
    @Override
    public Reducer<InhabitantRecord, Double> newReducer(Region s) {
        return new EmploymentReducerFactory.EmploymentReducer();
    }

    private class EmploymentReducer extends Reducer<InhabitantRecord, Double> {

        private Long employedPerRegion = 0L;
        private Long unemployedPerRegion = 0L;

        @Override
        public void reduce(InhabitantRecord value) {
            if (EmploymentCondition.EMPLOYED.equals(value.getCondition())) {
                employedPerRegion++;
            } else if (EmploymentCondition.UNEMPLOYED.equals(value.getCondition())) {
                unemployedPerRegion++;
            }
        }

        @Override
        public Double finalizeReduce() {
            return unemployedPerRegion.doubleValue() / (unemployedPerRegion.doubleValue() + employedPerRegion.doubleValue());
        }
    }

}
