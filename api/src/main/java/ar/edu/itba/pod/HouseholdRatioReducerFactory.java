package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by traie_000 on 11/4/2017.
 */
public class HouseholdRatioReducerFactory implements ReducerFactory<Region, InhabitantRecord, Double> {
    @Override
    public Reducer<InhabitantRecord, Double> newReducer(Region s) {
        return new HouseholdRatioReducerFactory.HouseholdReducer();
    }

    private class HouseholdReducer extends Reducer<InhabitantRecord, Double> {

        private Set<Integer> householdids = new HashSet<>();
        private Integer inhabitants = 0;

        @Override
        public void reduce(InhabitantRecord value) {
            householdids.add(value.getHomeId());
            inhabitants++;
        }

        @Override
        public Double finalizeReduce() {
            return (double) inhabitants / householdids.size();
        }
    }
}
