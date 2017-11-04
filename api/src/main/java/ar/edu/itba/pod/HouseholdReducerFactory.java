package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by traie_000 on 11/3/2017.
 */
public class HouseholdReducerFactory implements ReducerFactory<Region, InhabitantRecord, Integer> {
    @Override
    public Reducer<InhabitantRecord, Integer> newReducer(Region s) {
        return new HouseholdReducerFactory.HouseholdReducer();
    }

    private class HouseholdReducer extends Reducer<InhabitantRecord, Integer> {

        private Set<Integer> householdids = new HashSet<>();

        @Override
        public void reduce(InhabitantRecord value) {
            householdids.add(value.getHomeId());
        }

        @Override
        public Integer finalizeReduce() {
            return householdids.size();
        }
    }
}
