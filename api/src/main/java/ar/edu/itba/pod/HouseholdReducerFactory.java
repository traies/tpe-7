package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * El HouseholdReducer simplemente almacena los hogarId en un hash set. Para obtener la cantidad de hogarId diferentes,
 * simplemente tomo el tamanio del set al finalizar.
 *
 * @Author tomas raies
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
