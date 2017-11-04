package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * El HouseholdRatioReducer almacena los hogarId en un set para despues no contar repetidos, y al mismo tiempo
 * va contando la cantidad de InhabitantRecords. Finalmente, retorna el ratio como la division entre la cantidad
 * de habitantes total sobre la cantidad de hogares que hay, y asi obtiene el promedio de personas por hogar.
 *
 * @Author tomas raies
 */
public class HouseholdRatioReducerFactory implements ReducerFactory<Region, InhabitantRecord, Double> {
    @Override
    public Reducer<InhabitantRecord, Double> newReducer(Region s) {
        return new HouseholdRatioReducerFactory.HouseholdRatioReducer();
    }

    private class HouseholdRatioReducer extends Reducer<InhabitantRecord, Double> {

        private Set<Integer> householdids = new HashSet<>();
        private Integer inhabitants = 0;

        @Override
        public void reduce(InhabitantRecord value) {
            householdids.add(value.getHomeId());
            inhabitants++;
        }

        @Override
        public Double finalizeReduce() {
            if (householdids.size() == 0) {
                throw new RuntimeException("HouseholdReducer finished without reducing any value");
            }
            return (double) inhabitants / householdids.size();
        }
    }
}
