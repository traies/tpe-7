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
public class HouseholdRatioReducerFactory implements ReducerFactory<Region, Pair<Set<Integer>, Integer>, Double> {
    @Override
    public Reducer<Pair<Set<Integer>, Integer>, Double> newReducer(Region s) {
        return new HouseholdRatioReducerFactory.HouseholdRatioReducer();
    }

    private class HouseholdRatioReducer extends Reducer<Pair<Set<Integer>, Integer>, Double> {

        private Set<Integer> householdids = new HashSet<>();
        private Integer inhabitants = 0;

        @Override
        public void reduce(Pair<Set<Integer>, Integer> value) {
            householdids.addAll(value.getFirstValue());
            inhabitants += value.getSecondValue();
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
