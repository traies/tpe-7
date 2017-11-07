package ar.edu.itba.pod.combiners;

import ar.edu.itba.pod.model.Province;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by traie_000 on 11/6/2017.
 */
public class DepartmentCounterCombinerFactory implements CombinerFactory<String, Province, Set<Province>> {

    @Override
    public Combiner<Province, Set<Province>> newCombiner(String department) {
        return new DepartmentCounterCombinerFactory.DepartmentCounterCombiner();
    }

    private class DepartmentCounterCombiner extends Combiner<Province, Set<Province>> {
        private Set<Province> set = new HashSet<>();

        @Override
        public void combine(Province province) {
            set.add(province);
        }

        @Override
        public Set<Province> finalizeChunk() {
            return new HashSet<>(set);
        }

        @Override
        public void reset() {
            set.clear();
        }
    }
}
