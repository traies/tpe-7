package ar.edu.itba.pod.combiners;

import ar.edu.itba.pod.model.InhabitantRecord;
import ar.edu.itba.pod.model.Pair;
import ar.edu.itba.pod.model.Region;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.HashSet;
import java.util.Set;

public class HouseholdCombinerFactory implements CombinerFactory<Region, InhabitantRecord, Pair<Set<Integer>, Integer>> {
    @Override
    public Combiner<InhabitantRecord, Pair<Set<Integer>, Integer>> newCombiner(Region s) {
        return new HouseholdCombinerFactory.HouseholdCombiner();
    }

    private class HouseholdCombiner extends Combiner<InhabitantRecord, Pair<Set<Integer>, Integer>> {
        private Set<Integer> set = new HashSet<>();
        private Integer count = 0;

        @Override
        public void combine(InhabitantRecord record) {
            set.add(record.getHomeId());
            count++;
        }

        @Override
        public Pair<Set<Integer>,Integer> finalizeChunk() {
            return new Pair<>(new HashSet<>(set), count);
        }

        @Override
        public void reset() {
            set.clear();
            count = 0;
        }
    }
}
