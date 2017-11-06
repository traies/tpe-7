package ar.edu.itba.pod.combiners;

import ar.edu.itba.pod.model.EmploymentCondition;
import ar.edu.itba.pod.model.InhabitantRecord;
import ar.edu.itba.pod.model.Pair;
import ar.edu.itba.pod.model.Region;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class EmploymentCombinerFactory implements CombinerFactory<Region, InhabitantRecord, Pair<Long, Long>> {

    @Override
    public Combiner<InhabitantRecord, Pair<Long, Long>> newCombiner(Region region) {
        return new EmploymentCombinerFactory.EmploymentCombiner();
    }

    private class EmploymentCombiner extends Combiner<InhabitantRecord, Pair<Long, Long>> {
        private Pair<Long, Long> count = new Pair<>(0L, 0L);

        @Override
        public void combine(InhabitantRecord record) {
            if (EmploymentCondition.UNEMPLOYED.equals(record.getCondition())) {
                count.setFirstValue(count.getFirstValue() + 1);
            } else if (EmploymentCondition.EMPLOYED.equals(record.getCondition())) {
                count.setSecondValue(count.getSecondValue() + 1);
            }
        }

        @Override
        public Pair<Long, Long> finalizeChunk() {
            return count;
        }

        @Override
        public void reset() {
            count = new Pair<>(0L, 0L);
        }
    }
}
