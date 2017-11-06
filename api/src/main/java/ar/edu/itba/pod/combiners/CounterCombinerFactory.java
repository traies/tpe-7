package ar.edu.itba.pod.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class CounterCombinerFactory<T, V> implements CombinerFactory<T, V, Long> {

    @Override
    public Combiner<V, Long> newCombiner(T t) {
        return new InhabitantsCounterCombiner();
    }

    private class InhabitantsCounterCombiner extends Combiner<V, Long> {
        private Long count = 0L;

        @Override
        public void combine(V record) {
            count++;
        }

        @Override
        public Long finalizeChunk() {
            return count;
        }

        @Override
        public void reset() {
            count = 0L;
        }
    }

}
