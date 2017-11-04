package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

/**
 * El RegionInhabitantsReducer simplemente cuenta las ocurrencias InhabitantRecord para cierta Region en particular.
 *
 * @Author nicolas marcantonio
 */
public class RegionInhabitantsReducerFactory implements ReducerFactory<Region, InhabitantRecord, Long> {

    @Override
    public Reducer<InhabitantRecord, Long> newReducer(Region s) {
        return new RegionInhabitantsReducer();
    }

    private class RegionInhabitantsReducer extends Reducer<InhabitantRecord, Long> {

        private Long inhabitantsPerRegion = 0L;

        @Override
        public void reduce(InhabitantRecord value) {
            inhabitantsPerRegion++;
        }

        @Override
        public Long finalizeReduce() {
            return inhabitantsPerRegion;
        }
    }
}
