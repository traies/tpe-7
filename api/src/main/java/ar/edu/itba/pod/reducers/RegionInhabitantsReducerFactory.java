package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.model.Region;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

/**
 * El RegionInhabitantsReducer simplemente cuenta las ocurrencias InhabitantRecord para cierta Region en particular.
 *
 * @Author nicolas marcantonio
 */
public class RegionInhabitantsReducerFactory implements ReducerFactory<Region, Long, Long> {

    @Override
    public Reducer<Long, Long> newReducer(Region s) {
        return new RegionInhabitantsReducer();
    }

    private class RegionInhabitantsReducer extends Reducer<Long, Long> {

        private Long inhabitantsPerRegion = 0L;

        @Override
        public void reduce(Long value) {
            inhabitantsPerRegion += value;
        }

        @Override
        public Long finalizeReduce() {
            return inhabitantsPerRegion;
        }
    }
}
