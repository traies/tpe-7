package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import org.omg.PortableInterceptor.INACTIVE;

import java.util.ArrayList;
import java.util.List;

public class RegionInhabitantsReducerFactory implements ReducerFactory<Region, Integer , Long> {

    @Override
    public Reducer<Integer, Long> newReducer(Region s) {
        return new EchoReducer();
    }

    private class EchoReducer extends Reducer<Integer, Long> {

        private Long inhabitantsPerRegion = 0L;

        @Override
        public void reduce(Integer value) {
            inhabitantsPerRegion++;
        }

        @Override
        public Long finalizeReduce() {
            return inhabitantsPerRegion;
        }
    }
}
