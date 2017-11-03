package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import org.omg.PortableInterceptor.INACTIVE;

import java.util.ArrayList;
import java.util.List;

public class RegionInhabitantsReducerFactory implements ReducerFactory<Region, InhabitantRecord, Long> {

    @Override
    public Reducer<InhabitantRecord, Long> newReducer(Region s) {
        return new EchoReducer();
    }

    private class EchoReducer extends Reducer<InhabitantRecord, Long> {

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
