package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.io.Serializable;
import java.util.StringTokenizer;

public class RegionMapper implements Mapper<Province, InhabitantRecord, Region, Integer>, Serializable {
    private static final Long ONE = 1L;

    @Override
    public void map(Province province, InhabitantRecord record, Context<Region, Integer> context){
        context.emit(province.getRegion(),1);
    }
}
