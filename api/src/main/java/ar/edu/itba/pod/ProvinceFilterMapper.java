package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.io.Serializable;

public class ProvinceFilterMapper implements Mapper<Province, InhabitantRecord, String, Long>, Serializable {
    private static final Long ONE = 1L;

    @Override
    public void map(Province key, InhabitantRecord record, Context<String, Long> context){
        context.emit(record.getDepartmentName(), 1L);
    }
}
