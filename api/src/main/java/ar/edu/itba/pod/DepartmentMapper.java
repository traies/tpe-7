package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.io.Serializable;

public class DepartmentMapper implements Mapper<String, InhabitantRecord, String, Integer>, Serializable {
    private static final Long ONE = 1L;

    @Override
    public void map(String key, InhabitantRecord record, Context<String, Integer> context){
        context.emit(record.getDepartmentName(),1);
    }
}
