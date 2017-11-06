package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class DepartmentMapper implements Mapper<Long, InhabitantRecord, String, Province>, Serializable {


    @Override
    public void map(Long p, InhabitantRecord record, Context<String, Province> context){
        context.emit(record.getDepartmentName(), record.getProvince());
    }

}
