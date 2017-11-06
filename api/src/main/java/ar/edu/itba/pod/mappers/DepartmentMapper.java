package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.model.InhabitantRecord;
import ar.edu.itba.pod.model.Province;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.io.Serializable;

public class DepartmentMapper implements Mapper<Long, InhabitantRecord, String, Province>, Serializable {


    @Override
    public void map(Long p, InhabitantRecord record, Context<String, Province> context){
        context.emit(record.getDepartmentName(), record.getProvince());
    }

}
