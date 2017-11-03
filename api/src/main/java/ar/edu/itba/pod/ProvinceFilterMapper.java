package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.io.Serializable;

public class ProvinceFilterMapper implements Mapper<Province, InhabitantRecord, String, Long>, Serializable {
    private static final Long ONE = 1L;
    private Province province ;
    public ProvinceFilterMapper(Province province) {
        this.province = province;
    }

    @Override
    public void map(Province key, InhabitantRecord record, Context<String, Long> context){
        if (province.equals(key)) {
            context.emit(record.getDepartmentName(), 1L);
        }
    }
}
