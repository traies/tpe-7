package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.io.Serializable;

/**
 * RegionMapper Recibe los InhabitantRecords mapeados por Province, y los mapea a si mismos, pero con su region como
 * clave. Para ver las regiones de las provincias, ver el enum Province
 *
 * @See Province
 * @Author nicolas marcantonio
 */
public class RegionMapper implements Mapper<Long, InhabitantRecord, Region, InhabitantRecord>, Serializable {
    private static final Long ONE = 1L;

    @Override
    public void map(Long id, InhabitantRecord record, Context<Region, InhabitantRecord> context){
        context.emit(record.getProvince().getRegion(),record);
    }
}
