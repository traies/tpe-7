package ar.edu.itba.pod.client;

import ar.edu.itba.pod.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws FileNotFoundException, ExecutionException, InterruptedException {
        logger.info("tpe-7 Client Starting ...");

        final ClientConfig ccfg = new ClientConfig();
        ccfg.getGroupConfig().setName("tpe-7");
        ccfg.getGroupConfig().setPassword("tpe-7");
        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(ccfg);

        MultiMap<Province, InhabitantRecord> map = hz.getMultiMap("censoPodGrupo7");
        try (Reader r = new FileReader("census100.csv")) {
            CSVFormat format = CSVFormat.RFC4180.withHeader(RecordEnum.class);
            for (CSVRecord record : format.parse(r)) {
                EmploymentCondition condition = EmploymentCondition.getCondition(
                        Integer.valueOf(record.get(RecordEnum.EMPLOYMENT_CONDITION))
                );
                Integer homeId = Integer.valueOf(record.get(RecordEnum.HOMEID));
                String departmentName = record.get(RecordEnum.DEPARTMENT_NAME);
                Province province = Province.getProvince(record.get(RecordEnum.PROVINCE_NAME));
                map.put(province, new InhabitantRecord(condition, homeId, departmentName, province));
            }
        } catch (IOException e) {
            logger.error("ERROR", e);
        }
    }
}
