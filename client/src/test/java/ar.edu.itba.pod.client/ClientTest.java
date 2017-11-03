package ar.edu.itba.pod.client;

import ar.edu.itba.pod.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

/**
 * Created by traie_000 on 11/3/2017.
 */
public class ClientTest {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    private ClassLoader classLoader = getClass().getClassLoader();
    private JobTracker jobTracker;
    private KeyValueSource<Province, InhabitantRecord> source;

    @Before
    public void hazelcastSetUp() {
        final ClientConfig ccfg = new ClientConfig();
        ccfg.getGroupConfig().setName("tpe-7");
        ccfg.getGroupConfig().setPassword("tpe-7");
        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(ccfg);
        MultiMap<Province, InhabitantRecord> map = hz.getMultiMap("censoPodGrupo7");

        try (Reader r = new FileReader(Optional
                .ofNullable(classLoader.getResource("census100.csv"))
                .map(URL::getFile)
                .orElseThrow(FileNotFoundException::new))) {

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
        source = KeyValueSource.fromMultiMap(map);
        jobTracker = hz.getJobTracker("test");
    }

    @Test
    public void populationPerRegion() throws ExecutionException, InterruptedException {
        Job<Province, InhabitantRecord> job = jobTracker.newJob(source);

        ICompletableFuture<Map<Region, Long>> future = job
                .mapper(new RegionMapper())
                .reducer(new RegionInhabitantsReducerFactory())
                .submit();
        Map<Region, Long> result = future.get();

        for(Map.Entry<Region,Long> entry : result.entrySet()){
            logger.debug(entry.getKey().toString()+" "+entry.getValue());
        }
    }

    @Test
    public void nDepartmentsByPopulation() throws ExecutionException, InterruptedException {
        Job<Province, InhabitantRecord> job = jobTracker.newJob(source);
        final int n = 10;
        ICompletableFuture<List<Map.Entry<String,Long>>> future = job
                .mapper(new ProvinceFilterMapper(Province.SANTA_FE))
                .reducer(new InhabitantsPerDepartmentReducerFactory())
                .submit(iterable -> {

                    PriorityQueue<Map.Entry<String,Long>> entryPQueue = new PriorityQueue<>((a, b) -> b.getValue().compareTo(a.getValue()));
                    List<Map.Entry<String,Long>> ans = new ArrayList<>();
                    iterable.forEach(entryPQueue::add);

                    IntStream.range(0, Math.min(n,entryPQueue.size())).forEach((x) -> ans.add(entryPQueue.remove()));
                    return ans;
                });
        List<Map.Entry<String,Long>> result = future.get();

        for(Map.Entry<String,Long> entry : result){
            logger.debug(String.format("%s %d", entry.getKey(), entry.getValue()));
        }
    }
}
