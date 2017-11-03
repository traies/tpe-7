package ar.edu.itba.pod.client;

import ar.edu.itba.pod.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
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

    private Query query;

    @Before
    public void hazelcastSetUp() throws FileNotFoundException {
        ClassLoader classLoader = getClass().getClassLoader();
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        Job<Province, InhabitantRecord> job = Client.hazelcastSetUp(
                new String[] {"127.0.0.1"},
                Optional
                    .ofNullable(classLoader.getResource("census100.csv"))
                    .map(URL::getFile)
                    .orElseThrow(FileNotFoundException::new)
        );
        this.query = new Query(job);
    }

    @Test
    public void populationPerRegion() throws ExecutionException, InterruptedException {
        Map<Region, Long> result = query.populationPerRegion();

        for(Map.Entry<Region,Long> entry : result.entrySet()){
            logger.debug(entry.getKey().toString()+" "+entry.getValue());
        }
    }

    @Test
    public void nDepartmentsByPopulation() throws ExecutionException, InterruptedException {
        List<Map.Entry<String,Long>> result = query.nDepartmentsByPopulation(10);

        for(Map.Entry<String,Long> entry : result){
            logger.debug(String.format("%s %d", entry.getKey(), entry.getValue()));
        }
    }
}
