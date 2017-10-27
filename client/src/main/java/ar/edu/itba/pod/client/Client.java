package ar.edu.itba.pod.client;

import ar.edu.itba.pod.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws FileNotFoundException, ExecutionException, InterruptedException {
        logger.info("tpe-7 Client Starting ...");

        final ClientConfig ccfg = new ClientConfig();
        ccfg.getGroupConfig().setName("tpe-7");
        ccfg.getGroupConfig().setPassword("tpe-7");
        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(ccfg);


       JobTracker jobTracker = hz.getJobTracker("echo");



        final IList<InhabitantRecord> list = hz.getList("censoPodGrupo7");

       final KeyValueSource<String, InhabitantRecord> source = KeyValueSource.fromList(list);
       // list.forEach((x) -> logger.debug(x.toString()));

        Job<String, InhabitantRecord> job = jobTracker.newJob(source);
//
        ICompletableFuture<Map<Region, Long>> future = job
                .mapper(new EchoMapper())
                .reducer(new EchoReducerFactory())
                .submit();
       Map<Region, Long> result = future.get();

       for(Map.Entry<Region,Long> entry : result.entrySet()){
           logger.debug(entry.getKey().toString()+" "+entry.getValue());
       }
//
//        logger.info(result.toString());
    }
}
