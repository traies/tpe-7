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
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

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
                .mapper(new RegionMapper())
                .reducer(new RegionInhabitantsReducerFactory())
                .submit();
       Map<Region, Long> result = future.get();

       for(Map.Entry<Region,Long> entry : result.entrySet()){
           logger.debug(entry.getKey().toString()+" "+entry.getValue());
       }

        Job<String, InhabitantRecord> job2 = jobTracker.newJob(source);
         final int n = 10;
        ICompletableFuture<List<Map.Entry<String,Long>>> future2 = job2
                .mapper(new ProvinceFilterMapper(Province.SANTA_FE))
                .reducer(new InhabitantsPerDepartmentReducerFactory())
                .submit(iterable -> {

                    PriorityQueue<Map.Entry<String,Long>> entryPQueue = new PriorityQueue<>((a, b) -> b.getValue().compareTo(a.getValue()));
                    List<Map.Entry<String,Long>> ans = new ArrayList<>();
                    iterable.forEach(x -> {
                        entryPQueue.add(x);
                    });

                    IntStream.range(0, Math.min(n,entryPQueue.size())).forEach((x) -> {
                        ans.add(entryPQueue.remove());
                    });
                    return ans;
                });
        List<Map.Entry<String,Long>> result2 = future2.get();

        for(Map.Entry<String,Long> entry : result2){
            logger.debug(entry.getKey().toString()+" "+entry.getValue());
        }


//
//        logger.info(result.toString());
    }
}
