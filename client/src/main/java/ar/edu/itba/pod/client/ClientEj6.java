package ar.edu.itba.pod.client;

import ar.edu.itba.pod.DepartmentCounterFactory;
import ar.edu.itba.pod.DepartmentMapper;
import ar.edu.itba.pod.InhabitantRecord;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

public class ClientEj6 {
    private static Logger logger = LoggerFactory.getLogger(ClientEj6.class);

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
        Integer n = 5;
        ICompletableFuture< List<Map.Entry<String, Long>>> future = job
                .mapper(new DepartmentMapper())
                .reducer(new DepartmentCounterFactory())
                .submit( iterable -> {
                    PriorityQueue<Map.Entry<String,Long>> asd=new PriorityQueue<>(Comparator.comparing(x->-x.getValue()));
                    iterable.forEach(x-> asd.add(x));
                    List<Map.Entry<String, Long>> res = new ArrayList<>();
                    for (int i = 0; i < n; i++) {
                        res.add(asd.poll());
                    }
//                    Stream.of(iterable).collect(Collectors.toList());
                    return res;
                });
       List<Map.Entry<String, Long>> result = future.get();

       for(Map.Entry<String,Long> entry : result){
           logger.debug(entry.getKey()+" "+entry.getValue());
       }
//
//        logger.info(result.toString());
    }
}
