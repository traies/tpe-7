package ar.edu.itba.pod.client;

import ar.edu.itba.pod.*;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

/**
 * Created by traie_000 on 11/3/2017.
 */
public final class Query {
    private Job<Province, InhabitantRecord> job;

    public Query(Job<Province, InhabitantRecord> job) {
        this.job = job;
    }

    public Map<Region, Long> populationPerRegion() throws ExecutionException, InterruptedException {
        ICompletableFuture<Map<Region, Long>> future = job
                .mapper(new RegionMapper())
                .reducer(new RegionInhabitantsReducerFactory())
                .submit();
        return future.get();
    }

    public List<Map.Entry<String, Long>> nDepartmentsByPopulation(int n) throws ExecutionException, InterruptedException {
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
        return future.get();
    }
}
