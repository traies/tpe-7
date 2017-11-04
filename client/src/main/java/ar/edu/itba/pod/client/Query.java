package ar.edu.itba.pod.client;

import ar.edu.itba.pod.*;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

final class Query {
    private Job<Province, InhabitantRecord> job;

    Query(Job<Province, InhabitantRecord> job) {
        this.job = job;
    }

    Map<Region, Long> populationPerRegion() throws ExecutionException, InterruptedException {
        ICompletableFuture<Map<Region, Long>> future = job
                .mapper(new RegionMapper())
                .reducer(new RegionInhabitantsReducerFactory())
                .submit();
        return future.get();
    }

    List<Map.Entry<String, Long>> nDepartmentsByPopulation(Province prov, int n) throws ExecutionException, InterruptedException {
        ICompletableFuture<List<Map.Entry<String,Long>>> future = job
                .mapper(new ProvinceFilterMapper(prov))
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

    List<Map.Entry<Region, Double>> employmentPerRegion() throws ExecutionException, InterruptedException {
        ICompletableFuture<List<Map.Entry<Region, Double>>> future = job
                .mapper(new RegionMapper())
                .reducer(new EmploymentReducerFactory())
                .submit(iterable -> {
                    List<Map.Entry<Region, Double>> list = new ArrayList<>();
                    iterable.forEach(list::add);
                    list.sort((x, y) -> - x.getValue().compareTo(y.getValue()));
                    return list;
                });
        return future.get();
    }

    List<Map.Entry<Region, Integer>> householdsPerRegion() throws ExecutionException, InterruptedException {
        ICompletableFuture<List<Map.Entry<Region, Integer>>> future = job
                .mapper(new RegionMapper())
                .reducer(new HouseholdReducerFactory())
                .submit(iterable -> {
                    List<Map.Entry<Region, Integer>> list = new ArrayList<>();
                    iterable.forEach(list::add);
                    list.sort((x, y) -> - x.getValue().compareTo(y.getValue()));
                    return list;
                });
        return future.get();
    }

    List<Map.Entry<Region, Double>> householdRatioPerRegion() throws ExecutionException, InterruptedException {
        ICompletableFuture<List<Map.Entry<Region, Double>>> future = job
                .mapper(new RegionMapper())
                .reducer(new HouseholdRatioReducerFactory())
                .submit(iterable -> {
                    List<Map.Entry<Region, Double>> list = new ArrayList<>();
                    iterable.forEach(list::add);
                    list.sort((x, y) -> - x.getValue().compareTo(y.getValue()));
                    return list;
                });
        return future.get();
    }

    static <K, V> List<String> mapToStringList(Collection<Map.Entry<K,V>> collection) {
        return collection.stream().map(x -> String.format("%s, %s", x.getKey(), x.getValue())).collect(Collectors.toList());
    }
}
