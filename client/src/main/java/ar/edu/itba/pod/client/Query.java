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
                    PriorityQueue<Map.Entry<String,Long>> entryPQueue = new PriorityQueue<>((x, y) -> {
                        int res = - x.getValue().compareTo(y.getValue());
                        return res != 0 ? res : - x.getKey().compareTo(y.getKey());
                    });
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
                    list.sort((x, y) -> {
                        int res = - x.getValue().compareTo(y.getValue());
                        return res != 0 ? res : - x.getKey().compareTo(y.getKey());
                    });
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
                    list.sort((x, y) -> {
                        int res = - x.getValue().compareTo(y.getValue());
                        return res != 0 ? res : - x.getKey().compareTo(y.getKey());
                    });
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
                    list.sort((x, y) -> {
                        int res = - x.getValue().compareTo(y.getValue());
                        return res != 0 ? res : - x.getKey().compareTo(y.getKey());
                    });
                    return list;
                });
        return future.get();
    }

    List<Map.Entry<String, Long>> sharedDepartmentsAmongProvices(int n) throws  ExecutionException, InterruptedException {
        ICompletableFuture<List<Map.Entry<String, Long>>> future = job
                .mapper(new DepartmentMapper())
                .reducer(new DepartmentCounterReducerFactory())
                .submit(iterable -> {
                    PriorityQueue<Map.Entry<String,Long>> s = new PriorityQueue<>((a, b) -> b.getValue().compareTo(a.getValue()));
                    iterable.forEach(x-> {
                        if(x.getValue() >= n)
                            s.add(x);
                    });
                    List<Map.Entry<String,Long>> ans = new ArrayList<>();
                    IntStream.range(0, s.size()).forEach((x) -> ans.add(s.remove()));
                    return ans;
                });
        return future.get();
    }

    List<Map.Entry<ProvincePair, Long>> pairsOfProvincesThatHaveSharedDepartments(int n) throws ExecutionException, InterruptedException{
        ICompletableFuture<List<Map.Entry<ProvincePair,Long>>> future = job
                .mapper(new DepartmentMapper())
                .reducer(new SharedDepartmentsAmongProvincesReducerFactory())
                .submit(iterable -> {

                    Map<ProvincePair,Long> map = new HashMap<>();
                    iterable.forEach(x -> x.getValue().stream().forEach(y -> {
                        if(map.containsKey(y)){
                            map.put(y,map.get(y)+1L);
                        } else{
                            map.put(y,1L);
                        }
                    }));
                    PriorityQueue<Map.Entry<ProvincePair,Long>> pq = new PriorityQueue<>((a, b) -> b.getValue().compareTo(a.getValue()));

                    map.entrySet().stream().filter(x->x.getValue()>=n).forEach(x->pq.add(x));

                    List<Map.Entry<ProvincePair,Long>> ans = new ArrayList<>();
                    IntStream.range(0, pq.size()).forEach((x) -> ans.add(pq.remove()));
                    return ans;

                });
        return future.get();
    }

    static <K, V> List<String> mapToStringList(Collection<Map.Entry<K,V>> collection) {
        return collection.stream().map(x -> String.format("%s, %s", x.getKey(), x.getValue())).collect(Collectors.toList());
    }
}
