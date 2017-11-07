package ar.edu.itba.pod.client;

import ar.edu.itba.pod.combiners.CounterCombinerFactory;
import ar.edu.itba.pod.combiners.DepartmentCounterCombinerFactory;
import ar.edu.itba.pod.combiners.EmploymentCombinerFactory;
import ar.edu.itba.pod.combiners.HouseholdCombinerFactory;
import ar.edu.itba.pod.mappers.DepartmentMapper;
import ar.edu.itba.pod.mappers.ProvinceFilterMapper;
import ar.edu.itba.pod.mappers.RegionMapper;
import ar.edu.itba.pod.model.InhabitantRecord;
import ar.edu.itba.pod.model.Province;
import ar.edu.itba.pod.model.ProvincePair;
import ar.edu.itba.pod.model.Region;
import ar.edu.itba.pod.reducers.*;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

final class Query {
    private Job<Long, InhabitantRecord> job;

    Query(Job<Long, InhabitantRecord> job) {
        this.job = job;
    }


    /**
     * Para contar lo poblacion por regiones, el mapper mapea cada InhabitantRecord a la region de su Provincia, y
     * luego el reducer cuenta la cantidad de InhabitantRecords por Region.
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    List<Map.Entry<Region, Long>> populationPerRegion() throws ExecutionException, InterruptedException {
        ICompletableFuture<List<Map.Entry<Region, Long>>> future = job
                .mapper(new RegionMapper())
                .combiner(new CounterCombinerFactory<>())
                .reducer(new RegionInhabitantsReducerFactory())
                .submit(iterable -> {
                    List<Map.Entry<Region, Long>> list = new ArrayList<>();
                    iterable.forEach(list::add);
                    list.sort(new ReverseEntryValueComparator<>());
                    return list;
                });
        return future.get();
    }

    /**
     *  Tomando la Provincia y la cantidad de departamentos por parametro, con ProvinceFilterMapper emitimos unicamente
     *  los InhabitantRecords que corresponden a la provincia pasada como paramentro, y los emitmos con su departamento
     *  como clave. Luego, InhabitantsPerDepartmentReducer cuenta la cantidad de habitantes para cada departamento.
     *  Despues de esto, aplicamos un Collator que utiliza una PriorityQueue donde se almacenan todos los pares
     *  departamento-poblacion, usando como comparador la poblacion en orden descendente. Luego, se toman los primeros n
     *  valores que devuelve la PriorityQueue, donde n es el parametro.
     *
     *  Como alternativa al Collator implementado, una opcion podria haber sido ordenar todos los valores obtenidos segun
     *  su poblacion, almacenandolos primero en una lista. Otra, podria haber sido recorrer la lista una sola vez, llevando
     *  siempre los N departamentos con mayor poblacion hasta el momento, y comparando cada valor de la lista contra el
     *  enesimo departamento con mayor poblacion, para decidir si reemplaza o no a alguno de los departamentos de la lista
     *  de mayores.
     *
     *  Consideramos que nuestra solucion es un buen compromiso al ser computacionalmente menos compleja que la primera
     *  opcion descripta, y menos engorrosa de implementar que la segunda.
     * @param prov
     * @param n
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    List<Map.Entry<String, Long>> nDepartmentsByPopulation(Province prov, int n) throws ExecutionException, InterruptedException {
        ICompletableFuture<List<Map.Entry<String,Long>>> future = job
                .mapper(new ProvinceFilterMapper(prov))
                .combiner(new CounterCombinerFactory<>())
                .reducer(new InhabitantsPerDepartmentReducerFactory())
                .submit(iterable -> {
                    PriorityQueue<Map.Entry<String,Long>> entryPQueue = new PriorityQueue<>(new ReverseEntryValueComparator<String, Long>());
                    List<Map.Entry<String,Long>> ans = new ArrayList<>();
                    iterable.forEach(entryPQueue::add);
                    IntStream.range(0, Math.min(n,entryPQueue.size())).forEach((x) -> ans.add(entryPQueue.remove()));
                    return ans;
                });
        return future.get();
    }

    /**
     * Primero, el mapper mapea cada InhabitantRecord a la region de su Provincia, y
     * luego el reducer cuenta la cantidad de habitantes Empleados y Desempleados. Con esos datos, se calcula el
     * ratio y luego se ordena los resultados por el ratio.
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    List<Map.Entry<Region, Double>> employmentPerRegion() throws ExecutionException, InterruptedException {
        ICompletableFuture<List<Map.Entry<Region, Double>>> future = job
                .mapper(new RegionMapper())
                .combiner(new EmploymentCombinerFactory())
                .reducer(new EmploymentReducerFactory())
                .submit(iterable -> {
                    List<Map.Entry<Region, Double>> list = new ArrayList<>();
                    iterable.forEach(list::add);
                    list.sort(new ReverseEntryValueComparator<>());
                    return list;
                });
        return future.get();
    }

    /**
     * Primero, el mapper mapea cada InhabitantRecord a la region de su Provincia, y
     * luego el reducer va hasheando los hogarId para no obtener repetidos. Luego, retorna el tamanio del set y el
     * Collator ordena los resultados obtenidos de mayor a menor segun la cantidad de hogares.
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    List<Map.Entry<Region, Integer>> householdsPerRegion() throws ExecutionException, InterruptedException {
        ICompletableFuture<List<Map.Entry<Region, Integer>>> future = job
                .mapper(new RegionMapper())
                .combiner(new HouseholdCombinerFactory())
                .reducer(new HouseholdReducerFactory())
                .submit(iterable -> {
                    List<Map.Entry<Region, Integer>> list = new ArrayList<>();
                    iterable.forEach(list::add);
                    list.sort(new ReverseEntryValueComparator<>());
                    return list;
                });
        return future.get();
    }

    /**
     * Similar al anterior, pero ahora se cuenta tambien la poblacion total de cada region, y luego se divide la
     * poblacion de cada region sobre la cantidad de hogares, para obtener el promedio de personas por hogar. Tambien
     * se ordena de mayor a menor por el promedio de personas por hogar.
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    List<Map.Entry<Region, Double>> householdRatioPerRegion() throws ExecutionException, InterruptedException {
        ICompletableFuture<List<Map.Entry<Region, Double>>> future = job
                .mapper(new RegionMapper())
                .combiner(new HouseholdCombinerFactory())
                .reducer(new HouseholdRatioReducerFactory())
                .submit(iterable -> {
                    List<Map.Entry<Region, Double>> list = new ArrayList<>();
                    iterable.forEach(list::add);
                    list.sort(new ReverseEntryValueComparator<>());
                    return list;
                });
        return future.get();
    }

    /**
     *
     * La idea es que el Mapper emita pares <"Nombre de Departamento", "Provincia"> por cada
     * entrada del censo. De tal forma, cada uno de los reducers de los departamentos encontrados
     * mantiene un contador y un Set de las provincias ya contabilizadas (Para no contar más de una
     * vez la aparición de un departamento en una provincia dada). Luego, un Collator se ocupa de filtrar
     * los nombres de departamento cuyo número de coincidencias sea menor al "n" requerido, como así también
     * se encarga de ordenar los resultados por dicho número de coincidencias.
     *
     * @param n
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    List<Map.Entry<String, Integer>> sharedDepartmentsAmongProvices(int n) throws  ExecutionException, InterruptedException {
        ICompletableFuture<List<Map.Entry<String, Integer>>> future = job
                .mapper(new DepartmentMapper())
                .combiner(new DepartmentCounterCombinerFactory())
                .reducer(new DepartmentCounterReducerFactory())
                .submit(iterable -> {
                    PriorityQueue<Map.Entry<String,Integer>> s = new PriorityQueue<>((a, b) -> b.getValue().compareTo(a.getValue()));
                    iterable.forEach(x-> {
                        if(x.getValue() >= n)
                            s.add(x);
                    });
                    List<Map.Entry<String,Integer>> ans = new ArrayList<>();
                    IntStream.range(0, s.size()).forEach((x) -> ans.add(s.remove()));
                    return ans;
                });
        return future.get();
    }

    /**
     *
     * Similarmente al caso anterior, el mapper se encarga de emitir pares <"Nombre de Departamento", "Provincia">.
     * El reducer, en cambio, irá añadiendo a un Set las provincias en las que aparece un determinado departamento,
     * para luego emitir un Set de ProvincePair (un par de provincias ordenado según el orden alfabético de las
     * mismas).
     *
     * El collator se ocupará de contabilizar las múltiples apariciones de un ProvincePair entre los diferentes
     * departamentos, eliminará los resultados con menor "n" que el requerido y los devolverá ordenados.
     *
     * @param n
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    List<Map.Entry<ProvincePair, Long>> pairsOfProvincesThatHaveSharedDepartments(int n) throws ExecutionException, InterruptedException{
        ICompletableFuture<List<Map.Entry<ProvincePair,Long>>> future = job
                .mapper(new DepartmentMapper())
                .combiner(new DepartmentCounterCombinerFactory())
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
                    PriorityQueue<Map.Entry<ProvincePair,Long>> pq = new PriorityQueue<>(new ReverseEntryValueComparator<>());

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


    /**
     * This comparator has descending order on the value of the Entry. Key is secondary order, ascending.
     * @param <K>
     * @param <V>
     */
    private class ReverseEntryValueComparator<K extends Comparable<K>, V extends Comparable<V>> implements Comparator<Map.Entry<K, V>> {

        @Override
        public int compare(Map.Entry<K, V> x, Map.Entry<K, V> y) {
            int res = - x.getValue().compareTo(y.getValue());
            return res != 0 ? res : x.getKey().compareTo(y.getKey());
        }
    }
}
