package ar.edu.itba.pod.client;

import ar.edu.itba.pod.model.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by traie_000 on 11/3/2017.
 */
public class QueryTest {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    private final Double delta = 0.0000001;
    private Query query;
    private HazelcastInstance hz;
    private IMap<Long, InhabitantRecord> map;

    @Before
    public void hazelcastSetUp() throws FileNotFoundException {
        hz = new TestHazelcastInstanceFactory().newHazelcastInstance();
        map = hz.getMap("censoPodGrupo7");
        KeyValueSource<Long, InhabitantRecord> source = KeyValueSource.fromMap(map);
        JobTracker jobTracker = hz.getJobTracker("test");
        this.query = new Query(jobTracker.newJob(source));
    }

    @After
    public void hazelcastShutdown() {
        hz.shutdown();
    }

    private void insertInhabitantsRecords(Map<Long, InhabitantRecord> map, InhabitantRecordSerializationMode mode, Object[] ... records) {
        Long id = 0L;
        for (Object[] o : records) {
            InhabitantRecord record = new InhabitantRecord(
                    (EmploymentCondition) o[0],
                    (Integer) o[1],
                    (String) o[2],
                    (Province) o[3],
                    mode
                    );
            map.put(id++, record);
        }
    }

    private<K, V> void assertOrdered(List<Map.Entry<K, V>> list, K[] expectedKeys, V[] expectedValues) {
        Assert.assertEquals(expectedKeys.length, expectedValues.length);

        for (int i = 0; i < expectedKeys.length; i++) {
            Assert.assertEquals(expectedKeys[i], list.get(i).getKey());
            Assert.assertEquals(expectedValues[i], list.get(i).getValue());
        }
    }

    private<K> void assertOrderedDouble(List<Map.Entry<K, Double>> list, K[] expectedKeys, Double[] expectedValues, Double delta) {
        Assert.assertEquals(expectedKeys.length, expectedValues.length);
        Assert.assertEquals(list.size(), expectedKeys.length);
        for (int i = 0; i < expectedKeys.length; i++) {
            Assert.assertEquals(expectedKeys[i], list.get(i).getKey());
            Assert.assertEquals(expectedValues[i], list.get(i).getValue(), delta);
        }
    }

    @Test
    public void populationPerRegion() throws ExecutionException, InterruptedException {
        insertInhabitantsRecords(
                map,
                InhabitantRecordSerializationMode.QUERY_0,
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.BUENOS_AIRES,},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.CIUDAD_AUTONOMA_DE_BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.SANTIAGO_DEL_ESTERO},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.MENDOZA},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.NEUQUEN},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.RIO_NEGRO},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.BUENOS_AIRES}
        );

        List<Map.Entry<Region, Long>> result = query.populationPerRegion();
        assertOrdered(
                result,
                new Region[] {
                        Region.REGION_BUENOS_AIRES,
                        Region.REGION_PATAGONICA,
                        Region.REGION_CENTRO,
                        Region.REGION_DEL_NORTE_GRANDE_ARGENTINO,
                        Region.REGION_DEL_NUEVO_CUYO,
                },
                new Long[] {5L, 2L, 1L, 1L, 1L});
    }

    @Test
    public void nDepartmentsByPopulation() throws ExecutionException, InterruptedException {
        insertInhabitantsRecords(
                map,
                InhabitantRecordSerializationMode.QUERY_1,
                new Object[]{EmploymentCondition.INACTIVE, 0, "Rosario", Province.SANTA_FE,},
                new Object[]{EmploymentCondition.INACTIVE, 0, "San Cristobal", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.CIUDAD_AUTONOMA_DE_BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.TIERRA_DEL_FUEGO},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Rosario", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "San Cristobal", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.NEUQUEN},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Castellanos", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Rosario", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Rosario", Province.BUENOS_AIRES}
        );

        List<Map.Entry<String,Long>> result = query.nDepartmentsByPopulation(Province.SANTA_FE, 10);
        assertOrdered(result, new String[] {"Rosario", "San Cristobal", "Castellanos",}, new Long[] {3L, 2L, 1L,});
    }

    @Test
    public void employmentPerRegion() throws ExecutionException, InterruptedException {
        insertInhabitantsRecords(
                map,
                InhabitantRecordSerializationMode.QUERY_2,
                new Object[]{EmploymentCondition.EMPLOYED, 0, "", Province.BUENOS_AIRES,},
                new Object[]{EmploymentCondition.EMPLOYED, 0, "", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.CIUDAD_AUTONOMA_DE_BUENOS_AIRES},
                new Object[]{EmploymentCondition.EMPLOYED, 0, "", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.SANTIAGO_DEL_ESTERO},
                new Object[]{EmploymentCondition.UNEMPLOYED, 0, "", Province.MENDOZA},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.NEUQUEN},
                new Object[]{EmploymentCondition.EMPLOYED, 0, "", Province.RIO_NEGRO},
                new Object[]{EmploymentCondition.UNEMPLOYED, 0, "", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "", Province.BUENOS_AIRES}
        );
        List<Map.Entry<Region,Double>> result = query.employmentPerRegion();
        assertOrderedDouble(
                result,
                new Region[]{
                        Region.REGION_DEL_NUEVO_CUYO,
                        Region.REGION_BUENOS_AIRES,
                        Region.REGION_CENTRO,
                        Region.REGION_DEL_NORTE_GRANDE_ARGENTINO,
                        Region.REGION_PATAGONICA,
                },
                new Double[]{1.0, 1.0/3, 0.0, 0.0, 0.0,},
                delta
        );
    }

    @Test
    public void householdsPerRegion() throws ExecutionException, InterruptedException {
        insertInhabitantsRecords(
                map,
                InhabitantRecordSerializationMode.QUERY_3,
                new Object[]{EmploymentCondition.INACTIVE, 1, "", Province.BUENOS_AIRES,},
                new Object[]{EmploymentCondition.INACTIVE, 1, "", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 2, "", Province.CIUDAD_AUTONOMA_DE_BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 1, "", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 1, "", Province.SANTIAGO_DEL_ESTERO},
                new Object[]{EmploymentCondition.INACTIVE, 1, "", Province.MENDOZA},
                new Object[]{EmploymentCondition.INACTIVE, 1, "", Province.NEUQUEN},
                new Object[]{EmploymentCondition.INACTIVE, 1, "", Province.RIO_NEGRO},
                new Object[]{EmploymentCondition.INACTIVE, 3, "", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 3, "", Province.BUENOS_AIRES}
        );
        List<Map.Entry<Region,Integer>> result = query.householdsPerRegion();

        assertOrdered(
                result,
                new Region[]{
                        Region.REGION_BUENOS_AIRES,
                        Region.REGION_CENTRO,
                        Region.REGION_DEL_NORTE_GRANDE_ARGENTINO,
                        Region.REGION_DEL_NUEVO_CUYO,
                        Region.REGION_PATAGONICA,
                },
                new Integer[]{3, 1, 1, 1, 1}
        );
    }

    @Test
    public void householdRatioPerRegion() throws ExecutionException, InterruptedException {
        insertInhabitantsRecords(
                map,
                InhabitantRecordSerializationMode.QUERY_4,
                new Object[]{EmploymentCondition.INACTIVE, 1, "", Province.BUENOS_AIRES,},
                new Object[]{EmploymentCondition.INACTIVE, 1, "", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 2, "", Province.CIUDAD_AUTONOMA_DE_BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 1, "", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 1, "", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 1, "", Province.MENDOZA},
                new Object[]{EmploymentCondition.INACTIVE, 2, "", Province.MENDOZA},
                new Object[]{EmploymentCondition.INACTIVE, 1, "", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 3, "", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 3, "", Province.BUENOS_AIRES}
        );

        List<Map.Entry<Region,Double>> result = query.householdRatioPerRegion();

        assertOrderedDouble(
                result,
                new Region[] {
                        Region.REGION_CENTRO,
                        Region.REGION_BUENOS_AIRES,
                        Region.REGION_DEL_NUEVO_CUYO,
                },
                new Double[] {3.0, 5 / 3.0, 1.0, },
                delta
        );
    }

    @Test
    public void sharedDepartments() throws ExecutionException, InterruptedException {
        insertInhabitantsRecords(
                map,
                InhabitantRecordSerializationMode.QUERY_6,
                new Object[]{EmploymentCondition.INACTIVE, 0, "Capital", Province.BUENOS_AIRES,},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Arrabales", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Capital", Province.CIUDAD_AUTONOMA_DE_BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Capital", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Cochabamba", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Capital", Province.MENDOZA},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Cochabamba", Province.MENDOZA},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Pergamino", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Capital", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Pergamino", Province.TIERRA_DEL_FUEGO}
        );

        List<Map.Entry<String,Integer>> result = query.sharedDepartmentsAmongProvices(0);

        assertOrdered(
                result,
                new String[] {
                        "Capital",
                        "Cochabamba",
                        "Pergamino",
                        "Arrabales"
                },
                new Integer[] {4, 2, 2, 1}
        );
    }

    @Test
    public void pairsOfProvincesThatHaveSharedDepartments() throws ExecutionException, InterruptedException {
        insertInhabitantsRecords(
                map,
                InhabitantRecordSerializationMode.QUERY_6,
                new Object[]{EmploymentCondition.INACTIVE, 0, "Capital", Province.BUENOS_AIRES,},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Arrabales", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Capital", Province.CIUDAD_AUTONOMA_DE_BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Capital", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Cochabamba", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Capital", Province.MENDOZA},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Cochabamba", Province.MENDOZA},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Pergamino", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Capital", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Pergamino", Province.TIERRA_DEL_FUEGO}
        );

        List<Map.Entry<ProvincePair,Long>> result = query.pairsOfProvincesThatHaveSharedDepartments(0);

        assertOrdered(
                result,
                new ProvincePair[] {
                        new ProvincePair(Province.SANTA_FE, Province.MENDOZA),
                        new ProvincePair(Province.BUENOS_AIRES, Province.CIUDAD_AUTONOMA_DE_BUENOS_AIRES),
                        new ProvincePair(Province.BUENOS_AIRES, Province.MENDOZA),
                        new ProvincePair(Province.BUENOS_AIRES, Province.SANTA_FE),
                        new ProvincePair(Province.CIUDAD_AUTONOMA_DE_BUENOS_AIRES, Province.MENDOZA),
                        new ProvincePair(Province.CIUDAD_AUTONOMA_DE_BUENOS_AIRES, Province.SANTA_FE),
                        new ProvincePair(Province.TIERRA_DEL_FUEGO, Province.SANTA_FE),
                },
                new Long[] {2L, 1L, 1L, 1L, 1L, 1L, 1L }
        );
    }
}
