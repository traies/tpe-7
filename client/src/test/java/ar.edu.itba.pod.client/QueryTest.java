package ar.edu.itba.pod.client;

import ar.edu.itba.pod.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URL;
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
    private MultiMap<Province, InhabitantRecord> multiMap;

    @Before
    public void hazelcastSetUp() throws FileNotFoundException {
        hz = new TestHazelcastInstanceFactory().newHazelcastInstance();
        multiMap = hz.getMultiMap("censoPodGrupo7");
        KeyValueSource<Province, InhabitantRecord> source = KeyValueSource.fromMultiMap(multiMap);
        JobTracker jobTracker = hz.getJobTracker("test");
        this.query = new Query(jobTracker.newJob(source));
    }

    @After
    public void hazelcastShutdown() {
        hz.shutdown();
    }

    private void insertInhabitantsRecords(MultiMap<Province, InhabitantRecord> map, Object[] ... records) {
        for (Object[] o : records) {
            InhabitantRecord record = new InhabitantRecord(
                    (EmploymentCondition) o[0],
                    (Integer) o[1],
                    (String) o[2],
                    (Province) o[3]
                    );
            map.put(record.getProvince(), record);
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
                multiMap,
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.BUENOS_AIRES,},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.CIUDAD_AUTONOMA_DE_BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.SANTIAGO_DEL_ESTERO},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.MENDOZA},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.NEUQUEN},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.RIO_NEGRO},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.BUENOS_AIRES}
        );

        Map<Region, Long> result = query.populationPerRegion();
        Assert.assertEquals(5, (long) result.get(Region.REGION_BUENOS_AIRES));
        Assert.assertEquals(1, (long) result.get(Region.REGION_CENTRO));
        Assert.assertEquals(1, (long) result.get(Region.REGION_DEL_NUEVO_CUYO));
        Assert.assertEquals(2, (long) result.get(Region.REGION_PATAGONICA));
        Assert.assertEquals(1, (long) result.get(Region.REGION_DEL_NORTE_GRANDE_ARGENTINO));
    }

    @Test
    public void nDepartmentsByPopulation() throws ExecutionException, InterruptedException {
        insertInhabitantsRecords(
                multiMap,
                new Object[]{EmploymentCondition.INACTIVE, 0, "Rosario", Province.SANTA_FE,},
                new Object[]{EmploymentCondition.INACTIVE, 0, "San Cristobal", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.CIUDAD_AUTONOMA_DE_BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.TIERRA_DEL_FUEGO},
                new Object[]{EmploymentCondition.INACTIVE, 0, "Rosario", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "San Cristobal", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.NEUQUEN},
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
                multiMap,
                new Object[]{EmploymentCondition.EMPLOYED, 0, "aaaa", Province.BUENOS_AIRES,},
                new Object[]{EmploymentCondition.EMPLOYED, 0, "aaaa", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.CIUDAD_AUTONOMA_DE_BUENOS_AIRES},
                new Object[]{EmploymentCondition.EMPLOYED, 0, "aaaa", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.SANTIAGO_DEL_ESTERO},
                new Object[]{EmploymentCondition.UNEMPLOYED, 0, "aaaa", Province.MENDOZA},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.NEUQUEN},
                new Object[]{EmploymentCondition.EMPLOYED, 0, "aaaa", Province.RIO_NEGRO},
                new Object[]{EmploymentCondition.UNEMPLOYED, 0, "aaaa", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 0, "aaaa", Province.BUENOS_AIRES}
        );
        List<Map.Entry<Region,Double>> result = query.employmentPerRegion();
        assertOrderedDouble(
                result,
                new Region[]{
                        Region.REGION_DEL_NUEVO_CUYO,
                        Region.REGION_DEL_NORTE_GRANDE_ARGENTINO,
                        Region.REGION_BUENOS_AIRES,
                        Region.REGION_PATAGONICA,
                        Region.REGION_CENTRO,
                },
                new Double[]{
                        1.0,
                        1.0,
                        1.0/3,
                        0.0,
                        0.0,
                },
                delta
        );
    }

    @Test
    public void householdsPerRegion() throws ExecutionException, InterruptedException {
        insertInhabitantsRecords(
                multiMap,
                new Object[]{EmploymentCondition.INACTIVE, 1, "aaaa", Province.BUENOS_AIRES,},
                new Object[]{EmploymentCondition.INACTIVE, 1, "aaaa", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 2, "aaaa", Province.CIUDAD_AUTONOMA_DE_BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 1, "aaaa", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 1, "aaaa", Province.SANTIAGO_DEL_ESTERO},
                new Object[]{EmploymentCondition.INACTIVE, 1, "aaaa", Province.MENDOZA},
                new Object[]{EmploymentCondition.INACTIVE, 1, "aaaa", Province.NEUQUEN},
                new Object[]{EmploymentCondition.INACTIVE, 1, "aaaa", Province.RIO_NEGRO},
                new Object[]{EmploymentCondition.INACTIVE, 3, "aaaa", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 3, "aaaa", Province.BUENOS_AIRES}
        );
        List<Map.Entry<Region,Integer>> result = query.householdsPerRegion();

        assertOrdered(
                result,
                new Region[]{
                        Region.REGION_BUENOS_AIRES,
                        Region.REGION_PATAGONICA,
                        Region.REGION_DEL_NUEVO_CUYO,
                        Region.REGION_DEL_NORTE_GRANDE_ARGENTINO,
                        Region.REGION_CENTRO
                },
                new Integer[]{3, 1, 1, 1, 1}
        );
    }

    @Test
    public void householdRatioPerRegion() throws ExecutionException, InterruptedException {
        insertInhabitantsRecords(
                multiMap,
                new Object[]{EmploymentCondition.INACTIVE, 1, "aaaa", Province.BUENOS_AIRES,},
                new Object[]{EmploymentCondition.INACTIVE, 1, "aaaa", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 2, "aaaa", Province.CIUDAD_AUTONOMA_DE_BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 1, "aaaa", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 1, "aaaa", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 1, "aaaa", Province.MENDOZA},
                new Object[]{EmploymentCondition.INACTIVE, 2, "aaaa", Province.MENDOZA},
                new Object[]{EmploymentCondition.INACTIVE, 1, "aaaa", Province.SANTA_FE},
                new Object[]{EmploymentCondition.INACTIVE, 3, "aaaa", Province.BUENOS_AIRES},
                new Object[]{EmploymentCondition.INACTIVE, 3, "aaaa", Province.BUENOS_AIRES}
        );

        List<Map.Entry<Region,Double>> result = query.householdRatioPerRegion();

        assertOrderedDouble(
                result,
                new Region[] {
                        Region.REGION_CENTRO,
                        Region.REGION_BUENOS_AIRES,
                        Region.REGION_DEL_NUEVO_CUYO,
                },
                new Double[] {
                        3.0,
                        5 / 3.0,
                        1.0,
                },
                delta
        );
    }
}
