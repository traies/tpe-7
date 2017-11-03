package ar.edu.itba.pod.client;

import ar.edu.itba.pod.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.Job;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;
/**
 * Created by traie_000 on 11/3/2017.
 */
public class ClientTest {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    private Query query;
    private HazelcastInstance hz;
    @Before
    public void hazelcastSetUp() throws FileNotFoundException {
        hz = Hazelcast.newHazelcastInstance();
        ClassLoader classLoader = getClass().getClassLoader();
        Job<Province, InhabitantRecord> job = Client.hazelcastSetUp(
                new String[] {"127.0.0.1"},
                Optional
                    .ofNullable(classLoader.getResource("census100.csv"))
                    .map(URL::getFile)
                    .orElseThrow(FileNotFoundException::new)
        );
        this.query = new Query(job);
    }

    @After
    public void hazelcastShutdown() {
        hz.shutdown();
    }

    @Test
    public void populationPerRegion() throws ExecutionException, InterruptedException {
        Map<Region, Long> result = query.populationPerRegion();

        Assert.assertTrue(result.containsKey(Region.REGION_BUENOS_AIRES));
        Assert.assertEquals(47, (long) result.get(Region.REGION_BUENOS_AIRES));
        Assert.assertTrue(result.containsKey(Region.REGION_CENTRO));
        Assert.assertEquals(19, (long) result.get(Region.REGION_CENTRO));
        Assert.assertTrue(result.containsKey(Region.REGION_DEL_NUEVO_CUYO));
        Assert.assertEquals(3, (long) result.get(Region.REGION_DEL_NUEVO_CUYO));
        Assert.assertTrue(result.containsKey(Region.REGION_PATAGONICA));
        Assert.assertEquals(7, (long) result.get(Region.REGION_PATAGONICA));
        Assert.assertTrue(result.containsKey(Region.REGION_DEL_NORTE_GRANDE_ARGENTINO));
        Assert.assertEquals(24, (long) result.get(Region.REGION_DEL_NORTE_GRANDE_ARGENTINO));

    }

    @Test
    public void nDepartmentsByPopulation() throws ExecutionException, InterruptedException {
        List<Map.Entry<String,Long>> result = query.nDepartmentsByPopulation(Province.SANTA_FE, 10);

        Assert.assertEquals(5, (long) result.get(0).getValue());
        Assert.assertEquals("Rosario", result.get(0).getKey());
        Assert.assertEquals(2, (long) result.get(1).getValue());
        Assert.assertEquals("San Cristobal", result.get(1).getKey());
        Assert.assertEquals(1, (long) result.get(2).getValue());
        Assert.assertEquals("Castellanos", result.get(2).getKey());
    }

    @Test
    public void employmentPerRegion() throws ExecutionException, InterruptedException {
        List<Map.Entry<Region,Double>> result = query.employmentPerRegion();

        Assert.assertEquals(0.03571428571428571, result.get(0).getValue(), 0.0000001);
        Assert.assertEquals(Region.REGION_BUENOS_AIRES, result.get(0).getKey());
        Assert.assertEquals(0.0, result.get(1).getValue(), 0.00000001);
        Assert.assertEquals(0, result.get(2).getValue(), 0.00000001);
        Assert.assertEquals(0, result.get(3).getValue(),0.00000001);
        Assert.assertEquals(0, result.get(4).getValue(), 0.00000001);
    }
}
