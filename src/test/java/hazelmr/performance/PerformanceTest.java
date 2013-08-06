package hazelmr.performance;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import hazelmr.HazelcastMapReduceJob;
import org.junit.Test;

import java.util.*;

/**
 * User: hilter
 * Date: 8/5/13
 * Time: 3:58 PM
 */
public class PerformanceTest
{
    private static final int INSTANCE_COUNT = 0;
    private static final int TEST_COUNT = 1000000;

    @Test
    public void testWithMapReduce() throws Exception
    {
        Config config = new ClasspathXmlConfig("hazelcast-test.xml");

        List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>(INSTANCE_COUNT);
        for (int i = 0; i < INSTANCE_COUNT; i++) {
            instances.add(Hazelcast.newHazelcastInstance(config));
        }

        HazelcastInstance client = Hazelcast.newHazelcastInstance(config);

        // create data
        Map<Integer, String> map = client.getMap("testMap");
        String testId = loadTestData(map);

        // create job
        long t1, t2;
        t1 = System.currentTimeMillis();
        HazelcastMapReduceJob<Integer, String, String, Integer, String, Integer> job =
                new HazelcastMapReduceJob<Integer, String, String, Integer, String, Integer>()
                        .setMapper(PerformanceTestMapper.class)
                        .setReducer(PerformanceTestReducer.class)
                        .setData(map)
                        .setParameter("target", testId);

        // get results
        Map<String, Integer> result = job.execute(client);
        t2 = System.currentTimeMillis();

        System.out.println("..and the oscar goes to:");
        System.out.println(result);
        System.out.println("done (" + (t2-t1) + " ms)");
    }

    @Test
    public void testLocal()
    {
        Map<Integer, String> map = new HashMap<Integer, String>(TEST_COUNT);
        testGetPerformance(map);
    }

    @Test
    public void testDistributed()
    {
        Config config = new ClasspathXmlConfig("hazelcast-test.xml");
        HazelcastInstance client = Hazelcast.newHazelcastInstance(config);

        IMap<Integer, String> map = client.getMap("testMap");
        testGetPerformance(map);
    }

    // iterates over all map
    private void testGetPerformance(Map<Integer, String> map)
    {
        String testId = loadTestData(map);

        long t1, t2;
        System.out.println("starting test...");
        t1 = System.currentTimeMillis();
        for (Integer key : map.keySet()) {
            String value = map.get(key);
            if (value.equals(testId)) {
                System.out.println("match: " + value);
            }
        }
        t2 = System.currentTimeMillis();
        System.out.println("test done (" + (t2-t1) + " ms)");
    }

    // creates test data for given map
    private String loadTestData(Map<Integer, String> map)
    {
        long t1, t2;
        t1 = System.currentTimeMillis();
        System.out.println("creating test data...");
        for (int i = 0; i < TEST_COUNT; i++) {
            if (i % 1000 == 0) {
                System.out.println(i);
            }
            map.put(i, UUID.randomUUID().toString());
        }
        String testId = UUID.randomUUID().toString();
        map.put(666, testId);
        t2 = System.currentTimeMillis();
        System.out.println("test data ready (" + (t2-t1) + " ms)");

        return testId;
    }
}
