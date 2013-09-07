package hazelmr.localmap;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import hazelmr.HazelcastMapReduceJob;
import junit.framework.Assert;
import org.junit.Test;

import java.util.*;

/**
 * User: hilter
 * Date: 8/5/13
 * Time: 3:58 PM
 */
public class LocalMapTest {

    private static final int INSTANCE_COUNT = 3;
    private static final int TEST_COUNT = 1000000 / INSTANCE_COUNT;

    @Test
    public void testWithMapReduce() throws Exception {
        Config config = new ClasspathXmlConfig("hazelcast-test.xml");

        List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>(INSTANCE_COUNT);
        for (int i = 0; i < INSTANCE_COUNT; i++) {
            instances.add(Hazelcast.newHazelcastInstance(config));
        }

        HazelcastInstance client = Hazelcast.newHazelcastInstance(config);

        // create data
        Map<String, Object> map = client.getMap("testMap");
        String testId = loadTestData(instances, map);

        // create job
        long t1, t2;
        t1 = System.currentTimeMillis();
        HazelcastMapReduceJob<String, Object, String, Integer, String, Integer> job =
                new HazelcastMapReduceJob<String, Object, String, Integer, String, Integer>()
                        .setMapper(LocalMapTestMapper.class)
                        .setReducer(LocalMapTestReducer.class)
                        .setData(map)
                        .setParameter("target", testId);

        // get results
        Map<String, Integer> result = job.execute(client);
        t2 = System.currentTimeMillis();

        System.out.println("..and the oscar goes to:");
        System.out.println(result);
        System.out.println("done (" + (t2 - t1) + " ms)");

        Assert.assertNotNull(result.get(testId));
        Assert.assertEquals(INSTANCE_COUNT, result.get(testId).intValue());
    }

    // creates test data for given map
    private String loadTestData(List<HazelcastInstance> instances, Map<String, Object> globalMap) {
        long t1, t2;
        t1 = System.currentTimeMillis();
        System.out.println("creating test data...");
        String testId = UUID.randomUUID().toString();

        // create a map for each node
        for (HazelcastInstance instance : instances) {
            Map<Integer, String> map = new HashMap<Integer, String>();
            for (int i = 0; i < TEST_COUNT; i++) {
                if (i % 1000 == 0) {
                    System.out.println(i);
                }
                map.put(i, UUID.randomUUID().toString());
            }
            map.put(666, testId);
            globalMap.put(instance.getName(), map);
        }

        t2 = System.currentTimeMillis();
        System.out.println("test data ready (" + (t2 - t1) + " ms)");

        return testId;
    }

}
