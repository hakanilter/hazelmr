package hazelmr.wordcount;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import hazelmr.HazelcastMapReduceJob;
import org.junit.Assert;
import org.junit.Test;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class WordCountTest 
{
    private static final int INSTANCE_COUNT = 2;

	@Test
	public void test() throws Exception
	{
		Config config = new ClasspathXmlConfig("hazelcast-test.xml");

		List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>(INSTANCE_COUNT);
		for (int i = 0; i < INSTANCE_COUNT; i++) {
			instances.add(Hazelcast.newHazelcastInstance(config));
		}

        HazelcastInstance client = Hazelcast.newHazelcastInstance(config);

		// create data
		Map<Integer, String> map = client.getMap("testMap");
		map.put(1, "aaaaa bbbbb");
		map.put(2, "aaaaa aaaaa bbbbb ccccc");
		map.put(3, "aaaaa ccccc ddddd");
		map.put(4, "eeeee aaaaa fffff");
		
		// create job
        long t1 = System.currentTimeMillis();
		HazelcastMapReduceJob<Integer, String, String, Integer, String, Integer> job =
				new HazelcastMapReduceJob<Integer, String, String, Integer, String, Integer>()
			.setMapper(WordCountMapper.class)
			.setReducer(WordCountReducer.class)	
			.setData(map);		

		// get results
		Map<String, Integer> result = job.execute(client);
        long t2 = System.currentTimeMillis();

		System.out.println("..and the oscar goes to:");
		System.out.println(result);
        System.out.println("elapsed " + (t2-t1) + " ms");
		
		Assert.assertEquals(5, result.get("aaaaa").intValue());
		Assert.assertEquals(2, result.get("bbbbb").intValue());
		Assert.assertEquals(2, result.get("ccccc").intValue());
        Assert.assertEquals(1, result.get("ddddd").intValue());
        Assert.assertEquals(1, result.get("eeeee").intValue());
		Assert.assertEquals(1, result.get("fffff").intValue());
	}
}


