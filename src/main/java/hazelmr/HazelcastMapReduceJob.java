package hazelmr;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.MultiTask;

public class HazelcastMapReduceJob<KEYIN, VALUEIN, KEYMID, VALUEMID, KEYOUT, VALUEOUT>
{
    private Logger logger = Logger.getLogger(getClass().getName());

	private Class<? extends Mapper<KEYIN, VALUEIN, KEYMID, VALUEMID>> mapper;
	private Class<? extends Reducer<KEYMID, VALUEMID, KEYOUT, VALUEOUT>> reducer;
	private Map<KEYIN, VALUEIN> data;

    private boolean autoDestroy = true;
    private long timeout = 60;

    private ExecutorService executorService;
    private Set<Member> members;
    private MultiMap<KEYMID, Container<VALUEMID>> tempData;
	
	public HazelcastMapReduceJob() {
		
	}

	public Map<KEYOUT, VALUEOUT> execute(HazelcastInstance hazelcast) throws Exception
	{
        this.executorService = hazelcast.getExecutorService();
        this.members = hazelcast.getCluster().getMembers();
        this.tempData = hazelcast.getMultiMap(UUID.randomUUID().toString());
        return executeMapReduce();
	}

    private Map<KEYOUT, VALUEOUT> executeMapReduce() throws Exception
    {
        Map<KEYOUT, VALUEOUT> result = new HashMap<KEYOUT, VALUEOUT>();

        try {
            // execute map reduce
            executeMap(executorService, members, tempData);
            executeReduce(executorService, members, tempData, result);
        } finally {
            // clear temp data
            if (autoDestroy) {
                logger.fine("destroying temporary map...");
                tempData.destroy();
            }
        }

        return result;
    }

    private void executeMap(ExecutorService executorService, Set<Member> members,
            MultiMap<KEYMID, Container<VALUEMID>> tempData) throws Exception
    {
        logger.fine("map started...");

        // create map task
        MapTask<KEYIN, VALUEIN, KEYMID, VALUEMID> mapTask =
                new MapTask<KEYIN, VALUEIN, KEYMID, VALUEMID>()
                .setMapper(mapper)
                .setData(data)
                .setTempData(tempData);

        // execute map task
        MultiTask<Void> distributedTask = new MultiTask<Void>(mapTask, members);
        executorService.execute(distributedTask);
        distributedTask.get(timeout, TimeUnit.SECONDS);
    }

    private Map<KEYOUT, VALUEOUT> executeReduce(
            ExecutorService executorService, Set<Member> members,
            MultiMap<KEYMID, Container<VALUEMID>> tempData,
            Map<KEYOUT, VALUEOUT> result) throws Exception
    {
        logger.fine("reduce started...");

        // create reduce task
        ReduceTask<KEYMID, VALUEMID, KEYOUT, VALUEOUT> reduceTask =
                new ReduceTask<KEYMID, VALUEMID, KEYOUT, VALUEOUT>()
                .setReducer(reducer)
                .setData(tempData)
                .setTempData(result);
        // execute reduce task
        reduceTask.call();
        return result;
    }

	// getter setter
	
	public Class<? extends Mapper<KEYIN, VALUEIN, KEYMID, VALUEMID>> getMapper() {
		return mapper;
	}

	public Class<? extends Reducer<KEYMID, VALUEMID, KEYOUT, VALUEOUT>> getReducer() {
		return reducer;
	}

	public Map<KEYIN, VALUEIN> getData() {
		return data;
	}

    public long getTimeout() {
        return timeout;
    }

    public boolean isAutoDestroy() {
        return autoDestroy;
    }

	public HazelcastMapReduceJob<KEYIN, VALUEIN, KEYMID, VALUEMID, KEYOUT, VALUEOUT> setMapper(
			Class<? extends Mapper<KEYIN, VALUEIN, KEYMID, VALUEMID>> mapper) 
	{
		this.mapper = mapper;
		return this;
	}
	
	public HazelcastMapReduceJob<KEYIN, VALUEIN, KEYMID, VALUEMID, KEYOUT, VALUEOUT> setReducer(
			Class<? extends Reducer<KEYMID, VALUEMID, KEYOUT, VALUEOUT>> reducer) 
	{
		this.reducer = reducer;
		return this;
	}
	
	public HazelcastMapReduceJob<KEYIN, VALUEIN, KEYMID, VALUEMID, KEYOUT, VALUEOUT> setData(
			Map<KEYIN, VALUEIN> data) 
	{
		this.data = data;
		return this;
	}

    public HazelcastMapReduceJob<KEYIN, VALUEIN, KEYMID, VALUEMID, KEYOUT, VALUEOUT> setTimeout(long timeout)
    {
        this.timeout = timeout;
        return this;
    }

    public HazelcastMapReduceJob<KEYIN, VALUEIN, KEYMID, VALUEMID, KEYOUT, VALUEOUT> setAutoDestroy(boolean autoDestroy)
    {
        this.autoDestroy = autoDestroy;
        return this;
    }
}
