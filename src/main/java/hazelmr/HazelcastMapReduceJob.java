package hazelmr;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.MultiTask;

public class HazelcastMapReduceJob<KEYIN, VALUEIN, KEYMID, VALUEMID, KEYOUT, VALUEOUT>
{
	private Class<? extends Mapper<KEYIN, VALUEIN, KEYMID, VALUEMID>> mapper;
	private Class<? extends Reducer<KEYMID, VALUEMID, KEYOUT, VALUEOUT>> reducer;
	private Class<? extends Serializable> outputType;
	private Map<KEYIN, VALUEIN> data;
	
	public HazelcastMapReduceJob() {
		
	}
		
	public Map<KEYOUT, VALUEOUT> execute(HazelcastInstance hazelcast) throws Exception 
	{
		// create temporary map for intermediate results
		MultiMap<KEYMID, Container<VALUEMID>> tempData = hazelcast.getMultiMap(UUID.randomUUID().toString());
		
		// create map task		
		MapTask<KEYIN, VALUEIN, KEYMID, VALUEMID> mapTask = new MapTask<KEYIN, VALUEIN, KEYMID, VALUEMID>()
			.setMapper(mapper)
			.setData(data)
			.setTempData(tempData);
		
		// execute map task		
		MultiTask<Void> distributedTask = new MultiTask<Void>(mapTask, hazelcast.getCluster().getMembers());			    
		ExecutorService executorService = hazelcast.getExecutorService();	    
	    executorService.execute(distributedTask);
	    distributedTask.get(60, TimeUnit.SECONDS);
	   
	    // create reduce task
	    Map<KEYOUT, VALUEOUT> result = new HashMap<KEYOUT, VALUEOUT>(); 
	    ReduceTask<KEYMID, VALUEMID, KEYOUT, VALUEOUT> reduceTask = new ReduceTask<KEYMID, VALUEMID, KEYOUT, VALUEOUT>()
	    	.setReducer(reducer)
	    	.setData(tempData)
	    	.setTempData(result);
	    // execute reduce task
	    reduceTask.call();	    

	    // clear temp data	   
	    tempData.destroy();
	    
	    return result;
	}	
	
	// getter setter
	
	public Class<? extends Mapper<KEYIN, VALUEIN, KEYMID, VALUEMID>> getMapper() {
		return mapper;
	}

	public Class<? extends Reducer<KEYMID, VALUEMID, KEYOUT, VALUEOUT>> getReducer() {
		return reducer;
	}
	
	public Class<? extends Serializable> getOutputType() {
		return outputType;
	}

	public Map<KEYIN, VALUEIN> getData() {
		return data;
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
	
	public HazelcastMapReduceJob<KEYIN, VALUEIN, KEYMID, VALUEMID, KEYOUT, VALUEOUT> setOutputType(
			Class<? extends Serializable> outputType) 
	{
		this.outputType = outputType;
		return this;
	}
	
	public HazelcastMapReduceJob<KEYIN, VALUEIN, KEYMID, VALUEMID, KEYOUT, VALUEOUT> setData(
			Map<KEYIN, VALUEIN> data) 
	{
		this.data = data;
		return this;
	}
}
