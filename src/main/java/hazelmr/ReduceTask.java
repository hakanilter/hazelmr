package hazelmr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;


import com.hazelcast.core.MultiMap;

public class ReduceTask<KEYIN, VALUEIN, KEYOUT, VALUEOUT>  
{
	private Class<? extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> reducer;
	private MultiMap<KEYIN, Container<VALUEIN>> data;
	private Map<KEYOUT, VALUEOUT> tempData;
	
	public ReduceTask() {
		
	}
	
	public void call() throws InstantiationException, IllegalAccessException
	{
		// iterate over keys
		for (KEYIN key : data.keySet()) {
			Collection<VALUEIN> values = toCollection(data.get(key));
			// create reducer
			Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> instance = reducer.newInstance();
			instance.reduce(key, values);
			Map<KEYOUT, VALUEOUT> result = instance.getResults();			
			if (result != null) {
				// combine results
				for (KEYOUT resultKey : result.keySet()) {
					tempData.put(resultKey, result.get(resultKey));
				}
			}
		}
	}
	
	private Collection<VALUEIN> toCollection(Collection<Container<VALUEIN>> containers) 
	{
		Collection<VALUEIN> values = new ArrayList<VALUEIN>();
		for (Container<VALUEIN> container : containers) {
			values.add(container.getValue());
		}
		return values;
	}
	
	// getter setter
	
	public Class<? extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> getMapper() {
		return reducer;
	}
	
	public MultiMap<KEYIN, Container<VALUEIN>> getData() {
		return data;
	}
	
	public Map<KEYOUT, VALUEOUT> getTempData() {
		return tempData;
	}
	
	public ReduceTask<KEYIN, VALUEIN, KEYOUT, VALUEOUT> setReducer(Class<? extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> reducer) 
	{
		this.reducer = reducer;
		return this;
	}
	
	public ReduceTask<KEYIN, VALUEIN, KEYOUT, VALUEOUT> setData(MultiMap<KEYIN, Container<VALUEIN>> data) 
	{
		this.data = data;
		return this;
	}
	
	public ReduceTask<KEYIN, VALUEIN, KEYOUT, VALUEOUT> setTempData(Map<KEYOUT, VALUEOUT> tempData) 
	{
		this.tempData = tempData;
		return this;
	}
}
