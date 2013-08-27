package hazelmr;

import static java.util.Map.Entry;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.impl.MProxyImpl;

public class MapTask<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements Callable<Void>, Serializable 
{
	private static final long serialVersionUID = 9107135773832469237L;
	
	private Class<? extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> mapper;
    private Map<Object, Object> parameters;
	private Map<KEYIN, VALUEIN> data;
    private MultiMap<KEYOUT, Container<VALUEOUT>> tempData;
	
	public MapTask() {
		
	}
	
	@Override
	public Void call() throws Exception 
	{
		if (!Thread.currentThread().isInterrupted()) { 
			process();
		}
		return null;
	}

    private void process() throws IllegalAccessException, InstantiationException
    {
        // get local keys
        IMap<KEYIN, VALUEIN> imap = ((IMap<KEYIN, VALUEIN>) data);
        Set<KEYIN> keys = imap.localKeySet();

        // iterate values and give them to mapper
        for (KEYIN key : keys) {
            VALUEIN value = data.get(key);
            // create mapper
            Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> instance = mapper.newInstance();
            instance.setParameters(parameters);
            instance.map(key, value);
            Map<KEYOUT, Collection<VALUEOUT>> result = instance.getResults();
            // combine results
            for (Map.Entry<KEYOUT, Collection<VALUEOUT>> entry : result.entrySet()) {
                for (VALUEOUT mapValue : entry.getValue()) {
                    // use container for individual results
                    tempData.put(entry.getKey(), new Container<VALUEOUT>(mapValue));
                }
            }
        }
    }
	
	// getter setter
	
	public Class<? extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> getMapper() {
		return mapper;
	}
	
	public Map<KEYIN, VALUEIN> getData() {
		return data;
	}
	
	public MultiMap<KEYOUT, Container<VALUEOUT>> getTempData() {
		return tempData;
	}

    public Map<Object, Object> getParameters() {
        return parameters;
    }
	
	public MapTask<KEYIN, VALUEIN, KEYOUT, VALUEOUT> setMapper(Class<? extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> mapper) 
	{
		this.mapper = mapper;
		return this;
	}
	
	public MapTask<KEYIN, VALUEIN, KEYOUT, VALUEOUT> setData(Map<KEYIN, VALUEIN> data) 
	{
		this.data = data;
		return this;
	}
	
	public MapTask<KEYIN, VALUEIN, KEYOUT, VALUEOUT> setTempData(MultiMap<KEYOUT, Container<VALUEOUT>> tempData) 
	{
		this.tempData = tempData;
		return this;
	}

    public MapTask<KEYIN, VALUEIN, KEYOUT, VALUEOUT> setParameters(Map<Object, Object> parameters)
    {
        this.parameters = parameters;
        return this;
    }
}
