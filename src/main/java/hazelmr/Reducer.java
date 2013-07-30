package hazelmr;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
{
	private Map<KEYOUT, VALUEOUT> result;
	
	public Reducer() {
		result = new HashMap<KEYOUT, VALUEOUT>();
	}
	
	public abstract void reduce(KEYIN key, Collection<VALUEIN> values);
	
	public void emit(KEYOUT key, VALUEOUT value) 
	{
		result.put(key, value);
	}
	
	public final Map<KEYOUT, VALUEOUT> getResults() {
		return result;
	}
}
