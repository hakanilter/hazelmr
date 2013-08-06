package hazelmr;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

public abstract class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
{
    private Map<Object, Object> parameters;
	private Map<KEYOUT, Collection<VALUEOUT>> result;
	
	public Mapper() {
		result = new HashMap<KEYOUT, Collection<VALUEOUT>>();
	}
	
	public abstract void map(KEYIN key, VALUEIN value);
	
	public void emit(KEYOUT key, VALUEOUT value) 
	{
		if (!result.containsKey(key)) {
			result.put(key, new Vector<VALUEOUT>());
		}
		result.get(key).add(value);
	}
	
	public final Map<KEYOUT, Collection<VALUEOUT>> getResults() {
		return result;
	}

    public Object getParameter(String key) {
        return parameters.get(key);
    }

    public Map<Object, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<Object, Object> parameters) {
        this.parameters = parameters;
    }
}
