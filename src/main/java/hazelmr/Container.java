package hazelmr;

import java.io.Serializable;

public class Container<T> implements Serializable 
{
	private static final long serialVersionUID = -1084346676259189349L;

	public T value;
	
	public Container(T value) {
		this.value = value;
	}
	
	public T getValue() {
		return value;
	}
}
