package hazelmr.wordcount;

import hazelmr.Reducer;

import java.util.Collection;

public class WordCountReducer extends Reducer<String, Integer, String, Integer>
{
	@Override
	public void reduce(String key, Collection<Integer> values) {
		emit(key, values.size());
	}
}