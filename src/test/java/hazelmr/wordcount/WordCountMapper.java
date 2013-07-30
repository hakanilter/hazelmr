package hazelmr.wordcount;

import hazelmr.Mapper;

import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<Integer, String, String, Integer>
{
	@Override
	public void map(Integer key, String value) 
	{
		StringTokenizer tokenizer = new StringTokenizer(value);
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken(); 
			emit(token, 1);
		}
	}
}