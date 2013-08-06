package hazelmr.performance;

import hazelmr.Mapper;

import java.util.StringTokenizer;

/**
 * User: hilter
 * Date: 8/5/13
 * Time: 4:01 PM
 */
public class PerformanceTestMapper extends Mapper<Integer, String, String, Integer>
{
    @Override
    public void map(Integer key, String value)
    {
        String target = (String) getParameter("target");
        if (value.equals(target)) {
            emit(value, 1);
        }
    }
}