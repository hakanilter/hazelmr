package hazelmr.localmap;

import hazelmr.Mapper;

import java.util.Map;

/**
 * User: hilter
 * Date: 8/5/13
 * Time: 4:01 PM
 */
public class LocalMapTestMapper extends Mapper<String, Object, String, Integer> {

    @Override
    public void map(String key, Object value) {
        String target = (String) getParameter("target");

        Map<Integer, String> map = (Map<Integer, String>) value;
        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            if (entry.getValue().equals(target)) {
                emit(entry.getValue(), 1);
            }
        }

        System.out.println("one mapper done");
    }

}