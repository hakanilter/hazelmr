package hazelmr.localmap;

import hazelmr.Reducer;

import java.util.Collection;

/**
 * User: hilter
 * Date: 8/5/13
 * Time: 4:01 PM
 */
public class LocalMapTestReducer extends Reducer<String, Integer, String, Integer> {

    @Override
    public void reduce(String key, Collection<Integer> values) {
        emit(key, values.size());
    }

}