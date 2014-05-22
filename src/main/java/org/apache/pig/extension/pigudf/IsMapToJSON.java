package org.apache.pig.extension.pigudf;
import java.io.IOException;
import java.util.Map;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IsMapToJSON extends EvalFunc<Boolean> {
      private static final Logger LOG = LoggerFactory.getLogger(IsMapToJSON.class);

      @SuppressWarnings({ "unchecked" })
      @Override
      public Boolean exec(Tuple input) throws IOException {
            Map<String, Object> jsonMap = (Map<String, Object>) input.get(0);
            String jsonKeyPath = (String) input.get(1);
            if(jsonMap == null || jsonKeyPath == null || jsonKeyPath.isEmpty())
            	return false;
            
            String[] jsonKeys = jsonKeyPath.split("\\.");
            if(jsonKeys.length == 0)
            	return false;
            // iterate over subkeys
            Map<String, Object> map = jsonMap;
            for (String key : jsonKeys) {
                  Object val = map.get(key);
                  if(val == null)
                    return false;
                  if (val instanceof Map) {
                    map = (Map<String, Object>) val;
                  } else
                    return false;
             }
             return true;
      }
     
}
