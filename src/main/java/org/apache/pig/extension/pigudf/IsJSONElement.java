package org.apache.pig.extension.pigudf;
import java.io.IOException;
import java.util.Map;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IsJSONElement extends EvalFunc<Boolean> {
      private static final Logger LOG = LoggerFactory.getLogger(IsJSONElement.class);

      @SuppressWarnings({ "unchecked" })
      @Override
      public Boolean exec(Tuple input) throws IOException {
            Map<String, Object> jsonMap = (Map<String, Object>) input.get(0);
            String jsonKeyPath = (String) input.get(1);
            String pattern = (String) input.get(2);
            if(jsonMap == null || jsonKeyPath == null || jsonKeyPath.isEmpty())
            	return false;
            String[] jsonKeys = jsonKeyPath.split("\\.");
            if(jsonKeys.length == 0)
            	return false;
            // iterate over sub keys, if element type is string and the value matches pattern then return true
            Map<String, Object> map = jsonMap;
            for (String key : jsonKeys) {
                  Object val = map.get(key);
                  if(val == null)
                    return false; 
                  if (val instanceof Map) {
                    map = (Map<String, Object>) val;
                  } else if (val instanceof String ){
                	   String stringVal = (String) val;
                	   if(!stringVal.isEmpty()&& stringVal.equalsIgnoreCase(pattern))
                	   		return true;
                	   else
                		   return false;
                  }
                  else// when its bag
                    return false;
             }
             return true;
      }
}
