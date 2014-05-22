package org.apache.pig.extension.pigudf;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;
import java.util.Map;

public class JSONArrayToMap extends EvalFunc<String> {
	private static final Logger LOG = LoggerFactory
			.getLogger(JSONArrayToMap.class);

	public String exec(Tuple input) throws IOException {
		try {
			DataBag bag = null;
			StringBuffer result = new StringBuffer("");
			if (input != null && input.size() > 0) {
				Object obj = (Object) input.get(0);
				if (obj instanceof Map) {
					Map<String, Object> map = (Map<String, Object>) input
							.get(0);

					if (map == null) {
						return "";
					}
					bag = (DataBag) map.get("key");
				} else if (obj instanceof DataBag) {
					bag = (DataBag) input.get(0);
					
				}
				if (bag == null)
					return "";
				Iterator itr = bag.iterator();
				while (itr.hasNext()) {
					Tuple tuple = (Tuple) itr.next();
					String state = (String) tuple.get(0);
					result.append(state).append(",");
				}
				if (result.length() > 0)
					result.deleteCharAt(result.length() - 1);
				return result.toString();
			} else {
				return "";
			}

		} catch (Exception e) {
			return "";

		}
	}
}
