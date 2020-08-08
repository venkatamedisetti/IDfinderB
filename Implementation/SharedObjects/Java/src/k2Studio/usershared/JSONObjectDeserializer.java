package k2Studio.usershared;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import com.k2view.fabric.common.Log;
import org.json.JSONObject;

public class JSONObjectDeserializer implements Deserializer<JSONObject> {

	private static final Log logger = Log.a(JSONObjectDeserializer.class);

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public JSONObject deserialize(String topic, byte[] data) {
		if (data != null) {
			try {
				return new JSONObject(new String(data));
			} catch (Exception e) {
				logger.error("Failed to deserialize message for topic " + topic, e);
			}
		}
		return null;
	}

	@Override
	public void close() {

	}

}