/////////////////////////////////////////////////////////////////////////
// LU Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.lu.LOOKUP.LookupConsumerManager;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.k2view.cdbms.finder.DataChange;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.shared.Globals;
import com.k2view.cdbms.shared.user.UserCode;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.func.oracle.OracleToDate;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.cdbms.usercode.lu.LOOKUP.*;
import org.json.JSONObject;

import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.usercode.common.SharedLogic.*;
import static com.k2view.cdbms.usercode.lu.LOOKUP.Globals.*;
import static com.k2view.cdbms.usercode.lu.LOOKUP.Kafka.Logic.fnGetTopParCnt;

@SuppressWarnings({"unused", "DefaultAnnotationParam"})
public class Logic extends UserCode {


	@type(UserJob)
	public static void LookupManager() throws Exception {
		String wholeName = getLuType().luName + "." + "ParserLookupJob";
		try {
		    Map<String, TreeSet<Integer>> topicParMap = new HashMap<>();
		    Map<String, Map<String, String>> topicsMapDetails = getTranslationsData("trnLookupTopics");
		    for (Map.Entry<String, Map<String, String>> topicMapDetails : topicsMapDetails.entrySet()) {
		        TreeSet<Integer> parSet = new TreeSet<>();
		        if (topicMapDetails.getValue().get("Split_By_Partition") != null && "true".equalsIgnoreCase(topicMapDetails.getValue().get("Split_By_Partition"))) {
		            int numOfPar = fnGetTopParCnt(topicMapDetails.getKey());
		            for (int i = 0; i < numOfPar; i++) {
		                parSet.add(i);
		            }
		        }
		        topicParMap.put(topicMapDetails.getKey(), parSet);
		    }
		    //loop over all user jobs in k2_jobs table
		    Db.Rows rs = db(DB_CASS_NAME).fetch("select arguments,status from k2system.k2_jobs where type='USER_JOB' and name= ? ", wholeName);
		    for (Db.Row row : rs) {
		        String args = row.cell(0) + "";
		        String status = row.cell(1) + "";
		        if ("IN_PROCESS".equalsIgnoreCase(status) || "WAITING".equalsIgnoreCase(status)) {
		            JsonObject argsDet = new com.google.gson.JsonParser().parse(args).getAsJsonObject();
		            String topicName = argsDet.get("Topic_Name").getAsString();
		            if (topicParMap.get(topicName).size() != 0)
		                topicParMap.get(topicName).remove(((TreeSet) topicParMap.get(topicName)).last());
		            if (topicParMap.get(topicName).size() == 0) topicParMap.remove(topicName);
		        }
		    }
		
		    // loop over topics set
		    for (String topic : topicParMap.keySet()) {
		        Set<Integer> parSet = topicParMap.get(topic);
		        if (parSet.size() == 0)
		            DBExecute("FabricDB", "startjob USER_JOB NAME='" + wholeName + "' " + " UID='" + UUID.randomUUID() + "' ARGS='{\"Topic_Name\":\"" + topic + "\"}' AFFINITY='X_KAFKA_CONSUMERS'", null);
		        for (Integer partition : parSet) {
		            DBExecute("FabricDB", "startjob USER_JOB NAME='" + wholeName + "' " + " UID='" + UUID.randomUUID() + "' ARGS='{\"Topic_Name\":\"" + topic + "\"}' AFFINITY='X_KAFKA_CONSUMERS'", null);
					Thread.sleep(2000);
		        }
		    }
		} catch (Exception e) {
		    log.error("LookupManager", e);
		}
	}




	@type(UserJob)
	public static void ParserLookupJob(String Topic_Name) throws Exception {
		String clustName = "" + DBSelectValue(DB_CASS_NAME,"select cluster_name from system.local",null);
		String groupId = GROUP_ID + "_" + clustName + "_" + Topic_Name;
		
		LookupConsumer lookUpsKafkaConsumer = new LookupConsumer(groupId, Topic_Name, UserCode::DBExecute, UserCode::DBQuery, UserCode::DBSelectValue); 
		log.info("GROUP ID -  " + groupId + " START CONSUMING For TOPIC - " + Topic_Name);
		lookUpsKafkaConsumer.poll(); 
	}

	
	
	
	
}
