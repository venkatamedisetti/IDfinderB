/////////////////////////////////////////////////////////////////////////
// Project Shared Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.common.Get_Job;

import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import java.util.Date;

import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.shared.Globals;
import com.k2view.cdbms.shared.user.UserCode;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.func.oracle.OracleToDate;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.fabric.common.ini.Configurator;
import com.k2view.fabric.commonArea.producer.kafka.KafkaAdminProperties;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.json.JSONObject;

import static com.k2view.cdbms.shared.user.UserCode.*;
import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.usercode.common.SharedGlobals.DB_CASS_NAME;
import static com.k2view.cdbms.usercode.common.SharedLogic.*;


@SuppressWarnings({"unused", "DefaultAnnotationParam"})
public class SharedLogic {


    @type(UserJob)
    public static void deltaJobsExecutor() throws Exception {
        String topicName = "Delta_cluster_" + getLuType().luName.toUpperCase();
        int partitions = fnGetTopParCnt(topicName);
        // parser must be started from userJob, otherwise, exact topic name must be given
        for (int i = 0; i < partitions; i++) {
            db("FabricDB").execute("startjob PARSER NAME='" + getLuType().luName + ".deltaIid' UID='deltaIid_" + i + "' AFFINITY='FINDER_DELTA' ARGS='{\"topic\":\"" + topicName + "\"," + "\"partition\":\"" + i + "\"}'");
        }
    }


    @out(name = "result", type = Integer.class, desc = "")
    public static Integer fnGetTopParCnt(String topicName) throws Exception {
        final KafkaAdminProperties kafkaCommon = (KafkaAdminProperties) Configurator.load(KafkaAdminProperties.class);
        Producer<String, JSONObject> producer = null;
        try {
            Properties props = kafkaCommon.getProperties();
            if (!props.contains("key.serializer")) {
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            }
            if (!props.contains("value.serializer")) {
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            }
            producer = new KafkaProducer<>(props);
            int numOfPar = producer.partitionsFor(topicName).size();
            return numOfPar;
        } finally {
            if (producer != null) producer.close();
        }
    }

	@type(UserJob)
	public static void fnGetJobManager() throws Exception {
		if (Boolean.parseBoolean(getLuType().ludbGlobals.get("GET_JOBS_IND") + "")) {
		    final String topicName = "Delta_cluster_" + getLuType().luName.toUpperCase();
		    final int to = Integer.parseInt(getLuType().ludbGlobals.get("GET_JOB_START_TIME") + "");
		    final int from = Integer.parseInt(getLuType().ludbGlobals.get("GET_JOB_STOP_TIME") + "");
		    Date date = new Date();
		    Calendar c = Calendar.getInstance();
		    c.setTime(date);
		    int t = c.get(Calendar.HOUR_OF_DAY) * 100 + c.get(Calendar.MINUTE);
		    boolean isBetween = to > from && t >= from && t <= to || to < from && (t >= from || t <= to);
		
		    Object parserCount = null;
		    try (Db.Rows rs = db(DB_CASS_NAME).fetch("SELECT count(*) from k2system.k2_jobs WHERE type = 'PARSER' and name = '" + getLuType().luName + ".deltaIid' and status = 'IN_PROCESS' ALLOW FILTERING ")) {
		        parserCount = rs.firstValue();
		    }
		
		    long lag = (Long)getGroupLag();
		    log.info(String.format("fnGetJobManager: Running LU: %s Current Lag: %s Time Validation Result: %s", getLuType().luName, lag, isBetween));
		    if (isBetween || lag > Long.parseLong(getLuType().ludbGlobals.get("LAG_THRESHOLD") + "")) {
		        log.info(String.format("fnGetJobManager: Stopping Get Job Parser For %s", topicName));
		        if (parserCount != null && Integer.parseInt((parserCount + "")) > 1) {
		            db("FabricDB").execute("stopparser " + getLuType().luName + " deltaIid");
		        }
		    } else {
		        int partitions = fnGetTopParCnt(topicName);
		        log.info(String.format("fnGetJobManager: Starting Get Job Parser For %s Total Number Of Partitions:%s", topicName, partitions));
		        if (parserCount == null || Integer.parseInt((parserCount + "")) < partitions) {
		            db("FabricDB").execute("startjob USER_JOB name='" + getLuType().luName + ".deltaJobsExecutor'");
		        }
		    }
		}
	}

	@out(name = "result", type = Object.class, desc = "")
	public static Object getGroupLag() throws Exception {
		String clustName = "";
		try(Db.Rows rs = db(DB_CASS_NAME).fetch("select cluster_name from system.local")){
		    Object rowVal = rs.firstValue();
		    if(rowVal != null)clustName = rowVal + "";
		}
		long lag = -1;
		
		StringBuilder LuTopicsList = new StringBuilder();
		//String prefix = "";
		//for (String topicName : getTranslationsData("trnLUKafkaTopics").keySet()) {
		//    LuTopicsList.append(prefix + topicName.replace("\n", "").replace("\r", ""));
		//    prefix = "|";
		//}
		
		BufferedReader reader = null;
		InputStreamReader isr = null;
		InputStream isrErr = null;
		Process p = null;
		try {
		    p = Runtime.getRuntime().exec(new String[]{"bash", "-c", "/opt/apps/kafka/kafka/bin/kafka-consumer-groups --bootstrap-server " + IifProperties.getInstance().getKafkaBootsrapServers() + " --describe --group IDfinderGroupId_" + clustName + " --command-config " + System.getenv("K2_HOME") + "/.kafka_ssl/client-ssl.properties|grep -w -E '" + LuTopicsList.toString() + "'|awk '{lag += $5}END {print lag}'"});
		    p.waitFor();
		    isr = new InputStreamReader(p.getInputStream());
		    isrErr = p.getErrorStream();
		    if (isrErr.available() > 0) {
		        log.warn("fnGetJobManager: Failed Getting Group Lag For:" + "IDfinderGroupId_" + clustName);
		        log.warn(IOUtils.toString(isrErr, StandardCharsets.UTF_8.name()));
		    } else {
		        reader = new BufferedReader(isr);
		        String line;
		        while (reader != null && (line = reader.readLine()) != null) {
		            if(line.matches("[0-9]+"))lag = Long.parseLong(line);
		        }
		    }
		} finally {
		    if (isr != null) isr.close();
		    if (isrErr != null) isrErr.close();
		    if (reader != null) reader.close();
		    if (p != null) p.destroyForcibly();
		}
		return lag;
	}


}
