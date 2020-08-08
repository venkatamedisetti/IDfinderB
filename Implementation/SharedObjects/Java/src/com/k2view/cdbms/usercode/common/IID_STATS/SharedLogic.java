/////////////////////////////////////////////////////////////////////////
// Project Shared Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.common.IID_STATS;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;

import com.datastax.driver.core.Host;
import com.k2view.cdbms.cluster.CassandraClusterSingleton;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.shared.user.UserCode;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.func.oracle.OracleToDate;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;
import com.k2view.cdbms.usercode.common.SendEmail;
import com.k2view.fabric.common.stats.CustomStats;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import static com.k2view.cdbms.lut.FunctionDef.functionContext;
import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.shared.user.UserCode.*;
import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.usercode.common.Get_Job.SharedLogic.fnGetTopParCnt;
import static com.k2view.cdbms.usercode.common.SharedGlobals.*;
import static com.k2view.cdbms.usercode.lu.WS_TEST.WSExecute.fnInvokeWS;

@SuppressWarnings({"unused", "DefaultAnnotationParam"})
public class SharedLogic {


	public static void fnInsRecToLUStats(String operation_name, String operation_value, String status, Long jobRunSeq) throws Exception {
		db(DB_CASS_NAME).execute("Insert into " + getLuType().getKeyspaceName() + ".lu_stats (operation_name, operation_value, status, last_update_time, job_run_seq) values (?,?,?,?,?)", operation_name, operation_value, status, new Timestamp(System.currentTimeMillis()), jobRunSeq);
	}

	@type(UserJob)
	public static void fnGetLUStats() throws Exception {
		long jobRunSeq = 1;
		try (Db.Rows rs = db(DB_CASS_NAME).fetch("SELECT max(job_run_seq) from " + getLuType().getKeyspaceName() + ".lu_stats")) {
		    Object rowVal = rs.firstValue();
		    if (rowVal != null) {
		        jobRunSeq = Long.parseLong(rowVal + "") + 1;
		    }	
		}
		fnGetIIDStats(jobRunSeq);
		fnGetIIDFinderStats(jobRunSeq);
		fnGeLUDeltaTopicInfo(jobRunSeq);
	}


	public static void fnGetIIDStats(Long jobRunSeq) throws Exception {
		double totalAVG = 0;
		long totalIIDPassed = 0;
		long totalIIDFailed = 0;
		try (Db.Rows rs = db(DB_CASS_NAME).fetch("SELECT node, uid, status, iid_avg_run_time_in_sec, total_iid_passed, total_iid_failed from " + getLuType().getKeyspaceName() + ".iid_get_rate")) {
		    for (Db.Row row : rs) {
		        if ("Running".equalsIgnoreCase(("" + row.cell(2)))) {
		            totalAVG += Double.parseDouble(row.cell(3) + "");
		            totalIIDPassed += Long.parseLong(row.cell(4) + "");
		            totalIIDFailed += Long.parseLong(row.cell(5) + "");
		        }
		    }
		    fnInsRecToLUStats("total_iids_received", totalIIDPassed + totalIIDFailed + "", "completed", jobRunSeq);
		    fnInsRecToLUStats("total_iids_passed", totalIIDPassed + "", "completed", jobRunSeq);
		    fnInsRecToLUStats("total_iids_failed", totalIIDFailed + "", "completed", jobRunSeq);
		    fnInsRecToLUStats("avg_iid_per_sec", totalAVG + "", "completed", jobRunSeq);
		} catch (Exception e) {
		    fnInsRecToLUStats("total_iids_received", "0", "failed", jobRunSeq);
		    fnInsRecToLUStats("total_iids_passed", "0", "failed", jobRunSeq);
		    fnInsRecToLUStats("total_iids_failed", "0", "failed", jobRunSeq);
		    fnInsRecToLUStats("avg_iid_per_sec", "0", "failed", jobRunSeq);
		}
	}


	public static void fnGetIIDFinderStats(Long jobRunSeq) throws Exception {
		String clustName = "";
		try (Db.Rows rs = db(DB_CASS_NAME).fetch("select cluster_name from system.local")) {
		    Object rowVal = rs.firstValue();
		    if (rowVal != null) clustName = "" + rowVal;
		}
		long group_offset_pre = -1;
		long topics_offset_pre = -1;
		long group_offset = -1;
		long topics_offset = -1;
		long lag = -1;
		boolean failed = false;
		String SSL = "";
		if (com.k2view.cdbms.shared.IifProperties.getInstance().getProp().getSection("finder_kafka_ssl_properties").getBoolean("SSL_ENABLED", false)) {
			SSL = " --command-config " + System.getenv("K2_HOME") + "/.kafka_ssl/client-ssl.properties";
		}
		StringBuilder LuTopicsList = new StringBuilder();
		String prefix = "";
		for (String topicName : getTranslationsData("trnLUKafkaTopics").keySet()) {
		    LuTopicsList.append(prefix + topicName.replace("\n", "").replace("\r", ""));
		    prefix = "|";
		}
		
		boolean run = true;
		String errorMSG = "";
		outerloop:
		while (run) {
		    BufferedReader reader = null;
		    InputStream isrErr = null;
		    InputStreamReader isr = null;
		    Process p = null;
		
		    try {
		        p = Runtime.getRuntime().exec(new String[]{"bash", "-c", PATH_TO_KAFKA_BIN + "kafka-consumer-groups --bootstrap-server " + IifProperties.getInstance().getKafkaBootsrapServers() + " --describe --group IDfinderGroupId_" + clustName + SSL + "|grep -w -E '" + LuTopicsList.toString() + "'|awk '{consume += $3} {offset += $4} {lag += $5}END {print consume,offset,lag}'"});
		        p.waitFor();
		        isr = new InputStreamReader(p.getInputStream());
		        isrErr = p.getErrorStream();
		        if (isrErr.available() > 0) {
					errorMSG = "fnGetIIDFinderStats: Failed Getting Group Lag For:" + "IDfinderGroupId_" + clustName + ", " + IOUtils.toString(isrErr, StandardCharsets.UTF_8.name());
		            failed = true;
		            break;
		        } else {
		            reader = new BufferedReader(isr);
		            String line;
		            while (reader != null && (line = reader.readLine()) != null) {
		                String[] vals = line.split(" ");
		                if(vals.length != 3) {
							errorMSG = "fnGetIIDFinderStats: Unexpected script output error!, " + line;
							failed = true;
							break outerloop;
						}
		                if (group_offset_pre == -1) {
		                    group_offset_pre = Long.parseLong(vals[0]);
		                    topics_offset_pre = Long.parseLong(vals[1]);
		                } else {
		                    run = false;
		                    group_offset = Long.parseLong(vals[0]);
		                    topics_offset = Long.parseLong(vals[1]);
		                    lag = Long.parseLong(vals[2]);
		                }
		            }
		        }
		    } finally {
		        if (isr != null) isr.close();
		        if (isrErr != null) isrErr.close();
		        if (reader != null) reader.close();
		        if (p != null) p.destroyForcibly();
		    }
		    Thread.sleep(10000);
		}
		if (failed) {
			log.warn(errorMSG);
		    fnInsRecToLUStats("iidfinder_consumption_rate_sec", "", "failed to get stats - " + errorMSG, jobRunSeq);
		    fnInsRecToLUStats("iidfinder_kafka_inset_rate_sec", "", "failed to get stats - " + errorMSG, jobRunSeq);
		    fnInsRecToLUStats("iidfinder_lag", "-1", "failed to get stats - " + errorMSG, jobRunSeq);
		    fnInsRecToLUStats("iidfinder_group_offset", "-1", "failed to get stats - " + errorMSG, jobRunSeq);
		} else {
		    fnInsRecToLUStats("iidfinder_consumption_rate_sec", ((group_offset - group_offset_pre) / 10) + "", "completed", jobRunSeq);
		    fnInsRecToLUStats("iidfinder_kafka_inset_rate_sec", ((topics_offset - topics_offset_pre) / 10) + "", "completed", jobRunSeq);
		    fnInsRecToLUStats("iidfinder_lag", lag + "", "completed", jobRunSeq);
		    fnInsRecToLUStats("iidfinder_group_offset", group_offset_pre + "", "completed", jobRunSeq);
		}
	}


	public static void fnGeLUDeltaTopicInfo(Long jobRunSeq) throws Exception {
		long group_offset_pre = -1;
		long topics_offset_pre = -1;
		long group_offset = -1;
		long topics_offset = -1;
		long lag = -1;
		boolean failed = false;
		boolean run = true;
		String errorMSG = "";
		
		outerloop:
		while (run) {
		    BufferedReader reader = null;
		    InputStream isrErr = null;
		    InputStreamReader isr = null;
		    Process p = null;
		
		    try {
		        p = Runtime.getRuntime().exec(new String[]{"bash", "-c", PATH_TO_KAFKA_BIN + "kafka-consumer-groups --bootstrap-server " + IifProperties.getInstance().getProp().getSection("delta_kafka_producer").getString("BOOTSTRAP_SERVERS") + " --describe --group " + GET_PARSER_GROUP_ID + "|grep Delta_cluster_" + getLuType().luName.toUpperCase() + "|awk '{consume += $3} {offset += $4} {lag += $5}END {print consume,offset,lag}'"});
		        p.waitFor();
		        isr = new InputStreamReader(p.getInputStream());
		        isrErr = p.getErrorStream();
				String err = IOUtils.toString(isrErr, StandardCharsets.UTF_8.name());
		        if (isrErr.available() > 0 && !err.contains("has no active members")) {
					errorMSG = "fnGeLUDeltaTopicLag: Failed Getting Delta Topic Info For:Delta_cluster_" + getLuType().luName.toUpperCase() + ", " + err;
		            failed = true;
		            break;
		        } else {
		            reader = new BufferedReader(isr);
		            String line;
		            while (reader != null && (line = reader.readLine()) != null) {
		                String[] vals = line.split(" ");
		                if(vals.length != 3) {
							errorMSG = "fnGeLUDeltaTopicLag: Unexpected script output error!, " + line;
							failed = true;
							break outerloop;
						}
		                if (group_offset_pre == -1) {
		                    group_offset_pre = Long.parseLong(vals[0]);
		                    topics_offset_pre = Long.parseLong(vals[1]);
		                } else {
		                    run = false;
		                    group_offset = Long.parseLong(vals[0]);
		                    topics_offset = Long.parseLong(vals[1]);
		                    lag = Long.parseLong(vals[2]);
		                }
		            }
		        }
		    } finally {
		        if (isr != null) isr.close();
		        if (isrErr != null) isrErr.close();
		        if (reader != null) reader.close();
		        if (p != null) p.destroyForcibly();
		    }
		    Thread.sleep(10000);
		}
		if (failed) {
			log.warn(errorMSG);
		    fnInsRecToLUStats("parser_get_consumption_rate_sec", "", "failed to get stats - " + errorMSG, jobRunSeq);
		    fnInsRecToLUStats("iidfinder_insert_rate_to_delta_topic_sec", "", "failed to get stats - " + errorMSG, jobRunSeq);
		    fnInsRecToLUStats("delta_topic_lag", "-1", "failed to get stats - " + errorMSG, jobRunSeq);
		    fnInsRecToLUStats("delta_topic_offset", "-1", "failed to get stats - " + errorMSG, jobRunSeq);
		} else {
		    fnInsRecToLUStats("parser_get_consumption_rate_sec", ((group_offset - group_offset_pre) / 10) + "", "completed", jobRunSeq);
		    fnInsRecToLUStats("iidfinder_insert_rate_to_delta_topic_sec", ((topics_offset - topics_offset_pre) / 10) + "", "completed", jobRunSeq);
		    fnInsRecToLUStats("delta_topic_lag", lag + "", "completed", jobRunSeq);
		    fnInsRecToLUStats("delta_topic_offset", group_offset_pre + "", "completed", jobRunSeq);
		}
	}
	


	@type(UserJob)
	public static void fnSendLUsStats() throws Exception {
		final String getLuStatsMaxSeq = "SELECT max(job_run_seq) FROM %s.lu_stats";
		final String getLuStatsInfo = "SELECT operation_name, last_update_time, operation_value, status from %s.lu_stats where job_run_seq = ?";
		final String getIDFinderStats = "SELECT sum(operation_value) from %s.idfinder_stats WHERE operation_name = ? ALLOW FILTERING";
		final String htmlBody = "<!DOCTYPE html><html><head><style>table {font-family: arial, sans-serif;border-collapse: collapse;width: 100%;}td, th {border: 1px solid #dddddd;text-align: left;padding: 8px;}tr:nth-child(even) {background-color: #dddddd;}</style></head><body><h2 align=\"center\">Location LUs Status</h2>";
		final com.k2view.cdbms.lut.DbInterface interfaceDetails = (com.k2view.cdbms.lut.DbInterface) InterfacesManager.getInstance().getInterface(DB_CASS_NAME);
		String[] luTypes = STATS_EMAIL_REPORT_LUS.split(",");
		Set<String> operationsList = new HashSet<>();
		StringBuilder tableRS = new StringBuilder().append("<table><tr><th>LU</th><th>Operation</th><th>Result</th><th>Update Time</th><th>Status</th></tr>");
		StringBuilder headerAggRs = new StringBuilder().append("<table><tr><th>LU</th>");
		Map<String, Map<String, Object>> statsResult = new HashMap<>();
		operationsList.add("Deleted_Delta_Plus_Orphans");
		operationsList.add("Delta_Table_Count");
		operationsList.add("Orphan_Table_Count");
		
		for (String luType : luTypes) {
			luType = luType.trim();
		    headerAggRs.append("<th>" + luType + "</th>");
		    Map<String, Object> statsResultLU = new HashMap<>();
		    String luKeyspace = "k2view_" + luType.toLowerCase();
		    Object rs = db(DB_CASS_NAME).fetch(String.format(getLuStatsMaxSeq, luKeyspace)).firstValue();
		    long jobRunSeq;
		    if (rs == null) {
		        continue;
		    } else {
		        jobRunSeq = Long.parseLong("" + rs);
		    }
		
		    getLUStatistics(jobRunSeq, getLuStatsInfo, tableRS, operationsList, statsResultLU, getIDFinderStats, luKeyspace, luType);
		    getIDFinderStats(tableRS, operationsList, statsResultLU, getIDFinderStats, luKeyspace, luType);
		
		    statsResult.put(luType, statsResultLU);
		}
		tableRS.append("</table>");
		headerAggRs.append("</tr>");
		String aggRs = aggResults(statsResult, headerAggRs.toString(), luTypes, operationsList);
		String emailBody = htmlBody + aggRs + "<br><br>" + tableRS.toString() + "</body></html>";
		
		String emailSender = "";
		String emailSenderHost = "";
		int emailSenderPort = 0;
		String emailList = "";
		Map<String, Map<String, String>> emailTrnRs = getTranslationsData("trnEmailDetails");
		for (Map.Entry<String, Map<String, String>> emailTrnRsEnt : emailTrnRs.entrySet()) {
		    Map<String, String> emailDet = emailTrnRsEnt.getValue();
		    emailSender = emailDet.get("EMAIL_SENDER_ADDRESS");
		    emailSenderHost = emailDet.get("EMAIL_SENDER_HOST");
		    emailSenderPort = Integer.parseInt(emailDet.get("EMAIL_SENDER_PORT"));
			emailList = emailDet.get("EMAIL_LISTS");
		}
		SendEmail.sendEmail(emailList, emailSender, emailSenderHost, emailSenderPort, null, null, 0, "Location LUs Status - " + CassandraClusterSingleton.getInstance().getClusterName(), emailBody, null);
	}

    private static void getLUStatistics(long jobRunSeq, String getLuStatsInfo, StringBuilder tableRS, Set<String> operationsList, Map<String, Object> statsResultLU, String getIDFinderStats, String luKeyspace, String luType) throws SQLException {
        try (Db.Rows rs1 = db(DB_CASS_NAME).fetch(String.format(getLuStatsInfo, luKeyspace), jobRunSeq)) {
            for (Db.Row row : rs1) {
                if (row.get("operation_name").toString().startsWith("total_")) continue;
                statsResultLU.put(row.get("operation_name").toString(), row.get("operation_value"));
                operationsList.add(row.get("operation_name").toString());
                tableRS.append("<tr><td>" + luType + "</td>");
                tableRS.append("<td>" + row.get("operation_name") + "</td>");
                tableRS.append("<td>" + row.get("operation_value") + "</td>");
                tableRS.append("<td>" + row.get("last_update_time") + "</td>");
                tableRS.append("<td>" + row.get("status") + "</td></tr>");
            }
        }
    }

    private static void getIDFinderStats(StringBuilder tableRS, Set<String> operationsList, Map<String, Object> statsResultLU, String getIDFinderStats, String luKeyspace, String luType) throws SQLException {
        String[] operations = new String[]{"IDFInder_Orphan_Count", "IDFInder_Delta_Count", "IDFInder_Deleted_Deltas_Count"};
        long IDFInder_Deleted_Deltas_Count = 0;
        long IDFInder_Orphan_Count = 0;
        for (String operation : operations) {
            Object rs1 = db(DB_CASS_NAME).fetch(String.format(getIDFinderStats, luKeyspace), operation).firstValue();
            if ("IDFInder_Deleted_Deltas_Count".equals(operation)) {
                IDFInder_Deleted_Deltas_Count = Long.parseLong("" + rs1);
            }
            if ("IDFInder_Orphan_Count".equals(operation)) {
                IDFInder_Orphan_Count = Long.parseLong("" + rs1);
            }
            statsResultLU.put(operation, rs1);
            operationsList.add(operation);
            tableRS.append("<tr><td>" + luType + "</td>");
            tableRS.append("<td>" + operation + "</td>");
            tableRS.append("<td>" + Long.parseLong("" + rs1) + "</td>");
            tableRS.append("<td></td>");
            tableRS.append("<td></td></tr>");
        }
        statsResultLU.put("Deleted_Delta_Plus_Orphans", (IDFInder_Orphan_Count + IDFInder_Deleted_Deltas_Count));
    }

    private static String aggResults(Map<String, Map<String, Object>> statsResult, String headerAggRs, String[] luTypes, Set<String> operationsList) {
        StringBuilder tableRS = new StringBuilder().append(headerAggRs);
        for (String operation : operationsList) {
            tableRS.append("<tr><td>" + operation + "</td>");
            for (String luType : luTypes) {
				luType = luType.trim();
                Map<String, Object> luRS = statsResult.get(luType);
                tableRS.append("<td>" + luRS.get(operation) + "</td>");
            }
            tableRS.append("</tr>");
        }
        tableRS.append("</table>");

        return tableRS.toString();
    }

}
