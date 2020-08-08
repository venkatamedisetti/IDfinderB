/////////////////////////////////////////////////////////////////////////
// Project Shared Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.common.IDFinder_Stats;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;

import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.shared.user.UserCode;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.func.oracle.OracleToDate;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;
import org.json.JSONObject;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.shared.user.UserCode.*;
import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.usercode.common.SharedGlobals.*;
import static com.k2view.cdbms.usercode.lu.WS_TEST.WSExecute.fnInvokeWS;

@SuppressWarnings({"unused", "DefaultAnnotationParam"})
public class SharedLogic {


	@type(UserJob)
	public static void fnGetIDFDeletedDeltas() throws Exception {
		final String insToIDFinderStatsTable = "Insert into " + getLuType().getKeyspaceName() + ".idfinder_stats (host, operation_name, operation_value, status, last_update_time,iteration) values (?,?,?,?,?,?)";
		final String getValueFromIDFinderStatsTable = "select operation_value from " + getLuType().getKeyspaceName() + ".idfinder_stats where host  = ? and operation_name = ? and iteration = ?";
		final String maxItr = "SELECT max(iteration) FROM " + getLuType().getKeyspaceName() + ".idfinder_stats WHERE host = ? and operation_name = ?";
		
		List<String> failedHosts = new ArrayList<>();
		if (getCustomProperties("wsToken") == null || getCustomProperties("wsToken").get("Password") == null || "".equals(getCustomProperties("wsToken").get("Password"))) {
		    log.warn("Can't find WS Token!, Please check.");
		    return;
		}
		
		try (Db.Rows rs2 = db("FabricDB").fetch("clusterstatus")) {
		    for (Db.Row row : rs2) {
		        String host = (row.cell(2) + "").trim();
				if("".equals(host))continue;
		        int iteration = 0;
		        try (Db.Rows rs3 = db(DB_CASS_NAME).fetch(maxItr, host, "IDFInder_Deleted_Deltas_Count")) {
		            Object rs1 = rs3.firstValue();
		            if (rs1 != null) {
		                iteration = Integer.parseInt(rs1 + "");
		            }
		        }
		
		        long deleted_deltas = 0;
		        try (Db.Rows rs3 = db(DB_CASS_NAME).fetch(getValueFromIDFinderStatsTable, host, "IDFInder_Deleted_Deltas_Count", iteration)) {
		            Object rs1 = rs3.firstValue();
		            if (rs1 != null) {
		                deleted_deltas = Long.parseLong(rs1 + "");
		            }
		        }
		        try {
		            String rs = fnInvokeWS("GET", "http://" + host + ":3213/api/wsGetDeletedDeltas?token=" + getCustomProperties("wsToken").get("Password") + "&lu_name=" + getLuType().luName.toLowerCase() + "&format=json", null, null, 0, 0);
		            if (!"".equals(rs)) {
		                JSONObject IIDJSon = new JSONObject(rs);
		                deleted_deltas = Long.parseLong(IIDJSon.get("deleted_iids") + "");
		                if (deleted_deltas > Long.parseLong(IIDJSon.get("deleted_iids") + "")) {
		                    iteration++;
		                }
		            }
		            db(DB_CASS_NAME).execute(insToIDFinderStatsTable, host, "IDFInder_Deleted_Deltas_Count", deleted_deltas, "completed", new Timestamp(System.currentTimeMillis()), iteration);
		        } catch (Exception e) {
		            db(DB_CASS_NAME).execute(insToIDFinderStatsTable, host, "IDFInder_Deleted_Deltas_Count", deleted_deltas, "failed to get stats", new Timestamp(System.currentTimeMillis()), iteration);
		        }
		    }
		}
	}


	@type(UserJob)
	public static void fnGetIDFinderJMXStats() throws Exception {
		final String insToIDFinderStatsTable = "Insert into " + getLuType().getKeyspaceName() + ".idfinder_stats (host, operation_name, operation_value, status, last_update_time,iteration) values (?,?,?,?,?,?)";
		final String getValueFromIDFinderStatsTable = "select operation_value from " + getLuType().getKeyspaceName() + ".idfinder_stats where host  = ? and operation_name = ? and iteration = ?";
		final String maxItr = "SELECT max(iteration) FROM " + getLuType().getKeyspaceName() + ".idfinder_stats WHERE host = ? and operation_name = ?";
		
		Map<String, String[]> environment = new HashMap<>();
		Map<String, String> IDFCre = getCustomProperties("IDFinderJMX");
		String hostList = IDFCre.get("Host");
		String jmxPort = IDFCre.get("Port");
		String jmxUser = IDFCre.get("User");
		String jmxPassword = IDFCre.get("Password");
		environment.put(JMXConnector.CREDENTIALS, new String[]{jmxUser, jmxPassword});
		
		for (String ip : hostList.split(",")) {
		    ip = ip.trim();
		    if("".equals(ip))continue;
		
		    int iterationDelta = 0;
		    try (Db.Rows rs3 = db(DB_CASS_NAME).fetch(maxItr, ip, "IDFInder_Delta_Count")) {
		        Object rs1 = rs3.firstValue();
		        if (rs1 != null) {
					iterationDelta = Integer.parseInt(rs1 + "");
		        }
		    }
		
		    long totalInsertToDelta = 0;
		    try (Db.Rows rs3 = db(DB_CASS_NAME).fetch(getValueFromIDFinderStatsTable, ip, "IDFInder_Delta_Count", iterationDelta)) {
		        Object rs = rs3.firstValue();
		        if (rs != null) {
		            totalInsertToDelta = Long.parseLong(rs + "");
		        }
		    }
		
			int iterationOrphan = 0;
			try (Db.Rows rs3 = db(DB_CASS_NAME).fetch(maxItr, ip, "IDFInder_Orphan_Count")) {
				Object rs1 = rs3.firstValue();
				if (rs1 != null) {
					iterationOrphan = Integer.parseInt(rs1 + "");
				}
			}
		    long totalInsertToOrphan = 0;
		    try (Db.Rows rs3 = db(DB_CASS_NAME).fetch(getValueFromIDFinderStatsTable, ip, "IDFInder_Orphan_Count", iterationOrphan)) {
		        Object rs = rs3.firstValue();
		        if (rs != null) {
		            totalInsertToOrphan = Long.parseLong(rs + "");
		        }
		    }
		    log.info("JMX STATS Connecting To:" + "service:jmx:rmi:///jndi/rmi://" + ip + ":" + jmxPort + "/jmxrmi");
		    JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + ip + ":" + jmxPort + "/jmxrmi");
		    String orphanStatus = "completed";
		    String deltaStatus = "completed";
		    try (JMXConnector jmConn = JMXConnectorFactory.connect(url, environment)) {
		        MBeanServerConnection msc = jmConn.getMBeanServerConnection();
		        try {
		            Object rs = msc.getAttribute(new ObjectName("com.k2view.fabric:type=stats,category=iidFinder,stats=updateQueries,key=K_" + getLuType().luName.substring(0, 3).toUpperCase() + "_delta"), "count");
		            if (rs != null) {
		                if (totalInsertToDelta > Long.parseLong(rs + "")) {
							iterationDelta++;
		                }
		                totalInsertToDelta = Long.parseLong(rs + "");
		            }
		        } catch (javax.management.InstanceNotFoundException e) {
		            deltaStatus = "Mbean Not Found On Node";
		        }
		
		        try {
		            Object rs = msc.getAttribute(new ObjectName("com.k2view.fabric:type=stats,category=iidFinder,stats=updateQueries,key=k_" + getLuType().luName.substring(0, 3).toUpperCase() + "_orphans"), "count");
		            if (rs != null) {
						if (totalInsertToOrphan > Long.parseLong(rs + "")) {
							iterationOrphan++;
						}
		            	totalInsertToOrphan = Long.parseLong(rs + "");
					}
		        } catch (javax.management.InstanceNotFoundException e) {
		            orphanStatus = "Mbean Not Found On Node";
		        }
		
		    } catch (Exception e) {
		        deltaStatus = "failed to get stats, Error:" + e.getMessage();
		        orphanStatus = "failed to get stats, Error:" + e.getMessage();
		        log.error(e);
		    }
		    db(DB_CASS_NAME).execute(insToIDFinderStatsTable, ip, "IDFInder_Orphan_Count", totalInsertToOrphan, orphanStatus, new Timestamp(System.currentTimeMillis()), iterationOrphan);
		    db(DB_CASS_NAME).execute(insToIDFinderStatsTable, ip, "IDFInder_Delta_Count", totalInsertToDelta, deltaStatus, new Timestamp(System.currentTimeMillis()), iterationDelta);
		}
	}


}
