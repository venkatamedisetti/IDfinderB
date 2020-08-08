/////////////////////////////////////////////////////////////////////////
// Project Web Services
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.lu.k2_ws.JMX_STATS;

import java.lang.management.ManagementFactory;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;

import com.k2view.cdbms.interfaces.jobs.custom.CustomInterface;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.shared.user.WebServiceUserCode;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.func.oracle.OracleToDate;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.cdbms.usercode.lu.k2_ws.*;
import com.k2view.fabric.common.stats.CustomStats;
//import org.apache.poi.hpsf.CustomProperties;
import org.json.JSONObject;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.usercode.common.SharedLogic.*;
import static com.k2view.cdbms.usercode.common.SharedGlobals.*;
import static com.k2view.cdbms.usercode.lu.WS_TEST.WSExecute.fnInvokeWS;
import com.k2view.fabric.api.endpoint.Endpoint.*;

@SuppressWarnings({"unused", "DefaultAnnotationParam"})
public class Logic extends WebServiceUserCode {




	@webService(path = "", verb = {MethodType.GET, MethodType.POST, MethodType.PUT, MethodType.DELETE}, version = "1", isRaw = true, produce = {Produce.XML, Produce.JSON})
	public static String wsGetDeletedDeltas(String lu_name) throws Exception {
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		JSONObject IIDJSon = new JSONObject();
		long deleted_iids = 0;
		try {
		    deleted_iids = Long.parseLong("" + mbs.getAttribute(new ObjectName("com.k2view.fabric:type=stats,category=custom,stats=idfinder_stats_" + lu_name + ",key=deleted_deltas"), "total"));
		} catch (javax.management.InstanceNotFoundException e) {
		}
		IIDJSon.put("deleted_iids", Long.parseLong(deleted_iids + ""));
		return IIDJSon.toString();
	}


}
