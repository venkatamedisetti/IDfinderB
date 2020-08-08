/////////////////////////////////////////////////////////////////////////
// Project Shared Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.common;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import java.util.Date;

import com.k2view.cdbms.finder.DataChange;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.shared.user.UserCode;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.func.oracle.OracleToDate;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;

import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.shared.user.UserCode.*;
import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.usercode.common.SharedGlobals.*;

@SuppressWarnings({"unused", "DefaultAnnotationParam"})
public class SharedLogic {


	@category("Testing")
	public static void fnCheckAccChanged(Object dc, Object i_userCodeDelegate) throws Exception {
		DataChange datachange = (DataChange) dc;
		Object RPSTRY_AFLT_CUST_LOC_ID_BEF = datachange.getBeforeValues().get("RPSTRY_AFLT_CUST_LOC_ID");
		Object RPSTRY_AFLT_CUST_LOC_ID = datachange.getValues().get("RPSTRY_AFLT_CUST_LOC_ID");
		if (datachange.getOperation().equals(DataChange.Operation.update) && RPSTRY_AFLT_CUST_LOC_ID_BEF != null && RPSTRY_AFLT_CUST_LOC_ID != null && !(RPSTRY_AFLT_CUST_LOC_ID_BEF + "").equals((RPSTRY_AFLT_CUST_LOC_ID + ""))) {
		    UserCodeDelegate ucd = (UserCodeDelegate) i_userCodeDelegate;
		    Object rs = ucd.getThreadGlobals("ACCOUNT_CHANGE");
			final java.text.DateFormat clsDateFormat = new java.text.SimpleDateFormat("yyyyMMdd");
			clsDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
			java.util.Date currentTime = new java.util.Date();
		    ucd.db(DB_CASS_NAME).execute("insert into " + ucd.getLuType().getKeyspaceName() + ".account_change (date,  account_number, current_timestamp, op_timestamp, process_timestamp, org_loc, dest_loc) values (?, ?, ?, ?, ?, ?, ?)", new Object[]{clsDateFormat.format(currentTime), datachange.getValues().get("BID_AFLT_ACCT_ID"), new Timestamp((datachange.getCurrentTimestamp() / 1000)), new Timestamp((datachange.getOpTimestamp() / 1000)), new Timestamp(System.currentTimeMillis()), RPSTRY_AFLT_CUST_LOC_ID_BEF, RPSTRY_AFLT_CUST_LOC_ID});
		    if (rs == null) {
		        ucd.setThreadGlobals("ACCOUNT_CHANGE", 1);
		    } else {
		        long cnt = Long.parseLong(rs + "") + 1;
		        ucd.setThreadGlobals("ACCOUNT_CHANGE", cnt);
		    }
		}
	}

	
	
	

}
