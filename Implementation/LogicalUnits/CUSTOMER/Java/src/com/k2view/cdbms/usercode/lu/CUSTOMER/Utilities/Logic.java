/////////////////////////////////////////////////////////////////////////
// LU Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.lu.CUSTOMER.Utilities;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import java.util.Date;

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
import com.k2view.cdbms.usercode.lu.CUSTOMER.*;
import org.sqlite.core.DB;

import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.usercode.common.SharedLogic.*;
import static com.k2view.cdbms.usercode.lu.CUSTOMER.Globals.*;

@SuppressWarnings({"unused", "DefaultAnnotationParam"})
public class Logic extends UserCode {

	@type(UserJob)
	public static void fnBlackListInstances() throws Exception {
		String select_query = "select last_name,ssn from customer where last_name like 'j%'";
		Db.Rows results = db("CRM_DB").fetch(select_query);
		for(Db.Row foreachResult: results){
			//if("j".equalsIgnoreCase((""+foreachResult.cell(0)).substring(0,1))){
				db("dbCassandra").execute("insert into k2staging_idfinderbasic.k_cus_black(id, cause) values (?,?)", new Object[]{foreachResult.cell(1), "black"});
			//}
		}
	}


	public static void fnPreOrderCheck(Object dc, Object i_userCodeDelegate) throws Exception {
		DataChange datachange = (DataChange) dc;
		Map<String, Object> keysMap = datachange.getValues();
		Object ORDER_DATE = keysMap.get("ORDER_DATE");
		if (ORDER_DATE != null) {
		    UserCodeDelegate ucd = (UserCodeDelegate) i_userCodeDelegate;
		    final java.text.DateFormat clsDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd");
		    clsDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		    java.util.Date currentTime = new java.util.Date();
			java.util.Date order_date = clsDateFormat.parse(""+ORDER_DATE);
		    if(currentTime.before(order_date)){
				keysMap.put("ORDER_STATUS", "Future");
			}
		}
	}


	public static void fnPostOrderCheck(Object dc, Object i_userCodeDelegate) throws Exception {
		ludb().execute("delete from orders_orders where order_status = 'Closed'");
	}
}
