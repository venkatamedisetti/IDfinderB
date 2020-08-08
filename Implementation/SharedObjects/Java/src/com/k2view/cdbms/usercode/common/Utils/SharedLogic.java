/////////////////////////////////////////////////////////////////////////
// Project Shared Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.common.Utils;

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

import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.shared.user.UserCode.*;
import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.usercode.common.SharedGlobals.*;

import com.k2view.graphIt.Graphit;
import com.k2view.graphIt.pool.GraphitPool;
import com.k2view.graphIt.serialize.Serializer;

@SuppressWarnings({"unused", "DefaultAnnotationParam"})
public class SharedLogic {



	@out(name = "rs", type = String.class, desc = "")
	public static String fnExecGraphit(String i_graphit_file, Object i_params, String i_format) throws Exception {
		Map<String, Object> params = null;
		if (i_params != null) params = (Map<String, Object>) i_params;
		
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
		     OutputStreamWriter osw = new OutputStreamWriter(baos);) {
		    GraphitPool.Entry entry = getLuType().graphitPool().get(i_graphit_file);
		    Graphit graphit = entry.get();
		    if ("json".equalsIgnoreCase(i_format)) {
		        graphit.run(params, Serializer.Type.JSON, osw);
		    } else {
		        graphit.run(params, Serializer.Type.XML, osw);
		    }
		    osw.flush();
		    return baos.toString();
		}
	}


}
