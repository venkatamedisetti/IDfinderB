/////////////////////////////////////////////////////////////////////////
// Project Shared Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.common.Orphan_Job_Parser;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;

import com.k2view.cdbms.finder.IidFinderUtils;
import com.k2view.cdbms.finder.LuDaoFactory;
import com.k2view.cdbms.finder.LuDaoInterface;
import com.k2view.cdbms.finder.TriggeredMessage;
import com.k2view.cdbms.finder.api.IidFinderApi;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.shared.user.UserCode;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.func.oracle.OracleToDate;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;
import com.k2view.fabric.fabricdb.datachange.TableDataChange;

import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.shared.user.UserCode.*;
import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.usercode.common.SharedGlobals.*;

@SuppressWarnings({"unused", "DefaultAnnotationParam"})
public class SharedLogic {

	@type(RootFunction)
	@out(name = "out", type = void.class, desc = "")
	public static void k2_MsgParserOrphan(String in) throws Exception {
		LuDaoInterface luDao = LuDaoFactory.getLuDao(getLuType().luName);
		luDao.setWorkerId("Orphan_Job_"+Thread.currentThread().getName());
		
		processMessages(msg -> {
			if (msg != null) {
				handleMsgOrphan(msg.getKey(), msg.getValue());
			}
			assertTerminate();
			return true;
		});
		
		yield(null);
	}
	
	private static void handleMsgOrphan(String iid, String msg) {
		try {
			TriggeredMessage triggeredMsg = IidFinderUtils.fromJson(msg, TriggeredMessage.class);
			long h = System.currentTimeMillis() - triggeredMsg.getHandleTime();

			int delay = Integer.parseInt(getLuType().ludbGlobals.get("ORPHAN_JOB_DELAY_TIME"));
			if (h < delay) {
				Thread.sleep(delay - h);
			}
			IidFinderApi.processOrphans(iid, triggeredMsg);
		} catch (Exception ex) {
			log.error(String.format("Failed to handle %s.%s", getLuType().luName, iid), ex);
		}
	}

}
