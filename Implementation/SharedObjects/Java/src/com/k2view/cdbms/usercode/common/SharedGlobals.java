/////////////////////////////////////////////////////////////////////////
// Shared Globals
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.common;

import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;

public class SharedGlobals {

	





	@desc("Indicate for how long to keep data in IIDF_RECENT_UPDATES Table")
	@category("IIDF")
	public static String IIDF_KEEP_HISTORY_TIME = "0";
	@desc("Indicate Project Cassandra interface name")
	@category("IIDF")
	public static final String DB_CASS_NAME = "dbCassandra";
	@desc("Indicate the delay between the source to kafka - In hours")
	@category("IIDF")
	public static String IIDF_SOURCE_DELAY = "0";
	@desc("Indicate Project Fabric interface name")
	@category("IIDF")
	public static final String DB_FABRIC_NAME = "dbFabric";
	@desc("Indicate what is the char used in the ludb to concat schema name and table name")
	@category("IIDF")
	public static final String IIDF_SCHEMA_TABLE_SEP_VAL = "_";

	@desc("Indicate if to run LUDB's source population map, Need to be set per LU")
	@category("IIDF")
	public static String IIDF_EXTRACT_FROM_SOURCE = "true";


	@desc("Indicate the list of  ludb's root table, Need to be set per LU")
	@category("IIDF")
	public static final String IIDF_ROOT_TABLE_NAME = "CRM_CUSTOMER";


	@category("IIDF")
	public static String STATS_ACTIVE = "false";








	@category("IIDF")
	public static String IID_CACHE_MAX_REQUESTS = "10000";



	@category("IIDF")
	public static String DELTA_AND_ORPHAN_TABLES_COUNT_TIMEOUT = "10";

	@category("IIDF")
	public static String SYNC_DURATION_IN_SEC = "2";

	@category("IIDF")
	public static String CHECK_DELTA = "true";

	@category("IIDF")
	public static String IID_STATS = "false";

	@category("IIDF")
	public static String CROSS_IID_SKIP_LOGIC = "false";

	@category("IIDF")
	public static String DEBUG_LOG_THRESHOLD = "200";

	@category("IIDF")
	public static String PATH_TO_KAFKA_BIN = "/opt/apps/kafka/kafka/bin/";

	@category("IIDF")
	public static String STATS_EMAIL_REPORT_LUS = "CUSTOMER";

	@category("IIDF")
	public static String GET_PARSER_GROUP_ID = "fabric_default";











	


}
