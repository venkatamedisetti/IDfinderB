/////////////////////////////////////////////////////////////////////////
// Project Shared Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.common.IIDF;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import java.util.Date;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import com.datastax.driver.core.Session;
import com.k2view.cdbms.cluster.CassandraClusterSingleton;
import com.k2view.cdbms.config.TableProperties;
import com.k2view.cdbms.exceptions.InstanceNotFoundException;
import com.k2view.cdbms.finder.DataChange;
import com.k2view.cdbms.finder.Table;
import com.k2view.cdbms.finder.TableProperty;
import com.k2view.cdbms.finder.api.IidFinderApi;
import com.k2view.cdbms.exceptions.SyncException;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.shared.logging.LogEntry;
import com.k2view.cdbms.shared.logging.MsgId;
import com.k2view.cdbms.shared.user.UserCode;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.func.oracle.OracleToDate;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.stats.CustomStats;
import com.k2view.fabric.session.FabricSession;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONArray;
import org.json.JSONObject;

import static com.k2view.cdbms.exceptions.CdbmsException.ErrorCodes.FAILED_CONNECTING_TO_SOURCE;
import static com.k2view.cdbms.finder.IidFinderUtils.addDeleteFinder;
import static com.k2view.cdbms.finder.IidFinderUtils.runDeleteFinder;
import static com.k2view.cdbms.finder.api.IidFinderApi.getDeltas;
import static com.k2view.cdbms.lut.FunctionDef.functionContext;
import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.shared.user.UserCode.*;
import static com.k2view.cdbms.shared.user.UserCode.getCustomProperties;
import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.usercode.common.SharedGlobals.*;

@SuppressWarnings({"unused", "DefaultAnnotationParam"})
public class SharedLogic {


	public static void fnIIDFDeleteFromChilds(Object tableList) throws Exception {
		//Loop throw table list and clean there orphan records
		Map<String, DataChange> childTables = (Map<String, DataChange>) tableList;
		if (childTables != null && !childTables.isEmpty()) {
		    HashMap<String, HashSet<String>> prntTable = fnIIDFGetParentTable(getLuType().luName);
		    TreeMap<String, Map<String, List<LudbRelationInfo>>> phyRel = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
		    phyRel.putAll(getLuType().getLudbPhysicalRelations());
		    setThreadGlobals("LUDB_PHYISICAL_RELATION", phyRel);
		
		    for (Map.Entry<String, DataChange> tableInfo : childTables.entrySet()) {
		        fnIIDFDeleteOrphans(tableInfo.getKey(), prntTable, null, tableInfo.getValue().getPos(), tableInfo.getValue().getOpTimestamp(), true, tableInfo.getValue().getOperation(), tableInfo.getValue().getCurrentTimestamp());
		    }
		}
	}


	public static void fnIIDFDeleteOrphans(String tableName, Object perantTble, String targetIid, String pos, Long op_ts, Boolean run_parent_delete, Object opetaion, Long cr_ts) throws Exception {
		DataChange.Operation ope = (DataChange.Operation) opetaion;
		SerializeFabricLu.Table table_Object = serializeLU(tableName, 0);//Get table information
		HashMap<String, HashSet<String>> prntTbl = (HashMap<String, HashSet<String>>) perantTble;
		
		boolean rec_deleted = true;
		if (run_parent_delete) rec_deleted = fnIIDFDeleteTableOrphanRecords(tableName, prntTbl, targetIid, pos, op_ts, cr_ts);
		if ((!rec_deleted && ope.equals(DataChange.Operation.update)) || (!rec_deleted && ope.equals(DataChange.Operation.delete) && !run_parent_delete)) {
		    return;
		}
		for (SerializeFabricLu.Table child : table_Object.tableChildren) {//Loop throw all table's childs and delete orphan records
		    //Get table record from trnIIDFOrphanRecV
		    Map<String, String> trnMap = getTranslationValues("trnIIDFOrphanRecV", new Object[]{child.tableName.toUpperCase()});
		    //Check if to run orphan record check on table
		    if (trnMap.get("SKIP_VALIDATE_IND") != null && trnMap.get("SKIP_VALIDATE_IND").equalsIgnoreCase("TRUE")) {
		        continue;
		    }
		
		    HashSet<String> prntTables = prntTbl.get(child.tableName.toUpperCase());
		    StringBuilder sqlQuery = new StringBuilder();
		    String prefix = "";
		    for (String tbl : prntTables) {
		
		        TreeMap<String, List<LudbRelationInfo>> phyRelTbl = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);//Get table -> child links
		        phyRelTbl.putAll(((TreeMap<String, Map<String, List<LudbRelationInfo>>>) getThreadGlobals("LUDB_PHYISICAL_RELATION")).get(tbl + ""));
		
		        List<LudbRelationInfo> childRelations = phyRelTbl.get(child.tableName);
		        HashMap<String, String> popRelMap = new HashMap<String, String>();
		        for (LudbRelationInfo chRel : childRelations) {
		            String popMapName = (String) chRel.to.get("populationObjectName");
		            if (popRelMap.containsKey(popMapName)) {
		                popRelMap.put(popMapName, popRelMap.get(popMapName) + " and " + chRel.from.get("column") + " = " + child.tableName + "." + chRel.to.get("column"));
		            } else {
		                popRelMap.put(popMapName, chRel.from.get("column") + " = " + child.tableName + "." + chRel.to.get("column"));
		            }
		        }
		
		        for (String con : popRelMap.values()) {
		            sqlQuery.append(prefix + " not exists (select 1 from " + getLuType().luName + "." + tbl + " where " + con + " ) ");
		            prefix = " and ";
		        }
		    }
		
		    //Check if to add another condition to orphan records check query
		    if (trnMap.get("VALIDATION_SQL") != null && !trnMap.get("VALIDATION_SQL").equalsIgnoreCase("")) {
		        sqlQuery.append(" and not exists (" + trnMap.get("VALIDATION_SQL") + ")");
		    }
		
		    rec_deleted = fnIIDFGetRecData(child.tableName, sqlQuery.toString(), targetIid, pos, op_ts, (!Boolean.parseBoolean(IIDF_EXTRACT_FROM_SOURCE) || getTranslationsData("trnIIDFForceReplicateOnDelete").keySet().contains(child.tableName.toUpperCase())) ? true : false, cr_ts);//Before deleting records send them to kafka
		    ludb().execute("delete from  " + getLuType().luName + "." + child.tableName + " where " + sqlQuery.toString());//Delete orphan records
		    if (rec_deleted) {
		        fnIIDFDeleteOrphans(child.tableName, perantTble, targetIid, pos, op_ts, false, ope, cr_ts);//Call function again with child table
		    }
		}
	}


    @type(UserJob)
    public static void fnIIDFSyncDelta() throws Exception {
        //User job to run in the background and sync instances from IIDF
        String lu_name = getLuType().luName;
        com.k2view.cdbms.finder.BulksIterator it = new com.k2view.cdbms.finder.BulksIterator(lu_name, (iids) -> {
            try {
                String escapingIids = iids.stream().map(s -> "\''" + s + "\''").collect(java.util.stream.Collectors.joining(","));
                db(DB_FABRIC_NAME).execute("MIGRATE " + lu_name + " FROM " + DB_CASS_NAME + " USING ('select DISTINCT iid from k2staging." + "k_" + lu_name.substring(0, 3).toLowerCase() + "_delta" + " where iid IN (" + escapingIids + ")')");
            } catch (Exception e) {
                log.warn("Failed to sync IIDF delta!", e);
            }
        });
        it.read();
    }


    public static HashMap<String, HashSet<String>> fnIIDFGetParentTable(String lut_name) {
        //Function to get table parent
        HashMap<String, HashSet<String>> prntTable = new HashMap<String, HashSet<String>>();
        LUType lut = LUTypeFactoryImpl.getInstance().getTypeByName(lut_name);
        LudbObject table = lut.getRootObject();
        setTblPrnt(table, prntTable);
        return prntTable;
    }


	@out(name = "result", type = Object.class, desc = "")
	public static Object fnIIDFGetTablePK(String table_name, String lu_name) throws Exception {
		LUType lut = LUTypeFactoryImpl.getInstance().getTypeByName(lu_name);
		if (lut != null) {
		    LudbObject rtTable = lut.ludbObjects.get(table_name);
		    if (rtTable != null) {
		        LudbIndexes luIndx = rtTable.indexes;
		        for (LudbIndex luLocIndx : luIndx.localIndexes) {
		            if (luLocIndx.pk) {
		                return luLocIndx.columns.stream().toArray(String[]::new);
		            }
		        }
		    }
		}
		
		String pkColumnsFromTrn = getTranslationValues("trnTable2PK", new Object[]{table_name.toUpperCase()}).get("pk_list");
		if (pkColumnsFromTrn == null) {
		    throw new Exception("Couldn't find Primary key columns for table: " + table_name + " neither in trnTable2PK nor on table indexes, Please check!");
		} else {
		    return pkColumnsFromTrn.split(",");
		}
	}


	public static void fnIIDFSendRecBack2Kafka(String table_name, Object column_list, Object val_list, String targetIid, String pos, Long op_ts, String reason, Long cr_ts) throws Exception {
		String delOrpIgnRep = getThreadGlobals("DELETE_ORPHAN_IGNORE_REPLICATES") == null ? "N" : getThreadGlobals("DELETE_ORPHAN_IGNORE_REPLICATES").toString();
		if (inDebugMode() || "Y".equals(delOrpIgnRep)) return;//Dont run
		
		String source_table_name = fnIIDFGetTableSourceFullName(table_name);//Get table source full name
		if (source_table_name == null) {
		    log.warn("Found table name null, Record was not sent to kafka!");
		    return;
		}
		
		
		String globalName = "";
		
		if ("R".equals(reason)) {
		    globalName = "SEND_BACK_CNT_R";
		} else if ("D".equals(reason)) {
		    globalName = "SEND_BACK_CNT_D";
		}
		
		Object rs = getThreadGlobals(globalName);
		if (rs == null) {
		    setThreadGlobals(globalName, 1);
		} else {
		    long cnt = Long.parseLong((rs + ""));
		    cnt++;
		    setThreadGlobals(globalName, cnt);
		}
		final java.text.DateFormat clsDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
		clsDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		java.util.Date currentTime = new java.util.Date();//Create date
		String currTime = clsDateFormat.format(currentTime);
		String current_ts = clsDateFormat.format(currentTime).replace(" ", "T");
		
		JSONObject IIDJSon = new JSONObject();
		IIDJSon.put("table", source_table_name);
		IIDJSon.put("op_type", "R");
		
		IIDJSon.put("op_ts", currTime);
		
		if (cr_ts != null && cr_ts != 0) {
		    Timestamp ts = new Timestamp((cr_ts / 1000));
		    Date tsToDate = new Date(ts.getTime());
		    IIDJSon.put("current_ts", clsDateFormat.format(tsToDate).replace(" ", "T"));
		} else if ("R".equals(reason)) {
			Timestamp ts = new Timestamp((op_ts / 1000));
		    Date tsToDate = new Date(ts.getTime());
		    IIDJSon.put("current_ts", clsDateFormat.format(tsToDate).replace(" ", "T"));
		} else {
			IIDJSon.put("current_ts", current_ts);
		}
		
		if (pos != null && !pos.equals("")) {
		    IIDJSon.put("pos", pos);
		} else {
		    IIDJSon.put("pos", "00000000020030806864");
		}
		
		if (targetIid != null && !targetIid.equals("")) {//If target iid was sent on data change send it back to kafka
		    IIDJSon.put("targetIid", targetIid);
		}
		
		//Get table primary keys
		JSONArray PK = new JSONArray();
		String[] pkCuls = (String[]) fnIIDFGetTablePK(source_table_name.split("\\.")[0].toUpperCase() + IIDF_SCHEMA_TABLE_SEP_VAL + source_table_name.split("\\.")[1].toUpperCase(), getLuType().luName);
		String prefix = "";
		for (String pkCul : pkCuls) {
		    PK.put(pkCul);
		}
		IIDJSon.put("primary_keys", PK);
		
		//Start build the json message
		Set<String> PKSet = new HashSet<>(Arrays.asList(pkCuls));
		StringBuilder msgKey = new StringBuilder();
		JSONObject after = new JSONObject();
		List<Object> valList = (List<Object>) val_list;
		int i = 0;
		for (String col : (Set<String>) column_list) {
		    if (PKSet.contains(col.toUpperCase())) msgKey.append(valList.get(i));
		    after.put(col.toUpperCase(), valList.get(i));
		    i++;
		}
		IIDJSon.put("after", after);
		
		//send message to kafka
		Properties props = new Properties();
		UserKafkaProperties(props);
		Producer<String, JSONObject> producer;
		if (getThreadGlobals("KAFKA_PRODUCER") == null) {
		    producer = new KafkaProducer<>(props);
		    setThreadGlobals("KAFKA_PRODUCER", producer);
		} else {
		    producer = (Producer<String, JSONObject>) getThreadGlobals("KAFKA_PRODUCER");
		}
		try {
		    producer.send(new ProducerRecord("WS.IDfinder." + source_table_name.split("\\.")[0].toUpperCase() + "." + source_table_name.split("\\.")[1].toUpperCase(), null, msgKey.toString(), IIDJSon.toString())).get();
		} catch (Exception e) {
		    log.warn("IIDF: Failed To Send Records To Kafka");
		    producer.close();
		    clearThreadGlobals();
		    throw e;
		}
	}


    public static void fnIIDFPopRecentUpdates(Object[] valueArrays, Long opTimeStamp, String iid, String sql, Long maxUpdated, Long optCurrentTimeStamp) throws Exception {
        //Function to send records to IIDF RECENT UPDATES
        StringBuilder strArray = new StringBuilder();
        String prefix = "";
        if (valueArrays != null) {
            for (Object val : valueArrays) {
                strArray.append(prefix + val);
                prefix = ",";
            }
        }
        ludb().execute("insert or replace into " + getLuType().luName + ".IIDF_RECENT_UPDATES (IID,SQL,VAL_ARRAY,OP_TIMESTAMP,CURRENT_TIMESTAMP,OP_CURRENT_TIMESTAMP) values (?,?,?,?,?,?)", new Object[]{iid, sql, strArray.toString(), opTimeStamp, maxUpdated, optCurrentTimeStamp});
    }


	@type(RootFunction)
	@out(name = "IID", type = String.class, desc = "")
	public static void fnIIDFPopTable(String IID) throws Exception {
		Map<String, List<Map<String, Object>>> idfTablesDeleteList = new HashMap<>();
		DataChange dataFromQ = null;
		try (Db.Rows qData = ludb().fetch("select DATA_CHANGE from " + getLuType().luName + ".IIDF_QUEUE order by \"current_timeStamp\" , POS")) {
		    for (Db.Row row : qData) {
		        dataFromQ = DataChange.fromJson(row.cell(0) + "");
		        String tableName = dataFromQ.getTablespace() + IIDF_SCHEMA_TABLE_SEP_VAL + dataFromQ.getTable();//Get table name from data change
		        String sql;
		        if (dataFromQ.getOperation().equals(DataChange.Operation.insert)) {
		            sql = dataFromQ.toSql(DataChange.Operation.upsert, tableName);
		        } else {
		            sql = dataFromQ.toSql(dataFromQ.getOperation(), tableName);
		        }
		        Object[] valueArrays = dataFromQ.sqlValues();
		        if (((("1").equals(getThreadGlobals("childCrossInsts" + tableName.toUpperCase())) || isFirstSync())) && ("true").equals(IIDF_EXTRACT_FROM_SOURCE)) {
		            //Get DC Keys
		            String prefix = "";
		            StringBuilder recKeys = new StringBuilder();
		            List<Object> keyVals = new ArrayList<>();
		            Map<String, Object> dcKeys = dataFromQ.getKeys();
		            for (Map.Entry<String, Object> dcKeysEnt : dcKeys.entrySet()) {
		                keyVals.add(dcKeysEnt.getValue());
		                recKeys.append(prefix + dcKeysEnt.getKey() + " = ? ");
		                prefix = " and ";
		            }
		
		            if (dataFromQ.getOperation().equals(DataChange.Operation.delete)) {
		                //Check if DC keys exists in LU table
		                if (ludb().fetch("select 1 from " + getLuType().luName + "." + tableName + " where " + recKeys.toString(), keyVals.toArray()).firstValue() != null) {
		                    if (idfTablesDeleteList.containsKey(tableName)) {
		                        List<Map<String, Object>> tblKeys = idfTablesDeleteList.get(tableName);
		                        tblKeys.add(dcKeys);
		                    } else {
		                        List<Map<String, Object>> tblKeys = new ArrayList<>();
		                        tblKeys.add(dcKeys);
		                        idfTablesDeleteList.put(tableName, tblKeys);
		                    }
		                }
		                //Check if DC keys exists in idfinder delete list
		            } else if (dataFromQ.getOperation().equals(DataChange.Operation.insert)) {
		                List<Map<String, Object>> tblKeys = idfTablesDeleteList.get(tableName);
		                if (tblKeys != null) {
		                	Iterator<Map<String, Object>> it = tblKeys.iterator();
			                outsideloop:
			                while (it.hasNext()) {
			                    Map<String, Object> dcKey = it.next();
			                    for (Map.Entry<String, Object> msgKeys : dcKey.entrySet()) {
			                        if (!msgKeys.getValue().equals(dcKeys.get(msgKeys.getKey()))) {
			                            continue outsideloop;
			                        }
			                    }
			                    it.remove();
			                }
						}
		            }
		            ludb().execute(sql, valueArrays);
		        }
		    }
		}
		//Loop throw the map and delete tables keys from idfinder cache
		Map<String, String> keys = null;
		for (Map.Entry<String, List<Map<String, Object>>> keysToRemove : idfTablesDeleteList.entrySet()) {
		    for (Map<String, Object> DCKeys : keysToRemove.getValue()) {
		        keys = new HashMap<>();
		        for (Map.Entry<String, Object> keyEnt : DCKeys.entrySet()) {
		            keys.put(keyEnt.getKey(), keyEnt.getValue() == null ? null : keyEnt.getValue().toString());
		        }
		        addDeleteFinder(getLuType().luName, getInstanceID(), keysToRemove.getKey(), keys);
		    }
		}
		if (keys != null) runDeleteFinder(getLuType().luName, getInstanceID());
		if (dataFromQ != null) {
		    Map<String, DataChange> deleteOrphan = getThreadGlobals("DELETE_ORPHAN") == null ? null : (Map<String, DataChange>) getThreadGlobals("DELETE_ORPHAN");
		    if (deleteOrphan != null) {
		        setThreadGlobals("DELETE_ORPHAN_IGNORE_REPLICATES", "Y");
		        fnIIDFDeleteFromChilds(deleteOrphan);//Execute delete orphan
		        setThreadGlobals("DELETE_ORPHAN_IGNORE_REPLICATES", "N");
		    }
		}
		if (false) yield(new Object[]{null});
	}


	@out(name = "rec_deleted", type = Boolean.class, desc = "")
	public static Boolean fnIIDFGetRecData(String table_name, String table_where, String targetIid, String pos, Long op_ts, Boolean send_rec_to_kafka, Long cr_ts) throws Exception {
		//Fetch table orphan records and call fnIIDFSendRecBack2Kafka to send it to kafka
		boolean rec_deleted = false;
		Set<String> tblColArr = getLuType().ludbObjects.get(table_name).ludbColumnMap.keySet();
		try (Db.Rows rs = ludb().fetch("Select " + String.join(",", tblColArr) + " From " + getLuType().luName + "." + table_name + " where " + table_where)) {
		    List<Object> vals = new LinkedList();
		    for (Db.Row row : rs) {
		        if (!rec_deleted) rec_deleted = true;
		        for (Object colVal : row.cells()) {
		            if (colVal == null) {
		                vals.add(JSONObject.NULL);
		            } else {
		                vals.add(colVal);
		            }
		        }
		        if (send_rec_to_kafka) fnIIDFSendRecBack2Kafka(table_name, tblColArr, vals, targetIid, pos, op_ts, "D", cr_ts);
		        vals.clear();
		    }
		}
		return rec_deleted;
	}


	@type(RootFunction)
	@out(name = "IID", type = String.class, desc = "")
	@out(name = "MAX_DC_UPDATE", type = Long.class, desc = "")
	public static void fnIIDFConsMsgs(String IID) throws Exception {
		if (inDebugMode()) {//In debug mode just yield IID and current time
		    yield(new Object[]{IID, 0});
		    return;
		}
		
		Map<String, Map<String, Integer>> statsMap = new HashMap<>();
		long maxUpdated = System.currentTimeMillis() * 1000;
		List<Object[]> savedDC = new ArrayList<>();
		List<DataChange> replicateRequests = new ArrayList<>();
		Map<String, DataChange> deleteOrphan = new LinkedHashMap<>();
		List<Long> crossInstanceList = new ArrayList<>();
		boolean isIidChanged = false;
		final String insertStmt = "insert into " + getLuType().luName + ".IIDF_QUEUE (DATA_CHANGE , iid , opt_timeStamp , current_timeStamp, POS ) values (?,?,?,?,?)";
		long maxHandleTime = 0;
		DataChange dataChange = null;
		Map<String, Map<String, String>> trnIIDFCrossIIDTablesList = getTranslationsData("trnIIDFCrossIIDTablesList");
		reportUserMessage("INSTANCE_TIME:"+Instant.now());
		
		setThreadGlobals("SYNC_TIME", Instant.now());
		reportUserMessage("INSTANCE_TIME:"+getThreadGlobals("SYNC_TIME"));
		List<Object> paramsStats = new LinkedList<>();
		
		try (Db.Rows updatedDate = ludb().fetch("select MAX_DC_UPDATE from " + getLuType().luName + ".IIDF ")) {//Fetch the latest data change time
		    Object rowVal = updatedDate.firstValue();
		    if (rowVal != null) {
		        maxHandleTime = Long.parseLong(rowVal.toString());
		    }
		}
		long maxQueueOPTimestamp = 0;
		long minDeltaOPTimestamp = -1;
		try (Db.Rows optTimestamp = ludb().fetch("select max(opt_timeStamp) from " + getLuType().luName + ".IIDF_QUEUE")) {
		    Object rowVal = optTimestamp.firstValue();
		    if (rowVal != null)
		        maxQueueOPTimestamp = Long.parseLong("" + rowVal);
		}
		
		Instant start = Instant.now();
		Set<Long> hashedDC = new HashSet<>();
		long dbExecuteStartTimeTTL = 0;
		try (Stream<DataChange> dataChangesFromDelta = IidFinderApi.getDeltas(getLuType().luName, IID, maxHandleTime)) {//Fetch all dc newer then
		    Iterable<DataChange> dataChangesFromDeltaItr = () -> dataChangesFromDelta.iterator();
		    for (DataChange dc : dataChangesFromDeltaItr) {//Loop throw all dc and insert to IIDF_QUEUE
		        long jsonHashed = dc.toJson().hashCode();
		        if (hashedDC.contains(jsonHashed)) continue;
		        hashedDC.add(jsonHashed);
		        if (minDeltaOPTimestamp == -1 || minDeltaOPTimestamp > dc.getOpTimestamp()) {
		            minDeltaOPTimestamp = dc.getOpTimestamp();
		        }
		
		        if (maxHandleTime == 0 || maxHandleTime < dc.getHandleTimestamp()) {
		            maxHandleTime = dc.getHandleTimestamp();
		        }
		
		        if (STATS_ACTIVE != null && "true".equalsIgnoreCase(STATS_ACTIVE)) {
		            Map<String, Integer> tableStatsMap = statsMap.get(dc.getTablespace() + IIDF_SCHEMA_TABLE_SEP_VAL + dc.getTable());
		            if (tableStatsMap == null) tableStatsMap = new HashMap<>();
		            fnIIDFSetStats("" + dc.getOperation(), tableStatsMap, dc.isIidChanged());
		            statsMap.put(dc.getTablespace() + IIDF_SCHEMA_TABLE_SEP_VAL + dc.getTable(), tableStatsMap);
		        }
		        Instant dbExecuteStartTime = Instant.now();
		        ludb().execute(insertStmt, dc.toJson(), IID, dc.getOpTimestamp(), dc.getCurrentTimestamp(), dc.getPos());
		        dbExecuteStartTimeTTL = dbExecuteStartTimeTTL + Duration.between(dbExecuteStartTime, Instant.now()).toMillis();
		    }
		}
		
		//if (!isFirstSync() && deltasCnt == 0 && !getConnection("fabric").unwrap(FabricSession.class).syncMode().name().equalsIgnoreCase("FORCE")) {
		//    setThreadGlobals("IID_SKIPPED", true);
		//    fnIIDFInsertIIDStats();
		//    fabric().execute("set IGNORE_SOURCE_EXCEPTION=true");
		//    throw new SyncException("IID Empty Delta Exception!", FAILED_CONNECTING_TO_SOURCE);
		//}
		
		paramsStats.add((Duration.between(start, Instant.now()).toMillis()) - dbExecuteStartTimeTTL);//delta_api_fetch_time
		paramsStats.add(dbExecuteStartTimeTTL);//insert_detlas_to_iidfQueue_for_sort
		paramsStats.add(hashedDC.size());//delta_count
		setThreadGlobals("IID_DELTA_COUNT", hashedDC.size());
		Object[] params = new Object[]{};
		String filterQueue = "";
		if (minDeltaOPTimestamp > maxQueueOPTimestamp) {
		    //filterQueue = " Where opt_timeStamp >= ? ";
		    //params = new Object[]{minDeltaOPTimestamp};
		}
		
		Map<String, Long> debugTiming = new HashMap<>();
		setThreadGlobals("DEBUG_TIMING_MAP", debugTiming);
		start = Instant.now();
		try (Db.Rows iidfQRS = ludb().fetch("select DATA_CHANGE, rowid from " + getLuType().luName + ".IIDF_QUEUE " + filterQueue + " order by \"current_timeStamp\" , POS", params)) {
		    for (Db.Row row : iidfQRS) {
		        Instant dcProcess = Instant.now();
		        if (row.cell(0) == null)
		            throw new RuntimeException("Found NULL value for DC in " + getLuType().luName + ".IIDF_QUEUE!");
		        Instant startTime = Instant.now();
		        dataChange = DataChange.fromJson(row.cell(0) + "");
		        debugTiming.put("Converting string to Datachange", Duration.between(startTime, Instant.now()).toMillis());
		
		
		        startTime = Instant.now();
		        DataChange.Operation Operation = dataChange.getOperation();
		        if (dataChange.getOrgOperation().equals(DataChange.Operation.replicate) && !Operation.equals(DataChange.Operation.replicate)) {
		            if (fnIIDFCheckRExists(dataChange)) continue;
		        }
		        debugTiming.put("fnIIDFCheckRExists", Duration.between(startTime, Instant.now()).toMillis());
		
		        if (fnIIDFExecNonLUTableDelta(dataChange)) {
		            continue;
		        }
		
		        startTime = Instant.now();
		        fnIIDFUpdateSelectiveTable(dataChange, deleteOrphan);
		        debugTiming.put("fnIIDFUpdateSelectiveTable", Duration.between(startTime, Instant.now()).toMillis());
		
		        startTime = Instant.now();
		        String tableName = dataChange.getTablespace() + IIDF_SCHEMA_TABLE_SEP_VAL + dataChange.getTable();//Get table name from data change
		        List<String> culListStr = new ArrayList<String>();
		
		        //Fetch table column list and add them to list
		        List<LudbColumn> culList = getLuType().ludbObjects.get(dataChange.getTablespace() + "_" + dataChange.getTable()).getOrderdObjectColumns();
		        for (LudbColumn culName : culList) {
		            culListStr.add(culName.getName());
		        }
		        debugTiming.put("Getting LU table columns", Duration.between(startTime, Instant.now()).toMillis());
		
		        startTime = Instant.now();
		        if (!Operation.equals(DataChange.Operation.delete) && !Operation.equals(DataChange.Operation.replicate) && dataChange.getValues() != null) {//Remove columns from data change that doesn't exists in LU table
		            dataChange.getValues().keySet().retainAll(culListStr);
		            if (dataChange.getValues().isEmpty()) continue;
		        }
		        debugTiming.put("Removing not existed columns", Duration.between(startTime, Instant.now()).toMillis());
		
		
		        startTime = Instant.now();
		        String targetIid = dataChange.getTargetIid();//Get targetIid from data change
		        boolean isCrossIID = dataChange.isCrossIID();//Indicate if table is cross instance table
		        isIidChanged = dataChange.isIidChanged();//Idicate if data change cause instance change
		
		        if (isIidChanged && getThreadGlobals("is_iid_change") != null) {
		            long cnt = Long.parseLong(getThreadGlobals("is_iid_change") + "");
		            cnt++;
		            setThreadGlobals("is_iid_change", cnt);
		        } else if (isIidChanged) {
		            setThreadGlobals("is_iid_change", 1);
		        }
		
		        long optTimeStamp = dataChange.getOpTimestamp();
		        long currTimeStamp = dataChange.getCurrentTimestamp();
		        debugTiming.put("Getting more info 2", Duration.between(startTime, Instant.now()).toMillis());
		
		        //If data change operation is of type REPLICAT, Add datachange to replicate list to execute after all data changes are processed
		        if (Operation.equals(DataChange.Operation.replicate)) {
		            Map<String, String> trnIIDFCrossIIDTablesListRS = trnIIDFCrossIIDTablesList.get(tableName.toUpperCase());
		            if (!("true").equals(IIDF_EXTRACT_FROM_SOURCE) || (("true").equals(IIDF_EXTRACT_FROM_SOURCE)) && trnIIDFCrossIIDTablesList.keySet().contains(tableName.toUpperCase()) && Boolean.parseBoolean(trnIIDFCrossIIDTablesListRS.get("ACTIVE"))) {
		                replicateRequests.add(dataChange);
		            }
		        } else {
		            setThreadGlobals("fnIIDFExecTableCustomCode", "Before");
		            fnIIDFExecTableCustomCode(dataChange, false); //Check if user added any specific activity for data change table before the LUDB sql execution
		
		            if (dataChange.getOperation().equals(DataChange.Operation.update) && dataChange.isIidChanged() && dataChange.getBeforeValues().size() == 0) {
		                startTime = Instant.now();
		                fnIIDFConvertDCToUpsert(dataChange);
		                debugTiming.put("fnIIDFConvertDCToUpsert", Duration.between(startTime, Instant.now()).toMillis());
		            }
		
		
		            startTime = Instant.now();
		            String sql;
		            if (Operation.equals(DataChange.Operation.insert)) {
		                sql = dataChange.toSql(DataChange.Operation.upsert, tableName);
		                Operation = DataChange.Operation.upsert;
		            } else {
		                sql = dataChange.toSql(dataChange.getOperation(), tableName);
		            }
		            Object[] valueArrays = dataChange.sqlValues();//Get messages values to use for execute
		            if (((System.currentTimeMillis() * 1000) - dataChange.getCurrentTimestamp()) <= Long.valueOf(IIDF_SOURCE_DELAY) * 60 * 60 * 1000 * 1000 && !getTranslationsData("trnIIDFForceReplicateOnDelete").keySet().contains(tableName))//Check if it is needed to insert data change to queue table
		                savedDC.add(new Object[]{dataChange.toJson(), IID, optTimeStamp, currTimeStamp, dataChange.getPos()});
		            debugTiming.put("Setting operation", Duration.between(startTime, Instant.now()).toMillis());
		            //If data change operation is of type UPDATE and data change table is croos instance table or data change cause instance change
		            //Or If data change operation is of type DELETE and data change table is croos instance
		            //Add data change to delete orphan table list to execute after all data changes are processed and execute data change
		            if (Operation.equals(DataChange.Operation.update) && isIidChanged && (dataChange.getBeforeValues() == null || dataChange.getBeforeValues().size() == 0)) {
		                startTime = Instant.now();
		                fnIIDFCleanIIDFinderCTables(dataChange);
		                debugTiming.put("fnIIDFCleanIIDFinderCTables", Duration.between(startTime, Instant.now()).toMillis());
		            }
		
		            if ((Operation.equals(DataChange.Operation.update) && isIidChanged) || Operation.equals(DataChange.Operation.delete)) {
		                if (!deleteOrphan.containsKey(tableName) || (deleteOrphan.containsKey(tableName) && deleteOrphan.get(tableName).equals(DataChange.Operation.update))) {
		                    deleteOrphan.put(tableName, dataChange);
		                }
		                startTime = Instant.now();
		                ludb().execute(sql, valueArrays);
		                debugTiming.put("ludb().execute 1", Duration.between(startTime, Instant.now()).toMillis());
		            } else if (Operation.equals(DataChange.Operation.upsert) && dataChange.getKeys().values().contains(null)) {//If PK holds null value for insert statment we run update instead
		                startTime = Instant.now();
		                fnIIDFExecInsNullPK(dataChange);
		                debugTiming.put("fnIIDFExecInsNullPK", Duration.between(startTime, Instant.now()).toMillis());
		            } else {//If data change is a normal data change execute it
		                startTime = Instant.now();
		                ludb().execute(sql, valueArrays);
		                debugTiming.put("ludb().execute 2", Duration.between(startTime, Instant.now()).toMillis());
		            }
		            if (dataChange.getCurrentTimestamp() > (maxUpdated - Long.valueOf(IIDF_KEEP_HISTORY_TIME) * 60 * 60 * 1000)) {
		                startTime = Instant.now();
		                fnIIDFPopRecentUpdates(valueArrays, optTimeStamp, IID, sql, maxUpdated, currTimeStamp);//Update IIDF_RECENT_UPDATES with data change details
		                debugTiming.put("fnIIDFPopRecentUpdates", Duration.between(startTime, Instant.now()).toMillis());
		            }
		        }
		
		        startTime = Instant.now();
		        if ((Operation.equals(DataChange.Operation.upsert) && (isCrossIID || isIidChanged)) || (Operation.equals(DataChange.Operation.update) && isIidChanged) || (dataChange.getOrgOperation().equals(DataChange.Operation.replicate) && !Operation.equals(DataChange.Operation.replicate))) {
		            crossInstanceList.add(Long.parseLong(row.cell(1) + ""));//Add datachange for cross instance list
		        }
		        debugTiming.put("Adding to crossInstanceList", Duration.between(startTime, Instant.now()).toMillis());
		
		        setThreadGlobals("fnIIDFExecTableCustomCode", "After");
		        fnIIDFExecTableCustomCode(dataChange, true);//Check if user added any specific activity for data change table after the LUDB sql execution
		
		        long ttlProccDC = Duration.between(dcProcess, Instant.now()).toMillis();
		        if (ttlProccDC > Long.parseLong(DEBUG_LOG_THRESHOLD)) {
		            debugTiming.put("Total Datachange proccess time", ttlProccDC);
		            LinkedHashMap<String, Long> sortedMap = new LinkedHashMap<>();
		            debugTiming.entrySet()
		                    .stream()
		                    .sorted(Map.Entry.<String, Long>comparingByValue(Comparator.reverseOrder()))
		                    .forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
		            StringBuilder sb = new StringBuilder().append("\n");
					int i = 0;
		            for (Map.Entry<String, Long> ent : sortedMap.entrySet()) {
		                if(ent.getValue() == 0)continue;
		                sb.append("IID Logging " + IID + " " + i + ":" + ent.getKey() + " = " + ent.getValue() + "\n");
						i++;
		            }
		            log.warn(sb.toString() +  "IID Logging " + IID + " DC:" + dataChange.toJson());
		        }
		    }
		    paramsStats.add((Duration.between(start, Instant.now()).toMillis()));//execute_deltas Time
		}
		
		//Updating IIDF_STATISTICS table with all data changes recieved for instance
		if (STATS_ACTIVE != null && "true".equalsIgnoreCase(STATS_ACTIVE)) fnIIDFUpdateStats(statsMap);
		
		try {
		    start = Instant.now();
		    for (DataChange repDC : replicateRequests) {//Execute all REPLICATE data change
		        fnIIDFFetchInstanceData(repDC.getTablespace() + IIDF_SCHEMA_TABLE_SEP_VAL + repDC.getTable(), repDC.getKeys(), null, repDC.getTargetIid(), repDC.getBeforeValues().keySet(), repDC.getPos(), repDC.getOpTimestamp(), repDC.getCurrentTimestamp());
		    }
		    paramsStats.add((Duration.between(start, Instant.now()).toMillis()));//execute_replicates
		    paramsStats.add(replicateRequests.size());//replicates_count
		
		    start = Instant.now();
		    fnIIDFDeleteFromChilds(deleteOrphan);//Execute delete orphan
		    paramsStats.add((Duration.between(start, Instant.now()).toMillis()));//execute_delete_orphan
		    paramsStats.add(deleteOrphan.size());//delete_orphan_count
		    setThreadGlobals("DELETE_ORPHAN", deleteOrphan);
		    setThreadGlobals("DELETED_TABLES", new HashSet<>());
		} finally {
		    if (getThreadGlobals("KAFKA_PRODUCER") != null) {
		        Producer<String, JSONObject> producer = (Producer<String, JSONObject>) getThreadGlobals("KAFKA_PRODUCER");
		        if (producer != null) producer.close();
		    }
		}
		
		start = Instant.now();
		if (("true").equals(IIDF_EXTRACT_FROM_SOURCE))
		    fnIIDFCheckCrossIns(crossInstanceList);//Executing cross instance check
		paramsStats.add((Duration.between(start, Instant.now()).toMillis()));//execute_cross
		paramsStats.add(crossInstanceList.size()); //cross_count
		
		setThreadGlobals("STATS", paramsStats);
		
		ludb().execute("DELETE FROM " + getLuType().luName + ".IIDF_QUEUE");//Clean IIDF_QUEUE table
		for (Object[] dcToKeepValues : savedDC) {
		    ludb().execute(insertStmt, dcToKeepValues);
		}
		
		long time_to_keep = Long.valueOf(IIDF_KEEP_HISTORY_TIME);//Check which records to delete from IIDF_RECENT_UPDATES
		String delete_sql = "delete from " + getLuType().luName + ".IIDF_RECENT_UPDATES where OP_TIMESTAMP < ?";
		ludb().execute(delete_sql, new Object[]{maxUpdated - time_to_keep * 60 * 60 * 1000});
		
		
		yield(new Object[]{IID, maxHandleTime});
	}


	public static void fnIIDFCheckIfInstanceFound() throws Exception {
		//Check if LUDB real root table as at least 1 record if not throwing instance doesn't exists exception, even if one table has entry break
		boolean rootFnd = false;
		if (!"".equals(IIDF_ROOT_TABLE_NAME.trim())) {
		    for (String tbl : IIDF_ROOT_TABLE_NAME.split(",")) {
		        Object rs1 = ludb().fetch("select 1 from " + getLuType().luName + "." + tbl.trim() + " limit 1").firstValue();
		        if (rs1 != null) {
		            rootFnd = true;
		            break;
		        }
		    }
		} else {
		    rootFnd = true;
		}
		
		if (!rootFnd) {
		    LogEntry lg = new LogEntry("INSTANCE NOT FOUND!", MsgId.INSTANCE_MISSING);
		    lg.luInstance = getInstanceID();
		    lg.luType = getLuType().luName;
		    throw new InstanceNotFoundException(lg, null);
		}
	}


	public static void fnIIDFFetchInstanceData(String table, Map<String,Object> root_table_pk, Map<Integer,Map<String,Object>> tableDataMap, String targetIid, Object linked_fields, String pos, Long op_ts, Long cr_ts) throws Exception {
		//Fetch Instances data based and send it to kafka
		//We first start with one table record and fetch all its childs records recrusive
		if (tableDataMap == null) tableDataMap = new HashMap<Integer, Map<String, Object>>();
		ResultSetWrapper rs = null;
		Set<String> linked_fieldsSet = null;
		
		try {
		    if (root_table_pk != null) {//Get table's primary keys
		        linked_fieldsSet = (Set<String>) linked_fields;
		        StringBuilder whereSB = new StringBuilder().append(" where ");
		        String prefix = "";
		        Object[] params = new Object[root_table_pk.size()];
		        int i = 0;
		        for (String table_column_name : root_table_pk.keySet()) {
		            whereSB.append(prefix + table_column_name + " = ? ");
		            params[i] = root_table_pk.get(table_column_name);
		            i++;
		            prefix = " and ";
		        }
		
		        Set<String> tblColSet = getLuType().ludbObjects.get(table).ludbColumnMap.keySet();//Get table's columms
		        rs = DBQuery("ludb", "select " + String.join(",", tblColSet) + " from " + getLuType().luName + "." + table + whereSB.toString(), params);
		        Map<String, Object> rootTableMap = rs.getFirstRowAsMap();
		
		        if (rootTableMap == null || rootTableMap.size() == 0) {
		            log.warn(String.format("Can't retrieve table's data, Key not found!, Table Name:%s, Key %s", table, Arrays.toString(params)));
		            return;
		        }
		
		        tableDataMap.put(1, rootTableMap);
		        StringBuilder valSB = new StringBuilder();
		        prefix = "";
		        for (String colVal : rootTableMap.keySet()) {//Get table's record values
		            valSB.append(prefix + "'" + rootTableMap.get(colVal) + "'");
		            prefix = ",";
		        }
		        //fnIIDFSendRecBack2Kafka(table, String.join(",", tblColSet), valSB.toString(), targetIid);
		
		    }
		
		    Map<String, Map<String, String>> trnIIDFCrossIIDTablesList = getTranslationsData("trnIIDFCrossIIDTablesList");
		    SerializeFabricLu.Table rootTableObject = serializeLU(table, 0);//Get table's childs
		    outerloop:
		    for (SerializeFabricLu.Table child_table : rootTableObject.tableChildren) {
		        Map<String, String> trnIIDFCrossIIDTablesListRS = trnIIDFCrossIIDTablesList.get(child_table.tableName.toUpperCase());
		        if (trnIIDFCrossIIDTablesList.keySet().size() > 0 && (!trnIIDFCrossIIDTablesList.keySet().contains(child_table.tableName.toUpperCase()) || !Boolean.parseBoolean(trnIIDFCrossIIDTablesListRS.get("ACTIVE")))){
		            continue;
				}
		        Map<Integer, Map<String, Object>> sonTableDataMap = new HashMap<Integer, Map<String, Object>>();
		        List<LudbRelationInfo> relToChild = getLuType().getLudbPhysicalRelations().get(rootTableObject.tableName).get(child_table.tableName);
		        StringBuilder sqlWhere = new StringBuilder();
		        Set<String> ParentColumnsSet = new LinkedHashSet<>();
		        int i = 0;
		        String prefix = "";
		        for (LudbRelationInfo chRel : relToChild) {//Set table's to child links
		            if (ParentColumnsSet.contains(chRel.from.get("column").toUpperCase())) continue;
		            ParentColumnsSet.add(chRel.from.get("column").toUpperCase());
		            sqlWhere.append(prefix + "(" + child_table.tableName + "." + chRel.to.get("column") + " = ?) ");
		            prefix = " and ";
		        }
		
		        if (root_table_pk != null && linked_fieldsSet != null) {//Validating replicate links match the son table links, Only for son of replicate table
		            if (linked_fieldsSet.size() != ParentColumnsSet.size()) {
		                continue;
		            } else {
		                for (String linkField : ParentColumnsSet) {
		                    if (!linked_fieldsSet.contains(linkField.toUpperCase())) continue outerloop;
		                    ;
		                }
		            }
		        }
		
		        String where = sqlWhere.toString();
		        sqlWhere = new StringBuilder();
		        Set<String> tblColSet = getLuType().ludbObjects.get(child_table.tableName).ludbColumnMap.keySet();//Get child table columns
		        int cnt = 1;
		        int ttl = tableDataMap.size();
		        prefix = "";
		        Map<Integer, Map<String, Object>> recMap = new HashMap<>();
		        for (Map<String, Object> tableDataRec : tableDataMap.values()) {//Loop throw all parent table records
		            sqlWhere.append(prefix + where);
		            prefix = " or ";
		            recMap.put(cnt, tableDataRec);
		            if (ttl + cnt > 1000) {
		                if (cnt % 1000 == 0) {
		                    //Fetch table records
		                    fnIIDFFetchTableData(recMap, ParentColumnsSet, tblColSet, sqlWhere.toString(), child_table.tableName, sonTableDataMap, targetIid, pos, op_ts, cr_ts);
		                    sqlWhere = new StringBuilder();
		                    prefix = "";
		                    cnt = 0;
		                    recMap = new HashMap<>();
		                }
		            } else if (ttl + cnt > 100) {
		                if (cnt % 100 == 0) {
		                    //Fetch table records
		                    fnIIDFFetchTableData(recMap, ParentColumnsSet, tblColSet, sqlWhere.toString(), child_table.tableName, sonTableDataMap, targetIid, pos, op_ts, cr_ts);
		                    sqlWhere = new StringBuilder();
		                    prefix = "";
		                    cnt = 0;
		                    recMap = new HashMap<>();
		                }
		            } else if (ttl + cnt > 10) {
		                if (cnt % 10 == 0) {
		                    //Fetch table records
		                    fnIIDFFetchTableData(recMap, ParentColumnsSet, tblColSet, sqlWhere.toString(), child_table.tableName, sonTableDataMap, targetIid, pos, op_ts, cr_ts);
		                    sqlWhere = new StringBuilder();
		                    prefix = "";
		                    cnt = 0;
		                    recMap = new HashMap<>();
		                }
		            }
		            cnt++;
		            ttl--;
		        }
		        //Fetch table records
		        if (sqlWhere.length() == 0) sqlWhere.append(where);
		        fnIIDFFetchTableData(recMap, ParentColumnsSet, tblColSet, sqlWhere.toString(), child_table.tableName, sonTableDataMap, targetIid, pos, op_ts, cr_ts);
		        cnt = 0;
		        recMap = new HashMap<>();
		        //Fetch child table childs records
		        fnIIDFFetchInstanceData(child_table.tableName, null, sonTableDataMap, targetIid, null, pos, op_ts, cr_ts);
		    }
		} finally {
		    if (rs != null) rs.closeStmt();
		}
	}


	public static void fnIIDFFetchTableData(Object tableDataRec, Object ParentColumnsSet, Object tblColSet, String sqlWhere, String child_table_name, Object sonTableDataMap, String targetIid, String pos, Long op_ts, Long cr_ts) throws Exception {
		Map<Integer, Map<String, Object>> recMap = (Map<Integer, Map<String, Object>>) tableDataRec;
		Object[] params = new Object[((Set) ParentColumnsSet).size() * recMap.size()];
		int i = 0;
		if(recMap.size() == 0)return;
		for (Map<String, Object> mapRec : ((Map<Integer, Map<String, Object>>) tableDataRec).values()) {
		    for (String parentTableColumnVal : ((Set<String>) ParentColumnsSet)) {
		        params[i] = mapRec.get(parentTableColumnVal);//Prepare params for execution
		        i++;
		    }
		}
		
		//Fetch records
		try (Db.Rows rs = ludb().fetch("select " + String.join(",", ((Set) tblColSet)) + " from " + getLuType().luName + "." + child_table_name + " where " + sqlWhere, params)) {
		    i = 0;
		    for (Db.Row row : rs) {
		        Map<String, Object> sonMap = new LinkedHashMap<>();
		        int colCnt = 0;
		        List<Object> vals = new LinkedList();
		        for (String column : ((Set<String>) tblColSet)) {
		            sonMap.put(column.toUpperCase(), row.cell(colCnt));
		            if (row.cell(colCnt) == null) {
		                vals.add(JSONObject.NULL);
		            } else {
		                vals.add(row.cell(colCnt));
		            }
		            colCnt++;
		        }
		        ((Map<Integer, Map<String, Object>>) sonTableDataMap).put(i, sonMap);//Add table column values to map
		        i++;
		        //Send record to kafka
		        fnIIDFSendRecBack2Kafka(child_table_name, tblColSet, vals, targetIid, pos, op_ts, "R", cr_ts);
		        vals.clear();
		    }
		}
	}


	public static void fnIIDFExecTableCustomCode(Object dc, Boolean post_exec) throws Exception {
		DataChange dataChange = (DataChange) dc;
		String tableName = dataChange.getTablespace() + IIDF_SCHEMA_TABLE_SEP_VAL + dataChange.getTable();
		//Check if table name exists in translaion
		Map<String, Long> debugTiming = (Map<String, Long>) getThreadGlobals("DEBUG_TIMING_MAP");
		Map<String, String> translationOutput = getTranslationValues("trnExecUserActivity", new Object[]{tableName, post_exec});
		if (!translationOutput.isEmpty()) {
		    //If global name is not null set table global
		    if (translationOutput.get("global_name") != null) {
		        String[] gloVals = translationOutput.get("global_value").split(",");
		        int i = 0;
		        for (String gloName : translationOutput.get("global_name").split(",")) {
		            if (gloName == null || "null".equalsIgnoreCase(gloName) || "".equals(gloName)) continue;
		            setThreadGlobals(gloName, gloVals[i]);
		            i++;
		        }
		    }
		    //If funcaion name is not null execute function
		    if (translationOutput.get("function_name") != null) {
		        String[] userMethodsLst = translationOutput.get("function_name").split(",");
		        for (String userMethod : userMethodsLst) {
		            if (userMethod == null || "null".equalsIgnoreCase(userMethod) || "".equals(userMethod)) continue;
		            FunctionDef method = (FunctionDef) LUTypeFactoryImpl.getInstance().getTypeByName(getLuType().luName).ludbFunctions.get(userMethod);
		            if (method == null) {
		                throw new NoSuchMethodException(String.format("user function '%s' was not found", translationOutput.get("function_name")));
		            } else {
		                try {
		                    Instant startTime = Instant.now();
		                    method.invoke(null, new Object[]{dataChange, functionContext()});
		                    debugTiming.put("fnIIDFExecTableCustomCode: " + getThreadGlobals("fnIIDFExecTableCustomCode") + " -> " + userMethod, Duration.between(startTime, Instant.now()).toMillis());
		                } catch (ReflectiveOperationException | InterruptedException e) {
		                    log.error("Failed to invoke user function!", e);
		                    if (inDebugMode())
		                        reportUserMessage("Failed to invoke user function!, Exception Details:" + e.getMessage());
		                }
		            }
		        }
		    }
		}
	}


    @out(name = "tableSourceFullName", type = String.class, desc = "")
    public static String fnIIDFGetTableSourceFullName(String table_name) throws Exception {
        //Getting LU table full source name (schema and table name)
        Optional<TableProperty> tablePropertes = TableProperties.getInstance().getTablePropertyForLu(getLuType().luName, table_name);
        if (tablePropertes.isPresent()) {
            TableProperty tblProp = tablePropertes.get();
            return tblProp.getTableKeyspace() + "." + tblProp.getTableName();
        } else {
            log.warn("Can't find table name!");
            return null;
        }
    }


	@type(DecisionFunction)
	@out(name = "decision", type = Boolean.class, desc = "")
	public static Boolean fnIIDFCheckExtractFromSourceInd() throws Exception {
		if ((isFirstSync() || SyncMode.FORCE.toString().equalsIgnoreCase(getSyncMode())) && Boolean.parseBoolean(IIDF_EXTRACT_FROM_SOURCE)) {
		    return true;
		} else if (("true").equals(IIDF_EXTRACT_FROM_SOURCE)) {
		    String tableName = getTableName();
		    if (tableName == null) tableName = "";
		    Set<String> parentsList = new HashSet<>();
		    if (getThreadGlobals("childCrossInsts" + tableName.toUpperCase()) != null) {
		        return true;
		    } else {
		        if (getThreadGlobals("IIDF_PARENT_TABLES") == null) {
		            setThreadGlobals("IIDF_PARENT_TABLES", fnIIDFGetParentTable(getLuType().luName));
		        } else {
		            HashMap<String, HashSet<String>> prntTable = (HashMap<String, HashSet<String>>) getThreadGlobals("IIDF_PARENT_TABLES");
		            if (prntTable != null && !prntTable.isEmpty()) {
		                parentsList = prntTable.get(tableName);
		            }
		        }
		        if (parentsList != null) {
		            for (String table : parentsList) {
		                if (getThreadGlobals("childCrossInsts" + table.toUpperCase()) != null) {
		                    setThreadGlobals("childCrossInsts" + tableName.toUpperCase(), "1");
		                    return true;
		                }
		            }
		        }
		    }
		}
		return false;
	}


    public static void fnIIDFUpdateStats(Object statsMapInp) throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        Db ci = ludb();
        ci.beginTransaction();
        Map<String, Map<String, Integer>> statsMap = (Map<String, Map<String, Integer>>) statsMapInp;
        for (Map.Entry<String, Map<String, Integer>> mapEnt : statsMap.entrySet()) {
            Map<String, Integer> tableStats = mapEnt.getValue();
            Db.Row rowData = null;
            try (Db.Rows rows = db(DB_FABRIC_NAME).fetch("Select TOTAL_UPDATE, TOTAL_INSERT, TOTAL_DELETE, TOTAL_REPLICATE, TOTAL_IIDCHANGE from IIDF_STATISTICS where LU_NAME = ? and IID = ? and TABLE_NAME = ?", new Object[]{getLuType().luName, getInstanceID(), mapEnt.getKey()})) {
                rowData = rows.firstRow();
            }
            int total_update = rowData.cell(0) != null ? Integer.parseInt(rowData.cell(0) + "") : 0;
            int total_insert = rowData.cell(1) != null ? Integer.parseInt(rowData.cell(1) + "") : 0;
            int total_delete = rowData.cell(2) != null ? Integer.parseInt(rowData.cell(2) + "") : 0;
            int total_repicate = rowData.cell(3) != null ? Integer.parseInt(rowData.cell(3) + "") : 0;
            int total_iidchange = rowData.cell(4) != null ? Integer.parseInt(rowData.cell(4) + "") : 0;
            for (Map.Entry<String, Integer> tblEnt : tableStats.entrySet()) {
                if (tblEnt.getKey().equalsIgnoreCase("UPDATE")) {
                    total_update += tblEnt.getValue();
                } else if (tblEnt.getKey().equalsIgnoreCase("DELETE")) {
                    total_delete += tblEnt.getValue();
                } else if (tblEnt.getKey().equalsIgnoreCase("INSERT")) {
                    total_insert += tblEnt.getValue();
                } else if (tblEnt.getKey().equalsIgnoreCase("REPLICATE")) {
                    total_insert += tblEnt.getValue();
                } else if (tblEnt.getKey().equalsIgnoreCase("IID_CHANGE")) {
                    total_iidchange += tblEnt.getValue();
                }
            }
            ci.execute("INSERT OR REPLACE INTO IIDF_STATISTICS (LU_NAME, IID, TABLE_NAME, TOTAL_UPDATE, TOTAL_INSERT, TOTAL_DELETE, TOTAL_REPLICATE, LAST_EXECUTION_TIME, TOTAL_IIDCHANGE) values (?,?,?,?,?,?,?,?,?)", new Object[]{getLuType().luName, getInstanceID(), mapEnt.getKey(), total_update, total_insert, total_delete, total_repicate, dateFormat.format(date), total_iidchange});
        }
        ci.commit();
    }


    public static void fnIIDFSetStats(String Operation, Object tableStatsMapInp, Boolean isIIDChange) throws Exception {
        Map<String, Integer> tableStatsMap = (Map<String, Integer>) tableStatsMapInp;
        if (Operation.equalsIgnoreCase("UPDATE")) {
            if (tableStatsMap.keySet().contains("UPDATE")) {
                int update = tableStatsMap.get("UPDATE");
                tableStatsMap.put("UPDATE", ++update);
            } else {
                tableStatsMap.put("UPDATE", 1);
            }
        } else if (Operation.equalsIgnoreCase("INSERT")) {
            if (tableStatsMap.keySet().contains("INSERT")) {
                int insert = tableStatsMap.get("INSERT");
                tableStatsMap.put("INSERT", ++insert);
            } else {
                tableStatsMap.put("INSERT", 1);
            }
        } else if (Operation.equalsIgnoreCase("DELETE")) {
            if (tableStatsMap.keySet().contains("DELETE")) {
                int delete = tableStatsMap.get("DELETE");
                tableStatsMap.put("DELETE", ++delete);
            } else {
                tableStatsMap.put("DELETE", 1);
            }
        } else if (Operation.equalsIgnoreCase("REPLICATE")) {
            if (tableStatsMap.keySet().contains("REPLICATE")) {
                int replicate = tableStatsMap.get("REPLICATE");
                tableStatsMap.put("REPLICATE", ++replicate);
            } else {
                tableStatsMap.put("REPLICATE", 1);
            }
        }
        if (isIIDChange) {
            if (tableStatsMap.keySet().contains("IID_CHANGE")) {
                int iidChange = tableStatsMap.get("IID_CHANGE");
                tableStatsMap.put("IID_CHANGE", ++iidChange);
            } else {
                tableStatsMap.put("IID_CHANGE", 1);
            }
        }
    }


	public static void fnIIDFCheckCrossIns(Object i_dataChanges) throws Exception {
		Set<String> syncFromSourceTableList;
		Object rs1 = getThreadGlobals("SOURCE_ACCESS_TABLES_LIIST");
		if (rs1 == null) {
		    syncFromSourceTableList = new HashSet<>();
		} else {
		    syncFromSourceTableList = (Set<String>) rs1;
		}
		List<Long> dataChanges = (List<Long>) i_dataChanges;
		boolean isLogicalChildChanged = false;
		List<Object> valuesForCrossInsts = new ArrayList<>();
		String[] fullChildLinksArr = null;
		String[] fullParentLinksArr = null;
		DataChange dataChange = null;
		for (Long dataChange_rowID : dataChanges) {
		    Object dc = ludb().fetch("select DATA_CHANGE from " + getLuType().luName + ".IIDF_QUEUE where rowid = ?", dataChange_rowID).firstValue();
		    if (dc == null) {
		        log.warn("fnIIDFCheckCrossIns - Cant find datachange");
		        continue;
		    }
		    dataChange = DataChange.fromJson(dc + "");
		    String tableName = dataChange.getTablespace() + IIDF_SCHEMA_TABLE_SEP_VAL + dataChange.getTable();
		    String Operation = "" + dataChange.getOperation();
		    isLogicalChildChanged = dataChange.isLogicalChildChanged();
		    SerializeFabricLu.Table table_Object = serializeLU(tableName, 1);
		    Set<SerializeFabricLu.Table> childs = table_Object.tableChildren;
		    outerloop:
		    for (SerializeFabricLu.Table child : childs) {
		        String childTblName = child.tableName;
		        if (Operation.equals("insert") && getThreadGlobals("checkCrossInstns" + tableName.toUpperCase()) != null) {
		            setThreadGlobals("checkCrossInstns" + childTblName.toUpperCase(), "1");
		            syncFromSourceTableList.add(childTblName.toUpperCase());
		        } else if (Operation.equals("insert") || ((Operation.equals("update") && isLogicalChildChanged))) {
		            Map<String, Object> keysMap = dataChange.getKeys();
		            Map<String, Object> valuesMap = dataChange.getValues();
		            Map<String, Object> allKeysMap = new HashMap<>();
		            allKeysMap.putAll(keysMap);
		            allKeysMap.putAll(valuesMap);
		
		            if (getThreadGlobals("childCrossInsts" + childTblName.toUpperCase()) != null) {
		                continue;
		            } else {
		                if (Boolean.parseBoolean(CROSS_IID_SKIP_LOGIC) || getTranslationsData("trnIIDFCrossForceTableSync").keySet().contains(child.tableName.toUpperCase()) || (Operation.equals("insert") && dataChange.getOrgOperation().equals(DataChange.Operation.replicate))) {
							setThreadGlobals("childCrossInsts" + childTblName.toUpperCase(), "1");
		                    syncFromSourceTableList.add(childTblName.toUpperCase());
		                    continue;
		                }
		
		
		                valuesForCrossInsts = new ArrayList<>();
		                StringBuilder selectStm = new StringBuilder().append("select 1 from " + getLuType().luName + "." + childTblName + " where ");
		
		                Set<Map<String, String>> tblRelLis = child.tblRelLis;
		                for (Map<String, String> relations : tblRelLis) {
		                    fullChildLinksArr = relations.get("linked_To_Columns").split(",");
		                    fullParentLinksArr = relations.get("linked_From_Columns").split(",");
		                }
		
		                String prefixSlct = "";
		                for (String childLinks : fullChildLinksArr) {
		                    selectStm.append(prefixSlct + childLinks + " = ? ");
		                    prefixSlct = " and ";
		                }
		
		                for (String parentLink : fullParentLinksArr) {
		                    if (allKeysMap.containsKey(parentLink.toUpperCase())) {
		                        valuesForCrossInsts.add(allKeysMap.get(parentLink.toUpperCase()));
		                    } else {
		                        List<Object> vals = new ArrayList<>();
		                        StringBuilder sb = new StringBuilder();
		                        String prefix = "";
		                        String[] pkCuls = (String[]) fnIIDFGetTablePK(tableName.toUpperCase(), getLuType().luName);
		                        for (String pkCul : pkCuls) {
		                            sb.append(prefix + pkCul + " = ? ");
		                            prefix = " and ";
		                            vals.add(allKeysMap.get(pkCul.toUpperCase()));
		                        }
		
		                        try (Db.Rows rs = ludb().fetch("select " + parentLink + " from " + getLuType().luName + "." + tableName + " where " + sb.toString(), vals.toArray())) {
		                            Object rowVal = rs.firstValue();
		                            if (rowVal != null) {
		                                valuesForCrossInsts.add(rowVal);
		                            } else {
		                                continue outerloop;
		                            }
		                        }
		                    }
		                }
		
		                if (valuesForCrossInsts.size() > 0) {
		                    try (Db.Rows rs = ludb().fetch(selectStm.toString(), valuesForCrossInsts.toArray())) {
		                        Object countChildObj = rs.firstValue();
		                        if (countChildObj == null) {
		                            setThreadGlobals("childCrossInsts" + childTblName.toUpperCase(), "1");
		                            syncFromSourceTableList.add(childTblName.toUpperCase());
		                        }
		                    }
		                }
		            }
		        }
		    }
		}
		setThreadGlobals("SOURCE_ACCESS_TABLES_LIIST", syncFromSourceTableList);
	}


	public static void fnIIDFExecInsNullPK(Object dc) throws Exception {
		DataChange dataChange = (DataChange) dc;
		Set<String> tblPKSet = dataChange.getKeys().keySet();
		Map<String, Object> TblAllValsMap = dataChange.getValues();
		String prefixWhere = "";
		String prefixSet = "";
		Set<Object> whereVals = new LinkedHashSet<>();
		Set<Object> setVals = new LinkedHashSet<>();
		StringBuilder updateStmt = new StringBuilder().append(" update " + getLuType().luName + "." + dataChange.getTablespace() + IIDF_SCHEMA_TABLE_SEP_VAL + dataChange.getTable() + " set ");
		StringBuilder whereStmt = new StringBuilder().append(" where ");
		for (Map.Entry<String, Object> tblVals : TblAllValsMap.entrySet()) {
		    if (tblPKSet.contains(tblVals.getKey())) {
		        if (tblVals.getValue() == null) {
		            whereStmt.append(prefixWhere + tblVals.getKey() + " is null ");
		        } else {
		            whereStmt.append(prefixWhere + tblVals.getKey() + " = ? ");
		            whereVals.add(tblVals.getValue());
		            prefixWhere = " and ";
		        }
		    } else {
		        updateStmt.append(prefixSet + tblVals.getKey() + " = ? ");
		        setVals.add(tblVals.getValue());
		        prefixSet = ",";
		    }
		}
		
		Set<Object> parmas = new LinkedHashSet<>();
		parmas.addAll(setVals);
		parmas.addAll(whereVals);
		Object rs = null;
		try (Db.Rows rs2 = ludb().fetch("select 1 from " + getLuType().luName + "." + dataChange.getTablespace() + IIDF_SCHEMA_TABLE_SEP_VAL + dataChange.getTable() + whereStmt.toString(), whereVals.toArray())) {
		    rs = rs2.firstValue();
		}
		if (rs == null) {//If its the first time we execute it we run insert
		    String tableName = dataChange.getTablespace() + IIDF_SCHEMA_TABLE_SEP_VAL + dataChange.getTable();//Get table name from data change
			ludb().execute(dataChange.toSql(DataChange.Operation.upsert, tableName), dataChange.sqlValues());
		} else {//If its not the first time we execute it we run update
		    ludb().execute(updateStmt.toString() + whereStmt.toString(), parmas.toArray());
		}
	}


	@out(name = "rec_deleted", type = Boolean.class, desc = "")
	public static Boolean fnIIDFDeleteTableOrphanRecords(String tableName, Object parent_tables, String targetIid, String pos, Long op_ts, Long cr_ts) throws Exception {
		Set<String> deletedTables = getThreadGlobals("DELETED_TABLES") != null ? (HashSet<String>) getThreadGlobals("DELETED_TABLES") : new HashSet<>();
		if (deletedTables.contains(tableName)) {
		    return false;
		} else {
		    deletedTables.add(tableName);
		    setThreadGlobals("DELETED_TABLES", deletedTables);
		}
		
		//Get table record from trnIIDFOrphanRecV
		Map<String, String> trnMap = getTranslationValues("trnIIDFOrphanRecV", new Object[]{tableName.toUpperCase()});
		//Check if to run orphan record check on table
		if (trnMap.get("SKIP_VALIDATE_IND") != null && trnMap.get("SKIP_VALIDATE_IND").equalsIgnoreCase("TRUE")) {
		    return false;
		}
		
		HashSet<String> prntTables = ((HashMap<String, HashSet<String>>) parent_tables).get(tableName.toUpperCase());
		StringBuilder sqlQuery = new StringBuilder();
		String prefix = "";
		for (String parent_table : prntTables) {//Loop throw all table's parents and delete orphan records
		
		    TreeMap<String, List<LudbRelationInfo>> phyRelTbl = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);//Get table -> child links
		    phyRelTbl.putAll(((TreeMap<String, Map<String, List<LudbRelationInfo>>>) getThreadGlobals("LUDB_PHYISICAL_RELATION")).get(parent_table + ""));
		
		    HashMap<String, String> popRelMap = new HashMap<String, String>();
		    List<LudbRelationInfo> parentRelations = phyRelTbl.get(tableName);
		    for (LudbRelationInfo chRel : parentRelations) {
		        String popMapName = (String) chRel.to.get("populationObjectName");
		        if (popRelMap.containsKey(popMapName)) {
		            popRelMap.put(popMapName, popRelMap.get(popMapName) + " and " + chRel.from.get("column") + " = " + tableName + "." + chRel.to.get("column"));
		        } else {
		            popRelMap.put(popMapName, chRel.from.get("column") + " = " + tableName + "." + chRel.to.get("column"));
		        }
		    }
		
		    for (String con : popRelMap.values()) {
		        sqlQuery.append(prefix + " not exists (select 1 from " + getLuType().luName + "." + parent_table + " where " + con + " ) ");
		        prefix = " and ";
		    }
		}
		
		//Check if to add another condition to orphan records check query
		if (trnMap.get("VALIDATION_SQL") != null && !trnMap.get("VALIDATION_SQL").equalsIgnoreCase("")) {
		    sqlQuery.append(" and not exists (" + trnMap.get("VALIDATION_SQL") + ")");
		}
		boolean rec_deleted = fnIIDFGetRecData(tableName, sqlQuery.toString(), targetIid, pos, op_ts, true, cr_ts);//Before deleting records send them to kafka
		ludb().execute("delete from  " + getLuType().luName + "." + tableName + " where " + sqlQuery.toString());//Delete orphan records
		return rec_deleted;
	}


    @type(RootFunction)
    @out(name = "IID", type = String.class, desc = "")
    public static void fnIIDFPopRecentDummy(String IID) throws Exception {
        if (false)
            yield(new Object[]{null});
    }


    private static void setTblPrnt(LudbObject table, HashMap<String, HashSet<String>> prntTable) {
        if (table.childObjects == null) {
            return;
        }
        for (LudbObject chiTbl : table.childObjects) {

            HashSet<String> prntArr = prntTable.get(chiTbl.k2StudioObjectName.toUpperCase());
            if (prntArr == null) {
                prntArr = new HashSet<String>();
            }
            prntArr.add(table.k2StudioObjectName);
            prntTable.put(chiTbl.k2StudioObjectName.toUpperCase(), prntArr);

            setTblPrnt(chiTbl, prntTable);
        }
    }

    private static Properties getSSLProperties() {
        Properties props = new Properties();
        if (com.k2view.cdbms.shared.IifProperties.getInstance().getProp().getBoolean("SSL_ENABLED", false)) {
            appendProperty(props, "security.protocol", com.k2view.cdbms.shared.IifProperties.getInstance().getProp().getString("SECURITY_PROTOCOL", null));
            appendProperty(props, "ssl.truststore.location", com.k2view.cdbms.shared.IifProperties.getInstance().getProp().getString("TRUSTSTORE_LOCATION", null));
            appendProperty(props, "ssl.truststore.password", com.k2view.cdbms.shared.IifProperties.getInstance().getProp().getString("TRUSTSTORE_PASSWORD", null));
            appendProperty(props, "ssl.keystore.location", com.k2view.cdbms.shared.IifProperties.getInstance().getProp().getString("KEYSTORE_LOCATION", null));
            appendProperty(props, "ssl.keystore.password", com.k2view.cdbms.shared.IifProperties.getInstance().getProp().getString("KEYSTORE_PASSWORD", null));
            appendProperty(props, "ssl.key.password", com.k2view.cdbms.shared.IifProperties.getInstance().getProp().getString("KEY_PASSWORD", null));
            props.setProperty("ssl.endpoint.identification.algorithm", "");
            appendProperty(props, "ssl.endpoint.identification.algorithm", com.k2view.cdbms.shared.IifProperties.getInstance().getProp().getString("ENDPOINT_IDENTIFICATION_ALGORITHM", null));
            appendProperty(props, "ssl.cipher.suites", com.k2view.cdbms.shared.IifProperties.getInstance().getProp().getString("SSL_CIPHER_SUITES", null));
            appendProperty(props, "ssl.enabled.protocols", com.k2view.cdbms.shared.IifProperties.getInstance().getProp().getString("SSL_ENABLED_PROTOCOLS", null));
            appendProperty(props, "ssl.truststore.type", com.k2view.cdbms.shared.IifProperties.getInstance().getProp().getString("SSL_TRUSTSTORE_TYPE", null));
            return props;
        } else {
            return props;
        }
    }

    public static void UserKafkaProperties(Properties props) {
        props.put("bootstrap.servers", IifProperties.getInstance().getKafkaBootsrapServers());
        props.put("acks", "all");
        props.put("retries", "5");
        props.put("batch.size", "" + IifProperties.getInstance().getKafkaBatchSize());
        props.put("linger.ms", 1);
        props.put("max.block.ms", "" + IifProperties.getInstance().getKafkaMaxBlockMs());
        props.put("buffer.memory", "" + IifProperties.getInstance().getKafkaBufferMemory());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Properties sslProps = getSSLProperties();
        props.putAll(sslProps);
    }

    public static void UserKafkaConsumProperties(Properties props, String groupId) {
        props.put("group.id", groupId == null ? "IIDFinderGroupId" : groupId);
        props.put("bootstrap.servers", IifProperties.getInstance().getKafkaBootsrapServers());
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", 50);
        props.put("session.timeout.ms", 120000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Properties sslProps = getSSLProperties();
        props.putAll(sslProps);
    }

	public static void fnIIDFCleanThreadGlobals() throws Exception {
		Object iidDeltaCount = getThreadGlobals("IID_DELTA_COUNT");
		if (iidDeltaCount != null) {
		    CustomStats.count("idfinder_stats_" + getLuType().luName.toLowerCase(), "deleted_deltas", Long.parseLong(iidDeltaCount + ""));
		}
		
		clearThreadGlobals();
	}

    private static void appendProperty(Properties p, String key, String value) {
        Objects.requireNonNull(key);
        if (!Util.isEmpty(value)) {
            p.put(key, value);
        }
    }

	public static void fnIIDFInsertIIDStats() throws Exception {
		Instant startTime = (Instant) getThreadGlobals("SYNC_TIME");
		long ttlSyncTime = Duration.between(startTime, Instant.now()).toMillis();
		List<Object> paramsStats = (LinkedList<Object>) getThreadGlobals("STATS");
		
		long replicates_send_back_r = 0;
		if (getThreadGlobals("SEND_BACK_CNT_R") != null) {
		    replicates_send_back_r = Long.parseLong(("" + getThreadGlobals("SEND_BACK_CNT_R")));
		}
		
		long replicates_send_back_d = 0;
		if (getThreadGlobals("SEND_BACK_CNT_D") != null) {
		    replicates_send_back_d = Long.parseLong(("" + getThreadGlobals("SEND_BACK_CNT_D")));
		}
		
		long accountChange = 0;
		if (getThreadGlobals("ACCOUNT_CHANGE") != null) {
		    accountChange = Long.parseLong(getThreadGlobals("ACCOUNT_CHANGE") + "");
		}
		
		long is_iid_change = 0;
		if (getThreadGlobals("is_iid_change") != null) {
		    is_iid_change = Long.parseLong(getThreadGlobals("is_iid_change") + "");
		}
		
		boolean iid_skipped = false;
		if (getThreadGlobals("IID_SKIPPED") != null) {
		    iid_skipped = Boolean.parseBoolean(getThreadGlobals("IID_SKIPPED") + "");
		}
		
		Set<String> syncFromSourceTableList;
		Object rs1 = getThreadGlobals("SOURCE_ACCESS_TABLES_LIIST");
		if (rs1 == null) {
		    syncFromSourceTableList = new HashSet<>();
		} else {
		    syncFromSourceTableList = (Set<String>) rs1;
		}
		
		class myRunnable implements Runnable {
		    private String iid;
		    private List<Object> paramsStats;
		    private long replicates_send_back_r;
		    private long replicates_send_back_d;
		    long ttlSyncTime;
		    long accountChange = 0;
		    UserCodeDelegate i_ucd;
		    long is_iid_change = 0;
		    boolean iid_skipped = false;
		    Set<String> syncFromSourceTableList;
		    Session cassandraSession = CassandraClusterSingleton.getInstance().getSession();
		
		    public myRunnable(String iid, List<Object> paramsStats, long replicates_send_back_r, long replicates_send_back_d, long ttlSyncTime, UserCodeDelegate i_ucd, long accountChange, long is_iid_change, boolean iid_skipped, Set<String> syncFromSourceTableList) {
		        this.iid = iid;
		        this.paramsStats = paramsStats;
		        this.replicates_send_back_r = replicates_send_back_r;
		        this.replicates_send_back_d = replicates_send_back_d;
		        this.ttlSyncTime = ttlSyncTime;
		        this.i_ucd = i_ucd;
		        this.accountChange = accountChange;
		        this.is_iid_change = is_iid_change;
		        this.iid_skipped = iid_skipped;
		        this.syncFromSourceTableList =  syncFromSourceTableList;
		    }
		
		    public void run() {
		        try {
		            com.datastax.driver.core.PreparedStatement preStmt;
		            Object rs = Globals.get("STATS_PREPARED_" + this.i_ucd.getLuType().luName);
		            if (rs == null) {
		                preStmt = cassandraSession.prepare("insert into " + this.i_ucd.getLuType().getKeyspaceName() + ".iid_stats (iid,iid_sync_time,sync_duration,time_to_fetch_deltas_from_api,time_to_insert_deltas_to_iidfqueue_table,iid_deltas_count,time_to_fetch_deltas_from_iidf_queue_and_execute,time_to_execute_replicates_request,replicates_requests_count,time_to_execute_delete_orphan_check,delete_orphan_check_count,time_to_execute_cross_instance_check,cross_instance_check_count,replicates_send_to_kafka_due_to_delete_orphan_count,replicates_send_to_kafka_due_to_replicate_request_count,iid_change_ind_count,account_change_count,iid_skipped) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		                Globals.set("STATS_PREPARED_" + this.i_ucd.getLuType().luName, preStmt);
		            } else {
		                preStmt = (com.datastax.driver.core.PreparedStatement) rs;
		            }
		            cassandraSession.execute(preStmt.bind(new Object[]{iid, new Timestamp(System.currentTimeMillis()), ttlSyncTime, paramsStats.get(0), paramsStats.get(1), paramsStats.get(2), paramsStats.get(3), paramsStats.get(4), paramsStats.get(5), paramsStats.get(6), paramsStats.get(7), paramsStats.get(8), paramsStats.get(9), replicates_send_back_d, replicates_send_back_r, this.is_iid_change, this.accountChange, this.iid_skipped}));
		
		            final java.text.DateFormat clsDateFormat = new java.text.SimpleDateFormat("yyyyMMddHH");
		            clsDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		            java.util.Date currentTime = new java.util.Date();
		
		            rs = Globals.get("SYNC_FROM_SOURCE_PREPARED_" + this.i_ucd.getLuType().luName);
		            if (rs == null) {
		                preStmt = cassandraSession.prepare("insert into " + this.i_ucd.getLuType().getKeyspaceName() + ".source_access_stats (sync_day_and_hour, iid_sync_time, iid, tables_count) values (?,?,?,?)");
		                Globals.set("SYNC_FROM_SOURCE_PREPARED_" + this.i_ucd.getLuType().luName, preStmt);
		            } else {
		                preStmt = (com.datastax.driver.core.PreparedStatement) rs;
		            }
		            if(this.syncFromSourceTableList.size() > 0)cassandraSession.execute(preStmt.bind(new Object[]{Integer.parseInt(clsDateFormat.format(currentTime)), new Timestamp(System.currentTimeMillis()), iid, this.syncFromSourceTableList.size()}));
		        } catch (Exception e) {
		            log.error("fnIIDFInsertIIDStats,", e);
		        }
		    }
		}
		if (Boolean.parseBoolean(getLuType().ludbGlobals.get("IID_STATS") + "")) {
		    java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(1);
		    executor.submit(new myRunnable(getInstanceID(), paramsStats, replicates_send_back_r, replicates_send_back_d, ttlSyncTime, functionContext(), accountChange, is_iid_change, iid_skipped, syncFromSourceTableList));
		    executor.shutdown();
		}
	}
	
	@out(name = "rs", type = Boolean.class, desc = "")
	public static Boolean fnIIDFExecNonLUTableDelta(Object i_dataChange) throws Exception {
		DataChange dataChange = (DataChange) i_dataChange;
		String tableName = dataChange.getTablespace() + IIDF_SCHEMA_TABLE_SEP_VAL + dataChange.getTable();
		Map<String, String> trnMap = getTranslationValues("trnIIDFExecNonLUTableDelta", new Object[]{tableName.toUpperCase()});
		if (trnMap.get("ACTIVE") != null && trnMap.get("ACTIVE").equalsIgnoreCase("TRUE")) {
		    setThreadGlobals("fnIIDFExecTableCustomCode", "fnIIDFExecNonLUTableDelta");
		    fnIIDFExecTableCustomCode(dataChange, false);
		    setThreadGlobals("fnIIDFExecTableCustomCode", "");
		    if (trnMap.get("SKIP") != null && trnMap.get("SKIP").equalsIgnoreCase("TRUE")) {
		        return true;
		    } else {
		        return false;
		    }
		} else {
		    return false;
		}
	}
	public static void fnIIDFUpdateSelectiveTable(Object dc, Object i_deleteOrphan) throws Exception {
		DataChange dataChange = (DataChange) dc;
		Map<String, DataChange> deleteOrphan = (Map<String, DataChange>) i_deleteOrphan;
		boolean execInsert = false;
		String tableName = dataChange.getTablespace() + IIDF_SCHEMA_TABLE_SEP_VAL + dataChange.getTable();
		if (!getTranslationsData("trnSelectiveTables").keySet().contains(tableName.toUpperCase())) return;
		Map<String, String> trnMap = getTranslationValues("trnSelectiveTables", new Object[]{tableName.toUpperCase()});
		if (dataChange.getOperation().equals(DataChange.Operation.update)) {
			Map<String, Object> beforeValues = dataChange.getBeforeValues();
		    Map<String, Object> afterValues = dataChange.getValues();
		    Map<String, Object> dcKeys = dataChange.getKeys();
		    for (Map.Entry<String, Object> keysEnt : dcKeys.entrySet()) {
		        if (!afterValues.get(keysEnt.getKey()).equals(keysEnt.getValue())) return;
		    }
		    String[] columnsList = trnMap.get("column_list") != null && trnMap.get("column_list").trim().length() > 0 ? trnMap.get("column_list").split(",") : null;
		    if (beforeValues.size() > 0 && columnsList != null) {
		        for (String columnName : columnsList) {
		            if (beforeValues.containsKey(columnName.toUpperCase()) && !beforeValues.get(columnName.toUpperCase()).toString().equalsIgnoreCase(afterValues.get(columnName.toUpperCase()).toString())) {
		                dataChange.setOperation(DataChange.Operation.upsert);
		                deleteOrphan.put(tableName, dataChange);
		                break;
		            }
		        }
		    } else {
		        dataChange.setOperation(DataChange.Operation.upsert);
		        deleteOrphan.put(tableName, dataChange);
		    }
		}
	}
	public static void fnIIDFCleanIIDFinderCTables(Object i_dc) throws Exception {
		DataChange dc = (DataChange) i_dc;
		String LUTableName = dc.getTablespace() + IIDF_SCHEMA_TABLE_SEP_VAL + dc.getTable();
		Map<String, String> keys = new HashMap<>();
		StringBuilder sqlGetRowValuesWhere = new StringBuilder();
		List<Object> params = new ArrayList<>();
		String prefix = "";
		Map<String, Object> dataChangeKeys = dc.getKeys();
		for (Map.Entry<String, Object> mapEnt : dataChangeKeys.entrySet()) {
		    sqlGetRowValuesWhere.append(prefix + mapEnt.getKey() + " = ? ");
		    prefix = " and ";
		    params.add(mapEnt.getValue());
		    keys.put(mapEnt.getKey(), mapEnt.getValue() + "");
		}
		
		Db.Row rs = ludb().fetch(String.format("Select * from " + getLuType().luName + ".%s where %s", LUTableName, sqlGetRowValuesWhere.toString()), params.toArray()).firstRow();
		
		TreeMap<String, List<LudbRelationInfo>> phyRel = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
		Map<String, Map<String, List<LudbRelationInfo>>> LUPR = getLuType().getLudbPhysicalRelations();
		Map<String, List<LudbRelationInfo>> LUTR = LUPR.get(LUTableName);
		if (LUTR == null) {
		    LUTR = LUPR.get(LUTableName.toLowerCase());
		    if (LUTR == null) return;
		}
		phyRel.putAll(LUTR);
		Set<String> tableLinksToChildes = new HashSet<>();
		Map<String, Object> dcAfterValues = dc.getValues();
		boolean rubDF = false;
		for (LudbObject childTable : getLuType().ludbObjects.get(LUTableName).childObjects) {
		    List<LudbRelationInfo> childParentRelation = phyRel.get(childTable.ludbObjectName);
		    for (LudbRelationInfo chRel : childParentRelation) {
		        tableLinksToChildes.add(chRel.from.get("column"));
		    }
		    Map<String, Object> linkedColumnValuesFromAfter = new HashMap<>();
		    for (String parentLinkedColumn : tableLinksToChildes) {
		        if (dcAfterValues.containsKey(parentLinkedColumn)) {
		            String rowColumnValue = rs.get(parentLinkedColumn) + "";
		            String afterValue = dcAfterValues.get(parentLinkedColumn) + "";
		            if (!rowColumnValue.equals(afterValue)) {
		                tableLinksToChildes.forEach(column -> keys.put(column, rs.get(column) + ""));
		                rubDF = true;
		                break;
		            }
		        }
		    }
		    if (rubDF) addDeleteFinder(getLuType().luName, getInstanceID(), LUTableName, keys);
		    tableLinksToChildes.clear();
		}
		
		Map<String, String> trnRS = getTranslationValues("trnIIDFCleanIIDFinderCTables", new Object[]{LUTableName});
		String linkedColumns = trnRS.get("LINKED_COLUMNS");
		if (linkedColumns != null && !"".equals(linkedColumns)) {
		    for (String linkedColumn : linkedColumns.split(",")) {
		        if (dcAfterValues.containsKey(linkedColumn)) {
		            String rowColumnValue = rs.get(linkedColumn) + "";
		            String afterValue = dcAfterValues.get(linkedColumn) + "";
		            if (!rowColumnValue.equals(afterValue)) {
		                for (String linkedColumn2 : linkedColumns.split(",")) {
		                    keys.put(linkedColumn2, rs.get(linkedColumn2) + "");
		                }
		                rubDF = true;
						addDeleteFinder(getLuType().luName, getInstanceID(), LUTableName, keys);
		                break;
		            }
		        }
		    }
		}
		if (rubDF)runDeleteFinder(getLuType().luName, getInstanceID());
	}


	@out(name = "rs", type = Boolean.class, desc = "")
	public static Boolean fnIIDFCheckRExists(Object i_dc) throws Exception {
		DataChange dc = (DataChange) i_dc;
		String tableName = dc.getTablespace() + IIDF_SCHEMA_TABLE_SEP_VAL + dc.getTable();
		if (getTranslationsData("trnIIDFForceReplicateOnDelete").keySet().contains(tableName)) return false;
		String prefix = "";
		StringBuilder recKeys = new StringBuilder();
		List<Object> keyVals = new ArrayList<>();
		Map<String, Object> dcKeys = dc.getKeys();
		for (Map.Entry<String, Object> dcKeysEnt : dcKeys.entrySet()) {
		    keyVals.add(dcKeysEnt.getValue());
		    recKeys.append(prefix + dcKeysEnt.getKey() + " = ? ");
		    prefix = " and ";
		}
		
		if (ludb().fetch("select 1 from " + getLuType().luName + "." + tableName + " where " + recKeys.toString(), keyVals.toArray()).firstValue() == null) {
		    return false;
		} else {
		    return true;
		}
	}
	
	public static void fnIIDFConvertDCToUpsert(Object i_dc) throws Exception {
		DataChange dc = (DataChange) i_dc;
		String tableName = dc.getTablespace() + IIDF_SCHEMA_TABLE_SEP_VAL + dc.getTable();
		Map<String, Object> dcKeys = dc.getKeys();
		StringBuilder stmtWhere = new StringBuilder();
		String prefix = "";
		Set<Object> params = new LinkedHashSet<>();
		for (Map.Entry<String, Object> dcKeysEnt : dcKeys.entrySet()) {
		    stmtWhere.append(prefix + dcKeysEnt.getKey() + "  =  ? ");
		    prefix = " and ";
		    params.add(dcKeysEnt.getValue());
		}
		Object rs = ludb().fetch("Select 1 from " + getLuType().luName + "." + tableName + " where " + stmtWhere.toString(), params.toArray()).firstValue();
		if (rs == null) dc.setOperation(DataChange.Operation.upsert);
	}
	
	@out(name = "rs", type = Object.class, desc = "")
    public static Object fnIIDFGetTablesCoInfo(String table_name, String lu_name) throws Exception {
        Map<String, String> tblcolMap = new HashMap<>();
        LUType lut = LUTypeFactoryImpl.getInstance().getTypeByName(lu_name);
        if (lut != null) {
            LudbObject rtTable = lut.ludbObjects.get(table_name);
            if (rtTable != null) {
                for (Map.Entry<String, LudbColumn> mapEnt : rtTable.getLudbColumnMap().entrySet()) {
                    tblcolMap.put(mapEnt.getKey().toUpperCase(), mapEnt.getValue().columnType.toUpperCase());
                }
                return tblcolMap;
            }
        }
        reportUserMessage("Failed To Get Table Columns Map!, Table Name:" + table_name + ", LUName:" + lu_name);
        throw new Exception("Failed To Get Table Columns Map!, Table Name:" + table_name + ", LUName:" + lu_name);
    }
}
