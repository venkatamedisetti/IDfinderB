/////////////////////////////////////////////////////////////////////////
// Project Web Services
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.lu.k2_ws.KAFKA_Utils;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;

import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.shared.user.WebServiceUserCode;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.func.oracle.OracleToDate;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.cdbms.usercode.lu.k2_ws.*;
import com.k2view.fabric.parser.JSQLParserException;
import com.k2view.fabric.parser.expression.Expression;
import com.k2view.fabric.parser.expression.operators.relational.ExpressionList;
import com.k2view.fabric.parser.parser.CCJSqlParserManager;
import com.k2view.fabric.parser.schema.Column;
import com.k2view.fabric.parser.statement.Delete;
import com.k2view.fabric.parser.statement.Insert;
import com.k2view.fabric.parser.statement.Statement;
import com.k2view.fabric.parser.statement.update.UpdateTable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.usercode.common.IIDF.SharedLogic.fnIIDFGetTablePK;
import static com.k2view.cdbms.usercode.common.IIDF.SharedLogic.fnIIDFGetTablesCoInfo;
import static com.k2view.cdbms.usercode.common.SharedLogic.*;
import static com.k2view.cdbms.usercode.common.SharedGlobals.*;
import com.k2view.fabric.api.endpoint.Endpoint.*;

@SuppressWarnings({"unused", "DefaultAnnotationParam"})
public class Logic extends WebServiceUserCode {


	@webService(path = "", verb = {MethodType.GET, MethodType.POST, MethodType.PUT, MethodType.DELETE}, version = "1", isRaw = false, produce = {Produce.XML, Produce.JSON})
	public static String wsUpdateKafka_new(String sql_stmt, @param(description="Can Be ref/lookup Or Left Empty For LU Tables") String tblType, @param(description="For Use For Lookup Tables Topic Only") String partiKey, @param(description="If To Send op_tp as R") Boolean replicate, @param(description="Mandatory If Table PK Is Not Set In trnTable2PK") String lu_name, @param(description="If You Wish To Override The Topic Logi") String topic_name, @param(description="If Set To True WS Will Not Send Records To Kafka") Boolean debug, @param(description="If You Wish To Customise The Message Time") String op_ts, @param(description="If You Wish To Customise The pos Value") String pos, @param(description="If You Wish To Customise targetIid To Be sent To IIDFinder") String targetIid, @param(description="If You Wish To Have No Before") Boolean no_before) throws Exception {
		final String LU_TABLES = "IidFinder";
		final String REF = "REF";
		final String LOOKUP = "LKUP";
		final java.util.regex.Pattern patternInsert = java.util.regex.Pattern.compile("(?i)^insert(.*)");
		final java.util.regex.Pattern patternUpdate = java.util.regex.Pattern.compile("(?i)^update(.*)");
		final java.util.regex.Pattern patternDelete = java.util.regex.Pattern.compile("(?i)^delete(.*)");
		final java.text.DateFormat clsDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
		clsDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		CCJSqlParserManager parserManager = new CCJSqlParserManager();
		Statement sqlStmt = null;
		String topicName = null;
		java.util.regex.Matcher matcher = null;
		Insert insStmt = null;
		UpdateTable upStmt = null;
		Delete delStmt = null;
		String luTableName = "";
		String sourceTableName = "";
		StringBuilder out = new StringBuilder();
		try {
		    sqlStmt = parserManager.parse(new StringReader(sql_stmt));
		} catch (JSQLParserException e) {
			out.append("Failed To Parse SQL Statement, Please Check! - " + sql_stmt + "\n");
		    throw e;
		}
		
		JSONObject IIDJSon = new JSONObject();
		if (op_ts != null && !op_ts.equals("")) {
		    IIDJSon.put("op_ts", op_ts);
		} else {
		    IIDJSon.put("op_ts", clsDateFormat.format(new java.util.Date()));
		}
		
		IIDJSon.put("current_ts", clsDateFormat.format(new java.util.Date()).replace(" ", "T"));
		
		if (pos != null && !pos.equals("")) {
		    IIDJSon.put("pos", pos);
		} else {
		    IIDJSon.put("pos", "00000000020030806864");
		}
		
		if (targetIid != null && !targetIid.equals("")) {
		    IIDJSon.put("targetIid", targetIid);
		}
		
		matcher = patternInsert.matcher(sql_stmt);
		if (matcher.find()) {
		    insStmt = (Insert) sqlStmt;
		    luTableName = insStmt.getTable().getSchemaName() + IIDF_SCHEMA_TABLE_SEP_VAL + insStmt.getTable().getName();
		    if (insStmt.getTable().getSchemaName() == null) {
				out.append("Failed To Parse Schema And Table Name!, Schema Name And Table Name Should Be Seperated By Dot\n");
		        throw new Exception("Failed To Parse Schema And Table Name!, Schema Name And Table Name Should Be Seperated By Dot");
		    }
		    Map<String, String> tblColMap = (Map<String, String>) fnIIDFGetTablesCoInfo(luTableName, lu_name);
		    sourceTableName = insStmt.getTable().getSchemaName() + "." + insStmt.getTable().getName();
		
		    //Get table primary keys
		    JSONArray PK = new JSONArray();
		    String[] pkCuls = (String[]) fnIIDFGetTablePK(luTableName, lu_name);
		    for (String pkCul : pkCuls) {
		        PK.put(pkCul);
		    }
		    IIDJSon.put("primary_keys", PK);
		
		    if (replicate != null && replicate) {
		        IIDJSon.put("op_type", "R");
		    } else {
		        IIDJSon.put("op_type", "I");
		    }
		//if((val[0] + "").equalsIgnoreCase("null"))
		    JSONObject after = new JSONObject();
		    List<Column> exp = insStmt.getColumns();
		    Object[] val = ((ExpressionList) insStmt.getItemsList()).getExpressions().toArray();
		    int i = 0;
		    for (Column x : exp) {
		        if (tblColMap.get(x.getColumnName().toUpperCase()).equals("TEXT")) {
					if("NULL".equalsIgnoreCase(val[i] + "")){
						after.put(x.getColumnName().toUpperCase(), JSONObject.NULL);
					}else{
						String textVal  = (val[i] + "").replaceAll("^'|'$", "");
						after.put(x.getColumnName().toUpperCase(), textVal);
					}
		            
		        } else if (tblColMap.get(x.getColumnName().toUpperCase().toUpperCase()).equals("INTEGER") && (val[i] + "").length() <= 11) {
		            int intVal = 0;
		            try {
		                intVal = Integer.parseInt((val[i] + ""));
		            } catch (Exception e) {
						out.append("Failed To Parse Integer Value For Column " + x.getColumnName() + ", Value Found:" + val[i] + "\n");
		                throw e;
		            }
		            after.put(x.getColumnName().toUpperCase(), intVal);
		        }  else if (tblColMap.get(x.getColumnName().toUpperCase().toUpperCase()).equals("INTEGER") && (val[i] + "").length() > 11) {
					long intVal = 0;
					try {
						intVal = Long.parseLong((val[i] + ""));
					} catch (Exception e) {
						out.append("Failed To Parse Long Value For Column " + x.getColumnName() + ", Value Found:" + val[i] + "\n");
						throw e;
					}
					after.put(x.getColumnName().toUpperCase(), intVal);
				}else if (tblColMap.get(x.getColumnName().toUpperCase().toUpperCase()).equals("REAL")) {
		            double doubleValue = 0;
		            try {
		                doubleValue = Double.parseDouble((val[i] + ""));
		            } catch (Exception e) {
						out.append("Failed To Parse Double Value For Column " + x.getColumnName() + ", Value Found:" + val[i] + "\n");
		                throw e;
		            }
		            after.put(x.getColumnName().toUpperCase(), doubleValue);
		        }
		        i++;
		    }
		
		    IIDJSon.put("after", after);
			List<String> pkList = Arrays.asList(pkCuls);
			for(String keyColumn : pkList){
				if(!after.has(keyColumn)){
					out.append("All Primary Key Columns Must Be Part In Where Part Of Query!\n");
					throw new Exception("All Primary Key Columns Must Be Part In Where Part Of Query!");
				}
			}
		
		} else {
		    matcher = patternUpdate.matcher(sql_stmt);
		    if (matcher.find()) {
		        upStmt = (UpdateTable) sqlStmt;
		        luTableName = upStmt.getTable().getSchemaName() + IIDF_SCHEMA_TABLE_SEP_VAL + upStmt.getTable().getName();
		        if (upStmt.getTable().getSchemaName() == null) {
					out.append("Failed To Parse Schema And Table Name!, Schema Name And Table Name Should Be Seperated By Dot\n");
		            throw new Exception("Failed To Parse Schema And Table Name!, Schema Name And Table Name Should Be Seperated By Dot");
		        }
		        Map<String, String> tblColMap = (Map<String, String>) fnIIDFGetTablesCoInfo(luTableName, lu_name);
		        sourceTableName = upStmt.getTable().getSchemaName() + "." + upStmt.getTable().getName();
				IIDJSon.put("op_type", "U");
		        //Get table primary keys
		        JSONArray PK = new JSONArray();
		        String[] pkCuls = (String[]) fnIIDFGetTablePK(luTableName, lu_name);
		        for (String pkCul : pkCuls) {
		            PK.put(pkCul);
		        }
		        IIDJSon.put("primary_keys", PK);
		
		        String whereStr = upStmt.getWhere().toString();
		
		        String[] culsNdVals = upStmt.getWhere().toString().split("(?i)( and )");
		        JSONObject before = new JSONObject();
		        for (String culNdVal : culsNdVals) {
		            String columnName = culNdVal.split("=")[0].trim();
		            String columnValue = culNdVal.split("=")[1].trim();
		
		            if (tblColMap.get(columnName.toUpperCase()).equals("TEXT")) {
		                String textVal = columnValue.replaceAll("^'|'$", "");
		                before.put(columnName.toUpperCase(), textVal);
		            } else if (tblColMap.get(columnName.toUpperCase()).equals("INTEGER") && columnValue.length() <= 11) {
		                int intVal = 0;
		                try {
		                    intVal = Integer.parseInt(columnValue);
		                } catch (Exception e) {
							out.append("Failed To Parse Integer Value For Column " + columnName + ", Value Found:" + columnValue + "\n");
		                    throw e;
		                }
		                before.put(columnName.toUpperCase(), intVal);
		            } else if (tblColMap.get(columnName.toUpperCase()).equals("INTEGER") && columnValue.length() > 11) {
						long intVal = 0;
						try {
							intVal = Long.parseLong(columnValue);
						} catch (Exception e) {
							out.append("Failed To Parse Long Value For Column " + columnName + ", Value Found:" + columnValue + "\n");
							throw e;
						}
						before.put(columnName.toUpperCase(), intVal);
					}else if (tblColMap.get(columnName.toUpperCase()).equals("REAL")) {
		                double doubeValue = 0;
		                try {
		                    doubeValue = Double.parseDouble(columnValue);
		                } catch (Exception e) {
							out.append("Failed To Parse Double Value For Column " + columnName + ", Value Found:" + columnValue + "\n");
		                    throw e;
		                }
		                before.put(columnName.toUpperCase(), doubeValue);
		            }
		
		        }
		        if(no_before == null || !no_before)IIDJSon.put("before", before);
		
		        JSONObject after = new JSONObject();
		        List<Expression> exp = upStmt.getExpressions();
		        int i = 0;
		        for (Expression x : exp) {
		            String columnName = ((Column) upStmt.getColumns().get(i)).getColumnName();
		            String columnValue = (x + "");
		
		            if (tblColMap.get(columnName.toUpperCase()).equals("TEXT")) {
		                String textVal = columnValue.replaceAll("^'|'$", "");
		                after.put(columnName.toUpperCase(), textVal);
		            } else if (tblColMap.get(columnName.toUpperCase()).equals("INTEGER") && columnValue.length() <= 11) {
		                int intVal = 0;
		                try {
		                    intVal = Integer.parseInt(columnValue);
		                } catch (Exception e) {
		                    reportUserMessage("Failed To Parse Integer Value For Column " + columnName + ", Value Found:" + columnValue);
		                    throw e;
		                }
		                after.put(columnName.toUpperCase(), intVal);
		            }  else if (tblColMap.get(columnName.toUpperCase()).equals("INTEGER") && columnValue.length() > 11) {
						long intVal = 0;
						try {
							intVal = Long.parseLong(columnValue);
						} catch (Exception e) {
							reportUserMessage("Failed To Parse Long Value For Column " + columnName + ", Value Found:" + columnValue);
							throw e;
						}
						after.put(columnName.toUpperCase(), intVal);
					}else if (tblColMap.get(columnName.toUpperCase()).equals("REAL")) {
		                double doubeValue = 0;
		                try {
		                    doubeValue = Double.parseDouble(columnValue);
		                } catch (Exception e) {
		                    reportUserMessage("Failed To Parse Double Value For Column " + columnName + ", Value Found:" + columnValue);
		                    throw e;
		                }
		                after.put(columnName.toUpperCase(), doubeValue);
		            }
		
		        }
		        IIDJSon.put("after", after);
		
		        List<String> pkList = Arrays.asList(pkCuls);
		        Iterator<String> beforeKeys = before.keys();
		        while (beforeKeys.hasNext()) {
		            String key = beforeKeys.next();
		            if(pkList.contains(key.toUpperCase()) &&  !after.has(key)){
		                after.put(key, before.get(key));
		            }
		        }
		
		        for(String keyColumn : pkList){
		        	if(!before.has(keyColumn)){
						reportUserMessage("All Primary Key Columns Must Be Part In Where Part Of Query!");
						throw new Exception("All Primary Key Columns Must Be Part In Where Part Of Query!");
					}
				}
		
		    } else {
		        matcher = patternDelete.matcher(sql_stmt);
		        if (matcher.find()) {
		            IIDJSon.put("op_type", "D");
		            delStmt = (Delete) sqlStmt;
		            luTableName = delStmt.getTable().getSchemaName() + IIDF_SCHEMA_TABLE_SEP_VAL + delStmt.getTable().getName();
		            if (delStmt.getTable().getSchemaName() == null) {
		                reportUserMessage("Failed To Parse Schema And Table Name!, Schema Name And Table Name Should Be Seperated By Dot");
		                throw new Exception("Failed To Parse Schema And Table Name!, Schema Name And Table Name Should Be Seperated By Dot");
		            }
		            sourceTableName = delStmt.getTable().getSchemaName() + "." + delStmt.getTable().getName();
		
		            //Get table primary keys
		            JSONArray PK = new JSONArray();
		            String[] pkCuls = (String[]) fnIIDFGetTablePK(luTableName, lu_name);
		            for (String pkCul : pkCuls) {
		                PK.put(pkCul);
		            }
		            IIDJSon.put("primary_keys", PK);
		
		            String[] culsNdVals = delStmt.getWhere().toString().split("(?i)( and )");
		            JSONObject before = new JSONObject();
		            Map<String, String> tblColMap = (Map<String, String>) fnIIDFGetTablesCoInfo(luTableName, lu_name);
		            for (String culNdVal : culsNdVals) {
		
		                String columnName = culNdVal.split("=")[0].trim();
		                String columnValue = culNdVal.split("=")[1].trim();
		
		                if (tblColMap.get(columnName.toUpperCase()).equals("TEXT")) {
		                    String textVal = columnValue.replaceAll("^'|'$", "");
		                    before.put(columnName.toUpperCase(), textVal);
		                } else if (tblColMap.get(columnName.toUpperCase()).equals("INTEGER") && columnValue.length() <= 11) {
		                    int intVal = 0;
		                    try {
		                        intVal = Integer.parseInt(columnValue);
		                    } catch (Exception e) {
		                        reportUserMessage("Failed To Parse Integer Value For Column " + columnName + ", Value Found:" + columnValue);
		                        throw e;
		                    }
		                    before.put(columnName.toUpperCase(), intVal);
		                }  else if (tblColMap.get(columnName.toUpperCase()).equals("INTEGER") && columnValue.length() > 11) {
							long intVal = 0;
							try {
								intVal = Long.parseLong(columnValue);
							} catch (Exception e) {
								reportUserMessage("Failed To Parse Long Value For Column " + columnName + ", Value Found:" + columnValue);
								throw e;
							}
							before.put(columnName.toUpperCase(), intVal);
						}else if (tblColMap.get(columnName.toUpperCase()).equals("REAL")) {
		                    double doubeValue = 0;
		                    try {
		                        doubeValue = Double.parseDouble(columnValue);
		                    } catch (Exception e) {
		                        reportUserMessage("Failed To Parse Double Value For Column " + columnName + ", Value Found:" + columnValue);
		                        throw e;
		                    }
		                    before.put(columnName.toUpperCase(), doubeValue);
		                }
		
		            }
		            IIDJSon.put("before", before);
					List<String> pkList = Arrays.asList(pkCuls);
					for(String keyColumn : pkList){
						if(!before.has(keyColumn)){
							reportUserMessage("All Primary Key Columns Must Be Part In Where Part Of Query!");
							throw new Exception("All Primary Key Columns Must Be Part In Where Part Of Query!");
						}
					}
		        }
		    }
		}
		
		IIDJSon.put("table", sourceTableName.toUpperCase());
		
		if (tblType != null && tblType.equalsIgnoreCase("ref")) {
		    topicName = REF + ".<>";
		} else if (tblType != null && tblType.equalsIgnoreCase("lookup")) {
		    topicName = LOOKUP + ".<>_LKUP_" + partiKey;
		} else {
		    topicName = LU_TABLES + ".<>";
		}
		topicName = topicName.replace("<>", sourceTableName.toUpperCase());
		
		Properties props = new Properties();
		com.k2view.cdbms.usercode.common.IIDF.SharedLogic.UserKafkaProperties(props);
		if (topic_name != null && !"".equals(topic_name)) topicName = topic_name;
		out.append("Topic Name:" + topicName + "\n");
		out.append("Msg:" + IIDJSon.toString() + "\n");
		if (!debug) {
		    try (Producer<String, JSONObject> producer = new KafkaProducer<>(props)) {
		        producer.send(new ProducerRecord(topicName, null, null, IIDJSon.toString())).get();
		        return "Done Publishing Messeage To Kafka!";
		    } catch (Exception e) {
				out.append("Failed Publishing Messeage To Kafka, Please check Log File\n");
		        throw e;
		    }
		}
		out.append("Debug Complete With No Errors");
		return out.toString();
	}

	
	

	
}
