/////////////////////////////////////////////////////////////////////////
// Project Shared Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.common.Get_Job_Parser;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.k2view.cdbms.finder.IidFinderUtils;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.shared.user.UserCode;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.func.oracle.OracleToDate;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;
import com.k2view.fabric.common.stats.CustomStats;
import com.k2view.fabric.session.FabricSession;
import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.management.ObjectName;

import static com.k2view.cdbms.lut.FunctionDef.functionContext;
import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.shared.user.UserCode.*;
import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.usercode.common.SharedGlobals.*;

@SuppressWarnings({"unused", "DefaultAnnotationParam"})
public class SharedLogic {

    static final LoadingCache<String, IidTimeStamp> iidsCache = CacheBuilder.newBuilder()
            .maximumSize(Integer.parseInt(getLuType().ludbGlobals.get("DELTA_JOB_MAX_CACHE_SIZE")))
            .build(new CacheLoader<String, IidTimeStamp>() {
                @Override
                public IidTimeStamp load(String iid) {
                    return null;
                }
            });

    static final String luName = getLuType().luName;
    static final String deltaTableName = IidFinderUtils.getDeltaTableName(IidFinderUtils.getAlias(luName));
    static final String query = String.format("select iid from %s.%s where iid=? limit 1", IidFinderUtils.getKeyspace(), deltaTableName);
    private static final int TEN_MINUTES = 10 * 60 * 1000;
    private static DecimalFormat dcFor = new DecimalFormat("##.##");

    @type(RootFunction)
    @out(name = "out", type = String.class, desc = "")
    public static void k2_MsgParser2(String in) throws Exception {
        Map<String, String> parserArgs = parserParams();
        String parserUID = "deltaIid_" + parserArgs.get("partition");
        Stats stats = new Stats(parserUID);
        stats.insertStats("Started");

        processMessages(msg -> {
            if (msg != null) {
                handleMsg(msg.getKey(), msg.getValue(), stats);
            }
            assertTerminate();
            return true;
        });

        stats.insertStats("Stopped");
        yield(null);
    }

    private static void handleMsg(String iid, long timestamp, Stats stats) throws InterruptedException, SQLException {
        IidTimeStamp prevIidTs = iidsCache.getIfPresent(iid);
        IidTimeStamp currIidTs = new IidTimeStamp(iid, timestamp);
        try {
            long h = System.currentTimeMillis() - timestamp;

            int delay = Integer.parseInt(getLuType().ludbGlobals.get("DELTA_JOB_DELAY_TIME"));
            if (h < delay) {
                Thread.sleep(delay - h);
            }
			
            if (prevIidTs != null) {
                if (prevIidTs.ts + Integer.parseInt(getLuType().ludbGlobals.get("DELTA_JOB_PREV_MESSAGE_DIFF_EPSILON_MS")) < currIidTs.ts) {
                    handleNewIID(currIidTs, stats);
                }
            } else {
                handleNewIID(currIidTs, stats);
            }
        } catch (SQLException ex) {
            log.error(String.format("Failed to handle %s.%s", luName, iid), ex);
            logFailedIID(iid, ex);
	       }
    }

    private static void handleNewIID(IidTimeStamp currIidTs, Stats stats) throws SQLException, InterruptedException {
        long currentTimeMillis = System.currentTimeMillis();
        try (Db.Rows rs = db(DB_CASS_NAME).fetch(query, currIidTs.iid)) {
            if (rs.firstValue() != null) {
                Thread.sleep(Long.parseLong(getLuType().ludbGlobals.get("IID_GET_DELAY")));
                final Db fabric = db("FabricDB");
                stats.setTotal_iid_processed_in_ten_min();
                try {
                    fabric.execute("GET ?.?", luName, currIidTs.iid);
                    stats.setTotal_iid_passed();
                } catch (Exception e) {
                    stats.setTotal_iid_failed();
                    throw e;
                }

            }
        }
        // Add to the cache
        iidsCache.put(currIidTs.iid, new IidTimeStamp(currIidTs.iid, currentTimeMillis));
        if (System.currentTimeMillis() > stats.getTenAgo()) {
            stats.insertStats("Running");
        }
    }

    private static class IidTimeStamp {
        String iid;
        Long ts;

        IidTimeStamp(String iid, Long ts) {
            this.iid = iid;
            this.ts = ts;
        }
    }

    private static class Stats {
        long total_iid_failed;
        long total_iid_passed;
        long total_iid_processed;
        long total_iid_processed_in_ten_min;
        long tenAgo;
        String host;
        String parserUID;

        private Stats(String parserUID) throws UnknownHostException {
            this.host = InetAddress.getLocalHost().getHostAddress();
            this.tenAgo = System.currentTimeMillis() + TEN_MINUTES;
            this.parserUID = parserUID;
        }

        private long getTotal_iid_failed() {
            return total_iid_failed;
        }

        private long getTotal_iid_passed() {
            return total_iid_passed;
        }

        private long getTotal_iid_processed() {
            return total_iid_processed;
        }

        private long getTtotal_iid_processed_in_ten_min() {
            return total_iid_processed_in_ten_min;
        }

        private long getTenAgo() {
            return tenAgo;
        }

        private void setTotal_iid_failed() {
            total_iid_failed++;
        }

        private void setTotal_iid_passed() {
            total_iid_passed++;
        }

        private void setTotal_iid_processed(long total_iid_processed_in_ten_min) {
            total_iid_processed += total_iid_processed_in_ten_min;
        }

        private void setTotal_iid_processed_in_ten_min() {
            total_iid_processed_in_ten_min++;
        }

        private void insertStats(String status) throws SQLException {
            if ("Started".equals(status)) {
                db(DB_CASS_NAME).execute("Insert into " + getLuType().getKeyspaceName() + ".iid_get_rate (node,uid,status,update_time) values (?,?,?,?)", host, this.parserUID, status, new Timestamp(System.currentTimeMillis()));
                return;
            }
            double avg = (total_iid_processed_in_ten_min / (TEN_MINUTES / 1000.0));
            avg = Double.parseDouble(dcFor.format(avg));
			setTotal_iid_processed(total_iid_processed_in_ten_min);
            tenAgo = System.currentTimeMillis() + TEN_MINUTES;
            db(DB_CASS_NAME).execute("Insert into " + getLuType().getKeyspaceName() + ".iid_get_rate (node,uid,iid_avg_run_time_in_sec,status,total_iid_failed,total_iid_passed,total_iid_processed,total_iid_processed_in_ten_min,update_time) values (?,?,?,?,?,?,?,?,?)", host, this.parserUID, avg, status, total_iid_failed, total_iid_passed, total_iid_processed, total_iid_processed_in_ten_min, new Timestamp(System.currentTimeMillis()));
			total_iid_processed_in_ten_min = 0;
        }
    }

    private static void logFailedIID(String iid, SQLException ex) throws SQLException {
        db(DB_CASS_NAME).execute("Insert into " + getLuType().getKeyspaceName() + ".get_jobs_failed_iids (iid, failed_time, failure_reason, full_error_msg) values (?,?,?,?)", iid, new Timestamp(System.currentTimeMillis()), ex.getMessage(), ExceptionUtils.getStackTrace(ex));
    }


}
