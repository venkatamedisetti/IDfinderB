package com.k2view.cdbms.usercode.common;


import java.sql.*;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.k2view.cdbms.shared.*;
import com.sun.javafx.binding.StringFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.k2view.cdbms.usercode.common.SharedGlobals.*;

public class UserCache {

    private Cache<String, String> IIDCache;
    private long SYNC_DURATION_IN_SEC_CLS = 10;
    private int IID_CACHE_MAX_REQUESTS_CLS = 10000;
    private static UserCache userCatchInstance = new UserCache();
    protected static Logger log = LoggerFactory.getLogger(UserCache.class.getName());
    private final String deltaSQL = "SELECT iid from k2staging.k_%s_delta WHERE iid = ? limit 1";
    private final String entitySQL = "SELECT id from k2view_%s.entity WHERE id = ?";

    private UserCache() {
        //Setting up Sync Duration From Studio Global
        if (SYNC_DURATION_IN_SEC != null && !SYNC_DURATION_IN_SEC.equals("")) {
            this.SYNC_DURATION_IN_SEC_CLS = Long.parseLong(SYNC_DURATION_IN_SEC);
        }

        if (IID_CACHE_MAX_REQUESTS != null && !IID_CACHE_MAX_REQUESTS.equals("")) {
            this.IID_CACHE_MAX_REQUESTS_CLS = Integer.parseInt(IID_CACHE_MAX_REQUESTS);
        }

        this.IIDCache = CacheBuilder.newBuilder()
                .maximumSize(this.IID_CACHE_MAX_REQUESTS_CLS)
                .expireAfterWrite(this.SYNC_DURATION_IN_SEC_CLS, TimeUnit.SECONDS)
                .build();
    }

    public static UserCache getInstance() {
        return userCatchInstance;
    }

    public boolean checkIfNeedToSync(UserCodeDelegate ucd, String luName, String iid, boolean checkDeltaNdEntity) throws SQLException {
        String IIDKey = luName + "." + iid;
        //Check If SYNC_DURATION_IN_SEC Global Was Change
        if ((IID_CACHE_MAX_REQUESTS != null && !IID_CACHE_MAX_REQUESTS.equals("") && this.IID_CACHE_MAX_REQUESTS_CLS != Integer.parseInt(IID_CACHE_MAX_REQUESTS)) || (SYNC_DURATION_IN_SEC != null && !SYNC_DURATION_IN_SEC.equals("") && this.SYNC_DURATION_IN_SEC_CLS != Long.parseLong(SYNC_DURATION_IN_SEC))) {
            this.SYNC_DURATION_IN_SEC_CLS = Long.parseLong(SYNC_DURATION_IN_SEC);
            this.IIDCache = CacheBuilder.newBuilder()
                    .maximumSize(10000)
                    .expireAfterWrite(this.SYNC_DURATION_IN_SEC_CLS, TimeUnit.SECONDS)
                    .build();
        }

        //Check If To Sync Instance
        synchronized (this) {
            String IIDExitsInCache = this.IIDCache.getIfPresent(IIDKey);
            if (IIDExitsInCache == null) {
                IIDCache.put(IIDKey, "Y");
                log.debug("USER_CACHE: PUTTING IID IN CACHE");
	            log.debug("USER_CACHE: setting sync mode to on");
	            ucd.fabric().execute("set sync on");
                if(checkDeltaNdEntity)checkSyncRequired(ucd, luName, iid);
                return true;
            } else {
                log.debug("USER_CACHE: IID FOUND IN CACHE");
				log.debug("USER_CACHE: setting sync mode to off");
                ucd.fabric().execute("set sync off");
                return false;
            }
        }
    }

    private void checkSyncRequired(UserCodeDelegate ucd, String luName, String iid) throws SQLException {
        Object entityFound = null;
        if ("true".equals(CHECK_DELTA)) {
            log.debug("USER_CACHE: CHECKING Delta TABLE");
            entityFound = ucd.db(DB_CASS_NAME).fetch(String.format(deltaSQL, luName.toLowerCase().substring(0, 3)), iid).firstValue();
            if (entityFound != null) {
                log.debug("USER_CACHE: setting sync mode to on");
                ucd.fabric().execute("set sync on");
            }
        }

        if (entityFound == null) {
            log.debug("USER_CACHE: CHECKING Entity TABLE");
            entityFound = ucd.db(DB_CASS_NAME).fetch(String.format(entitySQL, luName.toLowerCase()), iid).firstValue();
            if (entityFound == null) {
                log.debug("USER_CACHE: setting sync mode to on");
                ucd.fabric().execute("set sync on");
            } else {
                log.debug("USER_CACHE: setting sync mode to off");
                ucd.fabric().execute("set sync off");
            }
        }
    }

}
