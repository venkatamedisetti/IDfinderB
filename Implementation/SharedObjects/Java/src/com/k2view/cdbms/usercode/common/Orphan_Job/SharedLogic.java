/////////////////////////////////////////////////////////////////////////
// Project Shared Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.common.Orphan_Job;

import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import java.util.Date;

import com.k2view.cdbms.finder.IidFinderUtils;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.shared.Globals;
import com.k2view.cdbms.shared.user.UserCode;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.func.oracle.OracleToDate;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.fabric.common.ini.Configurator;
import com.k2view.fabric.commonArea.producer.kafka.KafkaAdminProperties;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.json.JSONObject;

import static com.k2view.cdbms.shared.user.UserCode.*;
import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.usercode.common.Get_Job.SharedLogic.fnGetTopParCnt;
import static com.k2view.cdbms.usercode.common.SharedGlobals.DB_CASS_NAME;
import static com.k2view.cdbms.usercode.common.SharedLogic.*;
import static com.k2view.cdbms.usercode.common.SharedGlobals.*;
import com.k2view.fabric.fabricdb.datachange.TableDataChange;


@SuppressWarnings({"unused", "DefaultAnnotationParam"})
public class SharedLogic {


    @type(UserJob)
    public static void orphanJobsExecutor() throws Exception {
        final String topicName = IidFinderUtils.getKafkaOrphanTopic(getLuType().luName);
        final String startJob = "startjob PARSER NAME='%s.orphanIid' UID='orphanIid%s' AFFINITY='FINDER_ORPHAN' ARGS='{\"topic\":\"%s\"," + "\"partition\":\"%s\"}'";
        int partitions = fnGetTopParCnt(topicName);
        for (int i = 0; i < partitions; i++) {
            try {
                db(DB_FABRIC_NAME).execute(String.format(startJob, getLuType().luName, i, topicName, i));
            } catch (SQLException e) {
                if (!e.getMessage().contains("Job is running")) throw e;
            }
        }
    }


	@type(UserJob)
	public static void fnOrphanJobManager() throws Exception {
		final String topicName = IidFinderUtils.getKafkaOrphanTopic(getLuType().luName);
		final String startJob = "startjob USER_JOB name='%s.orphanJobsExecutor'";
		final String getRunningJobs = "SELECT count(*) from k2system.k2_jobs WHERE type = 'PARSER' and name = '%s.orphanIid' and status = 'IN_PROCESS' ALLOW FILTERING ";
		final Object parserCount = db(DB_CASS_NAME).fetch(String.format(getRunningJobs, getLuType().luName)).firstValue();
		
		int partitions = fnGetTopParCnt(topicName);
		if (parserCount == null || Integer.parseInt((parserCount + "")) < partitions) {
		    log.info(String.format("fnGetJobManager: Starting Get Job Parser For %s Total Number Of Partitions:%s", topicName, partitions));
		    db(DB_FABRIC_NAME).execute(String.format(startJob, getLuType().luName));
		}
	}


}

