package com.k2view.cdbms.usercode.lu.WS_TEST;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WSExecute implements Runnable{
    protected static Logger log = LoggerFactory.getLogger(WSExecute.class.getName());
	private String URL;

    public WSExecute(String i_URL){
        this.URL = i_URL;
    }

    @Override
    public void run() {
        try {
            fnInvokeWS("GET", this.URL, null, null, 0, 0);
        } catch (Exception e) {
            log.error("WSExecute - \nURL - " + this.URL,e);
        }
    }

    public static String fnInvokeWS(String reqMethod, String URL, String reqBody, Object reqMap, Integer reqConnTOInMS, Integer reqReadTOInMS) throws Exception {
        java.net.HttpURLConnection conn = null;
        StringBuilder sb = new StringBuilder();
        java.io.BufferedReader br = null;
        java.io.OutputStream os = null;
        InputStreamReader reqIS = null;

        try {
            java.net.URL url = new java.net.URL(URL);
            conn = (java.net.HttpURLConnection) url.openConnection();

            if(reqConnTOInMS != 0){
                conn.setConnectTimeout(reqConnTOInMS);
            }

            if(reqReadTOInMS != 0){
                conn.setReadTimeout(reqReadTOInMS);
            }

            if(reqMethod != null && !"".equals(reqMethod)) {
                conn.setRequestMethod(reqMethod);
            }else{
                throw new RuntimeException("Failed To Invoke WS: Request Method Can't Be Empty!");
            }

            if(reqMap != null) {
                for(java.util.Map.Entry<String, String> mapEnt : ((java.util.Map<String, String>)reqMap).entrySet()) {
                    conn.setRequestProperty(mapEnt.getKey(), mapEnt.getValue());
                }
            }

            if(reqBody != null && !"".equalsIgnoreCase(reqBody)) {
                conn.setDoOutput(true);
                os = conn.getOutputStream();
                os.write(reqBody.getBytes());
                os.flush();
            }

            if(conn.getResponseCode() != 200) {
                reqIS = new java.io.InputStreamReader(conn.getErrorStream());
            }else{
                reqIS = new java.io.InputStreamReader(conn.getInputStream());
            }

            br = new java.io.BufferedReader(reqIS);
            String output;
            while ((output = br.readLine()) != null) {
                sb.append(output);
            }

            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed To Inoke WS: Error Code : " + conn.getResponseCode() + " \nError Message:" + sb.toString());
            }

            return sb.toString();
        } finally{
            if(os != null)os.close();
            if(br != null)br.close();
            if(reqIS != null)reqIS.close();
            if(conn != null)conn.disconnect();
        }
    }
}