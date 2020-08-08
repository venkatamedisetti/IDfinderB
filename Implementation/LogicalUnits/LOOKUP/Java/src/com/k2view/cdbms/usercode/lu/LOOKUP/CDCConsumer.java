package com.k2view.cdbms.usercode.lu.LOOKUP;

import java.util.*;
import java.sql.*;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import com.k2view.cdbms.shared.*;
import com.k2view.fabric.cdc.Serialization;
import com.k2view.fabric.cdc.msgs.CdcMessage;
import com.k2view.fabric.cdc.msgs.CdcMessageType;
import com.k2view.fabric.cdc.transaction.publisher.CdcDataPublishProperties;
import com.k2view.fabric.common.ini.Configurator;
import com.k2view.fabric.commonArea.common.config.CommonAreaConfig;
import com.k2view.fabric.commonArea.common.config.CommonAreaMemoryQueueConfig;
import com.k2view.fabric.commonArea.producer.kafka.KafkaAdminProperties;
import com.k2view.fabric.fabricdb.datachange.DataChangeType;
import com.k2view.fabric.fabricdb.datachange.TableDataChange;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import static com.k2view.cdbms.shared.user.UserCode.getLuType;
import static com.k2view.cdbms.shared.user.UserCode.isAborted;
import static com.k2view.cdbms.usercode.lu.LOOKUP.Globals.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CDCConsumer {

    protected static Logger log = LoggerFactory.getLogger(LookupConsumer.class.getName());
    final private java.text.DateFormat clsDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
    public KafkaConsumer<String, JSONObject> consumer;
    private Producer<String, JSONObject> producer;
    private String IDFINDER_PREFIX;
    private long del = 0;
    private long ins = 0;
    private long up = 0;
    private long msg = 0;
    private final int MINUTE = 60000;
    private long minAgo = 0;

    public CDCConsumer(String groupId, String topicName, String IDFINDER_PREFIX) throws ExecutionException, InterruptedException {
        clsDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        this.consumer = new KafkaConsumer<>(getConsumerProp(groupId));
        this.producer = new KafkaProducer<>(getProducerProp());
        this.IDFINDER_PREFIX = IDFINDER_PREFIX;
        poll(topicName);
    }

    protected void poll(String topicName) throws JSONException, ExecutionException, InterruptedException {
        this.consumer.subscribe(Pattern.compile(topicName), new NoOpConsumerRebalanceListener());
        this.minAgo = System.currentTimeMillis() + this.MINUTE;
        try {
            while (!isAborted()) {
                ConsumerRecords<String, JSONObject> records = consumer.poll(1000);
                for (ConsumerRecord<String, JSONObject> record : records) {
                    CdcMessage cdcMsg = Serialization.fromJson(record.value().toString());
                    if (cdcMsg.dataType().equals(CdcMessageType.DATA_CHANGE)) {
                        this.msg++;
                        JSONObject GGJsonMsg = new JSONObject();
                        convertCDCToGGJson(cdcMsg, GGJsonMsg);
                    }
                }
                this.consumer.commitAsync();
                if (System.currentTimeMillis() > this.minAgo) {
                    this.minAgo = System.currentTimeMillis() + this.MINUTE;
                    log.info("CDCConsumer stats: Total messages consumed:" + msg + ", Total inserts:" + ins + ", Total updates:" + up + ", Total deletes:" + del);
                }
            }
        } finally {
            consumer.unsubscribe();
            if (consumer != null) consumer.close();
        }
    }

    private void convertCDCToGGJson(CdcMessage cdcMsg, JSONObject GGJsonMsg) throws ExecutionException, InterruptedException {
        TableDataChange dataChange = cdcMsg.tblChange().dataChange();
        String[] tblDet = dataChange.getTable().split("_", 2);
        String tableName =  tblDet[0] + "." + tblDet[1];
        GGJsonMsg.put("table", tableName);
        Timestamp ts = new Timestamp(cdcMsg.info().ts());
        Date OPTS = new Date(ts.getTime());
        GGJsonMsg.put("op_ts", clsDateFormat.format(OPTS));
        java.util.Date currentTime = new java.util.Date();
        GGJsonMsg.put("current_ts", clsDateFormat.format(currentTime).replace(" ", "T"));
        GGJsonMsg.put("pos", "00000000000001122013");

        JSONArray PK = new JSONArray();
        List<String> tableKeys = cdcMsg.tblChange().pk();
        for (String pkColumn : tableKeys) {
            PK.put(pkColumn);
        }
        GGJsonMsg.put("primary_keys", PK);

        StringBuilder msgKey = new StringBuilder();
        if (dataChange.getType().equals(DataChangeType.INSERT)) {
            this.ins++;
            GGJsonMsg.put("op_type", "I");
            setGGJsonOnInsert(dataChange, GGJsonMsg, msgKey, tableKeys);
        } else if (dataChange.getType().equals(DataChangeType.UPDATE)) {
            this.up++;
            GGJsonMsg.put("op_type", "U");
            setGGJsonOnUpdate(dataChange, GGJsonMsg, msgKey, tableKeys);
        } else if (dataChange.getType().equals(DataChangeType.DELETE)) {
            this.del++;
            GGJsonMsg.put("op_type", "D");
            setGGJsonOnDelete(dataChange, GGJsonMsg, msgKey, tableKeys);
        }

        publishMessage(GGJsonMsg, tableName, msgKey);
    }

    private void setGGJsonOnInsert(TableDataChange dataChange, JSONObject GGJsonMsg, StringBuilder msgKey, List<String> tableKeys) {
        JSONObject after = new JSONObject();
        for (Map.Entry<String, Object> newVals : dataChange.newValues().entrySet()) {
            if (tableKeys.contains(newVals.getKey())) msgKey.append(newVals.getValue());
            after.put(newVals.getKey(), newVals.getValue());
        }
        GGJsonMsg.put("after", after);
    }

    private void setGGJsonOnUpdate(TableDataChange dataChange, JSONObject GGJsonMsg, StringBuilder msgKey, List<String> tableKeys) {
        JSONObject before = new JSONObject();
        for (Map.Entry<String, Object> newVals : dataChange.oldValues().entrySet()) {
            before.put(newVals.getKey(), newVals.getValue());
        }
        GGJsonMsg.put("before", before);

        JSONObject after = new JSONObject();
        for (Map.Entry<String, Object> newVals : dataChange.newValues().entrySet()) {
            after.put(newVals.getKey(), newVals.getValue());
        }
        GGJsonMsg.put("after", after);
    }

    private void setGGJsonOnDelete(TableDataChange dataChange, JSONObject GGJsonMsg, StringBuilder msgKey, List<String> tableKeys) {
        JSONObject before = new JSONObject();
        for (Map.Entry<String, Object> newVals : dataChange.oldValues().entrySet()) {
            before.put(newVals.getKey(), newVals.getValue());
        }
        GGJsonMsg.put("before", before);
    }

    private void publishMessage(JSONObject GGJsonMsg, String tableName, StringBuilder msgKey) throws ExecutionException, InterruptedException {
        try {
            this.producer.send(new ProducerRecord(this.IDFINDER_PREFIX + tableName, null, msgKey.toString(), GGJsonMsg.toString())).get();
        } catch (Exception e) {
            log.error("CDCConsumer: Failed To Send Records To Kafka");
            if (producer != null) producer.close();
            throw e;
        }
    }

    private Properties getConsumerProp(String groupId) {
        Properties props = new Properties();
        props.put("group.id", groupId);
        props.put("bootstrap.servers", Configurator.load(CdcDataPublishProperties.class).BOOTSTRAP_SERVERS);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", 50);
        props.put("session.timeout.ms", 120000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "com.k2view.cdbms.kafka.JSONObjectDeserializer");

        return props;
    }

    private Properties getProducerProp() {
        Properties props = new Properties();
        props.put("bootstrap.servers", IifProperties.getInstance().getKafkaBootsrapServers());
        props.put("acks", "all");
        props.put("retries", "5");
        props.put("batch.size", "" + IifProperties.getInstance().getKafkaBatchSize());
        props.put("linger.ms", 1);
        props.put("max.block.ms", "" + IifProperties.getInstance().getKafkaMaxBlockMs());
        props.put("buffer.memory", "" + IifProperties.getInstance().getKafkaBufferMemory());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

}