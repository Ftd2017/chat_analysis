package com.closer.prophet.handler;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.PutRecordsResult;
import com.aliyun.datahub.model.RecordEntry;
import com.closer.prophet.DateFormatUtils;
import com.closer.prophet.OdpsValueSetter;
import com.closer.prophet.classifier.DefaultClassifier;
import com.closer.prophet.datahub.DatahubClientBuilder;
import com.closer.prophet.filter.SystemLogFilter;
import com.closer.prophet.mapper.RecordMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DefaultHandler implements AbstractHandler<String>, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(DefaultHandler.class);

    // filter definition.
    private static final BrowsingHistoryHandler browsingHistoryHandler = new BrowsingHistoryHandler();
    private static final ChatHistoryHandler chatHistoryHandler = new ChatHistoryHandler();
    private static final SystemLogFilter systemLogFilter = new SystemLogFilter();

    private static final Function<String, Boolean> filterBrowsingHistory =
            e -> DefaultClassifier.BROWSING_HISTORY == DefaultClassifier.classify(e);
    private static final Function<String, Boolean> filterChatHistory =
            e -> DefaultClassifier.CHAT_HISTORY == DefaultClassifier.classify(e);

    private static final RecordMapper mapper = new RecordMapper();

    private static final String PROJECT_NAME = "closer_prod";
    private static final String TOPIC_NAME = "ods_fact_logs_his";

    /**
     * Remove system log and send others to Data Hub.
     *
     * @param rdd All logs.
     */
    public void process(JavaRDD<String> rdd) {
        //process each rdd
        JavaRDD<String> validLogs = rdd.filter(systemLogFilter::filter);
        validLogs.persist(StorageLevel.MEMORY_AND_DISK_SER());

        // sending log to datahub
        JavaRDD<Map<String, Object>> data = validLogs.map(mapper::handler);
        if (data.count() > 0) {
            sendToDataHub(data);
        }

        // handling browsing history and chat history.
        JavaRDD<String> browsingHistory = validLogs.filter(filterBrowsingHistory);
        browsingHistoryHandler.process(browsingHistory);

        JavaRDD<String> chatHistory = validLogs.filter(filterChatHistory);
        chatHistoryHandler.process(chatHistory);
    }

    /**
     * Send log to datahub, then log will be synchronized to ODPS table.
     *
     * @param rdd RDD holding log.
     */
    public void sendToDataHub(JavaRDD<Map<String, Object>> rdd) {
        rdd.foreachPartition(partition -> {
            //get configuration
            DatahubClient client = DatahubClientBuilder.builder.build();
            RecordSchema schema = client.getTopic(PROJECT_NAME, TOPIC_NAME).getRecordSchema();

            //construct aliyun datahub RecordEntries
            List<RecordEntry> recordEntries = toRecordEntries(partition, schema);

            //send data to aliyun datahub
            if( ! recordEntries.isEmpty()) {
                PutRecordsResult result = client.putRecords(PROJECT_NAME, TOPIC_NAME, recordEntries);
                result.getFailedRecords().forEach(e -> logger.warn(String.format("Failed to send record to Datahub: %s", e.toJsonNode().toString())));
            }
        });
    }

    private List<RecordEntry> toRecordEntries(Iterator<Map<String, Object>> partition, RecordSchema schema) {
        final List<RecordEntry> entries = new ArrayList<>();

        while (partition.hasNext()) {
            entries.add(toRecordEntry(partition.next(), schema));
        }

        return entries;
    }

    private RecordEntry toRecordEntry(Map<String, Object> data, RecordSchema schema) {
        RecordEntry entry = new RecordEntry(schema);
        entry.setPartitionKey(RandomStringUtils.randomNumeric(10));

        // default partition key is system time
        long now = System.currentTimeMillis();


        //convert log to aliyun RecordEntry
        List<Field> fields = schema.getFields();
        for(Field field : fields) {
            String name = field.getName();
            Object value = data.get(name);
            FieldType type = field.getType();

            if(null != value) {
                setValueToEntry(name, value, type, entry);

                if(name.equals("ums_log_time")) {
                    //if 'ums_log_time' is not null, set 'ums_log_time' as partition key
                    now = DateTime.parse((String) value).getMillis();
                }
            }
        }

        entry.setString("p_dt", DateFormatUtils.formatDay(now));
        entry.setString("p_ht", DateFormatUtils.formatHour(now));

        return entry;
    }

    /**
     * Invoke different set method according to object type.
     *
     * @param key   Field name.
     * @param value Value of field.
     * @param type aliyun odps field type
     * @param entry Destination RecordEntry instance.
     */
    private static void setValueToEntry(String key, Object value, FieldType type, RecordEntry entry) {
        switch(type) {
            case BIGINT:
                OdpsValueSetter.setBigint(key, value, entry);
                break;
            case DOUBLE:
                OdpsValueSetter.setDouble(key, value, entry);
                break;
            case STRING:
                OdpsValueSetter.setString(key, value, entry);
                break;
            case BOOLEAN:
                OdpsValueSetter.setBoolean(key, value, entry);
                break;
            case TIMESTAMP:
                OdpsValueSetter.setTimestamp(key, value, entry);
                break;
            case DECIMAL:
                OdpsValueSetter.setDecimal(key, value, entry);
                break;
            default:
                //do nothing
                break;
        }
    }
}
