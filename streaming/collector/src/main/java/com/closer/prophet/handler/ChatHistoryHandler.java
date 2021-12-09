package com.closer.prophet.handler;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.RecordEntry;
import com.closer.prophet.datahub.DatahubClientBuilder;
import com.closer.prophet.datahub.Sender;
import com.closer.prophet.entity.ChatHistory;
import com.closer.prophet.filter.AbstractFilter;
import com.closer.prophet.filter.ChatHistoryFilter;
import com.closer.prophet.serialization.RecordEntryDeserializer;
import com.closer.prophet.serialization.json.ChatHistoryDeserializer;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.List;

public class ChatHistoryHandler implements AbstractHandler<String>, Serializable {
    private static final AbstractFilter CHAT_HISTORY_FILTER = new ChatHistoryFilter();

    private static final String PROJECT_NAME = "closer_prod";
    private static final String TOPIC_NAME = "ods_fact_browsing_his";

    private static final DatahubClient datahubClient = DatahubClientBuilder.builder.build();
    private static final RecordSchema schema = datahubClient.getTopic(PROJECT_NAME, TOPIC_NAME)
            .getRecordSchema();

    /**
     * Get all browsing history log and then send to Data Hub.
     *
     * @param data All log except system log.
     */
    @Override
    public void process(JavaRDD<String> data) {
        JavaRDD<String> histories = data.filter(CHAT_HISTORY_FILTER::filter);
        sendToDataHub(histories);
    }

    /**
     * Send log of browsing history to Data Hub.
     *
     * @param histories User browsing history.
     */
    private void sendToDataHub(JavaRDD<String> histories) {
        histories.foreachPartition(partition -> {
            List<ChatHistory> chatHistories = ChatHistoryDeserializer.deserialize(partition);
            List<RecordEntry> recordEntries = RecordEntryDeserializer.deserialize(ChatHistory.class, chatHistories, schema);
            Sender.send(PROJECT_NAME, TOPIC_NAME, recordEntries);
        });
    }
}
