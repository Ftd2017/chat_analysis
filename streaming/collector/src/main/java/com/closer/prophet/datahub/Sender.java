package com.closer.prophet.datahub;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.model.PutRecordsResult;
import com.aliyun.datahub.model.RecordEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Sender {
    private static final Logger logger = LoggerFactory.getLogger(Sender.class);

    private static DatahubClient datahubClient = DatahubClientBuilder.builder.build();

    public static void send(String project, String topic, List<RecordEntry> entries) {
        if (!entries.isEmpty()) {
            PutRecordsResult result = datahubClient.putRecords(project, topic, entries);
            List<RecordEntry> failedEntries = result.getFailedRecords();
            if (!failedEntries.isEmpty()) {
                logger.error(String.format("Failed to send message to Datahub: %s", failedEntries.toString()));
            }
        }
    }
}
