package com.closer.prophet.serialization.json;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.closer.prophet.entity.ChatHistory;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class ChatHistoryDeserializer {
    private static final Logger logger = LoggerFactory.getLogger(ChatHistoryDeserializer.class);

    ChatHistoryDeserializer() {
    }

    /**
     * Deserialize json log.
     *
     * @param logs An iterator of logs.
     * @return A list of RecordEntry.
     */
    public static List<ChatHistory> deserialize(Iterator<String> logs) {
        List<ChatHistory> chatHistories = new ArrayList<>();
        while(logs.hasNext()) {
            String log = logs.next();
            try{
                ChatHistory history = ChatHistoryDeserializer.deserialize(log);
                chatHistories.add(history);
            } catch (JSONException je) {
                logger.error("Failed to interpret json!", je);
            } catch (NullPointerException npe) {
                logger.error("NullPointerException catched.", npe);
            }

        }
        return chatHistories;
    }

    private static ChatHistory deserialize(String json) {
        JSONObject object;
        object = JSONObject.parseObject(json);

        String userId = object.getString("uid");
        String groupId = object.getString("groupId");
        String communityId = object.getString("communityId");
        String text = "";
        long responseStatus = object.getLong("response_status");

        String sendTo = null;
        JSONObject messageSent = object.getJSONObject("p-message-send");
        if (null != messageSent) {
            sendTo = messageSent.getString("to");
        }
        Date createTime = new Date();

        Date logReceivedTime = new Date(0);
        String umsLogTime = object.getString("ums_log_time");
        if (StringUtils.isNotBlank(umsLogTime)) {
            logReceivedTime = DateTime.parse(umsLogTime).toDate();
        }

        return new ChatHistory(userId, groupId, communityId, text,
                logReceivedTime, responseStatus, sendTo, createTime);
    }
}
