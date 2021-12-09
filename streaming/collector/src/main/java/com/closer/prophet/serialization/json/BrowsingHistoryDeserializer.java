package com.closer.prophet.serialization.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.closer.prophet.entity.Action;
import com.closer.prophet.entity.BrowsingHistory;
import com.closer.prophet.rate.RateStrategy;
import com.closer.prophet.rate.impl.SimpleActionBasedRateStrategy;
import com.closer.prophet.utils.SubjectClassifyUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class BrowsingHistoryDeserializer {
    private static final Logger logger = LoggerFactory.getLogger(BrowsingHistoryDeserializer.class);
    private static final RateStrategy strategy = new SimpleActionBasedRateStrategy();
    private static final String ARTICLE_ID_FIELD_NAME = "subjectid";

    BrowsingHistoryDeserializer() {
    }

    /**
     * Deserialize json log.
     *
     * @param logs An iterator of logs.
     * @return A list of RecordEntry.
     */
    public static List<BrowsingHistory> deserialize(Iterator<String> logs) {
        List<BrowsingHistory> browsingHistories = new ArrayList<>();
        while(logs.hasNext()) {
            String log = logs.next();
            try{
                BrowsingHistory history = BrowsingHistoryDeserializer.deserialize(log);
                browsingHistories.add(history);
            } catch (JSONException je) {
                logger.error("Failed to interpret json!", je);
            } catch (NullPointerException npe) {
                logger.error("NullPointerException catched.", npe);
            }

        }
        return browsingHistories;
    }

    /**
     * Deserialize log to BrowsingHistory instance.
     *
     * @param json Origin log.
     * @return BrowsingHistory instance.
     */
    public static BrowsingHistory deserialize(String json) {
        // TODO - replace fastjson with jackson, entity class should be update at the same time.
        JSONObject sourceData = JSON.parseObject(json);

        String command = sourceData.getString("command");
        Action action = Action.parseFrom(command);
        String subjectID = getSubjectId(action, sourceData);

        String userId = sourceData.getString("uid");
        int rate = strategy.rate(action);
        String udid = sourceData.getString("x_closer_udid");
        String userAgent = sourceData.getString("user_agent");

        int intType = sourceData.getIntValue("int_type");
        String videoStyle = sourceData.getString("video_style");

        int type = SubjectClassifyUtils.classify(intType, videoStyle);

        String title = "";
        Date publishTime = new Date(0);
        if (sourceData.containsKey("long_publish_time")) {
            publishTime = new Date(sourceData.getLong("long_publish_time"));
        }

        String umsLogTime = sourceData.getString("ums_log_time");
        if (null == umsLogTime) {
            umsLogTime = "0";
        }
        Date readTime = DateTime.parse(umsLogTime).toDate();

        return new BrowsingHistory(userId,
                subjectID,
                rate,
                readTime,
                title,
                publishTime,
                udid,
                userAgent,
                type);
    }

    /**
     * Get identifier of article.
     *
     * @param action Type of user action. Different action generates different type of log,
     *               it is necessary to provide command for get identifier of article.
     * @param data   Log entity in json format. It contains information of user action.
     * @return identifier of article.
     */
    private static String getSubjectId(Action action, JSONObject data) {
        String subjectId;

        switch (action) {
            case READ_COMMAND:
                subjectId = data.getJSONObject("p-closer_subject-show").getString(ARTICLE_ID_FIELD_NAME);
                break;
            case LIKE_COMMAND:
                subjectId = data.getJSONObject("p-closer_subject-like").getString(ARTICLE_ID_FIELD_NAME);
                break;
            case COLLECT_COMMAND:
                subjectId = data.getJSONObject("p-closer_subject-collect").getString(ARTICLE_ID_FIELD_NAME);
                break;
            case SHARE_COMMAND:
                subjectId = data.getJSONObject("p-closer_subject-share_subject").getString(ARTICLE_ID_FIELD_NAME);
                break;
            case SHARE_TO_GROUP_COMMAND:
                subjectId = data.getJSONObject("p-closer_subject-share").getString(ARTICLE_ID_FIELD_NAME);
                break;
            case COMMENT_COMMAND:
                subjectId = data.getJSONObject("p-closer_reply-add_reply").getString(ARTICLE_ID_FIELD_NAME);
                break;
            default:
                subjectId = null;
                break;
        }

        return subjectId;
    }
}
