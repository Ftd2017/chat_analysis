package com.closer.prophet;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.Account;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.RecordEntry;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class LivenessRetrievingUtils {
    private static Logger logger = LoggerFactory.getLogger(LivenessRetrievingUtils.class);

    public static void main(String[] args) {
        final String RETRIEVE_CLASS_SQL = "select id from ums_group.t_class;";

        try {
            logger.info("Setting up connection to Database...");
            ResultSet resultSet = DataSource.getInstance()
                    .getConnection()
                    .createStatement()
                    .executeQuery(RETRIEVE_CLASS_SQL);

            logger.info("Retrieving ids from table...");
            List<String> ids = getIdsFrom(resultSet);
            logger.info("Retrieving data from redis...");
            List<Liveness> liveness = getLivenessFrom(ids);

            // save to Data Hub
            logger.info("Sending data to DataHub...");
            save(liveness);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Loop ResultSet and return a list of ids.
     *
     * @param set Information of groups.
     * @return List of group ids.
     * @throws SQLException
     */
    private static List<String> getIdsFrom(ResultSet set) throws SQLException {
        final String label = "id";
        List<String> ids = new LinkedList<String>();

        while (set.next()) {
            ids.add(set.getString(label));
        }

        return ids;
    }

    /**
     * Calculating liveness of each group.
     *
     * @param ids A list contains id of each group.
     * @return group id and it's liveness.
     */
    private static List<Liveness> getLivenessFrom(List<String> ids) {
        Double endTime = (double) System.currentTimeMillis();
        Double startTime = endTime - 1000 * 60 * 60 * 24;

        List<Liveness> list = new LinkedList<Liveness>();

        final String redisHost = "bigdata-dev-1-1.redis.rds.aliyuncs.com";
        final int redisPort = 6379;
        final String pass = "Closer2018";
        final int timeout = 3000;

        JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), redisHost, redisPort, timeout, pass);
        Jedis jedis = jedisPool.getResource();

        for (String id : ids) {
            long memberLiveness = jedis.zcount("popularity_" + id, startTime, endTime);
            long memberContribution = jedis.zcount("Contribution_" + id + "", startTime, endTime);
            long memberParticipant = jedis.zcount("joinValue_" + id, startTime, endTime);

            long managerParticipant = 0;
            Set<String> managerParticipantKeys = jedis.keys("joinValue_" + id + "*");
            managerParticipantKeys.remove("joinValue_" + id);
            for (String key : managerParticipantKeys) {
                managerParticipant += jedis.zcount(key, startTime, endTime);
            }

            long managerContribution = 0;
            Set<String> managerContributionKeys = jedis.keys("Contribution_" + id + "*");
            managerContributionKeys.remove("Contribution_" + id);
            for (String key : managerContributionKeys) {
                managerContribution += jedis.zcount(key, startTime, endTime);
            }

            list.add(new Liveness(id, memberLiveness
                    + memberContribution * 2 + memberParticipant
                    + (managerParticipant * 2 + managerContribution) * 5));
        }

        return list;
    }

    /**
     * Save group's id and liveness to Data Hub.
     *
     * @param list A list contains id and liveness of each group.
     * @return 1 for success, 1 for error.
     */
    private static int save(List<Liveness> list) {
        final String PROJECT = "closer_prod";
        final String TOPIC = "ods_dim_t_class_liveness_his";

        final String ACCESS_ID = "LTAIIptInDGr8zFz";
        final String ACCESS_KEY = "p2MVsRN0qmQhaoEl9IBWoPsX0wUI7g";
        final String ENDPOINT = "http://dh-cn-hangzhou.aliyuncs.com";

        Account account = new AliyunAccount(ACCESS_ID, ACCESS_KEY);
        DatahubConfiguration conf = new DatahubConfiguration(account, ENDPOINT);
        DatahubClient datahubClient = new DatahubClient(conf);
        RecordSchema schema = datahubClient.getTopic(PROJECT, TOPIC).getRecordSchema();

        try {
            datahubClient.putRecords(PROJECT, TOPIC, toRecords(list, schema));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0;
    }

    /**
     * Formatting data to which DataHub supported.
     *
     * @param list   Record list.
     * @param schema Record schema of DataHub.
     * @return Data in the format of DataHub.
     */
    private static List<RecordEntry> toRecords(List<Liveness> list, RecordSchema schema) {
        List<RecordEntry> entries = new ArrayList<RecordEntry>();

        for (Liveness element : list) {
            RecordEntry entry = new RecordEntry(schema);
            entry.setString("id", element.getId());
            entry.setBigint("score", element.getLiveness());
            entry.setString("p_dt", DateFormatUtils.format(new Date(), "yyyyMMdd"));

            entries.add(entry);
        }

        return entries;
    }

    /**
     * Liveness of group.
     */
    private static final class Liveness {
        String id;
        long score;

        Liveness(String id, long liveness) {
            this.id = id;
            this.score = liveness;
        }

        public String getId() {
            return id;
        }

        public long getLiveness() {
            return score;
        }
    }
}
