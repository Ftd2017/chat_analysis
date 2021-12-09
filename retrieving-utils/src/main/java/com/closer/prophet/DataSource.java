package com.closer.prophet;

import org.apache.commons.dbcp.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * An sington connection instance.
 */
public class DataSource {
    //TODO - need to be update as connection pool

    private static String CLASS_NAME = "com.mysql.jdbc.Driver";
    private static String JDBC_URL = "jdbc:mysql://rm-bp1f60dfqn3vjb31e.mysql.rds.aliyuncs.com";
    private static String USER = "biz_umscloud";
    private static String PASS = "UMS@2018";

    private static DataSource datasource;
    private BasicDataSource ds;

    private DataSource() {
        ds = new BasicDataSource();
        ds.setDriverClassName(CLASS_NAME);
        ds.setUsername(USER);
        ds.setPassword(PASS);
        ds.setUrl(JDBC_URL);

        // the settings below are optional -- dbcp can work with defaults
        ds.setMinIdle(5);
        ds.setMaxIdle(20);
        ds.setMaxOpenPreparedStatements(180);

    }

    public static DataSource getInstance() {
        if (datasource == null) {
            datasource = new DataSource();
            return datasource;
        } else {
            return datasource;
        }
    }

    public Connection getConnection() throws SQLException {
        return this.ds.getConnection();
    }
}
