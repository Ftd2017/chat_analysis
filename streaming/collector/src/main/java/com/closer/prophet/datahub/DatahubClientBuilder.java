package com.closer.prophet.datahub;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.Account;
import com.aliyun.datahub.auth.AliyunAccount;

public enum DatahubClientBuilder {
    builder;

    private DatahubClient datahubClient;

    DatahubClientBuilder() {
    }

    public DatahubClient build() {
        if (null == datahubClient) {
            Account account = new AliyunAccount("LTAIIptInDGr8zFz", "p2MVsRN0qmQhaoEl9IBWoPsX0wUI7g");
            DatahubConfiguration conf = new DatahubConfiguration(account, "http://dh-cn-hangzhou.aliyuncs.com");
            datahubClient = new DatahubClient(conf);
        }

        return datahubClient;
    }
}
