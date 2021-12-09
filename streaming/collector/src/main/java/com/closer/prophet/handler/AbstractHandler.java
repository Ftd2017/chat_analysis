package com.closer.prophet.handler;

import org.apache.spark.api.java.JavaRDD;

public interface AbstractHandler<T> {
    void process(JavaRDD<T> rdd);
}
