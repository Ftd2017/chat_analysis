package com.closer.prophet.rate;

import com.closer.prophet.entity.Action;

public interface RateStrategy {
    int rate(Action action);
}