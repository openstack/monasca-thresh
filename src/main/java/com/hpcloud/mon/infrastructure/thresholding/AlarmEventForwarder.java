package com.hpcloud.mon.infrastructure.thresholding;

public interface AlarmEventForwarder {

    void send(String alertExchange, String alertRoutingKey, String json);

}
