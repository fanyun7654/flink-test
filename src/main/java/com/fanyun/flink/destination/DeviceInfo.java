package com.fanyun.flink.destination;

import java.io.Serializable;

public class DeviceInfo implements Serializable {
    public String din;
    public String device_type;
    public String device_version;

    public DeviceInfo() {}

    public DeviceInfo(String din, String device_type,String device_version) {
        this.din = din;
        this.device_type = device_type;
        this.device_version = device_version;
    }

    @Override
    public String toString() {
        return din + " : " + device_type + ":" + device_version;
    }
}
