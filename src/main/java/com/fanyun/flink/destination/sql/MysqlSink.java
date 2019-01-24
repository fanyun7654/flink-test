package com.fanyun.flink.destination.sql;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlSink extends RichSinkFunction<Tuple3<String,String,String>> {
    private static final long serialVersionUID = 1L;
    private Connection connection;
    private PreparedStatement preparedStatement;
    String username = "root";
    String password = "root123456";
    String drivername = "com.mysql.jdbc.Driver";
    String dburl = "jdbc:mysql://localhost:3306/flink_test";

    public void invoke(Tuple3<String, String, String> value) throws Exception {
        System.out.println("value1:"+value.f0);
        System.out.println("value2:"+value.f1);
        System.out.println("value3:"+value.f2);
        Class.forName(drivername);
        connection = DriverManager.getConnection(dburl, username, password);
        String sql = "replace into flink_test(din,device_type,device_version) values(?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, value.f0);
        preparedStatement.setString(2, value.f1);
        preparedStatement.setString(3, value.f2);
        preparedStatement.executeUpdate();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
