package com.fanyun.flink.destination.postgreSql;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.postgresql.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class PostGreSQLSink extends RichSinkFunction<Tuple3<String,String,String>> {
    private static final long serialVersionUID = 1L;
    private Connection connection;
    private PreparedStatement preparedStatement;
    static String username = "postgres";
    static String password = "postgres";
    static String drivername = "org.postgresql.Driver";
    static String dburl = "jdbc:postgresql://localhost:5432/flink_test";

    public void invoke(Tuple3<String, String, String> value) throws Exception {
        System.out.println("value1:"+value.f0);
        System.out.println("value2:"+value.f1);
        System.out.println("value3:"+value.f2);
        Class.forName(drivername);

        connection = DriverManager.getConnection(dburl, username, password);
        String sql = "INSERT into flink_test(din,device_type,device_version) values(?,?,?)";
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


    public static void main(String args[]) {
        Connection c = null;
        try {
            Class.forName(drivername);
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/flink_test",
                            "postgres", "root123456");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");
    }
}
