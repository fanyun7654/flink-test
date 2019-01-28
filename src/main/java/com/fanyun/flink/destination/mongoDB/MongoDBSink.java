package com.fanyun.flink.destination.mongoDB;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple3;

public class MongoDBSink extends RichSinkFunction<Tuple3<String, String, String>> {
    private static final long serialVersionUID = 1L;
    MongoClient mongoClient = null;

    public void invoke(Tuple5<String, String, String, String, String> value) {
        try {
            if (mongoClient != null) {
                mongoClient = getConnect();
                MongoDatabase db = mongoClient.getDatabase("soul_db");
                MongoCollection collection = db.getCollection("kafka");
                List<Document> list = new ArrayList<>();
                Document doc = new Document();
                doc.append("IP", value.f0);
                doc.append("TIME", value.f1);
                doc.append("CourseID", value.f2);
                doc.append("Status_Code", value.f3);
                doc.append("Referer", value.f4);
                list.add(doc);
                System.out.println("Insert Starting");
                collection.insertMany(list);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static MongoClient getConnect(){
        ServerAddress serverAddress = new ServerAddress("localhost", 27017);
        List<MongoCredential> credential = new ArrayList<>();
        //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
        MongoCredential mongoCredential1 = MongoCredential.createScramSha1Credential("root", "soul_db", "123456".toCharArray());
        credential.add(mongoCredential1);
        //通过连接认证获取MongoDB连接
        MongoClient mongoClient = new MongoClient(serverAddress, credential);
        return mongoClient;
    }
    public void open(Configuration parms) throws Exception {
        super.open(parms);
        mongoClient = getConnect();
    }

    public void close() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
