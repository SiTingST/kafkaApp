package org;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class kafkaStreamProducer implements Processor<String, String, String, String> {

    public void addItemToDB(String content){
        String url = "jdbc:mysql://localhost:3306/kafkaDatabase";
        String user = "root";
        String password = "newrootpassword";

        try {
            Connection connection = DriverManager.getConnection(url, user, password);

            String sqlStmt = "INSERT INTO kafkaContent (content) VALUES (?)";

            PreparedStatement statement = connection.prepareStatement(sqlStmt);
            statement.setString(1, content);

            int noOfRowsInserted = statement.executeUpdate();
            if (noOfRowsInserted > 0) {
                System.out.println("New content is added to db");
            }

            statement.close();
            connection.close();
        } catch (SQLException e) {
            System.err.println("Failed to add item to the database: " + e.getMessage());
        }
    }


    @Override
    public void process(Record record) {
        System.out.print("this is record value: " + record.value().toString());
        addItemToDB(record.value().toString());
    }
}
