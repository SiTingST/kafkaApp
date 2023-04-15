
package main;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class kafkaStreamProcessor implements Processor<String, String, String, String> {
    private static final Logger logger = LoggerFactory.getLogger(kafkaStreamProcessor.class);

    private void addItemToDB(String content){

//        String url = System.getenv("DB_URL");
//        String user = System.getenv("DB_USER");
//        String password = System.getenv("DB_PASSWORD");
//        System.out.println("url is" + System.getenv("DB_URL"));

        String url = "jdbc:mysql://localhost:3306/kafkaDatabase";
        String user = "root";
        String password = "newrootpassword";
        try {
            Connection connection = DriverManager.getConnection(url, user, password);

            String sqlStmt = "INSERT INTO adviceTable (advice) VALUES (?)";

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
        addItemToDB(record.value().toString());
    }
}
