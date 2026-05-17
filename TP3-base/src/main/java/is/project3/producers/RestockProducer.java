package is.project3.producers;

import com.google.gson.Gson;
import is.project3.events.BookInfo;
import is.project3.events.RestockEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class RestockProducer {

    private static final String TOPIC = "book-restocks";
    private static final String BOOTSTRAP_SERVERS = "broker1:9092,broker2:9092,broker3:9092";

    private static final String DB_URL = "jdbc:postgresql://database:5432/project3";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "nopass";

    public static void main(String[] args) throws Exception {
        List<BookInfo> books = loadBooksFromDatabase();

        if (books.isEmpty()) {
            System.out.println("No books found in database. Add books before running the RestockProducer.");
            return;
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        props.put("enable.idempotence", true);
        props.put("retries", Integer.MAX_VALUE);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Gson gson = new Gson();
        Random random = new Random();

        while (true) {
            BookInfo book = books.get(random.nextInt(books.size()));

            int units = random.nextInt(10) + 1;
            double purchasePrice = Math.round((book.base_price * 0.6) * 100.0) / 100.0;

            RestockEvent event = new RestockEvent(
                    book.book_id,
                    book.title,
                    units,
                    purchasePrice,
                    System.currentTimeMillis()
            );

            String key = String.valueOf(book.book_id);
            String value = gson.toJson(event);

            producer.send(new ProducerRecord<>(TOPIC, key, value));

            System.out.println("RESTOCK sent: " + value);

            Thread.sleep(5000);
        }
    }

    private static List<BookInfo> loadBooksFromDatabase() throws SQLException {
        List<BookInfo> books = new ArrayList<>();

        String sql = "SELECT id, title, base_price FROM books";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                books.add(new BookInfo(
                        rs.getInt("id"),
                        rs.getString("title"),
                        rs.getDouble("base_price")
                ));
            }
        }

        return books;
    }
}
