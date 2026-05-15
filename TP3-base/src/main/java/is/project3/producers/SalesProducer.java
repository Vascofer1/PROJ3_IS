package is.project3.producers;

import com.google.gson.Gson;
import is.project3.events.BookInfo;
import is.project3.events.SaleEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class SalesProducer {

    private static final String TOPIC = "book-sales";
    private static final String BOOTSTRAP_SERVERS = "broker1:9092";

    private static final String DB_URL = "jdbc:postgresql://database:5432/project3";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "nopass";

    public static void main(String[] args) throws Exception {
        List<BookInfo> books = loadBooksFromDatabase();
        List<UserInfo> users = loadUsersFromDatabase();

        if (books.isEmpty()) {
            System.out.println("No books found in database. Add books before running the SalesProducer.");
            return;
        }

        if (users.isEmpty()) {
            System.out.println("No users found in database. Add users before running the SalesProducer.");
            return;
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Gson gson = new Gson();
        Random random = new Random();

        while (true) {
            BookInfo book = books.get(random.nextInt(books.size()));
            UserInfo user = users.get(random.nextInt(users.size()));

            int units = random.nextInt(3) + 1;
            double salePrice = book.base_price;
            salePrice += random.nextInt(7); 
            double unitPurchasePrice = round(book.base_price * 0.6);
            double total_price = Math.round((salePrice * units) * 100.0) / 100.0;
            double profit = round(total_price - (unitPurchasePrice * units));

            SaleEvent event = new SaleEvent(
                    book.book_id,
                    book.title,
                    user.user_id,
                    user.name,
                    units,
                    salePrice,
                    unitPurchasePrice,
                    total_price,
                    profit,
                    System.currentTimeMillis()
            );

            String key = String.valueOf(book.book_id);
            String value = gson.toJson(event);

            producer.send(new ProducerRecord<>(TOPIC, key, value));

            System.out.println("SALE sent: " + value);

            Thread.sleep(3000);
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

    private static List<UserInfo> loadUsersFromDatabase() throws SQLException {
        List<UserInfo> users = new ArrayList<>();

        String sql = "SELECT id, name FROM users";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                users.add(new UserInfo(
                        rs.getInt("id"),
                        rs.getString("name")
                ));
            }
        }

        return users;
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    private static class UserInfo {
        private final int user_id;
        private final String name;

        private UserInfo(int user_id, String name) {
            this.user_id = user_id;
            this.name = name;
        }
    }
}
