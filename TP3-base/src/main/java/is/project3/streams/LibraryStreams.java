package is.project3.streams;

import com.google.gson.Gson;
import is.project3.events.RestockEvent;
import is.project3.events.SaleEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class LibraryStreams {

    private static final String BOOTSTRAP_SERVERS = "broker1:9092,broker2:9092,broker3:9092";

    private static final String SALES_TOPIC = "book-sales";
    private static final String RESTOCKS_TOPIC = "book-restocks";
    private static final String STATISTICS_TOPIC = "book-statistics";
    private static final String TOTAL_STATISTICS_TOPIC = "total-statistics";
    private static final String WINDOW_STATISTICS_TOPIC = "window-statistics";
    private static final String USER_STATISTICS_TOPIC = "user-statistics";
    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "library-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.producerPrefix("acks"), "all");
        props.put(StreamsConfig.producerPrefix("enable.idempotence"), true);
        props.put(StreamsConfig.producerPrefix("retries"), Integer.MAX_VALUE);

        Serde<SaleEvent> saleEventSerde = new JsonSerde<>(SaleEvent.class);
        Serde<RestockEvent> restockEventSerde = new JsonSerde<>(RestockEvent.class);
        Serde<BookDelta> bookDeltaSerde = new JsonSerde<>(BookDelta.class);
        Serde<BookStatistic> bookStatisticSerde = new JsonSerde<>(BookStatistic.class);
        Serde<TotalStatistic> totalStatisticSerde = new JsonSerde<>(TotalStatistic.class);
        Serde<WindowStatistic> windowStatisticSerde = new JsonSerde<>(WindowStatistic.class);
        Serde<UserStatistic> userStatisticSerde = new JsonSerde<>(UserStatistic.class);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, SaleEvent> salesStream = builder.stream(
                SALES_TOPIC,
                Consumed.with(Serdes.String(), saleEventSerde));

        KStream<String, RestockEvent> restocksStream = builder.stream(
                RESTOCKS_TOPIC,
                Consumed.with(Serdes.String(), restockEventSerde));

        KStream<String, BookDelta> saleDeltas = salesStream.map((key, sale) -> {
            double revenue = round(sale.units * getUnitSalePrice(sale));

            BookDelta delta = new BookDelta();
            delta.book_id = sale.book_id;
            delta.revenue_delta = revenue;
            delta.expenses_delta = 0.0;
            delta.stock_delta = -sale.units;
            delta.sale_count_delta = 1;

            return KeyValue.pair(String.valueOf(sale.book_id), delta);
        });

        KStream<String, BookDelta> restockDeltas = restocksStream.map((key, restock) -> {
            double expenses = round(restock.units * getUnitPurchasePrice(restock));

            BookDelta delta = new BookDelta();
            delta.book_id = restock.book_id;
            delta.revenue_delta = 0.0;
            delta.expenses_delta = expenses;
            delta.stock_delta = restock.units;
            delta.sale_count_delta = 0;

            return KeyValue.pair(String.valueOf(restock.book_id), delta);
        });

        KStream<String, BookDelta> allDeltas = saleDeltas.merge(restockDeltas);

        TimeWindows oneMinuteWindow = TimeWindows.of(Duration.ofMinutes(1));

        KTable<Windowed<String>, WindowStatistic> windowStatisticsTable = allDeltas
                .map((bookId, delta) -> KeyValue.pair("WINDOW", delta))
                .groupByKey(Grouped.with(Serdes.String(), bookDeltaSerde))
                .windowedBy(oneMinuteWindow)
                .aggregate(
                        WindowStatistic::new,
                        (key, delta, current) -> {
                            current.id = 1;
                            current.revenue = round(current.revenue + delta.revenue_delta);
                            current.expenses = round(current.expenses + delta.expenses_delta);
                            current.profit = round(current.revenue - current.expenses);

                            return current;
                        },
                        Materialized.with(Serdes.String(), windowStatisticSerde));

        windowStatisticsTable
                .toStream()
                .map((windowedKey, statistic) -> {
                    statistic.id = 1;
                    statistic.window_start = windowedKey.window().start();
                    statistic.window_end = windowedKey.window().end();

                    return KeyValue.pair("1", toKafkaConnectWindowJson(statistic));
                })
                .to(WINDOW_STATISTICS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, TotalStatistic> totalStatisticsTable = allDeltas
                .map((bookId, delta) -> KeyValue.pair("TOTAL", delta))
                .groupByKey(Grouped.with(Serdes.String(), bookDeltaSerde))
                .aggregate(
                        TotalStatistic::new,
                        (key, delta, current) -> {
                            current.id = 1;
                            current.revenue = round(current.revenue + delta.revenue_delta);
                            current.expenses = round(current.expenses + delta.expenses_delta);
                            current.profit = round(current.revenue - current.expenses);

                            return current;
                        },
                        Materialized.with(Serdes.String(), totalStatisticSerde));

        totalStatisticsTable
                .toStream()
                .mapValues(LibraryStreams::toKafkaConnectTotalJson)
                .to(TOTAL_STATISTICS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, BookStatistic> statisticsTable = allDeltas
                .groupByKey(Grouped.with(Serdes.String(), bookDeltaSerde))
                .aggregate(
                        BookStatistic::new,
                        (bookId, delta, current) -> {
                            current.book_id = delta.book_id;
                            current.revenue = round(current.revenue + delta.revenue_delta);
                            current.expenses = round(current.expenses + delta.expenses_delta);
                            current.profit = round(current.revenue - current.expenses);
                            current.stock = current.stock + delta.stock_delta;
                            current.sales_count = current.sales_count + delta.sale_count_delta;

                            return current;
                        },
                        Materialized.with(Serdes.String(), bookStatisticSerde));

        statisticsTable
                .toStream()
                .mapValues(LibraryStreams::toKafkaConnectBookJson)
                .to(STATISTICS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, UserStatistic> userStatisticsTable = salesStream
                .filter((key, sale) -> sale.user_id > 0)
                .groupBy((key, sale) -> String.valueOf(sale.user_id),
                        Grouped.with(Serdes.String(), saleEventSerde))
                .aggregate(
                        UserStatistic::new,
                        (userId, sale, current) -> {
                            double revenue = round(sale.units * getUnitSalePrice(sale));
                            double expenses = getSaleExpenses(sale, revenue);

                            current.user_id = Integer.parseInt(userId);
                            current.revenue = round(current.revenue + revenue);
                            current.expenses = round(current.expenses + expenses);
                            current.profit = round(current.revenue - current.expenses);
                            current.sales_count = current.sales_count + 1;

                            return current;
                        },
                        Materialized.with(Serdes.String(), userStatisticSerde));

        userStatisticsTable
                .toStream()
                .mapValues(LibraryStreams::toKafkaConnectUserJson)
                .to(USER_STATISTICS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();

        System.out.println("LibraryStreams started...");
    }

    private static double getUnitSalePrice(SaleEvent sale) {
        return sale.unit_sale_price;
    }

    private static double getUnitPurchasePrice(RestockEvent restock) {
        if (restock.unit_purchase_price != 0.0) {
            return restock.unit_purchase_price;
        }

        return restock.purchase_price;
    }

    private static double getSaleExpenses(SaleEvent sale, double revenue) {
        if (sale.unit_purchase_price != 0.0) {
            return round(sale.units * sale.unit_purchase_price);
        }

        if (sale.profit != 0.0) {
            return round(revenue - sale.profit);
        }

        return 0.0;
    }

    private static String toKafkaConnectBookJson(BookStatistic statistic) {
        return toKafkaConnectJson(
                List.of(
                        field("int32", "book_id", false),
                        field("double", "revenue", true),
                        field("double", "expenses", true),
                        field("double", "profit", true),
                        field("int32", "stock", true),
                        field("int32", "sales_count", true)),
                payload()
                        .add("book_id", statistic.book_id)
                        .add("revenue", statistic.revenue)
                        .add("expenses", statistic.expenses)
                        .add("profit", statistic.profit)
                        .add("stock", statistic.stock)
                        .add("sales_count", statistic.sales_count)
                        .build());
    }

    private static String toKafkaConnectTotalJson(TotalStatistic statistic) {
        return toKafkaConnectJson(
                List.of(
                        field("int32", "id", false),
                        field("double", "revenue", true),
                        field("double", "expenses", true),
                        field("double", "profit", true)),
                payload()
                        .add("id", statistic.id)
                        .add("revenue", statistic.revenue)
                        .add("expenses", statistic.expenses)
                        .add("profit", statistic.profit)
                        .build());
    }

    private static String toKafkaConnectWindowJson(WindowStatistic statistic) {
        return toKafkaConnectJson(
                List.of(
                        field("int32", "id", false),
                        field("int64", "window_start", true),
                        field("int64", "window_end", true),
                        field("double", "revenue", true),
                        field("double", "expenses", true),
                        field("double", "profit", true)),
                payload()
                        .add("id", statistic.id)
                        .add("window_start", statistic.window_start)
                        .add("window_end", statistic.window_end)
                        .add("revenue", statistic.revenue)
                        .add("expenses", statistic.expenses)
                        .add("profit", statistic.profit)
                        .build());
    }

    private static String toKafkaConnectUserJson(UserStatistic statistic) {
        return toKafkaConnectJson(
                List.of(
                        field("int32", "user_id", false),
                        field("double", "revenue", true),
                        field("double", "expenses", true),
                        field("double", "profit", true),
                        field("int32", "sales_count", true)),
                payload()
                        .add("user_id", statistic.user_id)
                        .add("revenue", statistic.revenue)
                        .add("expenses", statistic.expenses)
                        .add("profit", statistic.profit)
                        .add("sales_count", statistic.sales_count)
                        .build());
    }

    private static String toKafkaConnectJson(List<ConnectField> fields, Map<String, Object> payload) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "struct");

        List<Map<String, Object>> schemaFields = new ArrayList<>();
        for (ConnectField field : fields) {
            Map<String, Object> schemaField = new LinkedHashMap<>();
            schemaField.put("type", field.type);
            schemaField.put("optional", field.optional);
            schemaField.put("field", field.name);
            schemaFields.add(schemaField);
        }

        schema.put("fields", schemaFields);
        schema.put("optional", false);

        Map<String, Object> connectJson = new LinkedHashMap<>();
        connectJson.put("schema", schema);
        connectJson.put("payload", payload);

        return gson.toJson(connectJson);
    }

    private static ConnectField field(String type, String name, boolean optional) {
        return new ConnectField(type, name, optional);
    }

    private static PayloadBuilder payload() {
        return new PayloadBuilder();
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    public static class BookDelta {
        public int book_id;
        public double revenue_delta;
        public double expenses_delta;
        public int stock_delta;
        public int sale_count_delta;
    }

    public static class BookStatistic {
        public int book_id = 0;
        public double revenue = 0.0;
        public double expenses = 0.0;
        public double profit = 0.0;
        public int stock = 0;
        public int sales_count = 0;
    }

    public static class UserStatistic {
        public int user_id = 0;
        public double revenue = 0.0;
        public double expenses = 0.0;
        public double profit = 0.0;
        public int sales_count = 0;
    }

    public static class TotalStatistic {
        public int id = 1;
        public double revenue = 0.0;
        public double expenses = 0.0;
        public double profit = 0.0;
    }

    public static class WindowStatistic {
        public int id = 1;
        public long window_start = 0L;
        public long window_end = 0L;
        public double revenue = 0.0;
        public double expenses = 0.0;
        public double profit = 0.0;
    }

    private static class ConnectField {
        private final String type;
        private final String name;
        private final boolean optional;

        private ConnectField(String type, String name, boolean optional) {
            this.type = type;
            this.name = name;
            this.optional = optional;
        }
    }

    private static class PayloadBuilder {
        private final Map<String, Object> payload = new LinkedHashMap<>();

        private PayloadBuilder add(String name, Object value) {
            payload.put(name, value);
            return this;
        }

        private Map<String, Object> build() {
            return payload;
        }
    }
}
