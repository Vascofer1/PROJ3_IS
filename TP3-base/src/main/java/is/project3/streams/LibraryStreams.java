package is.project3.streams;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import java.time.Duration;
import java.util.Properties;

public class LibraryStreams {

    private static final String BOOTSTRAP_SERVERS = "broker1:9092";

    private static final String SALES_TOPIC = "book-sales";
    private static final String RESTOCKS_TOPIC = "book-restocks";
    private static final String STATISTICS_TOPIC = "book-statistics";
    private static final String TOTAL_STATISTICS_TOPIC = "total-statistics";
    private static final String WINDOW_STATISTICS_TOPIC = "window-statistics";
    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "library-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> salesStream = builder.stream(
                SALES_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> restocksStream = builder.stream(
                RESTOCKS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> saleDeltas = salesStream.map((key, value) -> {
            JsonObject sale = JsonParser.parseString(value).getAsJsonObject();

            int bookId = sale.get("book_id").getAsInt();
            int units = sale.get("units").getAsInt();

            double unitSalePrice;

            if (sale.has("unit_sale_price")) {
                unitSalePrice = sale.get("unit_sale_price").getAsDouble();
            } else {
                unitSalePrice = sale.get("sale_price").getAsDouble();
            }

            double revenue = round(units * unitSalePrice);

            BookDelta delta = new BookDelta();
            delta.book_id = bookId;
            delta.revenue_delta = revenue;
            delta.expenses_delta = 0.0;
            delta.stock_delta = -units;
            delta.sale_count_delta = 1;

            return KeyValue.pair(String.valueOf(bookId), gson.toJson(delta));
        });

        KStream<String, String> restockDeltas = restocksStream.map((key, value) -> {
            JsonObject restock = JsonParser.parseString(value).getAsJsonObject();

            int bookId = restock.get("book_id").getAsInt();
            int units = restock.get("units").getAsInt();

            double unitPurchasePrice;

            if (restock.has("unit_purchase_price")) {
                unitPurchasePrice = restock.get("unit_purchase_price").getAsDouble();
            } else {
                unitPurchasePrice = restock.get("purchase_price").getAsDouble();
            }

            double expenses = round(units * unitPurchasePrice);

            BookDelta delta = new BookDelta();
            delta.book_id = bookId;
            delta.revenue_delta = 0.0;
            delta.expenses_delta = expenses;
            delta.stock_delta = units;
            delta.sale_count_delta = 0;

            return KeyValue.pair(String.valueOf(bookId), gson.toJson(delta));
        });

        KStream<String, String> allDeltas = saleDeltas.merge(restockDeltas);

        TimeWindows oneMinuteWindow = TimeWindows.of(Duration.ofMinutes(1));

        KTable<Windowed<String>, String> windowStatisticsTable = allDeltas
                .map((bookId, deltaJson) -> KeyValue.pair("WINDOW", deltaJson))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(oneMinuteWindow)
                .aggregate(
                        () -> gson.toJson(new WindowStatistic()),
                        (key, deltaJson, currentJson) -> {
                            BookDelta delta = gson.fromJson(deltaJson, BookDelta.class);
                            WindowStatistic current = gson.fromJson(currentJson, WindowStatistic.class);

                            current.id = 1;
                            current.revenue = round(current.revenue + delta.revenue_delta);
                            current.expenses = round(current.expenses + delta.expenses_delta);
                            current.profit = round(current.revenue - current.expenses);

                            return gson.toJson(current);
                        },
                        Materialized.with(Serdes.String(), Serdes.String()));

        windowStatisticsTable
                .toStream()
                .map((windowedKey, statisticJson) -> {
                    WindowStatistic statistic = gson.fromJson(statisticJson, WindowStatistic.class);

                    statistic.id = 1;
                    statistic.window_start = windowedKey.window().start();
                    statistic.window_end = windowedKey.window().end();

                    return KeyValue.pair(
                            "1",
                            toKafkaConnectWindowJson(gson.toJson(statistic)));
                })
                .to(WINDOW_STATISTICS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, String> totalStatisticsTable = allDeltas
                .map((bookId, deltaJson) -> KeyValue.pair("TOTAL", deltaJson))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        () -> gson.toJson(new TotalStatistic()),
                        (key, deltaJson, currentJson) -> {
                            BookDelta delta = gson.fromJson(deltaJson, BookDelta.class);
                            TotalStatistic current = gson.fromJson(currentJson, TotalStatistic.class);

                            current.id = 1;
                            current.revenue = round(current.revenue + delta.revenue_delta);
                            current.expenses = round(current.expenses + delta.expenses_delta);
                            current.profit = round(current.revenue - current.expenses);

                            return gson.toJson(current);
                        },
                        Materialized.with(Serdes.String(), Serdes.String()));

        totalStatisticsTable
                .toStream()
                .mapValues(LibraryStreams::toKafkaConnectTotalJson)
                .to(TOTAL_STATISTICS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, String> statisticsTable = allDeltas
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        () -> gson.toJson(new BookStatistic()),
                        (bookId, deltaJson, currentJson) -> {
                            BookDelta delta = gson.fromJson(deltaJson, BookDelta.class);
                            BookStatistic current = gson.fromJson(currentJson, BookStatistic.class);

                            current.book_id = delta.book_id;
                            current.revenue = round(current.revenue + delta.revenue_delta);
                            current.expenses = round(current.expenses + delta.expenses_delta);
                            current.profit = round(current.revenue - current.expenses);
                            current.stock = current.stock + delta.stock_delta;
                            current.sales_count = current.sales_count + delta.sale_count_delta;

                            return gson.toJson(current);
                        },
                        Materialized.with(Serdes.String(), Serdes.String()));

        statisticsTable
                .toStream()
                .mapValues(LibraryStreams::toKafkaConnectJson)
                .to(STATISTICS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();

        System.out.println("LibraryStreams started...");
    }

    private static String toKafkaConnectJson(String statisticJson) {
        BookStatistic statistic = gson.fromJson(statisticJson, BookStatistic.class);

        return "{"
                + "\"schema\":{"
                + "\"type\":\"struct\","
                + "\"fields\":["
                + "{\"type\":\"int32\",\"optional\":false,\"field\":\"book_id\"},"
                + "{\"type\":\"double\",\"optional\":true,\"field\":\"revenue\"},"
                + "{\"type\":\"double\",\"optional\":true,\"field\":\"expenses\"},"
                + "{\"type\":\"double\",\"optional\":true,\"field\":\"profit\"},"
                + "{\"type\":\"int32\",\"optional\":true,\"field\":\"stock\"},"
                + "{\"type\":\"int32\",\"optional\":true,\"field\":\"sales_count\"}"
                + "],"
                + "\"optional\":false"
                + "},"
                + "\"payload\":{"
                + "\"book_id\":" + statistic.book_id + ","
                + "\"revenue\":" + statistic.revenue + ","
                + "\"expenses\":" + statistic.expenses + ","
                + "\"profit\":" + statistic.profit + ","
                + "\"stock\":" + statistic.stock + ","
                + "\"sales_count\":" + statistic.sales_count
                + "}"
                + "}";
    }

    private static String toKafkaConnectTotalJson(String totalJson) {
        TotalStatistic statistic = gson.fromJson(totalJson, TotalStatistic.class);

        return "{"
                + "\"schema\":{"
                + "\"type\":\"struct\","
                + "\"fields\":["
                + "{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},"
                + "{\"type\":\"double\",\"optional\":true,\"field\":\"revenue\"},"
                + "{\"type\":\"double\",\"optional\":true,\"field\":\"expenses\"},"
                + "{\"type\":\"double\",\"optional\":true,\"field\":\"profit\"}"
                + "],"
                + "\"optional\":false"
                + "},"
                + "\"payload\":{"
                + "\"id\":" + statistic.id + ","
                + "\"revenue\":" + statistic.revenue + ","
                + "\"expenses\":" + statistic.expenses + ","
                + "\"profit\":" + statistic.profit
                + "}"
                + "}";
    }

    private static String toKafkaConnectWindowJson(String windowJson) {
        WindowStatistic statistic = gson.fromJson(windowJson, WindowStatistic.class);

        return "{"
                + "\"schema\":{"
                + "\"type\":\"struct\","
                + "\"fields\":["
                + "{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},"
                + "{\"type\":\"int64\",\"optional\":true,\"field\":\"window_start\"},"
                + "{\"type\":\"int64\",\"optional\":true,\"field\":\"window_end\"},"
                + "{\"type\":\"double\",\"optional\":true,\"field\":\"revenue\"},"
                + "{\"type\":\"double\",\"optional\":true,\"field\":\"expenses\"},"
                + "{\"type\":\"double\",\"optional\":true,\"field\":\"profit\"}"
                + "],"
                + "\"optional\":false"
                + "},"
                + "\"payload\":{"
                + "\"id\":" + statistic.id + ","
                + "\"window_start\":" + statistic.window_start + ","
                + "\"window_end\":" + statistic.window_end + ","
                + "\"revenue\":" + statistic.revenue + ","
                + "\"expenses\":" + statistic.expenses + ","
                + "\"profit\":" + statistic.profit
                + "}"
                + "}";
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
}