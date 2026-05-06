package is.project3.cli;

import java.sql.*;
import java.util.Scanner;

public class AuthorCLI {

    private static final String DB_URL = "jdbc:postgresql://database:5432/project3";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "nopass";

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\n===== Author Management =====");
            System.out.println("1 - Add author");
            System.out.println("2 - List authors");
            System.out.println("3 - List window statistics");
            System.out.println("4 - List total statistics");
            System.out.println("0 - Exit");
            System.out.print("Choose an option: ");

            String option = scanner.nextLine();

            switch (option) {
                case "1":
                    addAuthor(scanner);
                    break;
                case "2":
                    listAuthors();
                    break;
                case "3":
                    listWindowStatistics();
                    break;
                case "4":
                    listTotalStatistics();
                    break;
                case "0":
                    System.out.println("Exiting...");
                    scanner.close();
                    return;
                default:
                    System.out.println("Invalid option.");
            }
        }
    }

    private static void addAuthor(Scanner scanner) {
        System.out.print("Author name: ");
        String name = scanner.nextLine();

        String sql = "INSERT INTO authors (name) VALUES (?)";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, name);
            stmt.executeUpdate();

            System.out.println("Author added successfully.");

        } catch (SQLException e) {
            System.out.println("Error adding author: " + e.getMessage());
        }
    }

    private static void listAuthors() {
        String sql = "SELECT id, name FROM authors ORDER BY id";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {

            System.out.println("\nAuthors:");
            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");

                System.out.println(id + " - " + name);
            }

        } catch (SQLException e) {
            System.out.println("Error listing authors: " + e.getMessage());
        }
    }

    private static void listWindowStatistics() {
        String sql = """
                    SELECT id, window_start, window_end, revenue, expenses, profit
                    FROM window_statistics
                    ORDER BY id
                """;

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {

            System.out.println("\nWindow statistics:");

            while (rs.next()) {
                int id = rs.getInt("id");
                long windowStart = rs.getLong("window_start");
                long windowEnd = rs.getLong("window_end");
                double revenue = rs.getDouble("revenue");
                double expenses = rs.getDouble("expenses");
                double profit = rs.getDouble("profit");

                System.out.println(
                        "ID: " + id +
                                " | Window start: " + windowStart +
                                " | Window end: " + windowEnd +
                                " | Revenue: " + revenue +
                                " | Expenses: " + expenses +
                                " | Profit: " + profit);
            }

        } catch (SQLException e) {
            System.out.println("Error listing window statistics: " + e.getMessage());
        }
    }

    private static void listTotalStatistics() {
        String sql = """
                    SELECT id, revenue, expenses, profit
                    FROM total_statistics
                    ORDER BY id
                """;

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {

            System.out.println("\nTotal statistics:");

            boolean hasResults = false;

            while (rs.next()) {
                hasResults = true;

                int id = rs.getInt("id");
                double revenue = rs.getDouble("revenue");
                double expenses = rs.getDouble("expenses");
                double profit = rs.getDouble("profit");

                System.out.println(
                        "ID: " + id +
                                " | Total revenue: " + revenue +
                                " | Total expenses: " + expenses +
                                " | Total profit: " + profit);
            }

            if (!hasResults) {
                System.out.println("No total statistics available yet.");
            }

        } catch (SQLException e) {
            System.out.println("Error listing total statistics: " + e.getMessage());
        }
    }

}