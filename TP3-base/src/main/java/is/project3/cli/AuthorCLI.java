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
            System.out.println("\n===== Library Management =====");
            System.out.println("1 - Add author");
            System.out.println("2 - List authors");
            System.out.println("3 - Add book");
            System.out.println("4 - List books");
            System.out.println("5 - List window statistics");
            System.out.println("6 - List total statistics");
            System.out.println("7 - Revenue per book");
            System.out.println("8 - Expenses per book");
            System.out.println("9 - Profit per book");
            System.out.println("10 - Current stock per book");
            System.out.println("11 - Average spent per sale per book");
            System.out.println("12 - Most profitable book");
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
                    addBook(scanner);
                    break;
                case "4":
                    listBooks();
                    break;
                case "5":
                    listWindowStatistics();
                    break;
                case "6":
                    listTotalStatistics();
                    break;
                case "7":
                    listRevenuePerBook();
                    break;
                case "8":
                    listExpensesPerBook();
                    break;
                case "9":
                    listProfitPerBook();
                    break;
                case "10":
                    listCurrentStockPerBook();
                    break;
                case "11":
                    listAverageSpentPerSalePerBook();
                    break;
                case "12":
                    listMostProfitableBook();
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

    private static void addBook(Scanner scanner) {
        System.out.print("Book title: ");
        String title = scanner.nextLine();

        System.out.print("Author ID: ");
        String authorIdInput = scanner.nextLine();

        System.out.print("Genre: ");
        String genre = scanner.nextLine();

        System.out.print("Base price: ");
        String basePriceInput = scanner.nextLine();

        int authorId;
        double basePrice;

        try {
            authorId = Integer.parseInt(authorIdInput);
        } catch (NumberFormatException e) {
            System.out.println("Invalid author ID. Please enter a numeric value.");
            return;
        }

        try {
            basePrice = Double.parseDouble(basePriceInput);
        } catch (NumberFormatException e) {
            System.out.println("Invalid base price. Please enter a numeric value.");
            return;
        }

        if (!authorExists(authorId)) {
            System.out.println("Author not found. Add the author first or choose a valid author ID.");
            return;
        }

        String sql = "INSERT INTO books (title, author_id, genre, base_price) VALUES (?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, title);
            stmt.setInt(2, authorId);
            stmt.setString(3, genre.isBlank() ? null : genre);
            stmt.setDouble(4, basePrice);
            stmt.executeUpdate();

            System.out.println("Book added successfully.");

        } catch (SQLException e) {
            System.out.println("Error adding book: " + e.getMessage());
        }
    }

    private static void listBooks() {
        String sql = """
                    SELECT b.id, b.title, a.name AS author_name, b.genre, b.base_price
                    FROM books b
                    JOIN authors a ON b.author_id = a.id
                    ORDER BY b.id
                """;

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {

            System.out.println("\nBooks:");

            boolean hasResults = false;

            while (rs.next()) {
                hasResults = true;

                int id = rs.getInt("id");
                String title = rs.getString("title");
                String authorName = rs.getString("author_name");
                String genre = rs.getString("genre");
                double basePrice = rs.getDouble("base_price");

                System.out.println(
                        id + " - " + title +
                                " | Author: " + authorName +
                                " | Genre: " + (genre != null ? genre : "N/A") +
                                " | Base price: " + basePrice);
            }

            if (!hasResults) {
                System.out.println("No books available yet.");
            }

        } catch (SQLException e) {
            System.out.println("Error listing books: " + e.getMessage());
        }
    }

    private static boolean authorExists(int authorId) {
        String sql = "SELECT 1 FROM authors WHERE id = ?";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setInt(1, authorId);

            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }

        } catch (SQLException e) {
            System.out.println("Error validating author: " + e.getMessage());
            return false;
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

    private static void listRevenuePerBook() {
        String sql = """
                    SELECT b.id, b.title, a.name AS author_name, bs.revenue
                    FROM book_statistics bs
                    JOIN books b ON bs.book_id = b.id
                    JOIN authors a ON b.author_id = a.id
                    ORDER BY b.id
                """;

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            System.out.println("\nRevenue per book:");
            boolean hasResults = false;

            while (rs.next()) {
                hasResults = true;
                int id = rs.getInt("id");
                String title = rs.getString("title");
                String author = rs.getString("author_name");
                double revenue = rs.getDouble("revenue");

                System.out.println(id + " - " + title + " | Author: " + author + " | Revenue: " + revenue);
            }

            if (!hasResults) {
                System.out.println("No revenue statistics available yet.");
            }

        } catch (SQLException e) {
            System.out.println("Error listing revenue per book: " + e.getMessage());
        }
    }

    private static void listExpensesPerBook() {
        String sql = """
                    SELECT b.id, b.title, a.name AS author_name, bs.expenses
                    FROM book_statistics bs
                    JOIN books b ON bs.book_id = b.id
                    JOIN authors a ON b.author_id = a.id
                    ORDER BY b.id
                """;

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            System.out.println("\nExpenses per book:");
            boolean hasResults = false;

            while (rs.next()) {
                hasResults = true;
                int id = rs.getInt("id");
                String title = rs.getString("title");
                String author = rs.getString("author_name");
                double expenses = rs.getDouble("expenses");

                System.out.println(id + " - " + title + " | Author: " + author + " | Expenses: " + expenses);
            }

            if (!hasResults) {
                System.out.println("No expenses statistics available yet.");
            }

        } catch (SQLException e) {
            System.out.println("Error listing expenses per book: " + e.getMessage());
        }
    }

    private static void listProfitPerBook() {
        String sql = """
                    SELECT b.id, b.title, a.name AS author_name, bs.profit
                    FROM book_statistics bs
                    JOIN books b ON bs.book_id = b.id
                    JOIN authors a ON b.author_id = a.id
                    ORDER BY b.id
                """;

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            System.out.println("\nProfit per book:");
            boolean hasResults = false;

            while (rs.next()) {
                hasResults = true;
                int id = rs.getInt("id");
                String title = rs.getString("title");
                String author = rs.getString("author_name");
                double profit = rs.getDouble("profit");

                System.out.println(id + " - " + title + " | Author: " + author + " | Profit: " + profit);
            }

            if (!hasResults) {
                System.out.println("No profit statistics available yet.");
            }

        } catch (SQLException e) {
            System.out.println("Error listing profit per book: " + e.getMessage());
        }
    }

    private static void listCurrentStockPerBook() {
        String sql = """
                    SELECT b.id, b.title, a.name AS author_name, bs.stock
                    FROM book_statistics bs
                    JOIN books b ON bs.book_id = b.id
                    JOIN authors a ON b.author_id = a.id
                    ORDER BY b.id
                """;

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            System.out.println("\nCurrent stock per book:");
            boolean hasResults = false;

            while (rs.next()) {
                hasResults = true;
                int id = rs.getInt("id");
                String title = rs.getString("title");
                String author = rs.getString("author_name");
                int stock = rs.getInt("stock");

                System.out.println(id + " - " + title + " | Author: " + author + " | Stock: " + stock);
            }

            if (!hasResults) {
                System.out.println("No stock statistics available yet.");
            }

        } catch (SQLException e) {
            System.out.println("Error listing current stock per book: " + e.getMessage());
        }
    }

    private static void listAverageSpentPerSalePerBook() {
        String sql = """
                    SELECT b.id,
                           b.title,
                           a.name AS author_name,
                           bs.sales_count,
                           CASE
                               WHEN bs.sales_count > 0 THEN ROUND((bs.revenue / bs.sales_count)::numeric, 2)
                               ELSE 0
                           END AS average_spent_per_sale
                    FROM book_statistics bs
                    JOIN books b ON bs.book_id = b.id
                    JOIN authors a ON b.author_id = a.id
                    ORDER BY b.id
                """;

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            System.out.println("\nAverage spent per sale per book:");
            boolean hasResults = false;

            while (rs.next()) {
                hasResults = true;
                int id = rs.getInt("id");
                String title = rs.getString("title");
                String author = rs.getString("author_name");
                int salesCount = rs.getInt("sales_count");
                double averageSpent = rs.getDouble("average_spent_per_sale");

                System.out.println(
                        id + " - " + title +
                                " | Author: " + author +
                                " | Sales: " + salesCount +
                                " | Avg spent/sale: " + averageSpent);
            }

            if (!hasResults) {
                System.out.println("No sale statistics available yet.");
            }

        } catch (SQLException e) {
            System.out.println("Error listing average spent per sale per book: " + e.getMessage());
        }
    }

    private static void listMostProfitableBook() {
        String sql = """
                    WITH max_profit AS (
                        SELECT MAX(profit) AS value
                        FROM book_statistics
                    )
                    SELECT b.id, b.title, a.name AS author_name, bs.profit
                    FROM book_statistics bs
                    JOIN books b ON bs.book_id = b.id
                    JOIN authors a ON b.author_id = a.id
                    JOIN max_profit mp ON bs.profit = mp.value
                    ORDER BY b.id
                """;

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            boolean hasResults = false;

            System.out.println("\nMost profitable book(s):");

            while (rs.next()) {
                hasResults = true;
                int id = rs.getInt("id");
                String title = rs.getString("title");
                String author = rs.getString("author_name");
                double profit = rs.getDouble("profit");

                System.out.println(id + " - " + title + " | Author: " + author + " | Profit: " + profit);
            }

            if (!hasResults) {
                System.out.println("\nNo profit statistics available yet.");
            }

        } catch (SQLException e) {
            System.out.println("Error listing most profitable book: " + e.getMessage());
        }
    }

}