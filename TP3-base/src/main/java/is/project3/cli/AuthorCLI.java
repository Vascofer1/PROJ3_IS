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
}