package is.project3.events;

public class BookInfo {
    public int book_id;
    public String title;
    public double base_price;

    public BookInfo(int book_id, String title, double base_price) {
        this.book_id = book_id;
        this.title = title;
        this.base_price = base_price;
    }
}