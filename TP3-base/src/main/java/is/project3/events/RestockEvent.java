package is.project3.events;

public class RestockEvent {
    public int book_id;
    public String title;
    public int units;
    public double purchase_price;
    public long timestamp;

    public RestockEvent(int book_id, String title, int units, double purchase_price, long timestamp) {
        this.book_id = book_id;
        this.title = title;
        this.units = units;
        this.purchase_price = purchase_price;
        this.timestamp = timestamp;
    }
}