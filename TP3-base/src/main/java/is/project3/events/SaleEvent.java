package is.project3.events;

public class SaleEvent {
    public int book_id;
    public String title;
    public int units;
    public double unit_sale_price;
    public long timestamp;
    public double total_price;

    public SaleEvent(int book_id, String title, int units, double unit_sale_price, double total_price, long timestamp) {
        this.book_id = book_id;
        this.title = title;
        this.units = units;
        this.unit_sale_price = unit_sale_price;
        this.total_price = total_price;
        this.timestamp = timestamp;
    }
}