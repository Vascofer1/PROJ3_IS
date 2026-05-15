package is.project3.events;

public class SaleEvent {
    public int book_id;
    public String title;
    public int user_id;
    public String user_name;
    public int units;
    public double unit_sale_price;
    public double unit_purchase_price;
    public double total_price;
    public double profit;
    public long timestamp;

    public SaleEvent(
            int book_id,
            String title,
            int user_id,
            String user_name,
            int units,
            double unit_sale_price,
            double unit_purchase_price,
            double total_price,
            double profit,
            long timestamp) {
        this.book_id = book_id;
        this.title = title;
        this.user_id = user_id;
        this.user_name = user_name;
        this.units = units;
        this.unit_sale_price = unit_sale_price;
        this.unit_purchase_price = unit_purchase_price;
        this.total_price = total_price;
        this.profit = profit;
        this.timestamp = timestamp;
    }
}
