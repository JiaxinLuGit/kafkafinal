package models;

public class Item {
	private String itemName;
	private String address;
	private String category;
	private double price;

	public Item() {}
	
	public Item(String itemName, String address, String type, double price) {
		this.itemName = itemName;
		this.address = address;
		this.category = type;
		this.price = price;
	}
	
	public String getItemName() {
		return itemName;
	}

	public void setItemName(String itemName) {
		this.itemName = itemName;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getType() {
		return category;
	}

	public void setType(String type) {
		this.category = type;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

}
