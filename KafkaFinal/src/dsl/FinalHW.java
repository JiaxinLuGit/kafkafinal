package dsl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import models.Item;
import models.Order;
import models.User;
import serdes.SerdesFactory;




public class FinalHW {

	public static void main(String[] args) throws IOException, InterruptedException {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-purchase-analysis2");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka0:19092");
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "zookeeper0:12181/kafka");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimestampExtractor.class);

		KStreamBuilder streamBuilder = new KStreamBuilder();
		KStream<String, Order> orderStream = streamBuilder.stream(Serdes.String(), SerdesFactory.serdFrom(Order.class), "orders");
		KTable<String, User> userTable = streamBuilder.table(Serdes.String(), SerdesFactory.serdFrom(User.class), "users", "users-state-store");
		KTable<String, Item> itemTable = streamBuilder.table(Serdes.String(), SerdesFactory.serdFrom(Item.class), "items", "items-state-store");

		//user和order的组合表
		KTable<Windowed<String>, String> kTable = orderStream
				.leftJoin(userTable, (Order order, User user) -> OrderUser.fromOrderUser(order, user), Serdes.String(), SerdesFactory.serdFrom(Order.class))
				//筛选出年龄在18到35岁之间的userorder
				.filter((String userName, OrderUser orderUser) -> orderUser.age>18 && orderUser.age<35)
				//把商品名字符串作为键，订单和用户的连接流作为值
				.map((String userName, OrderUser orderUser) -> new KeyValue<String, OrderUser>(orderUser.itemName, orderUser))
				.through(Serdes.String(), SerdesFactory.serdFrom(OrderUser.class), (String key, OrderUser orderUser, int numPartitions) -> (orderUser.getItemName().hashCode() & 0x7FFFFFFF) % numPartitions, "orderuser-repartition-by-item")
				//订单和用户的连接流与商品表连接形成订单用户商品表，其中OrderUserItem类除了有原来1个流两个表中的信息外，还添加了一个销售额的属性，在创建对象时用quantity*itemPrice计算
				.leftJoin(itemTable, (OrderUser orderUser, Item item) -> OrderUserItem.fromOrderUser(orderUser, item), Serdes.String(), SerdesFactory.serdFrom(OrderUser.class))
				//把商品类别作为新的键，OrderUserItem作为值做1次map
				.map((String item, OrderUserItem orderUserItem) ->new KeyValue<String, OrderUserItem>(orderUserItem.itemType, orderUserItem))
				//用商品分类分组
				.groupByKey()
				//求出每种商品分类销售额前10的订单
				.aggregate(
						//初始值，1个空的top10容器对象
						()->new Top10(), 
						//聚合用于排序求前10
						(category,osi,top)->{
							//把新来的OrderUserItem放到容器列表的第11个位置
							top.list[11]=osi;
							//对列表按照销售额进行排序
							Arrays.sort(
									top.list,
									(a,b)->{
										return Double.compare(b.orderCost,a.orderCost);
									}
									);
							//把第11个位置清空
							top.list[11]=null;
							//返回容器对象
							return top;
						}, 
						//每5秒输出一次，每次输出1小时的结果
						TimeWindows.of(6000*60).advanceBy(5000), 
						//容器类的Serde
						SerdesFactory.serdFrom(Top10.class), 
						"aggreStroe")
				
				//把各个类别求出的10订单映射为字符串，一个订单一行
				.mapValues(top->{
					//用于拼接10个订单
					StringBuffer sb=new StringBuffer();
					//对10个排名的订单进行拼接
					for(int i=0;i<10;i++){
						//窗口结束时间
						long end=top.list[i].getTransactionDate();
						//窗口开始时间
						long start=end-3600;
						//窗口字符串
					    String window=end+" "+start;
					    //拼接该订单其他属性，用i+1作为排名
						String tmp=window+top.list[i].getItemType()+" "+top.list[i].getItemName()+" "+top.list[i].getQuantity()+" "+top.list[i].getItemPrice()+" "+top.list[i].getOrderCost()+" "+(i+1);
						//把该订单信息拼入StringBuffer
						sb.append(tmp);
						//添加1个换行符
						sb.append("\n");
					}
					return sb.toString();
				});
				kTable
				.toStream()
				.to("top10 set");
		KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder, props);
		kafkaStreams.cleanUp();
		kafkaStreams.start();

		System.in.read();
		kafkaStreams.close();
		kafkaStreams.cleanUp();
	}
	
	//TOP10容器类
	public static class Top10{
		//用于盛放和排序的OrderUserItem列表
		public OrderUserItem[] list=new OrderUserItem[11];
		public Top10(){};
	}

	public static class OrderUser {
		private String userName;
		private String itemName;
		private long transactionDate;
		private int quantity;
		private String userAddress;
		private String gender;
		private int age;

		public String getUserName() {
			return userName;
		}

		public void setUserName(String userName) {
			this.userName = userName;
		}

		public String getItemName() {
			return itemName;
		}

		public void setItemName(String itemName) {
			this.itemName = itemName;
		}

		public long getTransactionDate() {
			return transactionDate;
		}

		public void setTransactionDate(long transactionDate) {
			this.transactionDate = transactionDate;
		}

		public int getQuantity() {
			return quantity;
		}

		public void setQuantity(int quantity) {
			this.quantity = quantity;
		}

		public String getUserAddress() {
			return userAddress;
		}

		public void setUserAddress(String userAddress) {
			this.userAddress = userAddress;
		}

		public String getGender() {
			return gender;
		}

		public void setGender(String gender) {
			this.gender = gender;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public static OrderUser fromOrder(Order order) {
			OrderUser orderUser = new OrderUser();
			if(order == null) {
				return orderUser;
			}
			orderUser.userName = order.getUserName();
			orderUser.itemName = order.getItemName();
			orderUser.transactionDate = order.getTransactionDate();
			orderUser.quantity = order.getQuantity();
			return orderUser;
		}

		public static OrderUser fromOrderUser(Order order, User user) {
			OrderUser orderUser = fromOrder(order);
			if(user == null) {
				return orderUser;
			}
			orderUser.gender = user.getGender();
			orderUser.age = user.getAge();
			orderUser.userAddress = user.getAddress();
			return orderUser;
		}
	}

	public static class OrderUserItem {
		private String userName;
		private String itemName;
		private long transactionDate;
		private int quantity;
		private String userAddress;
		private String gender;
		private int age;
		private String itemAddress;
		private String itemType;
		private double itemPrice;
		private double orderCost;

		public double getOrderCost() {
			return orderCost;
		}

		public void setOrderCost(double orderCost) {
			this.orderCost = this.itemPrice*this.quantity;
		}

		public String getUserName() {
			return userName;
		}

		public void setUserName(String userName) {
			this.userName = userName;
		}

		public String getItemName() {
			return itemName;
		}

		public void setItemName(String itemName) {
			this.itemName = itemName;
		}

		public long getTransactionDate() {
			return transactionDate;
		}

		public void setTransactionDate(long transactionDate) {
			this.transactionDate = transactionDate;
		}

		public int getQuantity() {
			return quantity;
		}

		public void setQuantity(int quantity) {
			this.quantity = quantity;
		}

		public String getUserAddress() {
			return userAddress;
		}

		public void setUserAddress(String userAddress) {
			this.userAddress = userAddress;
		}

		public String getGender() {
			return gender;
		}

		public void setGender(String gender) {
			this.gender = gender;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public String getItemAddress() {
			return itemAddress;
		}

		public void setItemAddress(String itemAddress) {
			this.itemAddress = itemAddress;
		}

		public String getItemType() {
			return itemType;
		}

		public void setItemType(String itemType) {
			this.itemType = itemType;
		}

		public double getItemPrice() {
			return itemPrice;
		}

		public void setItemPrice(double itemPrice) {
			this.itemPrice = itemPrice;
		}

		public static OrderUserItem fromOrderUser(OrderUser orderUser) {
			OrderUserItem orderUserItem = new OrderUserItem();
			if(orderUser == null) {
				return orderUserItem;
			}
			orderUserItem.userName = orderUser.userName;
			orderUserItem.itemName = orderUser.itemName;
			orderUserItem.transactionDate = orderUser.transactionDate;
			orderUserItem.quantity = orderUser.quantity;
			orderUserItem.userAddress = orderUser.userAddress;
			orderUserItem.gender = orderUser.gender;
			orderUserItem.age = orderUser.age;
			return orderUserItem;
		}

		public static OrderUserItem fromOrderUser(OrderUser orderUser, Item item) {
			OrderUserItem orderUserItem = fromOrderUser(orderUser);
			if(item == null) {
				return orderUserItem;
			}
			orderUserItem.itemAddress = item.getAddress();
			orderUserItem.itemType = item.getType();
			orderUserItem.itemPrice = item.getPrice();
			//单个订单的销售额
			orderUserItem.orderCost=orderUserItem.getQuantity()*item.getPrice();
			return orderUserItem;
		}
	}

}
