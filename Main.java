import com.mysql.cj.jdbc.MysqlDataSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/* Application that simulates a storefront which needs to communicate with a MySQL database.
 * The database has 2 tables: Order (parent) and Order details (child).
 *
 * This particular variation features the ability to input data from a .csv file and output it to JSON string.
 * This is in conjunction with use of CallableStatments and DDL statements to create a new schema + tables.
 *
 * As an additional challenge, some of the input orders had their dates purposefully formatted incorrectly.
 * The code needed extra design to parse and decide if dates were properly formatted.
 * If they weren't, the data following it was ignored.
 *
 * Alongside an IDE, MySQL workbench was used to set up the initial table and verify operation results.
 * Sample orders that were used for testing were stored and accessed in a .csv file
 * */

//pojo but needed for the map
record OrderDetail(String itemName, int itemQuantity) {}
public class Main
{
	private static final String USE_SCHEMA = "USE storefront";
	private static final int MYSQL_DB_NOT_FOUND = 1049; //error code for MySQL

	public static void main(String[] args)
	{
		var dataSource = new MysqlDataSource();
		dataSource.setServerName("localhost");
		dataSource.setPort(3306);
		dataSource.setUser(System.getenv("MYSQLUSER"));
		dataSource.setPassword(System.getenv("MYSQLPASS"));

		try(Connection conn = dataSource.getConnection())
		{
			DatabaseMetaData metaData = conn.getMetaData();//getting this so we can read any error codes
			System.out.println("getSQLStateType: " + metaData.getSQLStateType()); //drivers/vendors will have different codes
			System.out.println("--------------------------------------");
			if(!checkSchema(conn))
			{
				System.out.println("storefront schema does not exist");
				setUpSchema(conn);
				System.out.println("storefront created, exiting program");
				System.out.println("re-run to begin operations");
				return;
			}

			//parameters 1 and 2 are inputs provided by this code
			//1 is a timeStamp that will be formatted from a String
			//2 is the JSON string to be passed

			//parameters 3 and 4 are return values
			//3 is the orderID returned as an int
			//4 is the number of inserted records, returned as an int
			CallableStatement cs = conn.prepareCall("CALL storefront.addOrder(?,?,?,?)");

			//get all lines from file
			List<String> records = null;
			try
			{
				records = Files.readAllLines(Path.of("src\\Orders.csv"));
			}
			catch(IOException ioe)
			{
				throw new RuntimeException(ioe);
			}

			//this map will allow the number of inserted records to be verified against what the DB returns
			Map<String, List<OrderDetail>> orderMap = new HashMap<>();
			String currentOrderDate = null; //similar to another program on a different repo of mine
			//going to store the current order being looked at

			//at this point, the entire file has been read and is stored in List records
			//go through each line one by one
			for(String record : records)
			{
				String[] columns = record.split(",");

				//starting a new order, there may be orderDetails from the last being held
				if(columns[0].equalsIgnoreCase("order"))
				{
					currentOrderDate = columns[1];
					//no SQL changes are done yet, new order ends the last one

					//tries to insert the new date being looked at as a new key in the map
					//will return null if that key was already in the map
					//in other words: the desired outcome is that newOrderValidation is null every time
					List<OrderDetail> newOrderValidation = orderMap.putIfAbsent(currentOrderDate, new ArrayList<>());
					if(newOrderValidation != null) //if this is NOT null, means that date was already mapped to something
					{
						//print debug info, ideally this conditional block never executes
						System.out.println("currentOrderDate was already mapped to something");
						System.out.println("record: " + record);
						System.out.println("lastDate: " + currentOrderDate);
						orderMap.forEach((k, v) ->
						{
							System.out.println("k: " + k);
							System.out.println("v: " + v);
						});
						return;
					}
				}
				//begin adding new items as orderDetails
				else if(columns[0].equalsIgnoreCase("item") && currentOrderDate != null)
				{
					OrderDetail inputOrder = null;

					//the arrayList was already initialized when the order started
					//get a copy of the list from the map, preserving the integrity of the original
					ArrayList<OrderDetail> workingList = new ArrayList<>(orderMap.get(currentOrderDate));
					try
					{
						//first arg is item description as a String, second arg is quantity as an int
						inputOrder = new OrderDetail(columns[2], Integer.parseInt(columns[1]));
					}
					catch(Exception e)
					{
						System.out.println("error parsing new order details");
						System.out.println("record: " + record);
						System.out.println("lastDate: " + currentOrderDate);
						System.out.printf("c0: %s - c1: %s - c2: %s%n", columns[0], columns[1], columns[2]);
					}
					//this part of the code can still be reached if there is (somehow) a badly formatted item
					//so a check is mandatory to make sure the inputOrder to be added isn't null
					if(inputOrder != null)
					{
						workingList.add(inputOrder);
					}
					orderMap.put(currentOrderDate, workingList); //replace the list with the new one just updated
				}
				else
				{
					//this block should never happen but adding this just in case
					System.out.println("weird case parsing CSV");
					System.out.println("record: " + record);
					System.out.println("lastDate: " + currentOrderDate);
				}
			}

			//using uuuu for the year with strict resolve style, so incorrect dates will also fail
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
					.withResolverStyle(ResolverStyle.STRICT);

			//at this point, the entire file has been read and put into the orderMap
			//now attempts at adding items to the DB will begin
			System.out.println("beginning actual SQL stuff");
			for(String k : orderMap.keySet())
			{
				System.out.println("-------------");
				System.out.println(k); //purely debug/info
				try
				{
					LocalDateTime workingTime = LocalDateTime.parse(k, formatter);
					System.out.println("parsed successfully: " + workingTime);
					//if code reaches here, that means the time was good
					//else, it will fail now
					Timestamp timestamp = Timestamp.valueOf(workingTime);

					//parameters 1 and 2 are inputs provided by this code
					//1 is a timeStamp that will be formatted from a String
					//2 is the JSON string to be passed

					//parameters 3 and 4 are return values
					//3 is the orderID returned as an int
					//4 is the number of inserted records, returned as an int
					cs.setTimestamp(1, timestamp);

					//call to method MapToJSON(); will be explained there
					//tl;dr takes a list of orderDetails and returns a String formatted as JSON for MySQL
					String jsonParameter = MapToJSON(orderMap.get(k));
					System.out.println("json being imported: "); //debug/info
					System.out.println(jsonParameter); //debug/info
					cs.setString(2, jsonParameter);
					cs.registerOutParameter(3, Types.INTEGER);
					cs.registerOutParameter(4, Types.INTEGER);
					cs.execute();
					System.out.println("orderID: " + cs.getInt(3));
					System.out.println("insertedRecords: " + cs.getInt(4));
				}
				catch(Exception e)
				{
					System.out.println("error parsing date, probably bad date");
					System.out.println(e);
				}

			}

		}
		catch(SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	//method that receives a list of OrderDetail and outputs a String formatted for JSON
	//this is necessary because MySQL does not support Arrays
	//the output from this method will be used as a parameter in a CallableStatement
	private static String MapToJSON(List<OrderDetail> inputOrderList)
	{
		if(inputOrderList == null || inputOrderList.isEmpty())
		{
			//safeguard conditional that hopefully never executes
			return null;
		}
		StringBuilder sb = new StringBuilder("[\n");
		for(OrderDetail o : inputOrderList)
		{
			//this weird formatting is how JSON is. can be validated via an online checker
			String listToString = "{\"itemDescription\":\"%s\", \"qty\":%d},%n"
					.formatted(o.itemName(), o.itemQuantity());
			sb.append(listToString); //append it to the end, this will happen for every item
		}
		int trimEnd = sb.length() - 3; //index for trimming the last 3 characters at the end of the stringbuilder
		//there will be 3 extra chars that break JSON validation and visually don't look good
		sb = new StringBuilder(sb.substring(0, trimEnd));
		sb.append("\n]");
		return sb.toString();
	}

	//initializes the DB with cascade delete, so deleting a parent will delete children
	private static void setUpSchema(Connection conn) throws SQLException
	{
		//strings to initialize the storefront schema, order table
		String createSchema = "CREATE SCHEMA storefront";
		String createOrder = """
    			CREATE TABLE storefront.order(
    			order_id int NOT NULL AUTO_INCREMENT,
    			order_date DATETIME NOT NULL,
    			PRIMARY KEY (order_id)
				)""";

		//sets up order_detail table with parent-child relationship between the 2 tables (cascade deletion)
		//when the parent is deleted, they are treated as a single unit
		//in plain english: when an order is deleted, details related to that order are also deleted
		String createOrderDetails = """
   				CREATE TABLE storefront.order_details (
   				order_detail_id int NOT NULL AUTO_INCREMENT,
   				quantity int NOT NULL,
   				item_description text,
   				order_id int DEFAULT NULL,
   				PRIMARY KEY (order_detail_id),
   				KEY FK_ORDERID (order_id),
   				CONSTRAINT FK_ORDERID FOREIGN KEY (order_id)
   				REFERENCES storefront.order (order_id) ON DELETE CASCADE
   				) """;

		//DDL operations to create the schema and tables as written in the above strings
		try(Statement statement = conn.createStatement())
		{
			//DDL operations don't typically use PreparedStatement
			//usually only for DML operations, where statements are executed multiple times
			System.out.println("Creating storefront Database");
			statement.execute(createSchema);
			if(checkSchema(conn))
			{
				statement.execute(createOrder);
				System.out.println("Successfully Created Order");
				statement.execute(createOrderDetails);
				System.out.println("Successfully Created Order Details");
			}
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
	}

	//helper method to check if the schema already exists
	//returns true by default, unless there's an error thrown, the vendor is MySQL and
	//the error code matches MYSQL_DB_NOT_FOUND (1049), since error codes can vary from vendor to vendor
	private static boolean checkSchema(Connection conn) throws SQLException
	{
		try(Statement statement = conn.createStatement())
		{
			statement.execute(USE_SCHEMA);
		}
		catch(SQLException e)
		{
			e.printStackTrace();
			System.err.println("SQLState: " + e.getSQLState());
			System.err.println("Error Code: " + e.getErrorCode());
			System.err.println("Message: " + e.getMessage());

			if(conn.getMetaData().getDatabaseProductName().equals("MySQL")
					&& e.getErrorCode() == MYSQL_DB_NOT_FOUND)
			{
				return false;
			}
			else { throw e; }
		}
		return true;
	}
}