import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Calendar;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import java.io.IOException;
import java.sql.*;
public class MeshJoin {
	private Connection conn;
	private Connection dwh;
	private Relation_d masterdata;
	private Relation md1;
	private Stream transactions;
	private ArrayBlockingQueue<List<tuple>> stream_queue;
	private int num_of_relations; //same for queue size
	private MultiValuedMap<String,tuple> map = new ArrayListValuedHashMap<>();
	private MultiValuedMap<String,tuple> map2 = new ArrayListValuedHashMap<>();
	private int stream_size;
	private int off;
	private int counter=0;
	public MeshJoin(Connection conn,Connection dwh,int num_of_iterations, int stream_size)
	{
		this.conn=conn;
		this.stream_size=stream_size;
		this.num_of_relations=num_of_iterations;
		this.masterdata=new Relation_d(this.num_of_relations,conn);
		this.md1=new Relation((this.num_of_relations)/2,conn);
		this.transactions=new Stream(this.stream_size,conn);
		this.stream_queue=new ArrayBlockingQueue<List<tuple>>(num_of_iterations);
		this.off=1;
		this.dwh=dwh;
		
	}
	public void ProcessStream() throws SQLException
	{
		List<tuple> snew=this.transactions.next();
//		ResultSet stream=this.transactions.next();
		
		
		if(this.stream_queue.size()==this.num_of_relations || snew==null)
		{
			
			List<tuple> sfinal=stream_queue.poll();
			
			for(int i=0;i<sfinal.size();i++)
			{
				tuple temp=sfinal.get(i);
				//Mapping through prodID and custID seperately into the hashtable
				map.removeMapping(temp.getProductID(), temp);
				map.removeMapping(temp.getCustomerID(), temp);

			}		
			
		}
		
		if (snew == null) {
			if(stream_queue.isEmpty())
				this.off=0;
			return;
		}
		
		for(int i=0;i<snew.size();i++)
		{
			tuple temp=snew.get(i);
			map.put(temp.getProductID(), temp);
			map.put(temp.getCustomerID(), temp);

		}
		
		
		this.stream_queue.add(snew);
		
	}
		
	private  String day(Calendar calendar)
	{
		if(calendar.get(calendar.DAY_OF_WEEK) ==  calendar.MONDAY)
			return "Monday";
		if(calendar.get(calendar.DAY_OF_WEEK) ==  calendar.TUESDAY)
			return "Tuesday";
		if(calendar.get(calendar.DAY_OF_WEEK) ==  calendar.WEDNESDAY)
			return "Wednesday";
		if(calendar.get(calendar.DAY_OF_WEEK) ==  calendar.THURSDAY)
			return "Thursday";
		if(calendar.get(calendar.DAY_OF_WEEK) ==  calendar.FRIDAY)
			return "Friday";
		if(calendar.get(calendar.DAY_OF_WEEK) ==  calendar.SATURDAY)
			return "Saturday";
		if(calendar.get(calendar.DAY_OF_WEEK) ==  calendar.SUNDAY)
			return "Sunday";
		
		return null;
		
				
	}
		

	
	public String process_time(java.util.Date date , String time_id ) throws SQLException
	{
		
		int months[]={1,2,3,4,5,6,7,8,9,10,11,12};
		Date temp=(Date) date;
		String id=time_id;
		try {
			PreparedStatement dwh_query=dwh.prepareStatement("select * from Time_Table where DATE = ?");
			dwh_query.setDate(1, temp);
			ResultSet time_table=dwh_query.executeQuery();
			time_table.next();
			String time_d=time_table.getString("TimeID");
			
			return time_d;
			
		}catch (Exception e)
		{
			
			 Calendar calendar = Calendar.getInstance();
	
			 calendar.setTime(temp);

			 int qtr=1;
			 int month_num=calendar.get(calendar.MONTH)+1;
			 if(month_num>9)
				 qtr=4;
			 else if(month_num>6)
				 qtr=3;
			 else if (month_num>3)
				 qtr=2;
		     else if (month_num>1)
		    	 qtr=1;
			 
			 
			 PreparedStatement dwh_query=dwh.prepareStatement("insert into Time_Table(TimeID,day,qtr,month,year,DATE) values (?,?,?,?,?,?)");
			 dwh_query.setString(1, id);
			 dwh_query.setString(2, day(calendar));
			 dwh_query.setInt(3, qtr);
			 dwh_query.setInt(4, months[calendar.get(calendar.MONTH)]);
			 dwh_query.setInt(5, calendar.get(calendar.YEAR));
			 dwh_query.setDate(6, temp);
		
			 dwh_query.executeUpdate();
			
		}
		
//		PreparedStatement dwh_query=dwh.prepareStatement("select * from Time_Table where DATE = ?");
//		dwh_query.setDate(1, temp);
//		ResultSet time_table1=dwh_query.executeQuery();
//		time_table1.next();
//		String time_id1=time_id;
//		
		
		return time_id;
		
		
		
	}
	
	public void process_products(String product_id,String product_name,Double Price) throws SQLException
	{
		
		try {
			PreparedStatement dwh_query=dwh.prepareStatement("select * from Product_Table where ProductID = ?");
			dwh_query.setString(1, product_id);
			ResultSet product_table=dwh_query.executeQuery();
			product_table.next();
			product_table.getString("ProductName");
			product_table.getDouble("PRICE");
			
		}catch (Exception e)
		{
			 PreparedStatement dwh_query=dwh.prepareStatement("insert into Product_Table(ProductID,ProductName,PRICE) values (?,?,?)");
			 dwh_query.setString(1, product_id);
			 dwh_query.setString(2,product_name);
			 dwh_query.setDouble(3,Price);

			 dwh_query.executeUpdate();

		} 
	}
	public void process_suppliers(String supplier_id,String supplier_name) throws SQLException
	{
		try {
			PreparedStatement dwh_query=dwh.prepareStatement("select * from Supplier_Table where SupplierID = ?");
			dwh_query.setString(1, supplier_id);
			ResultSet supplier_table=dwh_query.executeQuery();
			supplier_table.next();
			supplier_table.getString("SupplierName");
			
		}catch (Exception e)
		{
			 
			 PreparedStatement dwh_query=dwh.prepareStatement("insert into Supplier_Table(SupplierID,SupplierName) values (?,?)");
			 dwh_query.setString(1, supplier_id);
			 dwh_query.setString(2,supplier_name);
			 
			 dwh_query.executeUpdate();

		} 

	}
	
	public void process_store(String store_id,String store_name) throws SQLException
	{
		
		try {
			PreparedStatement dwh_query=dwh.prepareStatement("select * from Store_Table where StoreID = ?");
			dwh_query.setString(1, store_id);
			ResultSet store_table=dwh_query.executeQuery();
			store_table.next();
			store_table.getString("StoreName");
			
		}catch (Exception e)
		{
			 
			 PreparedStatement dwh_query=dwh.prepareStatement("insert into Store_Table(StoreID,StoreName) values (?,?)");
			 dwh_query.setString(1, store_id);
			 dwh_query.setString(2,store_name);
			 
			 dwh_query.executeUpdate();

			
		} 

	}
	public void process_customer(String customer_id,String customer_name ) throws SQLException
	{
		
		try {
			PreparedStatement dwh_query=dwh.prepareStatement("select * from Customer_Table where CustomerID = ?");
			dwh_query.setString(1, customer_id);
			ResultSet customer_table=dwh_query.executeQuery();
			customer_table.next();
			customer_table.getString("CustomerName");
			
		}catch (Exception e)
		{
			 
			 PreparedStatement dwh_query=dwh.prepareStatement("insert into Customer_Table(CustomerID,CustomerName) values (?,?)");
			 dwh_query.setString(1, customer_id);
			 dwh_query.setString(2,customer_name);
			 
			 dwh_query.executeUpdate();

		} 

	}
	
	
	
	public void output(tuple tra,ResultSet md,ResultSet md1) throws SQLException
	{
		
		String customer_id=tra.getCustomerID();
		String product_id=md.getString("PRODUCT_ID");
		String supplier_id=md.getString("SUPPLIER_ID");
		String time_id=process_time(tra.getDate(),tra.getT_id());
		String store_id=tra.getStoreID();
		Integer tra_id=tra.getID();
		Integer Quanity=tra.getQuantity();
		
		
		process_store(tra.getStoreID(),tra.getStoreName());
		process_products(md.getString("PRODUCT_ID"),md.getString("PRODUCT_NAME"),md.getDouble("PRICE"));
		process_suppliers(md.getString("SUPPLIER_ID"),md.getString("SUPPLIER_NAME"));
		process_customer(tra.getCustomerID(),md1.getString("CUSTOMER_NAME"));
		
		
		try {
			
			PreparedStatement dwh_query=dwh.prepareStatement("select * from fact_table where TRANSACTION_ID=? and CustomerID = ? and SupplierID = ?"
					+ " and ProductID = ? and StoreID = ? and TimeID = ?");
			dwh_query.setDouble(1, tra_id);
			dwh_query.setString(2, customer_id);
			dwh_query.setString(3, supplier_id);
			dwh_query.setString(4,product_id);
			dwh_query.setString(5,store_id);
			dwh_query.setString(6,time_id);
			
			
			ResultSet fact_table=dwh_query.executeQuery();
			fact_table.next();
			float sales=fact_table.getFloat("sales");
			
		
			
//			int quantity =((tra.getQuantity()));
			sales=(sales+(md.getFloat("PRICE")*tra.getQuantity()));
			
			PreparedStatement update=dwh.prepareStatement("UPDATE fact_table SET sales=? where TRANSACTION_ID=? and CustomerID = ? and SupplierID = ?"
					+ " and ProductID = ? and StoreID = ? and TimeID = ?");
			
			update.setFloat(1, sales);
			dwh_query.setDouble(2, tra_id);
			update.setString(3, customer_id);
			update.setString(4, supplier_id);
			update.setString(5,product_id);
			update.setString(6,store_id);
			update.setString(7,time_id);
			update.executeUpdate();
			System.out.println(tra_id+" "+customer_id+" "+supplier_id+" "+product_id+" "+store_id+" "+time_id+" "+sales);

			
		}catch(Exception e)
		
		 {
		
			float sales=tra.getQuantity()*md.getFloat("PRICE");
//			int quanity=tra.getQuantity();
			
			PreparedStatement dwh_query=dwh.prepareStatement("Insert into fact_table(TRANSACTION_ID,CustomerID,SupplierID,ProductID,StoreID,TimeID,Sales,Quantity) values(?,?,?,?,?,?,?,?)");
			dwh_query.setDouble(1, tra_id);
			dwh_query.setString(2, customer_id);
			dwh_query.setString(3, supplier_id);
			dwh_query.setString(4,product_id);
			dwh_query.setString(5,store_id);
			dwh_query.setString(6,time_id);
			dwh_query.setFloat(7, sales);
			dwh_query.setInt(8, Quanity);

			
			dwh_query.executeUpdate();
			System.out.println(tra_id+" "+customer_id+" "+supplier_id+" "+product_id+" "+store_id+" "+time_id+" "+sales);
		}
		
	}
	
	
	public void join() throws SQLException
	{
		int counter=0;
		while(true)
		{
			ProcessStream();
			
			if(this.off == 0)
			{
				System.out.println("Stream Ended");
				System.out.println(counter);
				return;
				
			}
			ResultSet relation=masterdata.next();
			
			ResultSet relation2=md1.next();

//			ResultSet temp=relation2;
			while(relation.next())
			{
				
				relation2.next();
//				while(relation2.next()) {
//					Collection<tuple> matched=map.get(relation2.getString("CUSTOMER_ID")+relation.getString("PRODUCT_ID"));//if i remove this relation2 i get 10,000 transactions read but if I add it like this only 195 are read 
					if(map.get(relation.getString("PRODUCT_ID"))!=null)
					{
						if(map.get(relation2.getString("CUSTOMER_ID"))!=null)
						{
							Collection<tuple> matched=map.get(relation.getString("PRODUCT_ID"));
							for (tuple s:matched)
							{
//							    System.out.println(relation2.getString("CUSTOMER_ID")+relation.getString("PRODUCT_ID")+"$$$$");
								output(s,relation,relation2);
								counter++;
							}
						}
					}
					
//				}
//				relation2=temp;
				
			}
			relation.close();
			relation2.close();
			
		}
		
		
		
		
	}
	

}
