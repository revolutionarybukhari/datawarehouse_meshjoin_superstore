import java.util.ArrayList;
import java.sql.*;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Calendar;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.map.MultiValueMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Scanner;
public class Main {



	public static void main(String[] args) throws SQLException, InterruptedException, IOException {
		
//		 String myDriver = "com.mysql.cj.jdbc.Driver";
//	      String myUrl = "jdbc:mysql://localhost:3306/db";
//	      Class.forName(myDriver);
//	      Connection conn = DriverManager.getConnection(myUrl, "root", "password");
//		
	    Scanner myObj = new Scanner(System.in);
	    System.out.println("Enter username:");
	    String userName = myObj.nextLine();  
	    System.out.println("Username entered: " + userName);
		
		
	    System.out.println("Enter password for the DBMS");
	    String password = myObj.nextLine();  
	    System.out.println("Password entered: " + password);
	    
	    System.out.println("Enter name of schema(MD):");
	    String masterdata = myObj.nextLine();  
	    System.out.println("MD schema name: " + masterdata);
		
    	 Connection connection,dwh;
		 connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/"+masterdata, userName, password);
		 dwh= DriverManager.getConnection("jdbc:mysql://localhost:3306/dw_project", userName, password);
		 
		 System.out.println("Connected to DBMS");
		 System.out.println("Executing MESHJOIN now ;)");

		 MeshJoin temp =new MeshJoin(connection,dwh,10,50);//#of iterations + stream size
		 temp.join();
		 

    }


}