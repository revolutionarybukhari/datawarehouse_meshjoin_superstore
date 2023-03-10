
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import java.sql.*;


public class Stream {
	
	private int stream_size;
	private int current_pointer;
	private int total_size=10000;
	private Connection connection;
	
	
	
	public Stream(int stream_size,Connection conn)
	{
		this.stream_size=stream_size;
		this.current_pointer=0;
		this.connection=conn;
	}
	
	public List<tuple> next() throws SQLException
	{

		
		PreparedStatement preparedStatement=connection.prepareStatement("select * from TRANSACTIONS Limit ?,?");
		preparedStatement.setInt(1,this.current_pointer);
        preparedStatement.setInt(2,(this.stream_size));
		
        ResultSet data=preparedStatement.executeQuery();
        
        if(!data.isBeforeFirst()) {//this helps check if the whole table has been read or not
        	return null;
        }
        
        this.current_pointer+=this.stream_size;
        
        List <tuple> st=new ArrayList<tuple>();
        while(data.next())
        	st.add(new tuple(data));
        
        data.close();
        return st;
	}
	
	
}
