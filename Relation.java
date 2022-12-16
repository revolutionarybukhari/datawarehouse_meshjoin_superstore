
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import java.sql.*;

public class Relation {
	private int total_relations=50;
	private int buffer_relations;
	private int relation_per_buffer;
	private Connection connection;
	private int current_count;
	private int start_pointer;
	public Relation(int buffer_relations,Connection conn)
	{
		this.buffer_relations=buffer_relations;
		this.connection=conn;
		this.current_count=0;
		this.start_pointer=0;
		this.relation_per_buffer=50/this.buffer_relations;
	}
	public ResultSet next() throws SQLException
	{
		
		PreparedStatement preparedStatement=connection.prepareStatement("select * from CUSTOMERS Limit ?,?");
		if (current_count == buffer_relations)
			{
				this.current_count=0;
				this.start_pointer=0;
			}
		preparedStatement.setInt(1, start_pointer);
        preparedStatement.setInt(2, relation_per_buffer);
		
		ResultSet data=preparedStatement.executeQuery() ;
		this.current_count+=1;
		this.start_pointer+=this.relation_per_buffer;
		
		return data;
	}
	public int getCurrentCount()
	{
		return this.current_count;
	}

}
