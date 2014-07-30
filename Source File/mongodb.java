
/** 
 * The project is to extract data from relational database system (MySQL) and convert it into JSON format and insert them into
 * NoSQL Document Database(MongoDB).The values are retrieved from them and displayed as results
 * 
 * Author : ARAVIND KRISHNAKUMAR
 * 
 * Date Submitted : 04/29/2014 
 * 
 */


//Libraries imported to connect to MySQL and MongoDB Database 
//ArrayList and Hash Map are used to create a Key and multiple value pairs
//BasicDBObject is the API to insert the data into database

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;


class class1
{
	  private Connection connect = null;
	  
	  /**
	   * This module is used to connect to MongoDB and MySQL database
	   * 
	   * Connecting to MongoDB database using
	   * Database name : db3 and
	   * Collection name : department2
	   * 
	   * Connecting to MySQL database using
	   * Database name : db2 and
	   * Table name : department2
	   * 
	   */

	  public void readDataBase() throws Exception {
		  
		try {
			  /** Connect to MongoDB */
			  
			DB db=(new MongoClient("localhost",27017)).getDB("db3");
			  DBCollection collection = db.getCollection("department2");
		      
		      Class.forName("com.mysql.jdbc.Driver").newInstance();
		      
		      connect = DriverManager.getConnection("jdbc:mysql://localhost:3306/db2","root","$Ara9005"); 
		      
		      /** Connect to MySQL Database */
		      
		      System.out.println("->Connecting to a selected database...");
		      Statement stmt = connect.createStatement();
		      
		      /**Retrieve the records from MySQL*/
		      
		      String sql2 ="select dl.dnumber,dl.dlocation from dept_locations as dl";
		      ResultSet rs1 =stmt.executeQuery(sql2);
		      
		      //HashMap is used to create Key Value pair
		      HashMap <Integer, ArrayList<String>> map = new HashMap <Integer, ArrayList<String>>();
		      
		      int dnumber=0;
		      
		      /**
		       * "location" : [ "Bellaire" , "Houston" , "Sugarland"]}
		       * Since Location has multiple values we are creating a Key multiple Value Pair using HashMap data structure
		       */
		      		      
		      while(rs1.next())
		      {
		    	 dnumber=(rs1.getInt("dnumber"));
		    	 String dlocation=(rs1.getString("dlocation"));	
		    	 //Key and Value are sent to the module which creates a Key multiple Value Pair
		    	 addToMap(map,dnumber,dlocation); 
		      }
		      
		      String sql3="select d.dname,e.lname,e.dno from department as d ,employee as e where e.ssn=d.mgr_ssn";
		      ResultSet rs2 =stmt.executeQuery(sql3);
		     
		      
		      while(rs2.next())
		      {
		    	String dname=(rs2.getString("dname"));
		    	String lname=(rs2.getString("lname"));
		    	int dno=(rs2.getInt("dno"));	
		    	
		    	//Using the  Java MongoDB API to insert the data into a "document" 
		    	
		    	BasicDBObject inQuery = new BasicDBObject();
		    	inQuery.put("dname",dname);
		    	inQuery.put("lname",lname);
		    	inQuery.put("location",map.get(dno));
		    	System.out.println(inQuery);
		    	collection.insert(inQuery);
		      }  
		}
		catch (Exception e) 
			  {
		        throw e;
		      }
	}
	
	/**
	 * This module is used to store the records values in HashMap as a Key Value pairs   
	 * @param map
	 * @param key
	 * @param value
	 */
	  
	public void addToMap(HashMap <Integer, ArrayList<String>> map, Integer key, String value)
	{
		  if(!map.containsKey(key))
		  {
			  ArrayList<String> val=new ArrayList<String>();
			  val.add(value);
			  map.put(key, val);
		  }
		  else
		  {
			  ArrayList<String> al= (ArrayList)map.get(key);
			  al.add(value);
		  }
		}
}

class class2
{
	public void readDataBase() throws Exception 
	{   
		try {
			 /** Connect to MongoDB */
			
			  DB db=(new MongoClient("localhost",27017)).getDB("db3");
			  DBCollection collection = db.getCollection("employee2");
		      
		      Class.forName("com.mysql.jdbc.Driver").newInstance();
		     
		      Connection connect = DriverManager.getConnection("jdbc:mysql://localhost:3306/db2","root","$Ara9005");
		      
		      /** Connect to MySQL Database */
		      
		      System.out.println("->Connecting to a selected database...");
		      Statement stmt = connect.createStatement();
		      
		      /**Retrieve the records from MySQL*/
		      
		      String sql1="select p.pname,w.hours,w.essn from works_on as w,employee as e,project as p where e.ssn=w.essn and w.pno=p.pnumber";
		      ResultSet rs1 =stmt.executeQuery(sql1);
		      
		      //HashMap is used to create Key Value pair
		      HashMap <Integer, ArrayList<String>> map = new HashMap <Integer, ArrayList<String>>();
		      HashMap <Integer, ArrayList<Integer>> map3 = new HashMap <Integer, ArrayList<Integer>>();
		      
		      /**Since Location has multiple values we are creating a Key multiple Value Pair using HashMap data structure*/
		     
		      while(rs1.next())
		      {
		    	 int essn=(rs1.getInt("essn"));
		    	 int hours=(rs1.getInt("hours"));
		    	 String pname=(rs1.getString("pname"));	    	 
		    	 addToMap(map,essn,pname);
		    	 addToMap1(map3,essn,hours);
		    	 
		      }
		      
		      String sql3="select d.dependent_name,d.relationship,d.essn from dependent as d,employee as e where e.ssn=d.essn";
		      ResultSet rs3 =stmt.executeQuery(sql3);
		      
		      HashMap <Integer, ArrayList<String>> map1 = new HashMap <Integer, ArrayList<String>>();
		      HashMap <Integer, ArrayList<String>> map2 = new HashMap <Integer, ArrayList<String>>();
		      
		      while(rs3.next())
		      {
		    	 int essn=(rs3.getInt("essn"));
		    	 String dependent_name=(rs3.getString("dependent_name"));
		    	 String relationship=(rs3.getString("relationship"));
		    	 
		    	addToMap(map1,essn,dependent_name);
		    	addToMap(map2,essn,relationship);
		    	 
		      }
		      
		      String sql2 ="select e.lname,e.salary,d.dname,e.ssn from employee as e ,department as d where e.dno=d.dnumber";
		      ResultSet rs2 =stmt.executeQuery(sql2);
		      
		      while(rs2.next())
		      {
		    	 String lname=(rs2.getString("lname"));
		    	 int salary=(rs2.getInt("salary"));
		    	 String dname=(rs2.getString("dname"));
		    	 int ssn=(rs2.getInt("ssn"));
		    	 
		    	 BasicDBObject inQuery = new BasicDBObject();
			    	inQuery.put("lname",lname);
			    	inQuery.put("salary",salary);
			    	inQuery.put("dname",dname);
			    	inQuery.put("Project",map.get(ssn));
			    	inQuery.put("hours",map3.get(ssn));
			    	inQuery.put("dependent_name",map1.get(ssn));
			    	inQuery.put("relationship",map2.get(ssn));
			    	System.out.println(inQuery);
			    	//Using the  Java MongoDB API to insert the data into a "document" 
			    	collection.insert(inQuery);
		      }
			}
		 catch (Exception e) 
		 	  {
		        throw e;
		      }
	}
	
	/**
	 * This module is used to store the records values in HashMap as a Key Value pairs   
	 * @param map
	 * @param key
	 * @param value
	 */
	
	public void addToMap(HashMap <Integer, ArrayList<String>> map, Integer key, String value)
	{
		  if(!map.containsKey(key)){
			  ArrayList<String> val=new ArrayList<String>();
			  val.add(value);
			  map.put(key, val);
		  }
		  else
		  {
			  ArrayList<String> al= (ArrayList)map.get(key);
			  al.add(value);
		  }
		 
	}
	
	public void addToMap1(HashMap <Integer, ArrayList<Integer>> map, Integer key, int value)
	{
		  if(!map.containsKey(key))
		  {
			  ArrayList<Integer> val=new ArrayList<Integer>();
			  val.add(value);
			  map.put(key, val);
		  }
		  else
		  {
			  ArrayList<Integer> al= (ArrayList)map.get(key);
			  al.add(value);
		  }
		}
	}



class mongo
{
	public static void main(String args[]) throws Exception
	{
		class1 A = new class1();
		A.readDataBase();
		class2 B = new class2();
		B.readDataBase();
	}
}