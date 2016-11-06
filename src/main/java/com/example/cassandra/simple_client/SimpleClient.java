package com.example.cassandra.simple_client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

public class SimpleClient {
	public static void main(String args[])throws IOException
	{
		int n;
		long d,id,age;
		String q;
		BufferedReader br =new BufferedReader(new InputStreamReader(System.in));
		
		Cluster clusters;
		Session sessions;
		Statement statement;
		ResultSet rs;
		Insert insert;
		
		clusters =Cluster.builder().addContactPoint("127.0.0.1").build();
		sessions =clusters.connect("killrvideo"); 
		
		while(true)
		{
			System.out.println("----------------MENU-----------------");
			System.out.println("1.Create Table");       
			System.out.println("2.Enter Data");         //working (but records are getting replaced)
			System.out.println("3.Import Data from CSV file.");   //not supported
			System.out.println("4.Add User Data");		//only 3 columns getting added :/
			System.out.println("5.Delete Data");		
			System.out.println("6.Update Data");		
			System.out.println("7.Show Table");        
			System.out.println("8.Exit");
			System.out.println("Enter your choice");
			
			q =br.readLine();
			n =Integer.parseInt(q);
			if(n==1)
			{
				String query ="CREATE TABLE emp(id int PRIMARY KEY,age int, city text, firstname text, lastname text);";
				sessions.execute(query);
				System.out.println("Table Created");
			}
			if(n==2)
			{
				String query1 ="INSERT INTO emp(id, firstname, lastname, age, city) VALUES(1,'Jones','March',35,'Pune')";
				String query2 ="INSERT INTO emp(id, firstname, lastname, age, city) VALUES(2,'Bob','Frye',23,'Bombay')";
				String query3 ="INSERT INTO emp(id, firstname, lastname, age, city) VALUES(3,'Amit','Naik',21,'Cedar Rapids')";
				sessions.execute(query1);
				sessions.execute(query2);
				sessions.execute(query3);
				System.out.println("Data Inserted");
			}
			if(n==3)
			{
				System.out.println("Enter the path of the file name which you want to import.");
				//q =br.readLine();
				String query5 ="COPY demo.emp from '/home/student/test.csv' with header=true;";
				sessions.execute(query5);
				System.out.println("File imported.");
			}
			if(n==4)
			{
				System.out.println("Enter the ID, Age, City, First Name, Last Name.");
				q =br.readLine();
				id =Long.parseLong(q);
				String fname =br.readLine();
				String lname =br.readLine();
				age =Long.parseLong(q);
				String city =br.readLine();
				
				insert =QueryBuilder.insertInto("demo","emp").value("id", id).value("age",age).value("city", city).value("firstname",fname).value("lastname", lname);
				rs =sessions.execute(insert);
				System.out.println("Data Entered");
			}
			if(n==5)
			{
				System.out.println("The table is shown below.");
				System.out.println();
				System.out.println("    " +"ID " + "Age " + "City " + "FirstName " + "LastName" );
				statement = new SimpleStatement("select * from emp");
				statement.setFetchSize(1);
				rs =sessions.execute(statement);
				Iterator<Row> iter =rs.iterator();
				while(!rs.isFullyFetched())
				{
					rs.fetchMoreResults();
					Row row =iter.next();
					System.out.println(row);
				}
				System.out.println("Enter the id which you want to delete.");
				q =br.readLine();
				d =Long.parseLong(q);	
				statement =QueryBuilder.delete().from("demo", "emp").where(eq("ID",d));
				sessions.execute(statement);
				System.out.println("Record is deleted.");
			}
			if(n==6)
			{
				System.out.println("The table is shown below.");
				System.out.println();
				System.out.println("    " +"ID " + "Age " + "City " + "FirstName " + "LastName" );
				statement = new SimpleStatement("select * from emp");
				statement.setFetchSize(1);
				rs =sessions.execute(statement);
				Iterator<Row> iter =rs.iterator();
				while(!rs.isFullyFetched())
				{
					rs.fetchMoreResults();
					Row row =iter.next();
					System.out.println(row);
				}
				System.out.println("Enter the ID of the field which you want to update.");
				q =br.readLine();
				d =Long.parseLong(q);
				System.out.println("Enter the field which you want to update.");
				String field =br.readLine();
				System.out.println("Enter the updated value.");
				String value =br.readLine();
				
				statement =QueryBuilder.update("demo", "emp").with(set(field,value)).where(eq("id",d));
				sessions.execute(statement);
			}
			if(n==7)
			{
				statement = new SimpleStatement("select * from emp");
				statement.setFetchSize(1);
				rs =sessions.execute(statement);
				Iterator<Row> iter =rs.iterator();
				while(!rs.isFullyFetched())
				{
					rs.fetchMoreResults();
					Row row =iter.next();
					System.out.println(row);
				}
			}
			if(n==8)
			{
				clusters.close();
				break;
			}
		}
		System.out.println("Thank You for using Cassandra. We hope to see you again");
	}
}

