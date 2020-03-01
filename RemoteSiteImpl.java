import java.rmi.*;
import java.util.Properties;
import java.rmi.server.*;
import java.rmi.registry.*;
import java.sql.*;
import java.net.InetAddress;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
 

public class RemoteSiteImpl extends UnicastRemoteObject implements RemoteSite{ 

	/**
	 *
	 */
	private static final long serialVersionUID = 4891404420987536693L;

	private Properties connectionProps;
	private String url;
	private InetAddress localAddress;
	private CentralSite stub;

	RemoteSiteImpl(int siteNum) throws RemoteException {
		// constructor for parent class
		super();
		// Loading the Driver
		try {
			Class.forName("org.postgresql.Driver");
		} catch (final ClassNotFoundException e) {
			System.err.println("Couldn't find driver class:");
			e.printStackTrace();
		}

		stub = null;
		try {
			localAddress = InetAddress.getLocalHost();
		} catch (Exception e) {
			System.err.println("Unable to obtain local IP Address");
			e.printStackTrace();
			System.exit(-1);
		}
		// Establish connection properties for PostgreSQL database
		url = "jdbc:postgresql://localhost:5432/remotesite" + Integer.toString(siteNum);
		connectionProps = new Properties();
		connectionProps.setProperty("user", "remotereader");
		connectionProps.setProperty("password", "bb");
		connectionProps.setProperty("ssl", "false");
	}

	public static void main(String args[]){
		try {
			int siteNum = 0;
			RemoteSiteImpl obj = new RemoteSiteImpl(siteNum);
			obj.doWork();
		} catch(Exception e){}
	}

	private void doWork() {
		try{
			stub=(CentralSite)Naming.lookup("rmi://localhost:5000/sonoo");
			stub.registerSlave(this);
			clearAndCopy();

			String queryStr = "SELECT * FROM student;";
			queryParser(queryStr);
			
			/*stub.getLock("student", "read", "192.168.0.1");
			stub.getLock("student", "read", "192.168.0.2");
			stub.getLock("student", "read", "192.168.0.3");

			stub.getLock("student", "write", "192.168.0.4");

			stub.releaseLock("student", "read", "192.168.0.1");
			stub.releaseLock("student", "read", "192.168.0.2");
			stub.releaseLock("student", "read", "192.168.0.3");

			stub.getLock("student", "read", "192.168.0.1");
			stub.releaseLock("student", "write", "192.168.0.2");*/

		} catch(Exception e){
			System.err.println(e);
			e.printStackTrace();
		}
	}

	private void executeRead(String queryStr) {
		Statement st = null;
		ResultSet rs = null;
		Connection db = null;
		try {
			db = DriverManager.getConnection(url, connectionProps);
			System.out.println("The connection to the database was successfully opened.");
			st = db.createStatement();

			stub.getLock("student", "read", localAddress.toString());
			rs = st.executeQuery(queryStr);
			stub.releaseLock("student", "read", localAddress.toString());
			int element = 1;

			// Print out all elements in the table named telephonebook.
			while (rs.next()) {
				System.out.print("Element " + element + ": ");
				System.out.print(rs.getString(1));
				System.out.print(" ");
				System.out.print(rs.getString(2));
				System.out.print("\n");
				element = element + 1;
			}
		} catch (final Exception e) {
			System.out.println("DatabaseTest exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			// Close the ResultSet and the Statement variables and close
			// the connection to the database.
			try {
				rs.close();
				st.close();
				db.close();
				System.out.println("Closed connection to the database.");
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}
	private void executeUpdate(String queryStr) {
		//open connection
		//obtain write lock
		//do write, commit?
		//inform everyone (thru manager?)
		//receive all clear
		//release lock

	}
	private void executeInsert(String queryStr) {
		//open connection
		//lock needed? YES if lock granularity is by table
		//do update, commit?
		//inform everyone
		//receive all clear
		//release lock
	}
	
	public void receiveUpdate(String update) {
		System.out.println("Slave Receiving the update: " + update);
		Statement st = null;
		Connection db = null;
		try {
			db = DriverManager.getConnection(url, connectionProps);
			System.out.println("The connection to the database was successfully opened.");
			st = db.createStatement();
			st.executeUpdate(update);
		} catch (final Exception e) {
			System.out.println("DatabaseTest exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			// Close the ResultSet and the Statement variables and close
			// the connection to the database.
			try {
				//TODO implement sending a positive response to master
				//stub.updateComplete(timestamp, localAddress.toString());
				st.close();
				db.close();
				System.out.println("Closed connection to the database.");
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}

	private void queryParser(String queryStr) {
		if (queryStr.toLowerCase().contains("select"))
			executeRead(queryStr);
		else if (queryStr.toLowerCase().contains("update"))
			executeUpdate(queryStr);
		else if (queryStr.toLowerCase().contains("insert"))
			executeInsert(queryStr);
		else {
			System.err.println("Illegal or unsupported SQL syntax");
		}
	}

	private void clearAndCopy() {
		/* Deletes all rows from table student
		*  Has master query all and inserts all results into table
		*/
		Statement st = null;
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		Connection db = null;
		try {
			db = DriverManager.getConnection(url, connectionProps);
			st = db.createStatement();
			//clear table
			st.executeUpdate("DELETE FROM student");
			//get resultset from central site
			resultList = stub.queryAll();
			Map<String, Object> row = null;	
			for (int i = 0; i < resultList.size(); i++) {
				row = resultList.get(i);
				String studentID = row.get("id").toString();
				String studentName = row.get("name").toString();
				st.executeUpdate("INSERT INTO student values (" + studentID + ", '" + studentName + "')");
			}
			
		} catch (final Exception e) {
			System.out.println("DatabaseTest exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			// Close the ResultSet and the Statement variables and close
			// the connection to the database.
			try {
				st.close();
				db.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}
}
