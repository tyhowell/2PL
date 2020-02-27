import java.rmi.*;
import java.util.Properties;
import java.rmi.server.*;
import java.rmi.registry.*;
import java.sql.*;
import java.net.InetAddress;
 

public class MyClientRemote extends UnicastRemoteObject implements MyClient{ 

	/**
	 *
	 */
	private static final long serialVersionUID = 4891404420987536693L;

	private Properties connectionProps;
	private String url;
	private InetAddress localAddress;
	private CentralSite stub;

	MyClientRemote() throws RemoteException {
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
		url = "jdbc:postgresql://localhost:5432/test";
		connectionProps = new Properties();
		connectionProps.setProperty("user", "remotereader");
		connectionProps.setProperty("password", "bb");
		connectionProps.setProperty("ssl", "false");
		System.out.println("Connection Properties set in MCR");
	}

	public static void main(String args[]){
		try {
			MyClientRemote obj = new MyClientRemote();
			obj.doWork();
		} catch(Exception e){}
	}

	private void doWork() {
		try{
			stub=(CentralSite)Naming.lookup("rmi://localhost:5000/sonoo");

			stub.registerSlave(this);

			String queryStr = "SELECT * FROM student WHERE name = 'Emily'";
			queryParser(queryStr);
			
			/*stub.getLock("student", "read", "192.168.0.1");
			stub.getLock("student", "write", "192.168.0.2");
			stub.releaseLock("student", "read", "192.168.0.1");
			stub.getLock("student", "read", "192.168.0.1");
			stub.releaseLock("student", "write", "192.168.0.2");
			stub.pushUpdate("Need to add a student");*/

		} catch(Exception e){
			System.err.println(e);
			e.printStackTrace();
		}
	}

	private void read(String queryStr) {
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
	private void update(String queryStr) {

	}
	private void insert(String queryStr) {

	}
	
	public void receiveUpdate(String update) {
		System.out.println("Slave Receiving the update: " + update);
		Statement st = null;
		ResultSet rs = null;
		Connection db = null;
		try {
			db = DriverManager.getConnection(url, connectionProps);
			System.out.println("The connection to the database was successfully opened.");
			st = db.createStatement();
			rs = st.executeQuery(update);
		} catch (final Exception e) {
			System.out.println("DatabaseTest exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			// Close the ResultSet and the Statement variables and close
			// the connection to the database.
			try {
				//TODO implement sending a positive response
				//stub.updateComplete(timestamp, localAddress.toString());
				rs.close();
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
			read(queryStr);
		else if (queryStr.toLowerCase().contains("update"))
			update(queryStr);
		else if (queryStr.toLowerCase().contains("insert"))
			insert(queryStr);
		else {
			System.err.println("Illegal or unsupported SQL syntax");
		}
	}
}
