import java.rmi.*;
import java.rmi.server.*;
import java.sql.*;
import java.util.Properties;


public class CentralSiteRemote extends UnicastRemoteObject implements CentralSite{

	private final Properties connectionProps;
	private final String url;
	private Lock myOnlyLock;

	MyClientRemote myFirstRemoteClient;

	CentralSiteRemote() throws RemoteException {
		// constructor for parent class
		super();
		myOnlyLock = new Lock("student");
		// Loading the Driver
		try {
			Class.forName("org.postgresql.Driver");
		} catch (final ClassNotFoundException cnfe) {
			System.err.println("Couldn't find driver class:");
			cnfe.printStackTrace();
		}
		
		// Establish connection properties for PostgreSQL database
		url = "jdbc:postgresql://128.10.2.13:5432/test";
		connectionProps = new Properties();
		connectionProps.setProperty("user", "remotereader");
		connectionProps.setProperty("password", "bb");
		connectionProps.setProperty("ssl", "false");
	}

	public void registerSlave(MyClientRemote myCRemote){
		System.out.println("Test line 36 CSR");
		myFirstRemoteClient = myCRemote;
		myFirstRemoteClient.receiveUpdate("DID OUR FIRST SERVER TO SLAVE CALL WORK?");
	}

	public void getLock(final String table, final String lockType, String user) {
		/*String connectionInfo;
		try{
			//connectionInfo = getClientHost();
			System.out.println("Lock request from " + connectionInfo);
		} catch (Exception e) {
			System.err.println("Unable to get connection info on Client, canceling request");
			return;
		}*/
		System.out.println(lockType + " lock requested for table: " + table + " from " + user);
		// query to see if lock is available
		Transaction rqtTrans;
		if(lockType.equals("read")) {
			rqtTrans = new Transaction(transactionType.READ, "value", 0, "rest");
		}
		else {
			rqtTrans = new Transaction(transactionType.WRITE, "value", 0, "rest");
		}
		myOnlyLock.getLock(rqtTrans);
		// if not available, reply "wait", client must try again in X seconds
		// if available, mark as unavailable in lock table, reply "you have lock"
	}

	public void releaseLock(String table, String lockType, String user) {
		String connectionInfo;
		try{
			connectionInfo = getClientHost();
		} catch (Exception e) {
			System.err.println("Unable to get connection info on Client, canceling request");
			return;
		}
		System.out.println("Releasing lock on table " + table + " from client " + user);
		Transaction rqtTrans;
		if (lockType.equals("read")) {
			rqtTrans = new Transaction(transactionType.READ, "value", 0, "rest");
		}
		else {
			rqtTrans = new Transaction(transactionType.WRITE, "value", 0, "rest");
		}
		myOnlyLock.releaseLock(rqtTrans);
	}

	public void queryAll() throws RemoteException {
		Statement st = null;
		ResultSet rs = null;
		Connection db = null;
		try {
			db = DriverManager.getConnection(url, connectionProps);
			System.out.println("The connection to the database was successfully opened.");
			st = db.createStatement();

			rs = st.executeQuery("SELECT * FROM student WHERE name = 'Emily'");
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
	public void pushUpdate(String update){
		System.out.println("Update DB as follows: " + update);
	}

}
