import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.*;
import java.sql.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;


public class CentralSiteImpl extends UnicastRemoteObject implements CentralSite{

	/**
	 *
	 */
	private static final long serialVersionUID = -4710339008601446074L;

	private final Properties connectionProps;
	private final String url;
	private Lock myOnlyLock;

	RemoteSite myFirstRemoteClient;

	CentralSiteImpl() throws RemoteException {
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
		url = "jdbc:postgresql://localhost:5432/centralsite";
		connectionProps = new Properties();
		connectionProps.setProperty("user", "remotereader");
		connectionProps.setProperty("password", "bb");
		connectionProps.setProperty("ssl", "false");
	}

	public void registerSlave(final RemoteSite myCRemote){
		myFirstRemoteClient = myCRemote;
		/*try {
			myCRemote.receiveUpdate("insert into student values ('6', 'Chase');");
		} catch(Exception e) {}*/
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

	public List<Map<String, Object>> queryAll() throws RemoteException {
		/* retrieves all rows from table student
		 * returns results in List
		 */
		Statement st = null;
		ResultSet rs = null;
		Connection db = null;
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		try {
			db = DriverManager.getConnection(url, connectionProps);
			st = db.createStatement();
			rs = st.executeQuery("SELECT * FROM student");

			Map<String, Object> row = null;	
			ResultSetMetaData metaData = rs.getMetaData();
			Integer columnCount = metaData.getColumnCount();
		
			while (rs.next()) {
				row = new HashMap<String, Object>();
				for (int i = 1; i <= columnCount; i++) {
					row.put(metaData.getColumnName(i), rs.getObject(i));
				}
				resultList.add(row);
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
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
		return resultList;
	}

	public void pushUpdate(String update){
		System.out.println("Server push says: Update DB as follows: " + update);
		try {
			myFirstRemoteClient.receiveUpdate(update);
			//TODO implement wait for positive response
			//TODO implement multiple slaves
		} catch (Exception e) {}
	}
}
