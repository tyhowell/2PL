import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.*;
import java.sql.*;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
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
	private static final Logger LOGGER = Logger.getLogger(RemoteSiteImpl.class.getName());
	private Integer numRemoteConnections;
	List<RemoteSite> remoteSiteList; 

	CentralSiteImpl() throws RemoteException {
		// constructor for parent class
		super();

		myOnlyLock = new Lock("student");
		remoteSiteList = new ArrayList<RemoteSite>();
		LOGGER.setUseParentHandlers(false);
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
		numRemoteConnections = 0;
	}

	public void registerSlave(final RemoteSite myCRemote){
		remoteSiteList.add(myCRemote);
		numRemoteConnections++;
		System.out.println(numRemoteConnections + " remote site connections established");
	}

	public Boolean getLock(final String queryStr, Integer siteNum, Integer tID) {
		// returns true if lock obtained, else returns false
		LOGGER.log(Level.INFO, "lock requested for query: " + queryStr + " from " + Integer.toString(siteNum) + " tID: " + Integer.toString(tID));
		//System.out.println("lock requested for query: " + queryStr + " from " + Integer.toString(siteNum) + " tID: " + Integer.toString(tID));
		// query to see if lock is available
		Operation rqtOp;
		String queryType = "read";
		if (queryStr.toLowerCase().contains("select"))
			queryType = "read";
		else if (queryStr.toLowerCase().contains("update"))
			queryType = "write";
		else if (queryStr.toLowerCase().contains("insert"))
			queryType = "write";

		if(queryType.equals("read")) {
			rqtOp = new Operation(operationType.READ, "student", "value", tID, "rest", siteNum);
		}
		else {
			rqtOp = new Operation(operationType.WRITE, "student", "value", tID, "rest", siteNum);
		}
		return myOnlyLock.getLock(rqtOp);
	}

	public void releaseLock(String table, String lockType, Integer siteNum, Integer tID) {
		LOGGER.log(Level.INFO, "Releasing lock on table " + table + " from client " + Integer.toString(siteNum) + " tId " + Integer.toString(tID));
		System.out.println("Releasing lock on table " + table + " from client " + Integer.toString(siteNum) + " tId " + Integer.toString(tID));
		Operation rqtOp;
		if (lockType.equals("read")) {
			rqtOp = new Operation(operationType.READ, "student", "value", tID, "rest", siteNum);
		}
		else {
			rqtOp = new Operation(operationType.WRITE, "student", "value", tID, "rest", siteNum);
		}
		List<Operation> locksToBeGranted = new ArrayList<>();
		locksToBeGranted = myOnlyLock.releaseLock(rqtOp);
		while (locksToBeGranted.size() > 0) {
			//call each remoteSites lockObtained()
		}
	}
	public void releaseAllLocks(Integer tID, Integer siteNum, operationType reason) {
		Operation rqtReleaseOp = new Operation(reason, "student", "value", tID, "rest", siteNum);
		myOnlyLock.releaseLock(rqtReleaseOp);
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

	public void pushUpdate(String update, Integer fromSite) throws RemoteException{
		System.out.println("Update from " + fromSite + ", update DB as follows: " + update);
		try {
			for (Integer i = 0; i < numRemoteConnections; i++) {
				System.out.println("i: " + i + " fromsite: " + fromSite);
				if (!i.equals(fromSite)) { //avoid pushing update back to originating site
					System.out.println("Pushing update to site " + i);
					remoteSiteList.get(i).receiveUpdate(update);
				}
			}	
			//TODO implement wait for positive response
		} catch (Exception e) {}
		//update self
		Statement st = null;
		Connection db = null;
		try {
			db = DriverManager.getConnection(url, connectionProps);
			System.out.println("The connection to the database was successfully opened.");
			st = db.createStatement();
			st.executeUpdate(update);
		} catch (final Exception e) {
			System.out.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				//stub.updateComplete(timestamp, localAddress.toString());
				st.close();
				db.close();
				System.out.println("Closed connection to the database.");
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}
}
