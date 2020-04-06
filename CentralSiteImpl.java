import java.rmi.*;
import java.rmi.server.*;
import java.sql.*;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

public class CentralSiteImpl extends UnicastRemoteObject implements CentralSite {
	/** Class implements the Central Site
	 *  Responsible for master database, coordinate locks,
	 *  detect and resolve deadlocks
	 */
	private static final long serialVersionUID = -4710339008601446074L;

	private final Properties connectionProps;
	private final String url;
	Connection db;
	private List<Lock> lockList;
	private HashMap<String, Integer> tableNameToLockListIndex;
	private static final Logger LOGGER = Logger.getLogger(RemoteSiteImpl.class.getName());
	private Integer numRemoteConnections;
	private Integer numRequestedDisconnects;
	List<RemoteSite> remoteSiteList; 
	private GlobalWaitForGraph globalWaitForGraph;

	CentralSiteImpl() throws RemoteException {
		super();

		remoteSiteList = new ArrayList<RemoteSite>();
		globalWaitForGraph = new GlobalWaitForGraph();
		tableNameToLockListIndex = new HashMap<>();
		// Load the postgres river
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
		try {
			db = DriverManager.getConnection(url, connectionProps);
		} catch (Exception e) {
			System.err.println("Unable to connect to database");
			e.printStackTrace();
			System.exit(-1);
		}

		numRemoteConnections = 0;
		numRequestedDisconnects = 0;
		initializeLocks();
	}

	private void initializeLocks() {
		/* Initialize one lock per table
		 * Delete all entries from table
		 * Remote sites will copy these tables when they connect
		 */
		lockList = new ArrayList<>();
		Statement st = null;
		ResultSet rs = null;
		Integer counter = 0;
		try {
			st = db.createStatement();
			String selectAllTables = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";
			rs = st.executeQuery(selectAllTables);
			while (rs.next()) {
				String tableName = rs.getString("tablename");
				st = db.createStatement();
				st.executeUpdate("DELETE FROM " + tableName);
				tableNameToLockListIndex.put(tableName, counter);
				counter++;
				lockList.add(new Lock(tableName));
			}
			LOGGER.log(Level.INFO, "Initialized locks for " + lockList.size() + " tables");
		} catch (final Exception e) {
			System.out.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				st.close();
				rs.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}

	public void registerSlave(final RemoteSite myCRemote){
		remoteSiteList.add(myCRemote);
		numRemoteConnections++;
		System.out.println(numRemoteConnections + " remote site connections established");
	}

	public void disconnectSlave(Integer siteNum){
		numRequestedDisconnects++;
		LOGGER.log(Level.INFO, "Disconnect request received from site: " 
			+ Integer.toString(siteNum) + " have received " 
			+ Integer.toString(numRequestedDisconnects) + " disconnects");
		if (numRemoteConnections == numRequestedDisconnects) {
			disconnectAll();
		}
	}

	public void disconnectAll() {
		try {
			for (Integer i = 0; i < numRemoteConnections; i++) {
				LOGGER.log(Level.INFO, "Sending disconnect command to site: " + Integer.toString(i));
				remoteSiteList.get(i).disconnect();
			}	
			db.close();
			System.exit(0);
		} catch (Exception e) {}
	}

	public Boolean getLock(final String queryStr, Integer siteNum, Integer tID) {
		// returns true if lock obtained, else returns false
		// query to see if lock is available
		Operation rqtOp;
		Integer tableIndex = 0;
		for (int i = 0; i < lockList.size(); i++) {
			//loop tables to determine which lock to obtain
			//TODO only supports single table queries
			if (queryStr.toLowerCase().contains(lockList.get(i).getName()))
				tableIndex = i;
		}
		LOGGER.log(Level.INFO, "lock requested for query: " + queryStr + " from remotesite" 
			+ Integer.toString(siteNum) + " tID: " + Integer.toString(tID) 
			+ ", query is on table: " + lockList.get(tableIndex).getName());

		if (queryStr.toLowerCase().contains("update") || queryStr.toLowerCase().contains("insert"))
			rqtOp = new Operation(operationType.WRITE, lockList.get(tableIndex).getName(), "value", tID, "rest", siteNum);
		else if (queryStr.toLowerCase().contains("select"))
			rqtOp = new Operation(operationType.READ, lockList.get(tableIndex).getName(), "value", tID, "rest", siteNum);
		else {
			System.err.println("Illegal or unsupported SQL syntax");
			return false;
		}
		
		if (!globalWaitForGraph.transactionExists(tID)) {
			//node does not exist in gwfg yet
			ConcurrentLockNode newNode = new ConcurrentLockNode(tID);
			globalWaitForGraph.add_node(newNode);
		}
		//Boolean lockObtained = myOnlyLock.getLock(rqtOp);
		Boolean lockObtained = lockList.get(tableIndex).getLock(rqtOp);
		//return myOnlyLock.getLock(rqtOp);
		if (!lockObtained) {
			List<Integer> currentLockHolder = lockList.get(tableIndex).getCurrentLockHolder();//myOnlyLock.getCurrentLockHolder();
			globalWaitForGraph.add_dependency(tID, currentLockHolder.get(0));
			if (globalWaitForGraph.hasDeadlock()) {
				System.out.println("Deadlock detected");
				//release all locks on whichever transaction has the least number of locks (releaseAllLocks)
				Integer numLocksRequester = numLocksForTransaction(tID);
				Integer numLocksHolder = numLocksForTransaction(currentLockHolder.get(0));
				Integer tIDtoAbort = tID;
				Integer siteNumToAbort = siteNum;
				if (numLocksRequester > numLocksHolder) {
					// if lock requester currently holds more locks than lockHolder, choose
					// to abort current lock holder transaction
					tIDtoAbort = currentLockHolder.get(0);
					siteNumToAbort = currentLockHolder.get(1);
				}
				releaseAllLocks(tIDtoAbort, siteNumToAbort, operationType.ABORT);
				//give released lock to non-aborted transaction - automatic via releaseAllLocks
				//inform the remoteSite to abort/rollback transaction
				try {
					remoteSiteList.get(siteNumToAbort).abortCurrentTransaction();
				} catch (Exception e) {
					LOGGER.log(Level.WARNING, "Unable to notify site of aborted transaction");
				}
				//remoteSite repeat transaction
			} else {
				System.out.println("No deadlock detected");
			}
		}
		return lockObtained;
	}

	public void releaseLock(String table, String lockType, Integer siteNum, Integer tID) {
		LOGGER.log(Level.INFO, "Releasing lock on table " + table + " from client " + Integer.toString(siteNum) + " tId " + Integer.toString(tID));
		System.out.println("Releasing lock on table " + table + " from client " + Integer.toString(siteNum) + " tId " + Integer.toString(tID));
		Operation rqtOp;
		if (lockType.equals("read")) {
			rqtOp = new Operation(operationType.READ, table, "value", tID, "rest", siteNum);
		}
		else {
			rqtOp = new Operation(operationType.WRITE, table, "value", tID, "rest", siteNum);
		}
		List<Operation> grantedLocks = new ArrayList<>();
		Integer tableIndex = tableNameToLockListIndex.get(table);
		grantedLocks = lockList.get(tableIndex).releaseLock(rqtOp);//myOnlyLock.releaseLock(rqtOp);
		for (Operation i : grantedLocks) {
			Integer remoteSiteNum = i.remoteSiteNum;
			try {
				LOGGER.log(Level.INFO, "Notifying site number: " + Integer.toString(remoteSiteNum));
				remoteSiteList.get(remoteSiteNum).lockObtained(i.getType());
			} catch (Exception e) {
				LOGGER.log(Level.WARNING, "Unable to notify site of granted lock");
			}
		}
	}
	public void releaseAllLocks(Integer tID, Integer siteNum, operationType reason) {
		//TODO change student to be correct tableName
		Operation rqtReleaseOp = new Operation(reason, "student", "value", tID, "rest", siteNum);
		List<Operation> grantedLocks = new ArrayList<>();
		//attempt to release each lock TODO not the best way to do it
		for (int i = 0; i < lockList.size(); i++) {
			List<Operation> tempGrantedLocks = new ArrayList<>();
			tempGrantedLocks = lockList.get(i).releaseLock(rqtReleaseOp);
			for (int j = 0; j < tempGrantedLocks.size(); j++) {
				grantedLocks.add(tempGrantedLocks.get(j));
			}
		}
		//grantedLocks = myOnlyLock.releaseLock(rqtReleaseOp);
		LOGGER.log(Level.INFO, "releasing locks, notifying this many sites: " + Integer.toString(grantedLocks.size()));
		for (Operation i : grantedLocks) {
			Integer remoteSiteNum = i.remoteSiteNum;
			try {
				LOGGER.log(Level.INFO, "Notifying site number: " + Integer.toString(remoteSiteNum));
				remoteSiteList.get(remoteSiteNum).lockObtained(i.getType());
			} catch (Exception e) {
				LOGGER.log(Level.WARNING, "Unable to notify site of granted lock");
			}
		}
	}

	public List<Map<String, Object>> queryAll(String tableName) throws RemoteException {
		/* retrieves all rows from table student
		 * returns results in List
		 */
		Statement st = null;
		ResultSet rs = null;
		//Connection db = null;
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		try {
			//db = DriverManager.getConnection(url, connectionProps);
			st = db.createStatement();
			rs = st.executeQuery("SELECT * FROM " + tableName);

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
				//db.close();
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
		//Connection db = null;
		try {
			//db = DriverManager.getConnection(url, connectionProps);
			//System.out.println("The connection to the database was successfully opened.");
			st = db.createStatement();
			st.executeUpdate(update);
		} catch (final Exception e) {
			System.out.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				//stub.updateComplete(timestamp, localAddress.toString());
				st.close();
				//db.close();
				//System.out.println("Closed connection to the database.");
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}

	private Integer numLocksForTransaction(Integer tID) {
		Integer numLocks = 0;
		for (int i = 0; i < lockList.size(); i++) {
			if (lockList.get(i).getCurrentLockHolder().get(0) == tID)
				numLocks++;
		}
		return numLocks;
	}
}
