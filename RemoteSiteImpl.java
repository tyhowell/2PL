import java.rmi.*;
import java.util.Properties;
import java.rmi.server.*;
import java.rmi.registry.*;
import java.sql.*;
import java.net.InetAddress;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.Map;
import java.io.*;
 

public class RemoteSiteImpl extends UnicastRemoteObject implements RemoteSite{ 

	/**
	 *
	 */
	private class LockInfo {
		public String item;
		public String lockType;
		public String localAddress;
		public Integer transaction;
		LockInfo(String itemStr, String lockTypeStr, String localAddressStr, Integer transactionInt) {
			item = itemStr;
			lockType = lockTypeStr;
			localAddress = localAddressStr;
			transaction = transactionInt;
		}
	}

	private static final long serialVersionUID = 4891404420987536693L;
	private static final Logger LOGGER = Logger.getLogger(RemoteSiteImpl.class.getName());
	private String testFile;
	private Properties connectionProps;
	private String url;
	private InetAddress localAddress;
	private CentralSite stub;
	private Integer remoteSiteNum;
	private Boolean withinTransaction;
	private Boolean obtainedLock;
	List<String> updatesWithinTransaction;
	Integer activeTransaction;
	Integer nextTransactionID;
	List<LockInfo> held_locks;

	RemoteSiteImpl(int siteNum, String testFileStr) throws RemoteException {
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
		updatesWithinTransaction = new ArrayList<String>();
		held_locks = new ArrayList<LockInfo>();

		testFile = testFileStr;
		nextTransactionID = 0;
		withinTransaction = false;
		obtainedLock = true;
		activeTransaction = -1;
		// Establish connection properties for PostgreSQL database
		url = "jdbc:postgresql://localhost:5432/remotesite" + Integer.toString(siteNum);
		remoteSiteNum = siteNum;
		connectionProps = new Properties();
		connectionProps.setProperty("user", "remotereader");
		connectionProps.setProperty("password", "bb");
		connectionProps.setProperty("ssl", "false");
	}

	public static void main(String args[]){
		try {
			int siteNum = 0;
			String testFileStr = null;
			if (args.length > 0)
				siteNum = Integer.parseInt(args[0]);
				testFileStr = args[1];
			RemoteSiteImpl obj = new RemoteSiteImpl(siteNum, testFileStr);
			LOGGER.setUseParentHandlers(false);
			obj.doWork();
		} catch(Exception e){}
	}

	private void doWork() {
		try{
			stub=(CentralSite)Naming.lookup("rmi://localhost:5000/sonoo");
			stub.registerSlave(this);
			clearAndCopy();

			File file = new File("/homes/howell66/cs542/2PL/test/" + testFile); 
			BufferedReader br = new BufferedReader(new FileReader(file)); 
			String st; 
			String queryForMe = "r" + Integer.toString(remoteSiteNum) + ":";
			while ((st = br.readLine()) != null) {
				if (st.contains(queryForMe)) {
					queryParser(st.replaceFirst(queryForMe, ""));
				}
			} 
			/*String queryBegin = "BEGIN;";
			String queryCommit = "COMMIT;";
			String queryRollback = "ROLLBACK;";
			queryParser(queryBegin);
			String queryStr;
			queryStr = "SELECT * FROM student;";
			queryParser(queryStr);
			queryStr = "SELECT * FROM student WHERE id = 2;";
			//queryStr = "INSERT INTO student values (6, 'Annie')";
			if (remoteSiteNum == 5)
				queryParser(queryStr);
			queryParser(queryStr);
			queryParser(queryBegin);
			queryParser(queryStr);*/
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
			LOGGER.log(Level.INFO, "Read query, site: " + Integer.toString(remoteSiteNum) + " " + queryStr);
			db = DriverManager.getConnection(url, connectionProps);
			//System.out.println("The connection to " + url + " was successfully opened.");
			st = db.createStatement();
			obtainedLock = stub.getLock(queryStr, remoteSiteNum, activeTransaction);
			while (!obtainedLock) {}
			rs = st.executeQuery(queryStr);
			if (withinTransaction) {
				//add lock to list of to be released on commit/abort
				held_locks.add(new LockInfo("student", "read", localAddress.toString(), activeTransaction));
			} else {
				stub.releaseLock("student", "read", remoteSiteNum, activeTransaction);
			}
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
				//System.out.println("Closed connection to the database.");
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}
	private void executeUpdate(String queryStr) {
		//open connection
		Statement st = null;
		Connection db = null;
		try {
			db = DriverManager.getConnection(url, connectionProps);
			st = db.createStatement();
			//obtain write lock
			obtainedLock = stub.getLock(queryStr, remoteSiteNum, activeTransaction);
			while (!obtainedLock) {}
			//do write, commit?
			st.executeUpdate(queryStr);
			if (withinTransaction) {
				held_locks.add(new LockInfo("student", "write", localAddress.toString(), activeTransaction));
				updatesWithinTransaction.add(queryStr);
			}
			else {
				stub.pushUpdate(queryStr, remoteSiteNum);
				stub.releaseLock("student", "write", remoteSiteNum, activeTransaction);
			}
		} catch (final Exception e) {
			System.out.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			// Close ResultSet and the Statement variables and close
			// the connection to the database.
			try {
				//TODO implement sending a positive response to master
				//stub.updateComplete(timestamp, localAddress.toString());
				st.close();
				db.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}
	private void executeInsert(String queryStr) {
		Statement st = null;
		Connection db = null;
		try {
			db = DriverManager.getConnection(url, connectionProps);
			st = db.createStatement();
			//obtain write lock
			//lock needed? YES if lock granularity is by table TODO
			obtainedLock = stub.getLock(queryStr, remoteSiteNum, activeTransaction);
			while (!obtainedLock) {}
			//do insert, commit?
			st.executeUpdate(queryStr);
			if (withinTransaction) {
				held_locks.add(new LockInfo("student", "write", localAddress.toString(), activeTransaction));
				updatesWithinTransaction.add(queryStr);
			}
			if (!withinTransaction) {
				stub.pushUpdate(queryStr, remoteSiteNum);
				stub.releaseLock("student", "write", remoteSiteNum, activeTransaction);
			}
		} catch (final Exception e) {
			System.out.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			// Close the ResultSet and the Statement variables and close
			// the connection to the database.
			try {
				//TODO implement sending a positive response to master
				//stub.updateComplete(timestamp, localAddress.toString());
				st.close();
				db.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}
	private void executeBegin(String queryStr) {
		Integer tID = (nextTransactionID * 10) + remoteSiteNum;
		activeTransaction = tID;
		nextTransactionID = nextTransactionID + 1;
		withinTransaction = true;
		if (updatesWithinTransaction.size() > 0)
			updatesWithinTransaction.clear();
		if (held_locks.size() > 0)
			held_locks.clear();

		Statement st = null;
		Connection db = null;
		LOGGER.log(Level.INFO, "Begin transaction, site: " + Integer.toString(remoteSiteNum) + " " + queryStr);

		try {
			db = DriverManager.getConnection(url, connectionProps);
			st = db.createStatement();
			st.executeUpdate(queryStr);
		} catch (final Exception e) {
			System.out.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				st.close();
				db.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}
	private void executeCommit(String queryStr) {
		Statement st = null;
		Connection db = null;
		try {
			db = DriverManager.getConnection(url, connectionProps);
			st = db.createStatement();
			LOGGER.log(Level.INFO, "Commit transaction, site: " + Integer.toString(remoteSiteNum) + " " + queryStr);
			//execute sql
			st.executeUpdate(queryStr);
			//push to master one at a time
			ListIterator<String> queryIter = updatesWithinTransaction.listIterator();
			while(queryIter.hasNext()) {
				stub.pushUpdate(queryIter.next(), remoteSiteNum);
			}	
			//stub.pushUpdate(queryStr, remoteSiteNum);
			//release all locks
			stub.releaseAllLocks(activeTransaction, remoteSiteNum, operationType.COMMIT);
			withinTransaction = false;
		} catch (final Exception e) {
			System.out.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				st.close();
				db.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
		withinTransaction = false;

	}
	private void executeRollback(String queryStr) {
		//execute sql
		Statement st = null;
		Connection db = null;
		try {
			db = DriverManager.getConnection(url, connectionProps);
			st = db.createStatement();
			LOGGER.log(Level.INFO, "Rollback transaction, site: " + Integer.toString(remoteSiteNum) + " " + queryStr);
			st.executeUpdate(queryStr);
			//release all locks
			stub.releaseAllLocks(activeTransaction, remoteSiteNum, operationType.ABORT);
			withinTransaction = false;
		} catch (final Exception e) {
			System.out.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				st.close();
				db.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
		withinTransaction = false;

	}
	
	public void receiveUpdate(String update) {
	/*  Updates come from central site
	 *  Currently in SQL
	 * 
	 */
		LOGGER.log(Level.INFO, "Update from Master, site: " + Integer.toString(remoteSiteNum) + " " + update);

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

	public void lockObtained(String lockType) {
		obtainedLock = true;
	}

	private void queryParser(String queryStr) {
		//TODO add support for more complex queries?
		if (queryStr.toLowerCase().contains("select"))
			executeRead(queryStr);
		else if (queryStr.toLowerCase().contains("update"))
			executeUpdate(queryStr);
		else if (queryStr.toLowerCase().contains("insert"))
			executeInsert(queryStr);
		else if (queryStr.toLowerCase().contains("begin"))
			executeBegin(queryStr);
		else if (queryStr.toLowerCase().contains("commit"))
			executeCommit(queryStr);
		else if (queryStr.toLowerCase().contains("rollback"))
			executeRollback(queryStr);
		
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
