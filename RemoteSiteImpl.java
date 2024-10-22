/*
 * Ty Howell - CS54200 Spring 2020
*/
import java.rmi.*;
import java.util.Properties;
import java.rmi.server.*;
import java.sql.*;
import java.net.InetAddress;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.Map;
import java.io.Console;
import java.io.*;
 

public class RemoteSiteImpl extends UnicastRemoteObject implements RemoteSite{ 
	/**
	 *  Idea for shutting down with new thread came from https://stackoverflow.com/questions/241034/how-to-remotely-shutdown-a-java-rmi-server
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
	//name of testfile to be executed
	private String testFile;
	private Boolean slow_time;
	//hold name of db and uri
	private Properties connectionProps;
	private String url;
	private InetAddress localAddress;
	//reference to Central Site for RMI
	private CentralSite stub;
	//Remote Site Number, assigned at start from command line
	private Integer remoteSiteNum;
	private Boolean withinTransaction;
	private Boolean currentTransactionAborted;
	private Boolean obtainedReadLock;
	private Boolean obtainedWriteLock;
	//single connection to Postgres server maintained while RemoteSite is running
	private Connection db;
	//list of all queries within transaction in case of rollback
	private List<String> updatesWithinTransaction;
	//ID of current transaction
	private Integer activeTransaction;
	private Integer nextTransactionID;
	private List<LockInfo> held_locks;
	private Integer beginTransactionInputFileIndex;
	private Integer currentQueryInputFileIndex;

	RemoteSiteImpl(int siteNum, String testFileStr, String slowTime) throws RemoteException {
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
		currentTransactionAborted = false;
		obtainedReadLock = true;
		obtainedWriteLock = true;
		activeTransaction = -1;
		beginTransactionInputFileIndex = 0;
		currentQueryInputFileIndex = 0;
		// Establish connection properties for PostgreSQL database
		url = "jdbc:postgresql://localhost:5432/remotesite" + Integer.toString(siteNum);
		remoteSiteNum = siteNum;
		connectionProps = new Properties();
		connectionProps.setProperty("user", "remotereader");
		// take input from commandline or hardcoded password for ease of testing
		//char[] password = System.console().readPassword("Input PostgreSQL password: ");
		//connectionProps.setProperty("password", password.toString());
		connectionProps.setProperty("password", "bb");
		connectionProps.setProperty("ssl", "false");
		if (slowTime.contains("true")) {
			slow_time = true;
		} else {
			slow_time = false;
		}
		try {
			db = DriverManager.getConnection(url, connectionProps);
		} catch (Exception e) {
			System.err.println("Unable to connect to database");
			e.printStackTrace();
			System.exit(-1);
		}
		
	}

	public static void main(String args[]){
		try {
			int siteNum = 0;
			String testFileStr = null;
			String slowTime = "false";
			if (args.length > 0)
				siteNum = Integer.parseInt(args[0]);
				testFileStr = args[1];
				if (args.length > 2)
					slowTime = args[2];
			RemoteSiteImpl obj = new RemoteSiteImpl(siteNum, testFileStr, slowTime);
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
			List<String> inputFile = new ArrayList<>();
			while ((st = br.readLine()) != null) {
				inputFile.add(st);
			}
			br.close();	
			while (currentQueryInputFileIndex < inputFile.size()) {
				currentQueryInputFileIndex++;
				//since index gets incremented early, must substract 1 to be on 'current' index
				if (inputFile.get(currentQueryInputFileIndex - 1).contains(queryForMe)) {
					queryParser(inputFile.get(currentQueryInputFileIndex - 1).replaceFirst(queryForMe, ""));
					//5 second sleep to observe transaction locking in human time
					if (slow_time) {
						Thread.sleep(5000);
					}
				}	
			} 
		} catch(Exception e){
			System.err.println(e);
			e.printStackTrace();
		}
	}

	private synchronized void executeRead(String queryStr) {
		Statement st = null;
		ResultSet rs = null;
		try {
			LOGGER.log(Level.INFO, "Read query, site: " + Integer.toString(remoteSiteNum) 
				+ " " + queryStr);
			st = db.createStatement();
			obtainedReadLock = stub.getLock(queryStr, remoteSiteNum, activeTransaction);
			if (currentTransactionAborted) {
				st.close();
				//reset Boolean
				currentTransactionAborted = false;
				return;
			}
			while (!obtainedReadLock) {
				try{
					LOGGER.log(Level.INFO, "Waiting for read lock");
					wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt(); 
					LOGGER.log(Level.SEVERE, "Thread interrupted");
				}
			}
			LOGGER.log(Level.INFO, "Read lock granted");
			rs = st.executeQuery(queryStr);
			if (withinTransaction) {
				//add lock to list of to be released on commit/abort
				held_locks.add(new LockInfo("student", "read", localAddress.toString(), activeTransaction));
			} else {
				stub.releaseLock("student", "read", remoteSiteNum, activeTransaction);
			}
			int element = 1;

			// Print first two columns in the table for debugging
			while (rs.next()) {
				System.out.print("Element " + element + ": ");
				System.out.print(rs.getString(1));
				System.out.print(" ");
				System.out.print(rs.getString(2));
				System.out.print("\n");
				element = element + 1;
			}
		} catch (final Exception e) {
			System.err.println("DatabaseTest exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			// Close the ResultSet and the Statement variables
			try {
				rs.close();
				st.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}
	private synchronized void executeUpdate(String queryStr) {
		Statement st = null;
		try {
			LOGGER.log(Level.INFO, "Update stmt, site: " + Integer.toString(remoteSiteNum) 
				+ " " + queryStr);
			st = db.createStatement();
			//obtain write lock
			obtainedWriteLock = stub.getLock(queryStr, remoteSiteNum, activeTransaction);
			if (currentTransactionAborted) {
				st.close();
				currentTransactionAborted = false;
				return;
			}
			while (!obtainedWriteLock) {
				try{
					LOGGER.log(Level.INFO, "Waiting for write lock");
					wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt(); 
					LOGGER.log(Level.SEVERE, "Thread interrupted");
				}
			}
			LOGGER.log(Level.INFO, "Write lock granted");
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
			System.err.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				st.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}

	private void executeBegin(String queryStr) {
		//Make transaction id's globally unique, site 0: 0, 10, 20 ...
		//site 1: 1, 11, 21 ... site 2: 2, 12, 22 ...
		Integer tID = (nextTransactionID * 10) + remoteSiteNum;
		activeTransaction = tID;
		nextTransactionID = nextTransactionID + 1;
		withinTransaction = true;

		if (updatesWithinTransaction.size() > 0)
			updatesWithinTransaction.clear();
		if (held_locks.size() > 0)
			held_locks.clear();

		Statement st = null;
		LOGGER.log(Level.INFO, "Begin transaction, site: " 
			+ Integer.toString(remoteSiteNum) + " " + queryStr);

		try {
			st = db.createStatement();
			st.executeUpdate(queryStr);
		} catch (final Exception e) {
			System.err.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				st.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}

	private void executeCommit(String queryStr) {
		Statement st = null;
		try {
			st = db.createStatement();
			LOGGER.log(Level.INFO, "Commit transaction, site: " 
				+ Integer.toString(remoteSiteNum) + " " + queryStr);
			//execute sql
			st.executeUpdate(queryStr);
			//push to master one at a time
			ListIterator<String> queryIter = updatesWithinTransaction.listIterator();
			while(queryIter.hasNext()) {
				stub.pushUpdate(queryIter.next(), remoteSiteNum);
			}	
			//release all locks
			stub.releaseAllLocks(activeTransaction, remoteSiteNum, operationType.COMMIT);
			withinTransaction = false;
		} catch (final Exception e) {
			System.err.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				st.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
		withinTransaction = false;

	}
	private void executeRollback(String queryStr) {
		//execute sql
		Statement st = null;
		try {
			st = db.createStatement();
			LOGGER.log(Level.INFO, "Rollback transaction, site: " 
				+ Integer.toString(remoteSiteNum) + " " + queryStr);
			st.executeUpdate(queryStr);
			//release all locks
			stub.releaseAllLocks(activeTransaction, remoteSiteNum, operationType.ABORT);
			withinTransaction = false;
		} catch (final Exception e) {
			System.err.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				st.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
		withinTransaction = false;
	}

	public void abortCurrentTransaction() {
		LOGGER.log(Level.INFO, "Rollback current transaction, site: " 
			+ Integer.toString(remoteSiteNum)
			+ " file index set back to " + Integer.toString(beginTransactionInputFileIndex));
		Statement st = null;
		try {
			st = db.createStatement();
			st.executeUpdate("ROLLBACK");
			withinTransaction = false;
		} catch (final Exception e) {
			System.err.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				st.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
		currentTransactionAborted = true;
		currentQueryInputFileIndex = beginTransactionInputFileIndex;
	}
	
	public void receiveUpdate(String update) {
	/*  Updates come from central site
	 *  Currently passed as in order SQL statements vice results
	 */
		LOGGER.log(Level.INFO, "Update from Master: " + update);

		Statement st = null;
		try {
			st = db.createStatement();
			st.executeUpdate(update);
		} catch (final Exception e) {
			System.err.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			// Close the ResultSet and the Statement variables
			try {
				st.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}

	public synchronized void lockObtained(operationType opType) {
		LOGGER.log(Level.INFO, "Lock granted!");
		if (opType == operationType.READ)
			obtainedReadLock = true;
		else if (opType == operationType.WRITE)
			obtainedWriteLock = true;
		//since we wait for one lock at a time we now wake the thread that
		//is sleeping while waiting for the lock
		notify();
	}

	private void queryParser(String queryStr) {		
		if (queryStr.toLowerCase().contains("update"))
			executeUpdate(queryStr);
		else if (queryStr.toLowerCase().contains("insert"))
			executeUpdate(queryStr); 
		else if (queryStr.toLowerCase().contains("delete"))
			executeUpdate(queryStr);
		else if (queryStr.toLowerCase().contains("select"))
			executeRead(queryStr);
		else if (queryStr.toLowerCase().contains("begin")) {
			beginTransactionInputFileIndex = currentQueryInputFileIndex - 1;
			executeBegin(queryStr);
		}
		else if (queryStr.toLowerCase().contains("commit"))
			executeCommit(queryStr);
		else if (queryStr.toLowerCase().contains("rollback"))
			executeRollback(queryStr);
		else if (queryStr.toLowerCase().contains("shutdown")) {
			LOGGER.info("Parsing shutdown, site: " + Integer.toString(remoteSiteNum));
			try {
				stub.disconnectSlave(remoteSiteNum);
			} catch (Exception e) {}
		}
		else {
			System.err.println("Illegal or unsupported SQL syntax");
		}
	}

	private void clearAndCopy() {
		/* Deletes all rows from tables student, job
		*  Has master query all and inserts all results into table
		*/
		Statement st = null;
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		try {
			st = db.createStatement();
			//clear table
			st.executeUpdate("DELETE FROM student");
			st.executeUpdate("DELETE FROM job");
			//get resultset from central site
			resultList = stub.queryAll("student");
			Map<String, Object> row = null;	
			for (int i = 0; i < resultList.size(); i++) {
				row = resultList.get(i);
				String studentID = row.get("id").toString();
				String studentName = row.get("name").toString();
				st.executeUpdate("INSERT INTO student values (" + studentID + ", '" + studentName + "')");
			}
			resultList = stub.queryAll("job");
			for (int i = 0; i < resultList.size(); i++) {
				row = resultList.get(i);
				String jobID = row.get("job_id").toString();
				String jobName = row.get("job_name").toString();
				st.executeUpdate("INSERT INTO job values (" + jobID + ", '" + jobName + "')");
			}
			
		} catch (final Exception e) {
			System.err.println("DatabaseTest exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			// Close the ResultSet and the Statement variables
			try {
				st.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}

	public void disconnect() {
		try {
			db.close();
			Naming.unbind("rmi://localhost:5000/sonoo");
		} catch (Exception e) {}
		new Thread() {
			@Override
			public void run() {
			  LOGGER.log(Level.INFO, "Shutting down...");
			  try {
				sleep(2000);
			  } catch (Exception e) {}
			  LOGGER.log(Level.INFO, "complete");
			  System.exit(0);
			}
		  }.start();
	}
}
