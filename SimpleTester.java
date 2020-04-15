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
 

public class SimpleTester{ 
	/**
	 *  This class is for testing space and time overhead of implementation
	 */


	private static final Logger LOGGER = Logger.getLogger(RemoteSiteImpl.class.getName());
	private String testFile;
	private Properties connectionProps;
	private String url;
    private Connection db;
    private Integer remoteSiteNum;

	SimpleTester(String testFileStr){
		// constructor for parent class
		//super();
		// Loading the Driver
		try {
			Class.forName("org.postgresql.Driver");
		} catch (final ClassNotFoundException e) {
			System.err.println("Couldn't find driver class:");
			e.printStackTrace();
		}

        remoteSiteNum = 0;
        testFile = testFileStr;
		// Establish connection properties for PostgreSQL database
		Integer siteNum = 0;
		url = "jdbc:postgresql://localhost:5432/remotesite" + Integer.toString(siteNum);
		connectionProps = new Properties();
		connectionProps.setProperty("user", "remotereader");
		//char[] password = System.console().readPassword("Input PostgreSQL password: ");
		//connectionProps.setProperty("password", password.toString());
		connectionProps.setProperty("password", "bb");
		connectionProps.setProperty("ssl", "false");
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
			String testFileStr = null;
			if (args.length > 0)
				testFileStr = args[0];
			SimpleTester obj = new SimpleTester(testFileStr);
			obj.doWork();
		} catch(Exception e){}
	}

	private void doWork() {
		try{

			File file = new File("/homes/howell66/cs542/2PL/test/" + testFile); 
			BufferedReader br = new BufferedReader(new FileReader(file)); 
			String st; 
			String queryForMe = "r" + Integer.toString(remoteSiteNum) + ":";
			while ((st = br.readLine()) != null) {
				if (st.contains(queryForMe))
	                queryParser(st.replaceFirst(queryForMe, ""));
			}
			br.close();	
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
			rs = st.executeQuery(queryStr);
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
				//System.out.println("Closed connection to the database.");
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}
	private synchronized void executeUpdate(String queryStr) {
		//open connection
		Statement st = null;
		//Connection db = null;
		try {
			LOGGER.log(Level.INFO, "Update stmt, site: " + Integer.toString(remoteSiteNum) 
				+ " " + queryStr);
			//db = DriverManager.getConnection(url, connectionProps);
			st = db.createStatement();
			//obtain write lock
			LOGGER.log(Level.INFO, "Write lock granted");
			st.executeUpdate(queryStr);
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
				//db.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}

	private void executeBegin(String queryStr) {
		//Make transaction id's globally unique, site 0: 0, 10, 20 ...
		//site 1: 1, 11, 21 ... site 2: 2, 12, 22 ...
		Statement st = null;
		//Connection db = null;
		LOGGER.log(Level.INFO, "Begin transaction, site: " 
			+ Integer.toString(remoteSiteNum) + " " + queryStr);

		try {
			//db = DriverManager.getConnection(url, connectionProps);
			st = db.createStatement();
			st.executeUpdate(queryStr);
		} catch (final Exception e) {
			System.out.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				st.close();
				//db.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}

	private void executeCommit(String queryStr) {
		Statement st = null;
		//Connection db = null;
		try {
			//db = DriverManager.getConnection(url, connectionProps);
			st = db.createStatement();
			LOGGER.log(Level.INFO, "Commit transaction, site: " 
				+ Integer.toString(remoteSiteNum) + " " + queryStr);
			//execute sql
			st.executeUpdate(queryStr);
		} catch (final Exception e) {
			System.out.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				st.close();
				//db.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}
	private void executeRollback(String queryStr) {
		//execute sql
		Statement st = null;
		//Connection db = null;
		try {
			//db = DriverManager.getConnection(url, connectionProps);
			st = db.createStatement();
			LOGGER.log(Level.INFO, "Rollback transaction, site: " 
				+ Integer.toString(remoteSiteNum) + " " + queryStr);
			st.executeUpdate(queryStr);
		} catch (final Exception e) {
			System.out.println("Database exception: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				st.close();
				//db.close();
			} catch (final SQLException sqlErr) {
				sqlErr.printStackTrace();
			}
		}
	}

	private void queryParser(String queryStr) {
		//TODO add support for more complex queries?
		
		if (queryStr.toLowerCase().contains("update"))
			executeUpdate(queryStr);
		else if (queryStr.toLowerCase().contains("insert"))
			executeUpdate(queryStr); 
		else if (queryStr.toLowerCase().contains("delete"))
			executeUpdate(queryStr);
		else if (queryStr.toLowerCase().contains("select"))
			executeRead(queryStr);
		else if (queryStr.toLowerCase().contains("begin")) {
			executeBegin(queryStr);
		}
		else if (queryStr.toLowerCase().contains("commit"))
			executeCommit(queryStr);
		else if (queryStr.toLowerCase().contains("rollback"))
			executeRollback(queryStr);
		else if (queryStr.toLowerCase().contains("shutdown")) {
			LOGGER.info("Parsing shutdown, site: " + Integer.toString(remoteSiteNum));
            disconnect();
		}
		else {
			System.err.println("Illegal or unsupported SQL syntax");
		}
	}

	private void disconnect() {
		try {
			db.close();
		} catch (Exception e) {}
		new Thread() {
			@Override
			public void run() {
			  System.out.print("Shutting down...");
			  try {
				sleep(2000);
			  } catch (Exception e) {}
			  System.out.println("done");
			  System.exit(0);
			}
		  }.start();
		//System.exit(0);
	}
}
