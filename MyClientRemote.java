import java.rmi.*;
import java.util.Properties;
import java.rmi.server.*;
import java.sql.*;

public class MyClientRemote extends UnicastRemoteObject implements MyClient{ 

	private final Properties connectionProps;
	private final String url;

	MyClientRemote() throws RemoteException {
		// constructor for parent class
		super();
		// Loading the Driver
		try {
			Class.forName("org.postgresql.Driver");
		} catch (final ClassNotFoundException cnfe) {
			System.err.println("Couldn't find driver class:");
			cnfe.printStackTrace();
		}
		
		
		// Establish connection properties for PostgreSQL database
		url = "jdbc:postgresql://localhost:5432/test";
		connectionProps = new Properties();
		connectionProps.setProperty("user", "remotereader");
		connectionProps.setProperty("password", "bb");
		connectionProps.setProperty("ssl", "false");
	}

	public static void main(String args[]){
		try {
			new MyClientRemote().doWork();
		}
		catch(Exception e){}
	}

	private void doWork() {
		try{
			CentralSite stub=(CentralSite)Naming.lookup("rmi://localhost:5000/sonoo");

			stub.registerSlave(this);

			stub.getLock("student", "read", "192.168.0.1");
			stub.getLock("student", "write", "192.168.0.2");
			stub.releaseLock("student", "read", "192.168.0.1");
			stub.getLock("student", "read", "192.168.0.1");
			stub.releaseLock("student", "write", "192.168.0.2");
			stub.pushUpdate("Need to add a student");

		}catch(Exception e){System.out.println(e);}
	}
	public void receiveUpdate(String update) {
		System.out.println("Slave Receiving the update: " + update);
	}

}
