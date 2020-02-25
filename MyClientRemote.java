import java.rmi.*;
import java.util.Properties;
import java.rmi.server.*;
import java.sql.*;

public class MyClientRemote extends UnicastRemoteObject implements MyClient{ 

	private final Properties connectionProps;
	private final String url;
	private static MyClientRemote ob;

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
		url = "jdbc:postgresql://128.10.2.13:5432/test";
		connectionProps = new Properties();
		connectionProps.setProperty("user", "remotereader");
		connectionProps.setProperty("password", "bb");
		connectionProps.setProperty("ssl", "false");
		System.out.println("Did this ever get called?");
	}

	public static void main(String args[]){
		
		try{
			//MyClientRemote myCRemote = new MyClientRemote();
			ob = new MyClientRemote();
			//CentralSite stub=(CentralSite)Naming.lookup("rmi://localhost:5000/sonoo");
			CentralSite stub=(CentralSite)Naming.lookup("rmi://128.10.2.13:5000/sonoo");

			System.out.println("Test line 38 MCR");
			stub.registerSlave(ob);
			System.out.println("Test line 40 MCR");

			stub.getLock("student", "read", "192.168.0.1");
			stub.getLock("student", "write", "192.168.0.2");
			stub.releaseLock("student", "read", "192.168.0.1");
			stub.getLock("student", "read", "192.168.0.1");
			stub.releaseLock("student", "write", "192.168.0.2");

		}catch(Exception e){System.out.println(e);}
	}
	public void receiveUpdate(String update) {
		System.out.println("Receiving the update: " + update);
	}

}
