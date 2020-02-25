import java.rmi.*;
//import java.rmi.registry.*;

public class MyServer{

	public static void main(String args[]){
		try{

			CentralSite stub=new CentralSiteRemote();
			Naming.rebind("rmi://localhost:5000/sonoo",stub);

		}catch(Exception e){System.out.println(e);}
	}

}
