import java.rmi.*;

public class MyServer{

	public static void main(String args[]){
		try{
			String cleanDB = "true";
			if (args.length > 0)
				cleanDB = args[0];
			CentralSite stub=new CentralSiteImpl(cleanDB);
			Naming.rebind("rmi://localhost:5000/sonoo",stub);

		}catch(Exception e){System.out.println(e);}
	}

}
