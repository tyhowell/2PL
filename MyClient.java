import java.rmi.*;
public interface MyClient extends Remote{
	
	//public void main(String args[]) throws RemoteException;
	public void receiveUpdate(String update) throws RemoteException;

}
