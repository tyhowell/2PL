import java.rmi.*;
public interface RemoteSite extends Remote{
	
	public void receiveUpdate(String update) throws RemoteException;
	public void lockObtained(String lockType) throws RemoteException;

}
