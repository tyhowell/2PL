import java.rmi.*;
public interface RemoteSite extends Remote{
	
	public void receiveUpdate(String update) throws RemoteException;
	public void lockObtained(operationType opType) throws RemoteException;
	public void disconnect() throws RemoteException;

}
