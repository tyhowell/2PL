import java.rmi.*;
public interface CentralSite extends Remote{

	public void getLock(String table, String lockType, String user)throws RemoteException;
	public void releaseLock(String table, String lockType, String user)throws RemoteException;
	public void queryAll() throws RemoteException;
	public void pushUpdate(String update) throws RemoteException;
	public void registerSlave(final MyClient myCRemote) throws RemoteException;
	
}
