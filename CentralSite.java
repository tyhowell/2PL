import java.rmi.*;
import java.util.List;
import java.util.Map;
public interface CentralSite extends Remote{

	public Boolean getLock(String query, Integer siteNum, Integer tID)throws RemoteException;
	public void releaseLock(String table, String lockType, Integer siteNum, Integer tID)throws RemoteException;
	public void releaseAllLocks(Integer tID, Integer siteNum, operationType reason)throws RemoteException;
	public List<Map<String, Object>> queryAll() throws RemoteException;
	public void pushUpdate(String update, Integer siteNum) throws RemoteException;
	public void registerSlave(final RemoteSite myCRemote) throws RemoteException;
	public void disconnectSlave(Integer siteNum) throws RemoteException;	
	
}
