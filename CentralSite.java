import java.rmi.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
public interface CentralSite extends Remote{

	public void getLock(String table, String lockType, String user, Integer tID)throws RemoteException;
	public void releaseLock(String table, String lockType, String user, Integer tID)throws RemoteException;
	public void releaseAllLocks(Integer tID, operationType reason)throws RemoteException;
	public List<Map<String, Object>> queryAll() throws RemoteException;
	public void pushUpdate(String update, Integer siteNum) throws RemoteException;
	public void registerSlave(final RemoteSite myCRemote) throws RemoteException;
	
}
