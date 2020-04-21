/*
 * Ty Howell - CS54200 Spring 2020
 * Resource/tutorial utilized for cycle detection: https://www.baeldung.com/java-graph-has-a-cycle
*/

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Lock {

	private List<Operation> readQueue;
	private List<Operation> writeQueue;
	private List<Operation> readHeld;
	private List<Operation> writeHeld; 
	private String tableName;
	private Boolean isReadLocked;
	private Boolean isWriteLocked;
	private Integer numReaders;
	private static final Logger LOGGER = Logger.getLogger(Lock.class.getName());
	
	
	Lock(String name) {
		readQueue = new ArrayList<>();
		writeQueue = new ArrayList<>();
		readHeld = new ArrayList<>();
		writeHeld = new ArrayList<>();
		tableName = name;
		isReadLocked = false;
		isWriteLocked = false;
		numReaders = 0;
	}
	
	public String getName() {
		return tableName;
	}

	public List<Integer> getCurrentLockHolder() {
		// returns current lock holder's transaction ID and site number
		List<Integer> currentLockInfo = new ArrayList<>();
		if (isWriteLocked) {
			currentLockInfo.add(writeHeld.get(0).getTid());
			currentLockInfo.add(writeHeld.get(0).remoteSiteNum);	
		} else {
			currentLockInfo.add(readHeld.get(0).getTid());
			currentLockInfo.add(readHeld.get(0).remoteSiteNum);
		}
		return currentLockInfo;
	}

	public Boolean getLock(Operation requestingOperation) { 
		if (lockAlreadyHeld(requestingOperation))
			return true;


		if(requestingOperation.getType() == operationType.READ) {
			if (isReadLocked) {
				LOGGER.log(Level.INFO,"Read lock is held, concurrent read lock granted!");
				readHeld.add(requestingOperation);
				numReaders++;
				return true;
			}
			else if(isWriteLocked) {
				LOGGER.log(Level.INFO,"Write lock is held, you are added to read queue");
				readQueue.add(requestingOperation);

				return false;
			}
			else {
				LOGGER.log(Level.INFO,"Lock is available, you've got it!");
				isReadLocked = true;
				readHeld.add(requestingOperation);
				numReaders++;
				//readHeld.add(requestingOperation);
				return true;
			}
		}
		else if (requestingOperation.getType() == operationType.WRITE) {
			if (isReadLocked) {
				LOGGER.log(Level.INFO,"Read lock is held, you are added to write queue");
				writeQueue.add(requestingOperation);
				return false;
			} else if (isWriteLocked) {
				LOGGER.log(Level.INFO,"Write lock is held, you are added to write queue");
				writeQueue.add(requestingOperation);
				return false;
			}
			else {
				LOGGER.log(Level.INFO,"Write lock is available, you've got it!");
				isWriteLocked = true;
				writeHeld.add(requestingOperation);
				return true;
			}
		}
		else {
			LOGGER.log(Level.WARNING,"Malformed request, please try again");
			return false;
		}
	}

	public List<Operation> releaseLock(Operation requestingOperation) {
		//releases lock from requesting operation
		//returns list of operations whom locks were granted to
		//also removes transaction from any queues (important for rollingback a transaction)
		List<Operation> locksToBeGranted = new ArrayList<>();
		Boolean releasedReader = false;
		Boolean releasedWriter = false;
		ListIterator<Operation> readLockIter = readHeld.listIterator();
		while(readLockIter.hasNext()) {
			//search currently held read locks for transaction ID of operation requesting release
			Operation nextOp = readLockIter.next();
			if (nextOp.getTid() == requestingOperation.getTid()) {
				//found read lock 
				LOGGER.log(Level.INFO,"Releasing read lock");
				releasedReader = true;
				readLockIter.remove();
			}
		}
		ListIterator<Operation> writeLockIter = writeHeld.listIterator();
		//System.out.println("writeHeld size: " + writeHeld.size());
		while(writeLockIter.hasNext()) {
			//search currently held write locks for transaction ID of operation requesting release
			Operation nextOp = writeLockIter.next();
			if (nextOp.getTid() == requestingOperation.getTid()) {
				//found write lock 
				LOGGER.log(Level.INFO,"Releasing write lock");
				releasedWriter = true;
				writeLockIter.remove();
			}
		}
		ListIterator<Operation> writeQueueIter = writeQueue.listIterator();
		while(writeQueueIter.hasNext()) {
			Operation nextOp = writeQueueIter.next();
			if (nextOp.getTid() == requestingOperation.getTid()) {
				//found transaction in write queue
				writeQueueIter.remove();
			}
		}
		ListIterator<Operation> readQueueIter = readQueue.listIterator();
		while(readQueueIter.hasNext()) {
			Operation nextOp = readQueueIter.next();
			if (nextOp.getTid() == requestingOperation.getTid()) {
				//found transaction in write queue
				readQueueIter.remove();
			}
		}

		if(releasedReader) {
			numReaders--;
			if(numReaders == 0){
				isReadLocked = false;
				if(writeQueue.size() > 0 && !isWriteLocked) {
					Operation firstWriter = writeQueue.remove(0);
					writeHeld.add(firstWriter);
					locksToBeGranted.add(firstWriter);
					isWriteLocked = true;
				}
			}	
		}
		if(releasedWriter) {
			isWriteLocked = false;
			if(readQueue.size() > 0){
				while (readQueue.size() > 0) {
					//iterate readQueue, issuing all read locks 
					numReaders++;
					isReadLocked = true;
					Operation firstReader = readQueue.remove(0);
					readHeld.add(firstReader);
					locksToBeGranted.add(firstReader);
				}
			}
			else if (writeQueue.size() > 0) {
				isWriteLocked = true;
				Operation firstWriter = writeQueue.remove(0);
				writeHeld.add(firstWriter);
				locksToBeGranted.add(firstWriter);
			}
		}
		return locksToBeGranted;
	}
	private Boolean lockAlreadyHeld(Operation requestingOperation) {
		if (requestingOperation.getType() == operationType.READ) {
			for (int i = 0; i < readHeld.size(); i++) {
				if (readHeld.get(i).getTid() == requestingOperation.getTid())
					return true;
			}
			for (int i = 0; i < writeHeld.size(); i++) {
				if (writeHeld.get(i).getTid() == requestingOperation.getTid()) {
					isReadLocked = true;
					readHeld.add(requestingOperation);
					return true;
				}
			}
			return false;
		} else {
			for (int i = 0; i < writeHeld.size(); i++) {
				if (writeHeld.get(i).getTid() == requestingOperation.getTid())
					return true;
			}
			for (int i = 0; i < readHeld.size(); i++) {
				if (readHeld.get(i).getTid() == requestingOperation.getTid()) {
					isWriteLocked = true;
					writeHeld.add(requestingOperation);
					return true;
				}
			}
			return false;
		}
	}
}