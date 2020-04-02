/*
  Ty Howell - CS54200 Spring 2020
  Resource/tutorial utilized for cycle detection: https://www.baeldung.com/java-graph-has-a-cycle
*/

import java.lang.*; 
import java.util.*;

public class Lock {

	private List<Operation> readQueue;
	private List<Operation> writeQueue;
	private List<Operation> readHeld;
	private List<Operation> writeHeld; 
	private String tableName;
	private Boolean isReadLocked;
	private Boolean isWriteLocked;
	private Integer numReaders;
	
	
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
		System.out.println("Get current lock holder, iswritelocked isreadlocked writeheldsize readheldsize");
		System.out.println(isWriteLocked + " " + isReadLocked + " " + writeHeld.size() + " " + readHeld.size());
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
				System.out.println("Read lock is held, you can also read it!");
				//readQueue.add(requestingOperation);
				readHeld.add(requestingOperation);
				numReaders++;
				return true;
			}
			else if(isWriteLocked) {
				System.out.println("Write lock is held, you are added to read queue");
				readQueue.add(requestingOperation);

				return false;
			}
			else {
				System.out.println("Lock is available, you've got it!");
				isReadLocked = true;
				readHeld.add(requestingOperation);
				numReaders++;
				//readHeld.add(requestingOperation);
				return true;
			}
		}
		else if (requestingOperation.getType() == operationType.WRITE) {
			if (isReadLocked) {
				System.out.println("Read lock is held, you are added to write queue");
				writeQueue.add(requestingOperation);
				return false;
			} else if (isWriteLocked) {
				System.out.println("Write lock is held, you are added to write queue");
				writeQueue.add(requestingOperation);
				return false;
			}
			else {
				System.out.println("Write lock is available, you've got it!");
				isWriteLocked = true;
				writeHeld.add(requestingOperation);
				return true;
			}
		}
		else {
			System.out.println("Malformed request, please try again");
			return false;
		}
	}

	public List<Operation> releaseLock(Operation requestingOperation) {
		//releases lock from requesting operation
		//returns list of operations whom locks were granted to
		//also removes transaction from any queues (important for rollingback a transaction)
		System.out.println("Lock Manager releasing a lock for TID: " + Integer.toString(requestingOperation.getTid()));
		List<Operation> locksToBeGranted = new ArrayList<>();
		Boolean releasedReader = false;
		Boolean releasedWriter = false;
		ListIterator<Operation> readLockIter = readHeld.listIterator();
		while(readLockIter.hasNext()) {
			//search currently held read locks for transaction ID of operation requesting release
			//int nextOpIndex = readLockIter.nextIndex();
			Operation nextOp = readLockIter.next();
			if (nextOp.getTid() == requestingOperation.getTid()) {
				//found read lock 
				//System.out.println("Found read lock!");
				releasedReader = true;
				//readHeld.remove(nextOpIndex);
				readLockIter.remove();
			}
		}
		ListIterator<Operation> writeLockIter = writeHeld.listIterator();
		System.out.println("writeHeld size: " + writeHeld.size());
		while(writeLockIter.hasNext()) {
			//System.out.println("Looping write locks rqt TID: " + requestingOperation.getTid());
			//search currently held write locks for transaction ID of operation requesting release
			//int nextOpIndex = writeLockIter.nextIndex();
			Operation nextOp = writeLockIter.next();
			if (nextOp.getTid() == requestingOperation.getTid()) {
				//found write lock 
				//System.out.println("Found write lock!");
				releasedWriter = true;
				//writeHeld.remove(nextOpIndex);
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
			//readQueue.remove(requestingOperation);
			numReaders--;
			//System.out.println("Releasing a reader, writeQueue size: " + Integer.toString(writeQueue.size()));
			if(numReaders == 0){
				isReadLocked = false;
				if(writeQueue.size() > 0) {
					//System.out.println("Last read lock released, issuing write lock");
					Operation firstWriter = writeQueue.remove(0);
					writeHeld.add(firstWriter);
					locksToBeGranted.add(firstWriter);
					isWriteLocked = true;
				}
			}	
		}
		else if(releasedWriter) {
			isWriteLocked = false;
			if(readQueue.size() > 0){
				//System.out.println("Write lock released, issuing read locks to everyone");
				while (readQueue.size() > 0) {
					//iterate readQueue, issuing all read locks 
					//TODO is this the algorithm from the book?
					numReaders++;
					isReadLocked = true;
					Operation firstReader = readQueue.remove(0);
					readHeld.add(firstReader);
					locksToBeGranted.add(firstReader);
				}
			}
			else if (writeQueue.size() > 0) {
				//System.out.println("Write lock released, issuing next write lock");
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