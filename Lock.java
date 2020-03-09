/*
  Ty Howell - CS54200 Spring 2020
  TODO: Implement read and write locks, is logic correct on continuing to add read locks?
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

	public Boolean getLock(Operation requestingOperation) { //TODO eventually not void
		if(requestingOperation.getType() == operationType.READ) {
			if (isReadLocked) {
				System.out.println("Read lock is held, you can also read it!");
				//readQueue.add(requestingOperation);
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
				numReaders++;
				//readHeld.add(requestingOperation);
				return true;
			}
		}
		else if (requestingOperation.getType() == operationType.WRITE) {
			if (isReadLocked || isWriteLocked) {
				System.out.println("Read or write lock is held, you are added to write queue");
				writeQueue.add(requestingOperation);
				return false;
			}
			else {
				System.out.println("Write lock is available, you've got it!");
				isWriteLocked = true;
				return true;
			}
		}
		else {
			System.out.println("Malformed request, please try again");
			return false;
		}
	}

	public void releaseLock(Operation requestingOperation) {
		if(requestingOperation.getType() == operationType.READ) {
			//readQueue.remove(requestingOperation);
			numReaders--;
			if(numReaders == 0){
				isReadLocked = false;
				if(writeQueue.size() > 0) {
					System.out.println("Last read lock released, issuing write lock");
					Operation firstWriter = writeQueue.remove(0);
					//TODO send requesting write transaction lock
					isWriteLocked = true;
				}
			}	
		}
		else if(requestingOperation.getType() == operationType.WRITE) {
			isWriteLocked = false;
			//TODO add timestamps to lock requests
			//TODO Compare timestamps of first reader and first writer 
			//earliest timestamp gets lock - is this the book algorithm?
			if(readQueue.size() > 0){
				System.out.println("Write lock released, issuing read locks to everyone");
				while (readQueue.size() > 0) {
					//iterate readQueue, issuing all read locks 
					//TODO is this the algorithm from the book?
					//TODO send requesting read transactions the lock
					numReaders++;
					isReadLocked = true;
					Operation firstReader = readQueue.remove(0);
				}
			}
			else if (writeQueue.size() > 0) {
				System.out.println("Write lock released, issuing next write lock");
				//TODO send requesting write transaction lock
				isWriteLocked = true;
				Operation firstWriter = writeQueue.remove(0);
			}
		}
	}
	
}