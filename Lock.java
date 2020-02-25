/*
  Ty Howell - CS54200 Spring 2020
  TODO: Implement read and write locks, is logic correct on continuing to add read locks?
*/

import java.lang.*; 
import java.util.*;

public class Lock {

	private List<Transaction> readQueue;
	private List<Transaction> writeQueue;
	private List<Transaction> readHeld;
	private List<Transaction> writeHeld; 
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

	public Boolean getLock(Transaction requestingTransaction) { //TODO eventually not void
		if(requestingTransaction.getType() == transactionType.READ) {
			if (isReadLocked) {
				System.out.println("Read lock is held, you can also read it!");
				//readQueue.add(requestingTransaction);
				numReaders++;
				return true;
			}
			else if(isWriteLocked) {
				System.out.println("Write lock is held, you are added to read queue");
				readQueue.add(requestingTransaction);
				return false;
			}
			else {
				System.out.println("Lock is available, you've got it!");
				isReadLocked = true;
				numReaders++;
				//readHeld.add(requestingTransaction);
				return true;
			}
		}
		else if (requestingTransaction.getType() == transactionType.WRITE) {
			if (isReadLocked || isWriteLocked) {
				System.out.println("Read or write lock is held, you are added to write queue");
				writeQueue.add(requestingTransaction);
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

	public void releaseLock(Transaction requestingTransaction) {
		if(requestingTransaction.getType() == transactionType.READ) {
			//readQueue.remove(requestingTransaction);
			numReaders--;
			if(numReaders == 0){
				isReadLocked = false;
				if(writeQueue.size() > 0) {
					System.out.println("Last read lock released, issuing write lock");
					Transaction firstWriter = writeQueue.remove(0);
					//TODO send requesting write transaction lock
					isWriteLocked = true;
				}
			}	
		}
		else if(requestingTransaction.getType() == transactionType.WRITE) {
			isWriteLocked = false;
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
					Transaction firstReader = readQueue.remove(0);
				}
			}
			else if (writeQueue.size() > 0) {
				System.out.println("Write lock released, issuing next write lock");
				//TODO send requesting write transaction lock
				isWriteLocked = true;
				Transaction firstWriter = writeQueue.remove(0);
			}
		}
	}
	
}