import java.lang.*; 

enum transactionType{
	BEGINTRANSACTION, READ, WRITE, ABORT, COMMIT
}

public class Transaction {

	private transactionType transType;
	private String arg;// data item
	private String val;//value to be read or written
	private int tid;
	private String res; //probably shouldn't be string

	Transaction(transactionType tType, String value, int tID, String rest) {
		transType = tType;
		val = value;
		tid = tID;
		res = rest;
	}

	public transactionType getType() {
		return transType;
	}
}