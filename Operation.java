import java.lang.*; 

enum operationType{
	BEGINTRANSACTION, READ, WRITE, ABORT, COMMIT
}

public class Operation {

	private operationType opType;
	private String arg;// data item
	private String val;//value to be read or written
	private int tid;
	private String res; //probably shouldn't be string

	Operation(operationType oType, String value, int tID, String rest) {
		opType = oType;
		val = value;
		tid = tID;
		res = rest;
	}

	public operationType getType() {
		return opType;
	}
}