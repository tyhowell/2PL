import java.lang.*; 

enum operationType{
	BEGINTRANSACTION, READ, WRITE, ABORT, COMMIT
}

public class Operation {

	private operationType opType;
	private String arg;// data item
	private String val;//value to be read or written
	private Integer tid;
	private String res; //probably shouldn't be string
	public Integer remoteSiteNum;

	Operation(operationType oType, String argument, String value, int tID, String rest, Integer siteNum) {
		opType = oType;
		arg = argument;
		val = value;
		tid = tID;
		res = rest;
		remoteSiteNum = siteNum;
	}

	public operationType getType() {
		return opType;
	}
	public Integer getTid() {
		return tid;
	}
}