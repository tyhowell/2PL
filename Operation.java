enum operationType{
	BEGINTRANSACTION, READ, WRITE, ABORT, COMMIT
}

public class Operation {

	private operationType opType;
	// data item
	private String arg;
	//value to be read or written
	private String val;
	private Integer tid;
	//result of read or write
	private String res; 
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