import java.util.*;

public class ConcurrentLockNode {
    public Integer myTID;
    public List<ConcurrentLockNode> dependentOnTheseNodes;
    public Boolean beingTraversed;
    public Boolean traversed;		
 
    public ConcurrentLockNode(Integer transaction_id) {
        this.myTID = transaction_id;
        this.dependentOnTheseNodes = new ArrayList<>();
        this.beingTraversed = false;
        this.traversed = false;
    }

    public void setDependency(ConcurrentLockNode myDependency) {
        this.dependentOnTheseNodes.add(myDependency);
    }
}