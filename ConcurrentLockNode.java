import java.lang.*; 
import java.util.*;

public class ConcurrentLockNode {
    //Every transaction has one
    //Initiialized on BEGIN or First attempted lock?
    //and immediately added to GWFG
    //If lock is successfully obtained -> do nothing
    //If not (added to lock queue) update node (sitting in GWFG list) by adding
    //    dependency on whatever transaction currently holds the lock
    //At some interval (somebody added to queue?) run DFS recursively to check for cycle
    //Cycle in graph = Deadlock
    //If deadlock => ABORT/ROLLBACK one transaction (timestamp?) and release associated locks
    //TX must rollback because updates could have already been made locally based on locks already held
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
        //TODO trigger check for deadlock here?
    }
}