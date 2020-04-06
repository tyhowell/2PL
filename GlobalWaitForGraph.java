
import java.lang.*; 
import java.util.*;

public class GlobalWaitForGraph {
    //graph of all transactions
    private List<ConcurrentLockNode> graph_nodes;
    private HashMap<Integer, Integer> tid_to_graph_index;

    public GlobalWaitForGraph() {
        this.graph_nodes = new ArrayList<>();
        this.tid_to_graph_index = new HashMap<>();
    }
    public void add_node(ConcurrentLockNode newNode) {
        this.graph_nodes.add(newNode);
        Integer index = graph_nodes.size() - 1;
        tid_to_graph_index.put(newNode.myTID, index);
    }
    public Boolean transactionExists(Integer tid) {
        // returns true if transaction has already been added to global wait for graph
        return tid_to_graph_index.containsKey(tid);
    }

    public void add_dependency(Integer tid_has_dependency, Integer tid_has_lock) {
        if (tid_has_dependency == tid_has_lock) {
            System.out.println("Error, transaction cannot depend on itself");
            return;
        }
        Integer tid_has_dependency_index = tid_to_graph_index.get(tid_has_dependency);
        Integer tid_has_lock_index = tid_to_graph_index.get(tid_has_lock);
        /*for (Integer i = 0; i < this.graph_nodes.size(); i++) {
            if (this.graph_nodes.get(i).myTID == tid_has_dependency) {
                tid_has_dependency_index = i;
            }
            if (this.graph_nodes.get(i).myTID == tid_has_lock) {
                tid_has_lock_index = i;
            }
        }*/
        if (tid_has_dependency_index == null || tid_has_lock_index == null) {
            System.err.println("Error: Transaction ID not found in Global Wait For Graph");
            return;
        }
        graph_nodes.get(tid_has_dependency_index).setDependency(graph_nodes.get(tid_has_lock_index));
        System.out.println("Adding dependency, TID: " + tid_has_dependency + " depends on TID: " + tid_has_lock);
    }

    public Boolean hasDeadlock() {
        //recursive start of DFS traversal of GWFG
        for (ConcurrentLockNode dependencyNode : graph_nodes) {
            //System.out.println("Initial deadlock call, TID: " + dependencyNode.myTID);
            if (!dependencyNode.traversed && hasDeadlock(dependencyNode)) {
                return true;
            }
        }

        //reset all traversed/beingTraversed to false
        for (ConcurrentLockNode dependencyNode : graph_nodes) {
            dependencyNode.beingTraversed = false;
            dependencyNode.traversed = false;
        }

        return false;
    }
    public Boolean hasDeadlock(ConcurrentLockNode sourceNode) {
        //continued recursive DFS traversal of GWFG
        sourceNode.beingTraversed = true;
        Integer sourceNodeIndex = tid_to_graph_index.get(sourceNode.myTID);
        graph_nodes.get(sourceNodeIndex).beingTraversed = true;

        //System.out.println("hasDeadlock call, TID: " + sourceNode.myTID + " has the following dependencies: ");
        for (ConcurrentLockNode dependencyNode : sourceNode.dependentOnTheseNodes) {
            //System.out.println("dependencies loop, TID: " + dependencyNode.myTID);
            Integer dependencyNodeIndex = tid_to_graph_index.get(dependencyNode.myTID);
            //if (dependencyNode.beingTraversed == true) {
            if (graph_nodes.get(dependencyNodeIndex).beingTraversed == true) {
                // deadlock detected
                return true;
            //} else if (!dependencyNode.traversed && hasDeadlock(dependencyNode)) {
            } else if (!graph_nodes.get(dependencyNodeIndex).traversed && hasDeadlock(dependencyNode)) {
                return true;
            }
        }

        sourceNode.beingTraversed = false;
        graph_nodes.get(sourceNodeIndex).beingTraversed = false;
        sourceNode.traversed = true;
        graph_nodes.get(sourceNodeIndex).traversed = true;
        return false;
    }
}