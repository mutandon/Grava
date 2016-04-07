/*
 * Copyright (C) 2012 Davide Mottin <mottin@disi.unitn.eu>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package eu.unitn.disi.db.grava.vectorization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Stores node neighbour tables as inverted index on labels Node related method
 * need to iterate over all labels, but infors about a single lable are
 * optimized
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 * @see NeighborhoodPruningAlgorithm
 * @see GenerateNeighborTables
 */
public class InvertedIndexNeighborTables extends NeighborTables {

    /**
     * LabelID -> NodesList NodesList: [level]NodeID -> NumLabels
     */
    protected Map<Long, ArrayList<LinkedHashMap<Long, Integer>>> labelIndex;
    protected Set<Long> nodes = new HashSet<>();

    public InvertedIndexNeighborTables(int k) {
        labelIndex = new LinkedHashMap<>();
        this.k = k;
    }

    @Override
    public Set<Long> getNodes() {
        return this.nodes;
    }

    public Set<Long> getLabels(){
        return labelIndex.keySet();
              
    }
    
    /**
     * 
     * @return  the number of labels mapped
     */
    public int size(){
        return this.labelIndex.size();
    }
    
    public boolean hasMapped(long label){
        return this.labelIndex.containsKey(label);
    }
    
    /**
     *
     * @param label
     * @return all the nodes that a specific label
     */
    public Set<Long> getNodesForLabel(Long label) {
        if(!labelIndex.containsKey(label)){
            return null;
        }
        Set<Long> labelNodes = null;
        for (int i = 0; i < this.k; i++) {
            if (labelNodes == null) {
                labelNodes = new HashSet<>(labelIndex.get(label).get(i).keySet());
            } else {
                labelNodes.addAll(labelIndex.get(label).get(i).keySet());
            }
        }
        return labelNodes;
    }

    /**
     *
     * @param label
     * @param level
     * @return all the nodes that a specific label at some level
     */
    public Set<Long> getNodesForLabel(Long label, int level) {
        checkLevel(level);
        if(!labelIndex.containsKey(label)){
            return null;
        }
        return labelIndex.get(label).get(level).keySet();
    }

    /**
     * Adds  <Node, Value> pairs for label at a specific level
     *
     * @param label
     * @param level
     * @param nodesMap
     * @return all the nodes that a specific label at some level
     */
    public boolean addNodesForLabel(Long label, int level, LinkedHashMap<Long, Integer> nodesMap) {
        boolean retval = false;        
        ArrayList<LinkedHashMap<Long, Integer>> labelNodes = labelIndex.get(label);
        if (labelNodes == null) {
            labelNodes = new ArrayList<>(k);
            for (int i = 0; i < k; i++) {
                labelNodes.add(new LinkedHashMap<Long, Integer>());
            }
        }
        Map<Long, Integer> levelMap = labelNodes.get(level);
        for (Entry<Long, Integer> e : nodesMap.entrySet()) {
            retval = retval || null != levelMap.put(e.getKey(), e.getValue());
        }
        return retval;

    }

    
    /**
     * Adds  <Node, Count> pairs for label at a specific level
     *
     * @param label     
     * @param nodesMap List  <Node,Count> map for each level
     * @return all the nodes that a specific label at some level
     */
    public boolean addNodesForLabel(Long label, ArrayList<LinkedHashMap<Long, Integer>> nodesMap) {
        boolean retval = false;        
        ArrayList<LinkedHashMap<Long, Integer>> labelNodes = labelIndex.get(label);
        if (labelNodes == null) {
            labelNodes = new ArrayList<>(k);
            for (int i = 0; i < k; i++) {
                labelNodes.add(new LinkedHashMap<Long, Integer>());
            }
        }
        for (int i = 0; i < k; i++) {
            Map<Long, Integer> levelMap = labelNodes.get(i);
            for (Entry<Long, Integer> e : nodesMap.get(i).entrySet()) {
                if(e.getValue() > 0){
                    this.nodes.add(e.getKey());
                    retval = retval || null != levelMap.put(e.getKey(), e.getValue());
                }
            }            
        }
        return retval;

    }
    
    @Override
    public boolean addNodeLevelTable(Map<Long, Integer> levelNodeTable, long node, short level) {
        if(levelNodeTable.size() <1) {
            return false;
        }
            
        Set<Long> labels = levelNodeTable.keySet();
        ArrayList<LinkedHashMap<Long, Integer>> labelNodes;
        boolean retval = false;        
        boolean added = false;
        for (long label : labels) {
            if(levelNodeTable.get(label)<1){
                //debug("EMPTY NODE VALUE");
                continue;
            }
            //Add the node only if NOT_EMPTY is found
            if(!added){
                this.nodes.add(node);
                added=true;
            }
            labelNodes = labelIndex.get(label);

            if (labelNodes == null) {
                labelNodes = new ArrayList<>(k);
            }
            if (labelNodes.isEmpty()) {
                for (int i = 0; i < k; i++) {
                    labelNodes.add(new LinkedHashMap<Long, Integer>());
                }
            }
            
            retval = retval || null != labelNodes.get(level).put(node, levelNodeTable.get(label)); //node[tab]count
            //debug("Label %s level %s has %s nodes", label, level , labelNodes.get(level).size() );
            labelIndex.put(label, labelNodes);
        }
        return retval;
    }
    
    @Override
    public boolean addNodeTable(List<Map<Long, Integer>> nodeTable, Long node) {
 
        boolean value = false;
        if(nodeTable.size() != this.k){
            throw new IllegalStateException("Node table for "+ node +" has illegal length. Expected "+ this.k+" found "+ nodeTable.size());
        }

        for (short i = 0; i < this.k; i++) {
            value = addNodeLevelTable(nodeTable.get(i), node, i) || value;
        }
        return value;
        
    }
    

    
    
    
    
    
    
    
    
    

    @Override
    public List<Map<Long, Integer>> getNodeMap(long node) {
        if(!nodes.contains(node)){
            return null;
        }
               
        Set<Long> labels = labelIndex.keySet();
        List<Map<Long, Integer>> nodeTable = new ArrayList<>(k); // [level]<label,count>
        for(int i = 0; i<this.k; i++){
            nodeTable.add(new HashMap<Long, Integer>());
        }
        for (Long label : labels) {
            List<LinkedHashMap<Long, Integer>> labelNodes = labelIndex.get(label); //[level]<node,count>

            for (int i = 0; i < this.k; i++) {                
                Integer ct = labelNodes.get(i).get(node);
                nodeTable.get(i).put(label, ct == null ? 0 : ct);
            }

        }

        return nodeTable;
    }


    /**
     * 
     * @param label
     * @return  the list for very label of node-cardinality mappings
     */
    public ArrayList<LinkedHashMap<Long, Integer>> getLabelCounts(long label){
        return this.labelIndex.get(label);
    }
    
    
    /**
     *
     * @param label
     * @param level
     * @return the maximum cardinality for a label at distance
     */
    public int getBestLabelCount(Long label, int level) {        
        return getBestLabelCount(label, level, null);
    }

    /**
     *
     * @param label
     * @param level
     * @param skipList
     * @return the maximum cardinality for a label at distance skipping some
     * nodes
     */
    public int getBestLabelCount(Long label, int level, Collection<Long> skipList) {
        checkLevel(level);
        if(!labelIndex.containsKey(label)){
            return -1;
        }
        Long bestNode = getBestLabelCountNode(label, level, skipList == null ? new ArrayList<Long>(1) : skipList);
        ArrayList<LinkedHashMap<Long, Integer>> al = labelIndex.get(label);
        LinkedHashMap<Long, Integer> l = al.get(level);
        Integer count = l.get(bestNode);

        //throw new NullPointerException("No number for the node "+ bestNode+ " with the best count for label "+label);
        return count == null ? 0 : count;
    }

    /**
     *
     * @param label
     * @param level
     * @param skipList
     * @return
     */
    public Long getBestLabelCountNode(Long label, int level, Collection<Long> skipList) {
        checkLevel(level);
        Long bestNode = null;
        int bestCount = -1;
        HashMap<Long, Integer> levelMap = labelIndex.get(label).get(level);
        if (levelMap.isEmpty()) {
            debug("No node has %s at level %s", label, level);
            return null;
        }
        for (long node : levelMap.keySet()) {
            if (skipList == null || !skipList.contains(node)) {
                if (levelMap.get(node) > bestCount) {
                    bestCount = levelMap.get(node);
                    bestNode = node;
                }
            }
        }
        return bestNode;
    }

    
    public boolean merge(InvertedIndexNeighborTables table) {
        debug(" Inverted Merge");
        ArrayList<LinkedHashMap<Long, Integer>> levels;
        boolean retval = false;
        for (Long label : table.labelIndex.keySet()) {
            levels = table.labelIndex.get(label);
            for (int i = 0; i < this.k; i++) {
                for (Entry<Long, Integer> e : levels.get(i).entrySet()) {
                    if(e.getValue()>0){
                        this.nodes.add(e.getKey());
                        retval = retval || null != this.labelIndex.get(label).get(i).put(e.getKey(), e.getValue());
                    }
                }
            }
        }
        return retval;
    }

    @Override
    public String toString() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

        

    

}
