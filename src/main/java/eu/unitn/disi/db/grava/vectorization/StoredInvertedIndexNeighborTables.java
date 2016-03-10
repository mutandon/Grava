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

import eu.unitn.disi.db.mutilities.LoggableObject;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class that embeds the neighbor tables to be used in the neighborhood pruning
 * algorithm
 * 
 * Structure of The index:
 *  - 1 file for each label-distance 
 *  - N files, N = |labels| 
 *  - each file contains First Long and then array of integers
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 * @see NeighborhoodPruningAlgorithm
 * @see GenerateNeighborTables
 */
@PrimitiveObject
public class StoredInvertedIndexNeighborTables extends NeighborTables {
    protected Map<Long, ArrayList<LinkedHashMap<Long, Integer>>> labelIndex;

    

    public StoredInvertedIndexNeighborTables(int k) {
        labelIndex = new LinkedHashMap<>();
        this.k = k;
    }

   

    
    @Override
    public boolean addNodeLevelTable(Map<Long, Integer> levelNodeTable, long node, short level) {
        Set<Long> labels = levelNodeTable.keySet();
        ArrayList<LinkedHashMap<Long, Integer>> labelNodes;
        boolean retval = true;
        for (Long label : labels) {
            labelNodes = labelIndex.get(label);

            if (labelNodes == null) {
                labelNodes = new ArrayList<>(k);
            }
            if(labelNodes.isEmpty()){
                for (int i = 0; i < k; i++) {
                    labelNodes.add(new LinkedHashMap<Long, Integer>());
                }
            }

            labelNodes.get(level).put(node, levelNodeTable.get(label)); //node[tab]count
            labelIndex.put(label, labelNodes);
        }
        return retval;
    }

    


    @Override
    public List<Map<Long, Integer>> getNodeMap(long node) {
        Set<Long> labels = labelIndex.keySet();
        List<Map<Long, Integer>> nodeTable = new ArrayList<>(k); // [level]<label,count>

        for(Long label : labels ){
            List<LinkedHashMap<Long, Integer>> labelNodes = labelIndex.get(label); //[level]<node,count>

            for(int i = 0; i < labelNodes.size(); i++ ){
                Map<Long, Integer> labelCounts = nodeTable.get(i);
                if( labelCounts == null){
                    labelCounts = new HashMap<>();
                    nodeTable.set(i, labelCounts);
                }
                Integer ct =  labelNodes.get(i).get(node);
                labelCounts.put(label, ct == null ? 0 : ct);
            }

        }

        return nodeTable;
    }

    public Set<Long> getNodesForLabel(Long label, int level){
        return labelIndex.get(label).get(level).keySet();
    }

    public int getBestLabelCount(Long label, int level){
     return getBestLabelCount(label, level, null);
    }



    public int getBestLabelCount(Long label, int level, Collection<Long> skipList){
        
        Long bestNode = getBestLabelCountNode(label, level, skipList == null ? new ArrayList<Long>(1) : skipList);
//        if(bestNode == null){
//            debug("")
//        }
        ArrayList<LinkedHashMap<Long, Integer>> al = labelIndex.get(label);
        LinkedHashMap<Long, Integer>  l = al.get(level);
        Integer count = l.get(bestNode);
        if(count == null){
            //throw new NullPointerException("No number for the node "+ bestNode+ " with the best count for label "+label);
            return 0;
        }

        return  count;
    }

    public Long getBestLabelCountNode(Long label, int level, Collection<Long> skipList){
        Long bestNode = null;
        int bestCount = -1;
        HashMap<Long, Integer> levelMap = labelIndex.get(label).get(level);
        if(levelMap.isEmpty()){
            debug("No node has %s at level %s", label, level );
            return null;
        }
        for(long node : levelMap.keySet()){
            if(skipList == null || !skipList.contains(node)){
                if( levelMap.get(node)> bestCount){
                    bestCount =  levelMap.get(node);
                    bestNode = node;
                }
            }
        }
        return  bestNode;
    }



    @Override
    public String toString() {                       
        return "";
    }
    
    
    private static class IndexedLevelTable extends LoggableObject implements Serializable{

        private static final long serialVersionUID = 1L;
        
        public long firstNode;
        public int numNodes;
        public LinkedList<Long> nodes;
        
        public IndexedLevelTable(){
            nodes = new LinkedList<>();
        }
        
        public IndexedLevelTable(String path){
            throw new UnsupportedOperationException("Not implemented yet");
        }
        
        public int addNode(long toInsert){                                 
            int counter = 0;
            for (long cur : nodes) {
                if(cur>toInsert){
                    break;
                }
                counter++;
            }
            nodes.add(counter, toInsert);

            return counter;
        }
        
        public boolean serialize(String path){
            FileOutputStream fileOut;
            ObjectOutputStream objOut;
            int[] toSerialize = new int[nodes.size()+3];
 
            int[] first = split(nodes.getFirst());
            toSerialize[0] = nodes.size();
            toSerialize[1] = first[0];
            toSerialize[2] = first[1];
            
            
            try {
                fileOut = new FileOutputStream(path);
                objOut = new ObjectOutputStream(fileOut);
                objOut.writeObject(toSerialize);
                objOut.close();
                fileOut.close();
            } catch (FileNotFoundException ex) {
                error("Cannot serialize table",  ex);
                return false;
            } catch (IOException ex) {
                error("Cannot serialize table", ex);
                return false;
            }

            return true;
        }
        
        
        public static int[] split(long toSplit){
            int[] splitted = new int[2];
            
            splitted[0] = (int)(toSplit >> 32);
            splitted[1] = (int)toSplit;  
     
            return splitted;
        }
        
        public static long join(int left, int right){
            return    (long)left << 32 | right & 0xFFFFFFFFL;
        }
        
    }
    
}
