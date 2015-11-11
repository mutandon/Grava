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

import eu.unitn.disi.db.mutilities.exceptions.DataException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class that embeds the neighbor tables to be used in the neighborhood pruning
 * algorithm
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 * @see NeighborhoodPruningAlgorithm
 * @see GenerateNeighborTables
 */
@PrimitiveObject
public class InvertedIndexNeighborTables extends NeighborTables {
    protected Map<Long, ArrayList<LinkedHashMap<Long, Integer>>> labelIndex;

    protected int minFrequency;

    public InvertedIndexNeighborTables(int k) {
        this();
        this.k = k;
    }

    InvertedIndexNeighborTables() {
        labelIndex = new LinkedHashMap<>();
    }

    public void setMinFrequency(int minFrequency) {
        this.minFrequency = minFrequency;
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
    public boolean serialize() throws DataException {
       throw new UnsupportedOperationException("Not supported yet.");
    }


    @Override
    public Map<Long, Integer>[] getNodeMap(long node) {
        Set<Long> labels = labelIndex.keySet();
        List<HashMap<Long, Integer>> nodeTable = new ArrayList<>(k); // [level]<label,count>

        for(Long label : labels ){
            List<LinkedHashMap<Long, Integer>> labelNodes = labelIndex.get(label); //[level]<node,count>

            for(int i = 0; i < labelNodes.size(); i++ ){
                HashMap<Long, Integer> labelCounts = nodeTable.get(i);
                if( labelCounts == null){
                    labelCounts = new HashMap<>();
                    nodeTable.set(i, labelCounts);
                }
                Integer ct =  labelNodes.get(i).get(node);
                labelCounts.put(label, ct == null ? 0 : ct);
            }

        }


        return nodeTable.toArray(new Map[k]);
    }

    public Set<Long> getNodesForLabel(Long label, int level){
        return labelIndex.get(label).get(level).keySet();
    }

    public int getBestLabelCount(Long label, int level){
     return getBestLabelCount(label, level, null);
    }



    public int getBestLabelCount(Long label, int level, Collection<Long> skipList){
        skipList = skipList == null ? new ArrayList<Long>(1) : skipList;
        Long bestNode = getBestLabelCountNode(label, level, skipList);
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
}
