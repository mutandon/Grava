/*
 * Copyright (C) 2013 Davide Mottin <mottin@disi.unitn.eu>
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class MemoryNeighborTables extends NeighborTables {
    
    /**
     * levelTables: NodeID -> LabelsList
     *              LabelsList: LabelId -> NumLabels
     */
    protected Map<Long, List<Map<Long, Integer>>> levelTables = new HashMap<>();

    public MemoryNeighborTables(int k) {
        this.k = k;
        this.levelTables = new HashMap<>();
    }

    @Override
    public Set<Long> getNodes() {
        return levelTables.keySet();
    }
    
    @Override
    public boolean addNodeLevelTable(Map<Long, Integer> levelNodeTable, long node, short level) {
        List<Map<Long, Integer>> nodeTable = levelTables.get(node);
        if (nodeTable == null) {
            nodeTable = new ArrayList<>(k);
        }
        nodeTable.set(level, levelNodeTable);
        return levelTables.put(node, nodeTable) != null;
    }

    @Override
    public List<Map<Long, Integer>> getNodeMap(long node) {
        return levelTables.get(node);
    }

    
   

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        List<Map<Long, Integer>> tables;
        Map<Long, Integer> levelTable;
        for (Long l : levelTables.keySet()) {
            sb.append("Node: ").append(l).append("\n");
            tables = levelTables.get(l);
            for (int i = 0; i < tables.size(); i++) {
                sb.append("[").append(i+1).append("] {");
                levelTable = tables.get(i);
                for (Long label : levelTable.keySet()) {
                    sb.append("(").append(label).append(",").append(levelTable.get(label)).append(")");
                }
                sb.append("}\n");
            }
        }
        return sb.toString();
    }

    @Override
    public boolean addNodeTable(List<Map<Long, Integer>> nodeTable, Long node)  {
        boolean value = false;
        if(nodeTable.size() != this.k){
            throw new IllegalStateException("Node table for "+ node +" has illegal length. Expected "+ this.k+" found "+ nodeTable.size());
        }

        for (short i = 0; i < this.k; i++) {
            value = addNodeLevelTable(nodeTable.get(i), node, i) || value;
        }
        return value;
    }

    


    
}
