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

import eu.unitn.disi.db.grava.exceptions.DataException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class represents the neighbor tables used in the pruning algorithm and 
 * potentially can be used as similarity metric. 
 * 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public abstract class NeighborTables {   
    protected Map<Long, Map<Long, Integer>[]> levelTables = new HashMap<>();  
    protected int k;

    
    public abstract boolean addNodeLevelTable(Map<Long,Integer> levelNodeTable, long node, short level);
    
    public boolean addNodeTable(Map<Long,Integer>[] nodeTable, Long node) {
        boolean value = true;
        for (short i = 0; i < nodeTable.length; i++) {
            value = addNodeLevelTable(nodeTable[i], node, i) && value;
        }
        return value;
    }
    
    public abstract boolean serialize() throws DataException; 
    
    public abstract Map<Long, Integer>[] getNodeMap(long node);
    
    public Set<Long> getNodes() {
        return levelTables.keySet();
    }
    
    public void merge(NeighborTables tables) {
        Set<Long> nodes = tables.getNodes();
        
        for (Long node : nodes) {
            addNodeTable(tables.getNodeMap(node), node);
        }
    }
    
    @Override
    public abstract String toString();
}
