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

import eu.unitn.disi.db.mutilities.LoggableObject;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class represents the neighbor tables used in the pruning algorithm and
 * potentially can be used as similarity metric.
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public abstract class NeighborTables extends LoggableObject{    
    //Max Depth
    protected int k;


    /**
     * 
     * @return set of nodes mapped in this table
     */
    public abstract Set<Long> getNodes();
    
    /**
     *
     * @param levelNodeTable [ label, count ]
     * @param node
     * @param level
     * @return true if some table has been overwritten
     */
    public abstract boolean addNodeLevelTable(Map<Long,Integer> levelNodeTable, long node, short level);

   
    /**
     * 
     * @param node
     * @return Map [level][ label, count ]
     */
    public abstract List<Map<Long, Integer>> getNodeMap(long node);

    
           
    @Override
    public abstract String toString();
    
    
    /**
     *
     * @param nodeTable [level]<label, count>
     * @param node
     * @return true if it overwrites previous table for node
     */
    public abstract boolean addNodeTable(List<Map<Long,Integer>> nodeTable, Long node);
    
    
    
    /**
     * 
     * @return the Depth(k) at which the tables are computed
     */
    public int getMaxLevel(){
        return this.k;
    }
    
    /**
     * Merge a table into this one
     * @param tables 
     * @return  true if it overwrites any previous table
     */
    public boolean merge(NeighborTables tables) {
        Set<Long> nodes = tables.getNodes();
        boolean value = false;
        for (Long node : nodes) {
           value =  addNodeTable(tables.getNodeMap(node), node) || value;
        }
        return value;
    }

    
    /**
     * Check that the selected level is compatible with the table;
     * @param level 
     */
    protected void checkLevel(int level){
        if(level < 0 || level > this.k){
            throw new IndexOutOfBoundsException("Maximum level is "+this.k+" requsted "+level );
        }        
    }        
    
    
}
