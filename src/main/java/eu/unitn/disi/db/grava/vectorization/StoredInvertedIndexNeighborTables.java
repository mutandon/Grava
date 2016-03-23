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



import eu.unitn.disi.db.grava.vectorization.storage.StorableTable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

/**
 * Class that embeds the neighbor tables to be used in the neighborhood pruning
 * algorithm
 *
 * Structure of The index: - 1 file for each label-distance - N files, N =
 * |labels| - each file contains First Long and then array of integers
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 * @see NeighborhoodPruningAlgorithm
 * @see GenerateNeighborTables
 */
public class StoredInvertedIndexNeighborTables extends InvertedIndexNeighborTables {

    private final String dirPath;

    /**
     * LabelID -> NodesList NodesList: NodeID -> NumLabels
     */
    //protected Map<Long, ArrayList<LinkedHashMap<Long, Integer>>> labelIndex;
    /**
     *
     * @param k
     * @param dirPath
     * @throws java.io.IOException
     */
    public StoredInvertedIndexNeighborTables(int k, String dirPath) throws IOException {
        super(k);
        // First, make sure the path exists
        File savingDir = new File(dirPath);
        // This will tell you if it is a directory
        if (!savingDir.exists() || !savingDir.isDirectory() || !savingDir.canWrite()) {
            throw new IOException("Illegal directory path");
        }
        this.dirPath = dirPath;
    }

    

    public boolean hasStored(long label) throws IOException{
        StorableTable tb;
        
        tb = new StorableTable(dirPath, label, 0);
                
        return tb.isStored();
    }
    

    
    /**
     * load label table from file and overwrites to current index
     * @param label
     * @return true if label has been successfully loaded from file
     * @throws IOException 
     */
    public boolean load(Long label) throws IOException {
        StorableTable tb;
        ArrayList<LinkedHashMap<Long, Integer>> toAdd = new ArrayList<>();
        

        for (int i = 0; i < this.k; i++) {
            tb = new StorableTable(dirPath, label, i);
            //TODO: Parallel
            if(!tb.load()){
                return false;
            }
            
            nodes.addAll(tb.getNodes());
            toAdd.add(tb.getNodesMap());
        }
        
        labelIndex.put(label, toAdd);
        ///////
        return true;
    }
    
    public void store() throws IOException{
        StorableTable tb;
        ArrayList<LinkedHashMap<Long, Integer>> levels;
        int lv;
        for (Entry<Long, ArrayList<LinkedHashMap<Long, Integer>>> e : this.labelIndex.entrySet()) {
            levels = e.getValue();
            lv = 0;
            for (LinkedHashMap<Long, Integer> table : levels) {
                tb = new StorableTable(dirPath, e.getKey(), lv);
                tb.putAll(table);
                tb.save();                
            }            
        }               
    }
    
    
    
    /**
     *
     * @param label
     * @return null if loading raised an exception or table has not been stored
     */
    private ArrayList<LinkedHashMap<Long, Integer>> getOrLoad(Long label) {
        try {
            if (!labelIndex.containsKey(label)) {
               if(!this.load(label)){
                   return null;
               }
            }
        } catch (IOException ex) {
            error("Could not load table file", ex);
            return null;
        }
        return labelIndex.get(label);
    }

    
    
    
}
