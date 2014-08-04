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

import eu.unitn.disi.db.grava.exceptions.DataException;
import eu.unitn.disi.db.grava.utils.Utilities;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Class tha embeds the neighbor tables to be used in the neighborhoo pruning 
 * algorithm
 * 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 * @see NeighborhoodPruningAlgorithm
 * @see GenerateNeighborTables
 */
@PrimitiveObject
public class DirectoryNeighborTables extends NeighborTables {
    private File indexDirectory;
    private Map<Long, StringBuilder[]> labelIndex;
    private static final int PAGE_SIZE = 1024;
    private int minFrequency;
    private int k;
    
    public DirectoryNeighborTables(File indexDirectory, int k) {
        this();
        this.indexDirectory = indexDirectory;
        this.k = k;
    }

    private DirectoryNeighborTables() {
        labelIndex = new HashMap<Long, StringBuilder[]>();
    }

    public void setIndexDirectory(File indexDirectory) {
        this.indexDirectory = indexDirectory;
    }

    public void setMinFrequency(int minFrequency) {
        this.minFrequency = minFrequency;
    }
    
    @Override
    public boolean addNodeLevelTable(Map<Long, Integer> levelNodeTable, long node, short level) {
        Set<Long> labels = levelNodeTable.keySet();
        StringBuilder[] labelNodes;
        boolean retval = true;
        for (Long label : labels) {
            labelNodes = labelIndex.get(label);
            if (labelNodes == null) {
                labelNodes = new StringBuilder[k];
            }
            if (labelNodes[level] == null) {
                labelNodes[level] = new StringBuilder();
            }
            labelNodes[level].append(node).append('\t').append(levelNodeTable.get(label)).append('\n'); //node[tab]count
            labelIndex.put(label, labelNodes);
        }
        return retval;
    }
    
    @Override
    public boolean serialize() throws DataException {
        Set<Long> labels = labelIndex.keySet();
        File labelDirectory;
        StringBuilder[] nodes;
        BufferedWriter writer = null;
        int i;

        try {
            for (Long label : labels) {
                labelDirectory = new File(indexDirectory, label.toString());
                if (labelDirectory.mkdir()) {
                    Logger.getLogger(DirectoryNeighborTables.class.getCanonicalName()).finest(String.format("Label directory %s created, for label %d", labelDirectory.getCanonicalPath(), label));
                }
                nodes = labelIndex.get(label);
                for (i = 0; i < nodes.length; i++) {
                    if (nodes[i] != null) {
                        //labelDirectory = new File(labelDirectory, (i + 1) + "");
                        //labelDirectory.mkdir();
                        writer = new BufferedWriter(new FileWriter(labelDirectory.getCanonicalPath() + File.separator + (i + 1), true), PAGE_SIZE * PAGE_SIZE);
                        writer.append(nodes[i].toString());
                        Utilities.close(writer);
                    }
                }
            }
        } catch (IOException ex) {
            throw new DataException(ex);
        } finally {
            Utilities.close(writer);
        }
        labelIndex = new HashMap<Long, StringBuilder[]>();
        return true;
    }


    @Override
    public Map<Long, Integer>[] getNodeMap(long node) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String toString() {
        return "";
    }
}
