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
import eu.unitn.disi.db.mutilities.CollectionUtilities;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Class that embeds the neighbor tables to be used in the neighborhood pruning
 * algorithm
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 * @see NeighborhoodPruningAlgorithm
 * @see GenerateNeighborTables
 */
@PrimitiveObject
public class DirectoryNeighborTables extends InvertedIndexNeighborTables {
    private File indexDirectory;
    public static final int PAGE_SIZE = 1024;

    public DirectoryNeighborTables(File indexDirectory, int k) {
        super(k);
        this.indexDirectory = indexDirectory;
    }



    public void setIndexDirectory(File indexDirectory) {
        this.indexDirectory = indexDirectory;
    }




    @Override
    public boolean serialize() throws DataException {
        Set<Long> labels = labelIndex.keySet();
        File labelDirectory;
        List<LinkedHashMap<Long, Integer>> nodeMaps;
        BufferedWriter writer = null;
        int i;

        try {
            for (Long label : labels) {
                labelDirectory = new File(indexDirectory, label.toString());
                if (labelDirectory.mkdir()) {
                    Logger.getLogger(DirectoryNeighborTables.class.getCanonicalName()).finest(String.format("Label directory %s created, for label %d", labelDirectory.getCanonicalPath(), label));
                }
                nodeMaps = labelIndex.get(label);

                for (i = 0; i < nodeMaps.size(); i++) {
                    final LinkedHashMap<Long, Integer> nodeCounts = nodeMaps.get(i);
                    if ( nodeCounts != null) {
                        //labelDirectory = new File(labelDirectory, (i + 1) + "");
                        //labelDirectory.mkdir();
                        StringBuilder bs = new StringBuilder();
                        for(Long nde : nodeCounts.keySet()){
                            writer = new BufferedWriter(new FileWriter(labelDirectory.getCanonicalPath() + File.separator + (i + 1), true), PAGE_SIZE * PAGE_SIZE);
                            writer.append(bs.append(nde).append("\t").append(nodeCounts.get(nde)).append("\n").toString());
                            CollectionUtilities.close(writer);
                        }
                    }
                }
            }
        } catch (IOException ex) {
            throw new DataException(ex);
        } finally {
            CollectionUtilities.close(writer);
        }
        labelIndex = new HashMap<>();
        return true;
    }


    @Override
    public Map<Long, Integer>[] getNodeMap(long node) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String toString() {
        return "["+labelIndex+"] "+this.indexDirectory.getAbsolutePath();
    }
}
