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
package eu.unitn.disi.db.grava.graphs;

import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.collections.PartitionableCollection;
import eu.unitn.disi.db.grava.graphs.collections.PartitionableMap;
import eu.unitn.disi.db.grava.graphs.collections.PartitionedList;
import eu.unitn.disi.db.grava.graphs.collections.PartitionedMap;
import eu.unitn.disi.db.grava.utils.Utilities;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public final class PartitionedMultigraph extends BaseMultigraph {

    private int nodePartitions;

    private static final int DEFAULT_CAPACITY = 4;
    private static final float DEFAULT_LOAD_FACTOR = .75f;

    /**
     * Construct a multigraph specifying an initial capacity, the
     * allowRepetitions flag and the number of partitions for node and edge
     * sets. If the repetitions are allowed the vertices are stored in a
     * {@link List} to speed up performances, otherwise in a {@link Set} to
     * ensure the correcteness.
     *
     * @param initialCapacity The initial capacity of the graph
     * @param nodePartitions The number of partitions in the node set
     * @param edgePartitions The number of partitions in the edge set
     */
    public PartitionedMultigraph(int initialCapacity, int nodePartitions, int edgePartitions) {
        this.nodePartitions = nodePartitions;

        nodeEdges = new PartitionedMap<>(initialCapacity, DEFAULT_LOAD_FACTOR, this.nodePartitions);
        edges = new PartitionedList<>(initialCapacity, edgePartitions);
    }

    /**
     * Build a multigraph using information about the degree of each node. Each
     * line of the file has the following format
     *
     * line := nodeid SPACE incoming SPACE outgoing
     *
     * where outgoing and incoming are the numbers of outgoing and incoming
     * edges resectively
     *
     * @param nodeDegreeFile The file with the degrees for each node.
     * @param initialCapacity
     * @param nodePartitions
     */
    public PartitionedMultigraph(String nodeDegreeFile, int initialCapacity, int nodePartitions, int edgePartitions) throws IOException, ParseException {
        this(initialCapacity, nodePartitions, edgePartitions);

        try {
            File file = new File(nodeDegreeFile);
            BufferedReader in = new BufferedReader(new FileReader(file));

            long vertex;
            int outgoing;
            int incoming;

            String line, source, inc, out;
            String[] tokens;
            int count = 0;

            while ((line = in.readLine()) != null) {
                count++;
                if (!"".equals(line.trim()) && !line.trim().startsWith("#")) { //Comment
                    tokens = Utilities.fastSplit(line, ' ', 3); // split on whitespace
                    if (tokens.length < 3) { // line too short
                        tokens = Utilities.fastSplit(line, '\t', 3);
                        if (tokens.length != 3) {
                            throw new ParseException("Line %d is malformed", count);
                        }
                    }
                    source = tokens[0];
                    inc = tokens[1];
                    out = tokens[2];

                    vertex = Long.parseLong(source);
                    outgoing = Integer.parseInt(out);
                    incoming = Integer.parseInt(inc);

                    addFixedSizeVertex(vertex, incoming, outgoing);
                }
            }
        } catch (IOException ex) {
            throw ex;
        }
    }

    @Override
    protected EdgeContainer buildEdgeContainer() {
        return new EdgePartitionedContainer();
    }

    private void addFixedSizeVertex(long vertex, int incoming, int outgoing) {
        nodeEdges.put(vertex, new EdgePartitionedContainer(incoming, outgoing));
    }

    //TODO: maybe add a partitioning schema if the size is greater than a threshold
    protected class EdgePartitionedContainer extends BaseEdgeContainer {

        public EdgePartitionedContainer(int inNum, int outNum) {
            incoming = new HashSet<>(inNum);
            outgoing = new HashSet<>(outNum);
        }

        public EdgePartitionedContainer() {
            this(DEFAULT_CAPACITY, DEFAULT_CAPACITY);
        }
    }

    public int getNumEdgePartitions() {
        return ((PartitionableCollection) edges).getNumPartitions();
    }

    public int getEdgePartitionsSize(int i) {
        return ((PartitionableCollection) edges).getPartitionSize(i);
    }

    public int getNumNodePartitions() {
        return ((PartitionableMap) nodeEdges).getNumPartitions();
    }

    public int getNodePartitionsSize(int i) {
        return ((PartitionableMap) nodeEdges).getPartitionSize(i);
    }
}
