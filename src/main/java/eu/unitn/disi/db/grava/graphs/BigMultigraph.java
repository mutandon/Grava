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
import eu.unitn.disi.db.grava.utils.Utilities;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

/**
 * Stores a big multigraph in multidimensional arrays.
 *
 * This class is immutable, after having loaded the graph it is not possible to
 * modify it anymore.
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class BigMultigraph implements Multigraph, Iterable<Long>  {
    private long[][] inEdges;
    private long[][] outEdges;
    private long lastInVertex;
    private long lastOutVertex;
    private int[] lastInBounds;
    private int[] lastOutBounds;
    private int nodeNumber;
    private Set<Edge> edgeSet;
    //TODO: Use this
    private int numEdges;

    public BigMultigraph() {
    }

    /**
     * Takes in input the graph ordered by source and by dest to have a faster
     * computation
     * @param inFile
     * @param outFile
     * @param edges
     * @throws ParseException
     * @throws IOException
     */
    public BigMultigraph(String inFile, String outFile, int edges) throws ParseException, IOException {
        lastInVertex = -1;
        lastOutVertex = -1;
        nodeNumber = -1;
        lastInBounds = new int[2];
        lastOutBounds = new int[2];
        edgeSet = null;

        if (edges == -1) {
            edges = Utilities.countLines(inFile);
        }

        //TODO: Add a check on different sizes.
        inEdges = new long[edges][];
        outEdges = new long[edges][];
        loadEdges(inFile, true);
        loadEdges(outFile, false);
    }

    public BigMultigraph(String inFile, String outFile) throws ParseException, IOException {
        this(inFile, outFile, -1);
    }

    private void loadEdges(String edgeFile, boolean incoming) throws ParseException, IOException {
        try {
            File file = new File(edgeFile);
            BufferedReader in = new BufferedReader(new FileReader(file));

            long source;
            long dest;
            long label;

            String line;
            String[] tokens;
            int count = 0;
//            int index;
//            line = in.readLine();
//            if (line != null) {
//                line = line.trim();
//                if (edges == -1) {
//                    if (line.startsWith("#") && (index = line.indexOf("edges:")) != -1) {
//                        int i;
//                        for (i = index + 6; Character.isDigit(line.charAt(i)) && i < line.length(); i++);
//                        edges = Integer.parseInt(line.substring(index + 6, i));
//                    } else {
//                        edges = Utilities.countLines(edgeFile);
//                    }
//                }
            while((line = in.readLine()) != null) {
                line = line.trim();
                if (!"".equals(line) && !line.startsWith("#")) { //Comment
                    tokens = Utilities.fastSplit(line, ' ', 3); // split on whitespace
                    if (tokens.length < 3) { // line too short
                        tokens = Utilities.fastSplit(line, '\t', 3);
                        if (tokens.length != 3) {
                            throw new ParseException("Line[" + (count + 1) +  "]: " + line + " is malformed, num tokens " + tokens.length);
                        }
                    }
                    source = Long.parseLong(tokens[0]);
                    dest = Long.parseLong(tokens[1]);
                    label = Long.parseLong(tokens[2]);
                    if (incoming) {
                        inEdges[count] = new long[]{dest, source, label};
                    } else {
                        outEdges[count] = new long[]{source, dest, label};
                    }

                }
                count++;
                if (count % 50000000 == 0) {
                    System.out.printf("Processed %d lines of %s\n", count, edgeFile);
                }
            } // END WHILE
//            } // END IF
        } catch (IOException ex) {
            throw ex;
        }
//        return edges;
    }

    @Override
    public void addVertex(Long id) throws NullPointerException {
        throw new UnsupportedOperationException("This graph is immutable, this operation is not allowed.");
    }

    @Override
    public void addEdge(Long src, Long dest, Long label) throws IllegalArgumentException, NullPointerException {
        throw new UnsupportedOperationException("This graph is immutable, this operation is not allowed.");
    }

    @Override
    public void addEdge(Edge edge) throws IllegalArgumentException, NullPointerException {
        throw new UnsupportedOperationException("This graph is immutable, this operation is not allowed.");
    }


    public void setEdges(long[][] inEdges, long[][] outEdges) {
        lastInVertex = -1;
        lastOutVertex = -1;
        nodeNumber = -1;
        lastInBounds = new int[2];
        lastOutBounds = new int[2];
        edgeSet = null;
        Utilities.binaryTableSort(inEdges);
        Utilities.binaryTableSort(outEdges);
        this.inEdges = inEdges;
        this.outEdges = outEdges;
    }

    @Override
    public Collection<Long> vertexSet() {
        Set<Long> verteces = new HashSet<Long>();
        for (int i = 0; i < inEdges.length; i++) {
            verteces.add(inEdges[i][0]);
            verteces.add(outEdges[i][0]);
        }
        return verteces;
    }

    @Override
    public int numberOfNodes() {
        if (nodeNumber == -1) {
            nodeNumber = 0;
            Iterator<Long> it = iterator();
            while (it.hasNext()) {
                nodeNumber++;
                it.next();
            }
        }
        return nodeNumber;
    }

    @Override
    public Collection<Edge> edgeSet() {
        if (edgeSet == null) {
            edgeSet = new HashSet<>();
            for (int i = 0; i < outEdges.length; i++) {
                edgeSet.add(new Edge(outEdges[i][0], outEdges[i][1], outEdges[i][2]));
            }
        }
        return edgeSet;
    }

    @Override
    public int inDegreeOf(Long vertex) throws NullPointerException {
        int degree = degreeOf(inEdges, lastInBounds, vertex, lastInVertex);
        lastInVertex = vertex;
        return degree;
    }

    @Override
    public int outDegreeOf(Long vertex) throws NullPointerException {
        int degree = degreeOf(outEdges, lastOutBounds, vertex, lastOutVertex);
        lastOutVertex = vertex;
        return degree;
    }

    @Override
    public Collection<Edge> incomingEdgesOf(Long vertex) throws NullPointerException {
        Collection<Edge> edges = new ArrayList<>();
        long[][] aEdges = incomingArrayEdgesOf(vertex);
        if (aEdges != null) {
            for (int i = 0; i < aEdges.length; i++) {
                edges.add(new Edge(aEdges[i][1],vertex,aEdges[i][2]));
            }
        }
        return edges;
    }

    @Override
    public Collection<Edge> outgoingEdgesOf(Long vertex) throws NullPointerException {
        Collection<Edge> edges = new ArrayList<>();
        long[][] aEdges = outgoingArrayEdgesOf(vertex);
        if (aEdges != null) {
            for (int i = 0; i < aEdges.length; i++) {
                edges.add(new Edge(vertex, aEdges[i][1],aEdges[i][2]));
            }
        }
        return edges;
    }

    /**
     * Given a node returns an array of dest,source,label in a long format
     * @param vertex The vertex to find the incoming edges
     * @return An array of dest,source,label arrays
     */
    public long[][] incomingArrayEdgesOf(long vertex) {
        long[][] edges = edgesOf(inEdges, lastInBounds, vertex, lastInVertex);
        lastInVertex = vertex;
        return edges;
    }

    /**
     * Given a node returns an array of source,dest,label in a long format
     * @param vertex The vertex to find the outgoing edges
     * @return An array of source,dest,label arrays
     */

    public long[][] outgoingArrayEdgesOf(long vertex) {
        long[][] edges = edgesOf(outEdges, lastOutBounds, vertex, lastOutVertex);
        lastOutVertex = vertex;
        return edges;
    }


    private static long[][] edgesOf(long[][] edges, int[] bounds, long vertex, long lastVertex) {
        if (vertex != lastVertex) {
            boundsOf(edges, bounds, vertex);
        }
        if (bounds == null || bounds[0] == -1) {
            return null;
        }
        int length = bounds[1] - bounds[0];
        long[][] sublist = new long[length][3];
        System.arraycopy(edges, bounds[0], sublist, 0, length);
        return sublist;
    }

    private static int degreeOf(long[][] edges, int[] bounds, long vertex, long lastVertex) {
        if (vertex != lastVertex) {
            boundsOf(edges, bounds, vertex);
        }
        if (bounds == null || bounds[0] == -1) {
            return 0;
        }
        return bounds[1] - bounds[0];
    }

    private static void boundsOf(long[][] edges, int[] bounds, long vertex) {
        int i;
        int startingIndex = Utilities.binaryTableSearch(edges, vertex);
        if (startingIndex >= 0) {
            i = startingIndex;
            while (i < edges.length && edges[i][0] == vertex) { i++; }
            while (startingIndex >= 0 && edges[startingIndex][0] == vertex) { --startingIndex;}
            bounds[0] = startingIndex + 1;
            bounds[1] = i;
        } else {
            bounds[0] = -1;
            bounds[1] = -1;
        }
    }


    @Override
    public BaseMultigraph merge(BaseMultigraph graph) throws NullPointerException, ExecutionException {
        throw new UnsupportedOperationException("This graph is immutable, this operation is not allowed.");
    }

    //TODO: Implement this.
    @Override
    public boolean containsVertex(Long vertex) throws NullPointerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    private class NodeIterator implements Iterator<Long> {
        //Take into account the index in the inEdges and in the outEdges
        private int indexIn;
        private int indexOut;

        public NodeIterator() {
            indexIn = 0;
            indexOut = 0;
        }

        @Override
        public boolean hasNext() {
            if (indexIn < inEdges.length)
                return true;
            if (indexOut < outEdges.length)
                return true;
            return false;
        }

        @Override
        public Long next() {
            int index;
            long value;
            try {
                if (indexIn >= inEdges.length) {
                    index = searchNext(outEdges, indexOut, outEdges[indexOut][0]);
                    value = outEdges[indexOut][0];
                    indexOut = index;
                } else if (indexOut >= outEdges.length) {
                    index = searchNext(inEdges, indexIn, inEdges[indexIn][0]);
                    value = inEdges[indexIn][0];
                    indexIn = index;
                } else  {
                    long valueIn = inEdges[indexIn][0];
                    long valueOut = outEdges[indexOut][0];

                    if (valueIn < valueOut) {
                        index = searchNext(inEdges, indexIn, valueIn);
                        value = inEdges[indexIn][0];
                        indexIn = index;
                    } else if (valueIn > valueOut) {
                        index = searchNext(outEdges, indexOut, valueOut);
                        value = outEdges[indexOut][0];
                        indexOut = index;
                    } else {
                        index = searchNext(inEdges, indexIn, valueIn);
                        valueIn = inEdges[indexIn][0];
                        indexIn = index;
                        index = searchNext(outEdges, indexOut, valueOut);
                        valueOut = outEdges[indexOut][0];
                        indexOut = index;
                        value = valueIn < valueOut? valueIn : valueOut;
                    }
                }
                return value;
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new NoSuchElementException("No more elements to explore");
            }
            //assert false : "This part of the code should be unreachable";
            //return null;
        }

        @SuppressWarnings("empty-statement")
        private int searchNext(long[][] vector, int index, long value) {
            int i;
            for (i = index; i < vector.length && value == vector[i][0]; i++);
            return i;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("This graph is immutable, this operation is not allowed");
        }

    }

    @Override
    public Iterator<Long> iterator() {
        return new NodeIterator();
    }
}
