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

import eu.unitn.disi.db.mutilities.exceptions.ParseException;
import eu.unitn.disi.db.mutilities.data.CollectionUtilities;
import eu.unitn.disi.db.mutilities.LoggableObject;
import eu.unitn.disi.db.mutilities.Pair;
import eu.unitn.disi.db.mutilities.StringUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Stores a big multigraph in multidimensional arrays.
 *
 * This class is immutable, after having loaded the graph it is not possible to
 * modify it anymore.
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class BigMultigraph extends LoggableObject implements Multigraph, Iterable<Long> {

    private long[][] inEdges;
    private long[][] outEdges;
    private long lastInVertex;
    private long lastOutVertex;
    private int[] lastInBounds;
    private int[] lastOutBounds;
    private int nodeNumber;
    private Set<Edge> edgeSet;
    private Set<Long> labelSet;
    private Separator separator;
    private static final int SOURCE_POSITION = 0;
    private static final int DEST_POSITION = 1;
    private static final int REL_POSITION = 2;
    private static final int LOG_MARK = 50000000;
    private static final int LABELS = 10000;
    //TODO: Use this
    //private int numEdges;

    
    

    public enum Separator {
        SPACE(' '),
        TAB('\t');

        char delimiter;

        private Separator(char delimiter) {
            this.delimiter = delimiter;
        }

        public char getDelimiter() {
            return delimiter;
        }
    };

    public BigMultigraph(String graphFile) throws ParseException, IOException {
        this(graphFile, graphFile, -1, null, 1);
    }

    /**
     * Takes in input the non-ordered graph and orders it by source and by dest
     *
     * @param graphFile
     * @param numThreads
     * @throws ParseException
     * @throws IOException
     */
    public BigMultigraph(String graphFile, int numThreads) throws ParseException, IOException {
        this(graphFile, graphFile, -1, null, numThreads);
    }

    public BigMultigraph(String inFile, String outFile) throws ParseException, IOException {
        this(inFile, outFile, -1, null, 1);
    }

    public BigMultigraph(String inFile, String outFile, int edges) throws ParseException, IOException {
        this(inFile, outFile, edges, null, 1);
    }

    public BigMultigraph(String inFile, String outFile, Separator separator, int numThreads) throws ParseException, IOException {
        this(inFile, outFile, -1, separator, numThreads);
    }

    /**
     * Takes in input the non-ordered graph and orders it by source and by dest
     *
     * @param inFile The inbound file
     * @param outFile The outgoing file
     * @param edges Number of edges
     * @param sort Sort the input file
     * @param separator Separator of the file
     * @throws ParseException The input file is malformed
     * @throws IOException The input file is not readable
     */
    private BigMultigraph(String inFile, String outFile, int nEdges, Separator separator, int numThreads) throws ParseException, IOException {
        this.lastInVertex = -1;
        this.lastOutVertex = -1;
        this.nodeNumber = -1;
        this.lastInBounds = new int[2];
        this.lastOutBounds = new int[2];
        this.edgeSet = null;
        this.labelSet = new HashSet<>(LABELS);
        this.separator = separator;

        int numEdges = nEdges > 0 ? nEdges : StringUtils.countLines(inFile);

        //TODO: Add a check on different sizes.
        inEdges = new long[numEdges][];
        outEdges = new long[numEdges][];
        //If the file is the same load once and create a second array from the first array.
        if (inFile.equals(outFile)) {
            warn("Loading from a single file, creating a copy of the edges and sorting.");
            loadEdges(inFile, true);
            long[] edge;
            for (int i = 0; i < inEdges.length; i++) {
                edge = inEdges[i];
                outEdges[i] = new long[]{edge[1], edge[0], edge[2]};
            }
        } else {
            Pair<String, Boolean> in = new Pair<>(inFile, true);
            Pair<String, Boolean> out = new Pair<>(outFile, false);
            
            
            List<Pair<String, Boolean>> toLoad = new ArrayList<>(2);
            toLoad.add(in);
            toLoad.add(out);
            debug("Starting parallel Load");
            toLoad.parallelStream().forEach( what ->{
                try{
                    loadEdges(what.getFirst(), what.getSecond());
                } catch (IOException |  ParseException pe){
                    fatal("Failed to Parse %s edges from %s ", pe, what.getSecond() ? "incoming" : " outgoing", what.getFirst());                    
                }
            });
            
            //loadEdges(inFile, true);
            //loadEdges(outFile, false);
        }
        try {
            checkSort(inEdges, true, numThreads);
            checkSort(outEdges, false, numThreads);
        } catch (InterruptedException | ExecutionException ex) {
            throw new ParseException(ex);
        }
    }

    /*
    * Check if the arrays are sorted, sort otherwise
     */
    private void checkSort(long[][] edges, boolean incoming, int numThreads) throws InterruptedException, ExecutionException {
        boolean unsorted = false;
        long prev = Long.MIN_VALUE;
        for (long[] edge : edges) {
            if (prev <= edge[0]) {
                prev = edge[0];
            } else {
                unsorted = true;
                break;
            }
        }
        if (unsorted) {
            if (incoming) {
                warn("Incoming edges are unsorted: sorting ...");
            } else {
                warn("Outgoing edges are unsorted: sorting ...");
            }
            if (numThreads > 1) {
                CollectionUtilities.parallelBinaryTableSort(edges, numThreads);
            } else {
                CollectionUtilities.binaryTableSort(edges);
            }

            info("Sorting complete");
        }
    }

    private void loadEdges(String edgeFile, boolean incoming) throws ParseException, IOException {
        File file = new File(edgeFile);
        try (BufferedReader in = new BufferedReader(new FileReader(file))) {
            long source;
            long dest;
            long label;

            String line;
            String[] tokens;
            int count = 0;
            char delimiter = ' ';

            if (separator == null) {
                in.mark(2056);
                while ((line = in.readLine()) != null) {
                    if (!"".equals(line) && !line.startsWith("#")) { //Comment
                        tokens = StringUtils.fastSplit(line, ' ', 3); // try to split on whitespace
                        delimiter = ' ';
                        if (tokens.length != 3) { // line too short or too long
                            tokens = StringUtils.fastSplit(line, '\t', 3); //try to split on tab
                            if (tokens.length != 3) {
                                throw new ParseException("Token separator not recognized");
                            }
                            delimiter = '\t';
                        }
                        info("Recognized separator token '%c'\n", delimiter);
                        break;
                    }
                }
                in.reset();
            } else {
                delimiter = separator.delimiter;
            }

            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (!"".equals(line) && !line.startsWith("#")) { //Comment
                    tokens = StringUtils.fastSplit(line, delimiter, 3);

                    if (tokens.length != 3) {
                        throw new ParseException("Line[%d]: %s is malformed, num tokens %d", (count + 1), line, tokens.length);
                    }
                    source = Long.parseLong(tokens[SOURCE_POSITION]);
                    dest = Long.parseLong(tokens[DEST_POSITION]);
                    label = Long.parseLong(tokens[REL_POSITION]);
                    if (incoming) {
                        inEdges[count] = new long[]{dest, source, label};
                    } else {
                        outEdges[count] = new long[]{source, dest, label};
                    }

                }
                count++;
                if (count % LOG_MARK == 0) {
                    info("Processed %d lines of %s for %s Edges", count, edgeFile, incoming ? "_incoming_" : "_outgoing_");
                }
            }
        } catch (IOException ex) {
            throw ex;
        }
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
        CollectionUtilities.binaryTableSort(inEdges);
        CollectionUtilities.binaryTableSort(outEdges);
        this.inEdges = inEdges;
        this.outEdges = outEdges;
    }

    @Override
    public Collection<Long> vertexSet() {
        Set<Long> verteces = new HashSet<>(inEdges.length/2);
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
    public int numberOfEdges() {
        return inEdges.length;
    }

    @Override
    public Collection<Edge> edgeSet() {
        if (edgeSet == null) {
            edgeSet = new HashSet<>();
            for (long[] outEdge : outEdges) {
                edgeSet.add(new Edge(outEdge[0], outEdge[1], outEdge[2]));
            }
        }
        return edgeSet;
    }

    
    public int degreeOfNoCache(Long vertex) throws NullPointerException {
        int[] bounds = new int[2];
        int degree = degreeOf(inEdges, bounds, vertex, -1);
        degree += degreeOf(outEdges, bounds, vertex, -1);
        return degree;
    }
    
    @Override
    public synchronized int degreeOf(Long vertex) throws NullPointerException {
        int degree = degreeOf(inEdges, lastInBounds, vertex, lastInVertex);
        degree += degreeOf(outEdges, lastOutBounds, vertex, lastOutVertex);
        lastInVertex = vertex;
        lastOutVertex = vertex;
        return degree;
    }
    
    @Override
    public synchronized int inDegreeOf(Long vertex) throws NullPointerException {
        int degree = degreeOf(inEdges, lastInBounds, vertex, lastInVertex);
        lastInVertex = vertex;
        return degree;
    }

    @Override
    public synchronized int outDegreeOf(Long vertex) throws NullPointerException {
        int degree = degreeOf(outEdges, lastOutBounds, vertex, lastOutVertex);
        lastOutVertex = vertex;
        return degree;
    }

    @Override
    public Collection<Edge> incomingEdgesOf(Long vertex) throws NullPointerException {
        Collection<Edge> edges = new ArrayList<>();
        long[][] aEdges = incomingArrayEdgesOf(vertex);
        if (aEdges != null) {
            for (long[] aEdge : aEdges) {
                edges.add(new Edge(aEdge[1], vertex, aEdge[2]));
            }
        }
        return edges;
    }

    @Override
    public Collection<Edge> outgoingEdgesOf(Long vertex) throws NullPointerException {
        Collection<Edge> edges = new ArrayList<>();
        long[][] aEdges = outgoingArrayEdgesOf(vertex);
        if (aEdges != null) {
            for (long[] aEdge : aEdges) {
                edges.add(new Edge(vertex, aEdge[1], aEdge[2]));
            }
        }
        return edges;
    }

    /**
     * Given a node returns an array of dest,source,label in a long format
     *
     * @param vertex The vertex to find the incoming edges
     * @return An array of dest,source,label arrays
     */
    public synchronized long[][] incomingArrayEdgesOf(long vertex) {
        long[][] edges = edgesOf(inEdges, lastInBounds, vertex, lastInVertex);
        lastInVertex = vertex;
        return edges;
    }

    /**
     * Given a node returns an array of source,dest,label in a long format
     *
     * @param vertex The vertex to find the outgoing edges
     * @return An array of source,dest,label arrays
     */
    public synchronized long[][] outgoingArrayEdgesOf(long vertex) {
        long[][] edges = edgesOf(outEdges, lastOutBounds, vertex, lastOutVertex);
        lastOutVertex = vertex;
        return edges;
    }

    @Override
    public Iterator<Edge> incomingEdgesIteratorOf(Long vertex) throws NullPointerException {
        if(vertex==null){
            throw new NullPointerException("Vertex cannot be null");
        }
        int[] bounds = new int[2];        
        boundsOf(inEdges, bounds, vertex);
        
        int length, start;
        if (bounds == null || bounds[0] == -1) {
           start = 0;
           length = 0;
        } else {
            start = bounds[0];
            length = bounds[1]-bounds[0];
        }
        return new EdgeIterator(start, length, inEdges, true);
        
    }

    @Override
    public Iterator<Edge> outgoingEdgesIteratorOf(Long vertex) throws NullPointerException {
        if(vertex==null){
            throw new NullPointerException("Vertex cannot be null");
        }
        
        int[] bounds = new int[2];
        boundsOf(outEdges, bounds, vertex);

        int length, start;
        if (bounds == null || bounds[0] == -1) {
           start = 0;
           length = 0;
        } else {
            start = bounds[0];
            length = bounds[1]-bounds[0];
        }
        
       
        
        return new EdgeIterator(start, length, outEdges,false);
    }
    
    @Override
    public Iterator<Edge> edgesIterator() {
        return new EdgeIterator(0, outEdges.length, outEdges,false);
    }

    
    
    @Override
    public Iterator<Edge> labeledEdgesIteratorOf(Long label) throws NullPointerException {
        if(label==null){
            throw new NullPointerException("Label cannot be null");
        }
        return new LabeledEdgeIterator(new EdgeIterator(0, outEdges.length, outEdges,false), label);
    }

    @Override
    public Iterator<Edge> labeledEdgesIteratorOf(Set<Long> labels) throws NullPointerException {
        if(labels==null){
            throw new NullPointerException("Label cannot be null");
        }
        return new LabeledEdgeIterator(new EdgeIterator(0, outEdges.length, outEdges,false), labels);
    }
    
    
    

    @Override
    public Collection<Edge> edgesOf(Long id) throws NullPointerException {
        Collection<Edge> totalEdges = incomingEdgesOf(id);
        totalEdges.addAll(outgoingEdgesOf(id));
        return totalEdges; 
    }

    
    
    private static long[][] edgesOf(long[][] edges, int[] bounds, long vertex, long lastVertex) {
        if (vertex != lastVertex) {
            boundsOf(edges, bounds, vertex);
        }

        if (bounds == null || bounds[0] == -1) {
            return null;
        }
        int length = bounds[1] - bounds[0];
        if (length < 0) {
            return null;
        }
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

    /**
     * bounds will contain first position and last position+1 for vertex 
     * so edges[startingIndex][0] == vertex 
     * but edges[endingIndex][0] != vertex
     * @param edges
     * @param bounds
     * @param vertex 
     */
    private static void boundsOf(long[][] edges, int[] bounds, long vertex) {
        int i;
        int startingIndex = CollectionUtilities.binaryTableSearch(edges, vertex);
        if (startingIndex >= 0) {
            i = startingIndex;
            while (i < edges.length && edges[i][0] == vertex) {
                i++;
            }
            while (startingIndex >= 0 && edges[startingIndex][0] == vertex) {
                --startingIndex;
            }
            bounds[0] = startingIndex + 1;
            bounds[1] = i;
        } else {
            bounds[0] = -1;
            bounds[1] = -1;
        }
    }

    @Override
    public Multigraph merge(Multigraph graph) throws NullPointerException {
        throw new UnsupportedOperationException("This graph is immutable, this operation is not allowed.");
    }

    //TODO: Implement this.
    @Override
    public boolean containsVertex(Long vertex) throws NullPointerException {
        return CollectionUtilities.binaryTableSearch(inEdges, vertex) >= 0 || CollectionUtilities.binaryTableSearch(outEdges, vertex) >= 0;
    }

    
    @Override
    public Collection<Edge> getEdge(Long src, Long dest) throws NullPointerException {
        if(!containsVertex(src) || !containsVertex(dest)){
            return new HashSet<>(1);
        }
        Iterator<Edge> eit = this.outgoingEdgesIteratorOf(src);
        Set<Edge> out = new HashSet<>();
        Edge e;
        while(eit.hasNext()){
            e= eit.next();
            if(e.getDestination().equals(dest)){
                out.add(e);
            }
        }
        return out;
    }

    @Override
    public boolean containsEdge(Long src, Long dest) {
        if(!containsVertex(src) || !containsVertex(dest)){
            return false;
        }
        
        Iterator<Edge> eit = this.outgoingEdgesIteratorOf(src);
        
        Edge e;
        while(eit.hasNext()){
            e= eit.next();
            if(e.getDestination().equals(dest)){
                return true;
            }
        }
        return false;
    }
    
    @Override
    public void removeVertex(Long id) throws NullPointerException {
        throw new UnsupportedOperationException("This graph is immutable, this operation is not allowed.");
    }

    @Override
    public void removeEdge(Long src, Long dest, Long label) throws IllegalArgumentException, NullPointerException {
        throw new UnsupportedOperationException("This graph is immutable, this operation is not allowed.");
    }

    @Override
    public void removeEdge(Edge edge) throws IllegalArgumentException, NullPointerException {
        throw new UnsupportedOperationException("This graph is immutable, this operation is not allowed.");
    }

    @Override
    public Collection<Long> labelSet() {
        if (this.labelSet.isEmpty()) {
            for (long[] outEdge : outEdges) {
                labelSet.add(outEdge[2]);
            }
        }
        return this.labelSet;
    }
      
    
    @Override
    public Collection<Long> neighborsOf(Long id) {
        Set<Long> neighs = new HashSet<>((4/3)*(this.outDegreeOf(id)+ this.inDegreeOf(id)));
        Edge e;
        Iterator<Edge> eds;
        for (int i = 0; i < 2; i++) {
            eds = i==0 ? incomingEdgesIteratorOf(id) : outgoingEdgesIteratorOf(id);
            while(eds.hasNext()){
               e =  eds.next();
               neighs.add( i==0 ? e.getSource(): e.getDestination());
            }
        }
        return neighs;
        
    }

    
    
    private class EdgeIterator implements Iterator<Edge> {
        private int current;
        private final int end;
        private final long[][] edges;

        private final boolean incoming;
        
        public EdgeIterator(int st, int end, long[][] edges, boolean incoming) {
            this.end = st + end;
            this.current = st;
            this.edges = edges;
            this.incoming = incoming;
        }

        @Override
        public boolean hasNext() {
            return this.current < this.end;
        }

        @Override
        public Edge next() {                      
           if(this.current >= this.end){
               throw new NoSuchElementException("No more elements to explore");
           }
           long[] edge = this.edges[current];
           this.current++;           
           return new Edge(incoming ? edge[1] : edge[0], incoming? edge[0] : edge[1], edge[2]);
        }
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException("This graph is immutable, this operation is not allowed");
        }
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
            return indexIn < inEdges.length || indexOut < outEdges.length;
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
                } else {
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
                        value = valueIn < valueOut ? valueIn : valueOut;
                    }
                }
                return value;
            } catch (Exception ex) {
                //ex.printStackTrace();
                throw new NoSuchElementException("No more elements to explore");
            }
            //assert true : "This part of the code should be unreachable";
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

    public long[][] getEdges() {
        return inEdges;
    }
}
