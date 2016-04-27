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
package eu.unitn.disi.db.grava.graphs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * This class represents a multigraph that is a structure that holds a set of
 * vertices and labeled edges
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class BaseMultigraph implements Multigraph {

    private static final int MIN_SIZE_PARALLELIZATION = 2000;

    protected Map<Long, EdgeContainer> nodeEdges;
    protected Collection<Edge> edges;

    //Used to initialize ArrayList of Out/In Edges
    private int avgNodeDegree;

    private static final int DEFAULT_CAPACITY = 16;
    private static final float DEFAULT_DEGREE = 1f;

    /**
     * Construct a multigraph that has an initial capacity of 2
     */
    public BaseMultigraph() {
        this(DEFAULT_CAPACITY, DEFAULT_DEGREE);
    }

    public BaseMultigraph(int capacity) {
        this(capacity, DEFAULT_DEGREE);
    }

    /**
     * Construct a multigraph specifying an initial capacity, the vertices are
     * stored in a {@link Set} to ensure the correcteness.
     *
     * @param initialCapacity The initial capacity of the store (speed up).
     * @param avgDegree The average degree of each node
     */
    public BaseMultigraph(int initialCapacity, float avgDegree) {
        this.avgNodeDegree = (int) Math.ceil(avgDegree);
        nodeEdges = new HashMap<>(initialCapacity);
        edges = new HashSet<>((int) Math.ceil(initialCapacity * avgDegree));
    }

    /**
     * Add a vertex in the graph. This must be called on source and destination
     * node before the {@link #addEdge(java.lang.Long, java.lang.Long, java.lang.Long)
     * }
     * otherwise the latter will throw an exception
     *
     * @param id The id of the node to be inserted
     */
    @Override
    public void addVertex(Long id) throws NullPointerException {
        if (!nodeEdges.containsKey(id)) {
            nodeEdges.put(id, buildEdgeContainer());
        }
    }

    /**
     * Add an edge to the graph, if both source and destination exists. To add a
     * vertex, calls {@link #addVertex(java.lang.Long) } before. If either source
     * or dest do not exist throws an {@link IllegalArgumentException}
     *
     * @param src The source node in this directed multigraph
     * @param dest The dest node in this directed multigraph
     * @param label The label of the edge to be created
     * @throws IllegalArgumentException If src and dest are not present in the
     * vertex collection
     */
    @Override
    public void addEdge(Long src, Long dest, Long label) throws IllegalArgumentException, NullPointerException {
        EdgeContainer srcC = nodeEdges.get(src);
        EdgeContainer dstC = nodeEdges.get(dest);

        Edge e = new Edge(src, dest, label);

        if (srcC == null) {
            throw new IllegalArgumentException("Source node is not in the vertex list. Call addVertex before");
        }
        if (dstC == null) {
            throw new IllegalArgumentException("Destination node is not in the vertex list. Call addVertex before");
        }

        if (srcC.addOutgoingEdge(e)) {
            edges.add(e);
            dstC.addIncomingEdge(e);
        }
    }

    /**
     * Add an edge to the graph, if both source and destination exists. To add a
     * vertex, calls {@link #addVertex(java.lang.Long) } before. If either source
     * or dest do not exist throws an {@link IllegalArgumentException}
     *
     * @param edge The edge to be added into the graph
     * @throws IllegalArgumentException If src and edges are not present in the
     * vertex collection
     */
    @Override
    public void addEdge(Edge edge) throws NullPointerException {
        addEdge(edge.getSource(), edge.getDestination(), edge.getLabel());
    }

    /**
     * Add an edge to the graph. If either source or destination nodes do not
     * exist, it calls {@link #addVertex(java.lang.Long) }. 
     *
     * @param src The source node in this directed multigraph
     * @param dest The dest node in this directed multigraph
     * @param label The label of the edge to be created
     */
    public void forceAddEdge(Long src, Long dest, Long label) throws NullPointerException {
        EdgeContainer srcC = nodeEdges.get(src);
        EdgeContainer dstC = nodeEdges.get(dest);

        Edge e = new Edge(src, dest, label);

        if (srcC == null) {
            addVertex(src);
            srcC = nodeEdges.get(src);
        }
        if (dstC == null) {
            addVertex(dest);
            dstC = nodeEdges.get(dest);
        }

        if (srcC.addOutgoingEdge(e)) {
            edges.add(e);
            dstC.addIncomingEdge(e);
        }
    }

    /**
     * Add an edge to the graph. If either source or destination nodes do not
     * exist, it calls {@link #addVertex(java.lang.Long) }.
     *
     * @param edge
     */
    public void forceAddEdge(Edge edge) throws NullPointerException {
        forceAddEdge(edge.getSource(), edge.getDestination(), edge.getLabel());
    }
    
    
    
    
    /**
     * Returns the set of vertices of the graph
     *
     * @return The set of vertices
     */
    @Override
    public Collection<Long> vertexSet() {
        return nodeEdges.keySet();
    }

    /**
     * Returns the set of edges of the graph
     *
     * @return The set of edges
     */
    @Override
    public Collection<Edge> edgeSet() {
        return edges;
    }

    /**
     * Returns the "in degree" of the specified vertex. An in degree of a vertex
     * in a directed graph is the number of incoming directed edges from that
     * vertex. See <a href="http://mathworld.wolfram.com/Indegree.html">
     * http://mathworld.wolfram.com/Indegree.html</a>.
     *
     * @param vertex vertex whose degree is to be calculated.
     *
     * @return the degree of the specified vertex.
     */
    @Override
    public int inDegreeOf(Long vertex) throws NullPointerException {
        if (!nodeEdges.containsKey(vertex)) {
            throw new IllegalArgumentException("This graph does not contain node " + vertex);
        }
        return nodeEdges.containsKey(vertex) ? nodeEdges.get(vertex).getIncoming().size() : 0;
    }

    /**
     * Returns a set of all edges incoming into the specified vertex.
     *
     * @param vertex the vertex for which the list of incoming edges to be
     * returned.
     *
     * @return a set of all edges incoming into the specified vertex.
     */
    @Override
    public Collection<Edge> incomingEdgesOf(Long vertex) throws NullPointerException {
        if (!nodeEdges.containsKey(vertex)) {
            throw new IllegalArgumentException("This graph does not contain node " + vertex);
        }
        return nodeEdges.containsKey(vertex) ? nodeEdges.get(vertex).getIncoming() : null;
    }

    /**
     * Returns the "out degree" of the specified vertex. An out degree of a
     * vertex in a directed graph is the number of outward directed edges from
     * that vertex. See <a href="http://mathworld.wolfram.com/Outdegree.html">
     * http://mathworld.wolfram.com/Outdegree.html</a>.
     *
     * @param vertex vertex whose degree is to be calculated.
     *
     * @return the degree of the specified vertex.
     * @throws NullPointerException if the input vertex is null
     */
    @Override
    public int outDegreeOf(Long vertex) throws NullPointerException {
        if (!nodeEdges.containsKey(vertex)) {
            throw new IllegalArgumentException("This graph does not contain node " + vertex);
        }
        return nodeEdges.containsKey(vertex) ? nodeEdges.get(vertex).getOutgoing().size() : 0;
    }

    /**
     * Returns a set of all edges outgoing from the specified vertex.
     *
     * @param vertex the vertex for which the list of outgoing edges to be
     * returned.
     *
     * @return a set of all edges outgoing from the specified vertex.
     * @throws NullPointerException if the input vertex is null
     */
    @Override
    public Collection<Edge> outgoingEdgesOf(Long vertex) throws NullPointerException {
        if (!nodeEdges.containsKey(vertex)) {
            throw new IllegalArgumentException("This graph does not contain node " + vertex);
        }
        return nodeEdges.containsKey(vertex) ? nodeEdges.get(vertex).getOutgoing() : null;
    }

    /**
     * Merge this graph with the input graph. Parallelize the operations only if
     * needed otherwise go recursively
     *
     * @param graph The input graph to be merged to this
     * @return this graph
     * @throws NullPointerException if the input graph is null
     * @throws ExecutionException if something happens in the merge phase
     */
    @Override
    public BaseMultigraph merge(BaseMultigraph graph)
            throws ExecutionException,
            NullPointerException {
        if (nodeEdges.keySet().size() > MIN_SIZE_PARALLELIZATION) {
            ExecutorService pool = Executors.newFixedThreadPool(3);
            List<Future> tasks = new ArrayList<>();
            tasks.add(pool.submit(new AddToMap(graph)));
            //tasks.add(pool.submit(new AddToCollection(vertices, graph)));
            tasks.add(pool.submit(new AddToCollection(edges, graph)));

            try {
                for (Future task : tasks) {
                    task.get();
                }
            } catch (InterruptedException ex) {
                throw new ExecutionException(ex);
            }
        } else {
            EdgeContainer ec, sec;
            Map<Long, EdgeContainer> sourceEdges = graph.nodeEdges;
            Set<Long> vset = sourceEdges.keySet();
            for (Long v : vset) {
                ec = nodeEdges.get(v);
                sec = sourceEdges.get(v);
                if (ec == null) {
                    ec = new BaseEdgeContainer();
                }
                ec.getIncoming().addAll(sec.getIncoming());
                ec.getOutgoing().addAll(sec.getOutgoing());
                nodeEdges.put(v, ec);
            }
            //vertices.addAll(graph.vertices);
            edges.addAll(graph.edges);
        }
        return this;
    }

    /**
     * Check if the input vertex is contained in the Multigraph
     *
     * @param vertex The input vertex to be checked
     * @return True if it contains the node, false otherwise
     * @throws NullPointerException If the vertwx is null
     */
    @Override
    public boolean containsVertex(Long vertex) throws NullPointerException {
        return nodeEdges.containsKey(vertex);
    }

    protected EdgeContainer buildEdgeContainer() {
        return new BaseEdgeContainer();
    }

    @Override
    public Iterator<Long> iterator() {
        return nodeEdges.keySet().iterator();
    }

    @Override
    public void removeVertex(Long id) throws NullPointerException {
        EdgeContainer container = nodeEdges.remove(id);
        for (Edge edge : container.getIncoming()) {
            edges.remove(edge);
        }
        for (Edge edge : container.getOutgoing()) {
            edges.remove(edge);
        }
    }

    @Override
    public int numberOfNodes() {
        return this.nodeEdges.size();
    }

    @Override
    public void removeEdge(Long src, Long dest, Long label) throws IllegalArgumentException, NullPointerException {
        removeEdge(new Edge(src, dest, label));
    }

    @Override
    public void removeEdge(Edge edge) throws IllegalArgumentException, NullPointerException {
        edges.remove(edge);
        nodeEdges.get(edge.getSource()).getOutgoing().remove(edge);
        nodeEdges.get(edge.getDestination()).getIncoming().remove(edge);
    }

    @Override
    public int numberOfEdges() {
        return edges.size();
    }

    @Override
    public Collection<Edge> getEdge(Long src, Long dest) throws NullPointerException {
        EdgeContainer edgeContainer = nodeEdges.get(src);
        Collection<Edge> foundEdges = new ArrayList<>();
        if (edgeContainer != null) {
            Collection<Edge> outgoing = edgeContainer.getOutgoing();

            for (Edge e : outgoing) {
                if (Objects.equals(e.getDestination(), dest)) {
                    foundEdges.add(e);
                }
            }
        }
        return foundEdges;
    }

    @Override
    public boolean containsEdge(Long sourceVertex, Long targetVertex) {
        Collection<Edge> candidateEdges = getEdge(sourceVertex, targetVertex);
        return candidateEdges != null && !candidateEdges.isEmpty();
    }

    @Override
    public Collection<Edge> edgesOf(Long id) throws NullPointerException {
        Collection<Edge> totalEdges = incomingEdgesOf(id);
        totalEdges.addAll(outgoingEdgesOf(id));
        return totalEdges;
    }

    @Override
    public Collection<Long> labelSet() {

        HashSet<Long> labels = new HashSet<>();
        for (Edge e : edges) {
            labels.add(e.getLabel());
        }

        return labels;
    }

    @Override
    public Iterator<Edge> incomingEdgesIteratorOf(Long vertex) throws NullPointerException {
        if (vertex == null) {
            throw new NullPointerException("Vertex cannot be null");
        }
        Collection<Edge> vEdges = incomingEdgesOf(vertex);
        return vEdges == null ? Collections.<Edge>emptyIterator() : vEdges.iterator();
    }

    @Override
    public Iterator<Edge> outgoingEdgesIteratorOf(Long vertex) throws NullPointerException {
        if (vertex == null) {
            throw new NullPointerException("Vertex cannot be null");
        }
        Collection<Edge> vEdges = outgoingEdgesOf(vertex);
        return vEdges == null ? Collections.<Edge>emptyIterator() : vEdges.iterator();
    }

    @Override
    public Iterator<Edge> labeledEdgesIteratorOf(Long label) throws NullPointerException {
        if (label == null) {
            throw new NullPointerException("Label cannot be null");
        }
        return new LabeledEdgeIterator(this.edges.iterator(), label);
    }

    @Override
    public Iterator<Edge> labeledEdgesIteratorOf(Set<Long> labels) throws NullPointerException {
        if (labels == null) {
            throw new NullPointerException("Label cannot be null");
        }
        return new LabeledEdgeIterator(this.edges.iterator(), labels);
    }

    /*
     * Represents a container for the two set of edges (this prevents us to
     * use two different maps in the class)
     */
    protected class BaseEdgeContainer implements EdgeContainer {

        protected Collection<Edge> incoming;
        protected Collection<Edge> outgoing;

        public BaseEdgeContainer() {
            incoming = new HashSet<>(avgNodeDegree);
            outgoing = new HashSet<>(avgNodeDegree);
        }

        @Override
        public boolean addOutgoingEdge(Edge e) {
            return outgoing.add(e);
        }

        @Override
        public boolean addIncomingEdge(Edge e) {
            return incoming.add(e);
        }

        @Override
        public Collection<Edge> getIncoming() {
            return incoming;
        }

        @Override
        public Collection<Edge> getOutgoing() {
            return outgoing;
        }
    }

    /*
     * Classes used to parallelize the merge process and, hopefully, to
     * have better results
     */
    private class AddToCollection implements Runnable {

        private final Collection<Edge> coll;
        private final BaseMultigraph graph;

        public AddToCollection(Collection<Edge> coll, BaseMultigraph graph) {
            this.coll = coll;
            this.graph = graph;
            //this.isVertex = isVertex;
        }

        @Override
        public void run() {
            coll.addAll(graph.edges);
        }
    }

    private class AddToMap implements Runnable {

        private final BaseMultigraph graph;

        public AddToMap(BaseMultigraph graph) {
            this.graph = graph;
        }

        @Override
        public void run() {
            EdgeContainer ec, sec;
            Map<Long, EdgeContainer> sourceEdges = graph.nodeEdges;
            Set<Long> vset = sourceEdges.keySet();
            for (Long v : vset) {
                ec = nodeEdges.get(v);
                sec = sourceEdges.get(v);
                if (ec == null) {
                    ec = new BaseEdgeContainer();
                }
                ec.getIncoming().addAll(sec.getIncoming());
                ec.getOutgoing().addAll(sec.getOutgoing());
                nodeEdges.put(v, ec);
            }
        }
    }
}
