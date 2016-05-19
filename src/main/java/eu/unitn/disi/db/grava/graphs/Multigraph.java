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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Represents a multigraph as a mean of standard operations
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public interface Multigraph extends Iterable<Long> {

    /**
     * Add a vertex in the graph. This must be called on source and destination
     * node before the {@link #addEdge(java.lang.Long, java.lang.Long, java.lang.Long)
     * }
     * otherwise the latter will throw an exception
     *
     * @param id The id of the node to be inserted
     * @throws NullPointerException if the input vertex is null
     */
    public void addVertex(Long id) throws NullPointerException;

    /**
     * Add an edge to the graph, if both source and destination exists. To add a
     * vertex, calls {@link #addVertex(java.lang.Long) } before. If both source
     * and dest do not exist throws an {@link IllegalArgumentException}
     *
     * @param src The source node in this directed multigraph
     * @param dest The dest node in this directed multigraph
     * @param label The label of the edge to be created
     * @throws IllegalArgumentException If src and edges are not present in the
     * vertex collection
     * @throws NullPointerException if one of the input is null
     */
    public void addEdge(Long src, Long dest, Long label) throws IllegalArgumentException, NullPointerException;

    /**
     * Add an edge to the graph, if both source and destination exists. To add a
     * vertex, calls {@link #addVertex(java.lang.Long) } before. If both source
     * and dest do not exist throws an {@link IllegalArgumentException}
     *
     * @param edge The edge to be added into the graph
     * @throws IllegalArgumentException If src and edges are not present in the
     * vertex collection
     * @throws NullPointerException if the input edge is null
     */
    public void addEdge(Edge edge) throws IllegalArgumentException, NullPointerException;

    /**
     * Remove a vertex in the graph and its connected edges.
     *
     * @param id The id of the node to be deleted
     * @throws NullPointerException if the input vertex is null
     */
    public void removeVertex(Long id) throws NullPointerException;

    /**
     * Remove an edge from the graph. It does not remove dangling nodes;
     * {@link #removeVertex(java.lang.Long)} on the endpoints must be called
     * instead.
     *
     * @param src The source node in this directed multigraph
     * @param dest The dest node in this directed multigraph
     * @param label The label of the edge to be created
     * @throws IllegalArgumentException If src and dest are not present in the
     * vertex collection
     * @throws NullPointerException if one of the input is null
     */
    public void removeEdge(Long src, Long dest, Long label) throws IllegalArgumentException, NullPointerException;

    /**
     * Remove an edge from the graph. It does not remove dangling nodes;
     * {@link #removeVertex(java.lang.Long)} on the endpoints must be called
     * instead.
     *
     * @param edge The edge to be removed from the graph
     * @throws IllegalArgumentException If src and edges are not present in the
     * vertex collection
     * @throws NullPointerException if the input edge is null
     */
    public void removeEdge(Edge edge) throws IllegalArgumentException, NullPointerException;

    /**
     * Returns the set of vertices of the graph
     *
     * @return The set of vertices
     */
    public Collection<Long> vertexSet();

    /**
     * Returns The number of vertices
     *
     * @return The number of vertices
     */
    public int numberOfNodes();

    /**
     * Returns The number of edges
     *
     * @return The number of edges
     */
    public int numberOfEdges();

    /**
     * Returns the set of edges of the graph
     *
     * @return The set of edges
     */
    public Collection<Edge> edgeSet();

    /**
     * Returns the set of labels in the graph
     *
     * @return The set of labels
     */
    public Collection<Long> labelSet();

    
    /**
     * 
     * @param vertex
     * @return the sum of InDegree and OutDegree
     * @throws NullPointerException 
     */
    public abstract int degreeOf(Long vertex)  throws NullPointerException;
    
    /**
     * Returns the "in degree" of the specified vertex. An in degree of a vertex
     * in a directed graph is the number of incoming directed edges from that
     * vertex. See <a href="http://mathworld.wolfram.com/Indegree.html">
     * http://mathworld.wolfram.com/Indegree.html</a>.
     *
     * @param vertex vertex whose degree is to be calculated.
     *
     * @return the degree of the specified vertex.
     * @throws NullPointerException if the input vertex is null
     */
    public int inDegreeOf(Long vertex) throws NullPointerException;

    /**
     * Returns a set of all edges incoming into the specified vertex.
     *
     * @param vertex the vertex for which the list of incoming edges to be
     * returned.
     *
     * @return a set of all edges incoming into the specified vertex.
     * @throws NullPointerException if the input vertex is null
     */
    public Collection<Edge> incomingEdgesOf(Long vertex) throws NullPointerException;

    /**
     * Returns the iterator over all edges incoming into the specified vertex.
     *
     * @param vertex the vertex for which the list of incoming edges to be
     * returned.
     *
     * @return an iterator of all edges incoming into the specified vertex.
     * @throws NullPointerException if the input vertex is null
     */
    public Iterator<Edge> incomingEdgesIteratorOf(Long vertex) throws NullPointerException;

    /**
     * Returns the "out degree" of the specified vertex. An out degree of a
     * vertex in a directed graph is the number of outward directed edges from
     * that vertex. See <a href="http://mathworld.wolfram.com/Outdegree.html">
     * http://mathworld.wolfram.com/Outdegree.html</a>.
     *
     * @param vertex vertex whose degree is to be calculated.
     *
     * @return the degree of the specified vertex.
     *
     */
    public int outDegreeOf(Long vertex) throws NullPointerException;

    /**
     * Returns a set of all edges outgoing from the specified vertex.
     *
     * @param vertex the vertex for which the list of outgoing edges to be
     * returned.
     * @return a set of all edges outgoing from the specified vertex.
     * @throws NullPointerException if the input vertex is null
     */
    public Collection<Edge> outgoingEdgesOf(Long vertex) throws NullPointerException;

    /**
     * Returns an iterator over all edges outgoing from the specified vertex.
     *
     * @param vertex the vertex for which the list of outgoing edges to be
     * returned.
     * @return a in iterator of all edges outgoing from the specified vertex.
     * @throws NullPointerException if the input vertex is null
     */
    public Iterator<Edge> outgoingEdgesIteratorOf(Long vertex) throws NullPointerException;

    /**
     * Returns an iterator over all edges with a specific label.
     *
     * @param label the label of edges to be returned
     * @return a in iterator of all edges with such label
     * @throws NullPointerException if the input label is null
     */
    public Iterator<Edge> labeledEdgesIteratorOf(Long label) throws NullPointerException;

    
    /**
     * Returns an iterator over all edges with a specific set of labels.
     *
     * @param labels the set of labels of edges to be returned
     * @return a in iterator of all edges with one of such labels
     * @throws NullPointerException if the input label set is null
     */
    public Iterator<Edge> labeledEdgesIteratorOf(Set<Long> labels) throws NullPointerException;
    
    
    
    
    
    /**
     * Merge this graph with the input graph. Parallelize the operations only if
     * needed otherwise go recursively
     *
     * @param graph The input graph to be merged to this
     * @return this graph
     * @throws NullPointerException if the input graph is null
     * @throws ExecutionException if something happens in the merge phase
     */
    public BaseMultigraph merge(BaseMultigraph graph) throws NullPointerException, ExecutionException;

    /**
     * Check if the input vertex is contained in the Multigraph
     *
     * @param vertex The input vertex to be checked
     * @return True if it contains the node, false otherwise
     * @throws NullPointerException If the vertex is null
     */
    public boolean containsVertex(Long vertex) throws NullPointerException;

    /**
     * Get the edges between source and destination
     *
     * @param src The source node
     * @param dest The destination node
     * @return The edges between source and destination
     * @throws NullPointerException If either src or dest are null
     */
    public Collection<Edge> getEdge(Long src, Long dest) throws NullPointerException;

    /**
     * Returns <tt>true</tt> if and only if this graph contains an edge going
     * from the source vertex to the target vertex. In undirected graphs the
     * same result is obtained when source and target are inverted. If any of
     * the specified vertices does not exist in the graph, or if is <code>
     * null</code>, returns <code>false</code>.
     *
     * @param src source vertex of the edge.
     * @param dest target vertex of the edge.
     *
     * @return <tt>true</tt> if this graph contains the specified edge.
     */
    public boolean containsEdge(Long src, Long dest);

    /**
     * Return the edges (incoming and outgoing) of a specific node.
     *
     * @param id The id of the node
     * @return The set of edges associated with the input node
     * @throws NullPointerException If the id is null
     */
    public Collection<Edge> edgesOf(Long id) throws NullPointerException;
    
    public Collection<Long> neighborsOf(Long id);
    
    
    
    public class LabeledEdgeIterator implements Iterator<Edge> {

        private final Iterator<Edge> edges;        
        private final Set<Long> labels;        
        private Edge next = null;
        
        protected LabeledEdgeIterator(Iterator<Edge> edges, Long label) {
            this.edges = edges;            
            this.labels = new HashSet<>();
            this.labels.add(label);
        }
        
        protected LabeledEdgeIterator(Iterator<Edge> edges, Set<Long> labels) {
            this.edges = edges;
            this.labels = labels;

        }
        
        @Override
        public boolean hasNext() {
            if (next != null) {
                return true;
            }
            Edge tmp;
            while (edges.hasNext() && next == null) {
                tmp = edges.next();
                if (this.labels.contains(tmp.getLabel())) {
                    next = tmp;
                }
            }
            return next != null;
        }
        
        @Override
        public Edge next() {
            if (next == null) {
                throw new NoSuchElementException("No more elements to explore");
            }
            Edge tmp = next;
            next = null;
            return tmp;
        }
        
    }
    
}
