/*
 * Copyright (C) 2014 Davide Mottin <mottin@disi.unitn.eu>
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

package eu.unitn.disi.db.grava.utils;

import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class GraphAlgorithms {   
    
    private static class KeyPair implements Comparable<KeyPair> {
        private final long node; 
        private final double weight; 

        public KeyPair(long node, double weight) {
            this.node = node;
            this.weight = weight;
        }
        
        public long getNode() {
            return node;
        }

        public double getWeight() {
            return weight;
        }
        
        @Override
        public int hashCode() {
            int hash = 5;
            hash = 47 * hash + (int) (this.node ^ (this.node >>> 32));
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final KeyPair other = (KeyPair) obj;
            if (this.node != other.node) {
                return false;
            }
            return true;
        }
        
        @Override
        public int compareTo(KeyPair o) {
            if (weight > o.weight) 
                return 1; 
            if (weight < o.weight)
                return -1;
            return 0;
        }
    }
    
    
    private static class NodeDistances {
        private final Set<Long> visitedNodes; 
        private final TreeMap<KeyPair,Double> minDistNodes; 
        private final Long source; 
        private final Map<Long,Edge> previousEdge; 
        
        public NodeDistances(Long source) {
            this.source = source; 
            visitedNodes = new HashSet<>();
            minDistNodes = new TreeMap<>();//Ordered by weights
            minDistNodes.put(new KeyPair(source, 0.0), 0.0);
            previousEdge = new HashMap<>();
        }

        public TreeMap<KeyPair, Double> getMinDistNodes() {
            return minDistNodes;
        }
        
        public Set<Long> getVisitedNodes() {
            return visitedNodes;
        }
        
        public Long getSource() {
            return source;
        }

        public Map<Long, Edge> getPreviousNode() {
            return previousEdge;
        }
    }    
    
    public static List<List<Edge>> shortestPath(Multigraph graph, List<Long> sources, Map<Long, Double> edgeWeights) {
        final NodeDistances[] nodeDistances = new NodeDistances[sources.size()]; 
        NodeDistances dist2; 
        List<List<Edge>> shortestPaths = new ArrayList<>(); 
        KeyPair minDistNode;
        Pair<Integer, Integer> pathPair; 
        Double weight, edgeWeight; 
        long adjNode;
        Map<Long, Set<Integer>> commonVisitedNodes = new HashMap<>(); 
        Set<Integer> visitedSources; 
        Set<Pair<Integer,Integer>> pathPairs = new HashSet<>();
        Collection<Edge> neighborEdges; 
        List<Edge> path; 
        int i, j; 
        //1. Initialize
        i = 0; 
        for (Long src : sources) {
            nodeDistances[i] = new NodeDistances(src);
            i++;
        }
        //1.1 Build the path pairs to create. 
        for (i = 0; i <  sources.size(); i++) {
            for (j = i + 1; j < sources.size(); j++) {
                pathPairs.add(new Pair<>(i,j));
            }
        }
        while (!pathPairs.isEmpty()) {
            //2. Single Step in each source -- PARALLELIZABLE!!!
            for (i = 0; i < nodeDistances.length; i++) {
                NodeDistances dist =  nodeDistances[i];
                minDistNode = dist.minDistNodes.pollFirstEntry().getKey();
                neighborEdges = graph.incomingEdgesOf(minDistNode.getNode());
                for (Edge e : neighborEdges) {
                    adjNode = e.getSource();
                    if (!dist.visitedNodes.contains(adjNode)) {
                        weight = dist.minDistNodes.get(new KeyPair(adjNode, 0.0));
                        //if weight is null then the distance has not been computed yet
                        edgeWeight = edgeWeights.get(e.getLabel());
                        edgeWeight = edgeWeight == null? 0.0 : edgeWeight;
                        if (weight == null || minDistNode.getWeight() + edgeWeight < weight) {
                            //Update weights
                            dist.minDistNodes.put(new KeyPair(adjNode, minDistNode.getWeight() + edgeWeight), minDistNode.getWeight() + edgeWeight);
                            dist.previousEdge.put(adjNode, e);
                        }
                    }
                }
                visitedSources = commonVisitedNodes.get(minDistNode.node);
                //3. If visited intersect, store the path - increment the counter
                if (visitedSources == null) {
                    visitedSources = new HashSet<>();
                } else {
                    for (Integer source : visitedSources) {
                        int nodeA, nodeB; 
                        if (i < source) {
                            nodeA = i; 
                            nodeB = source;
                        } else {
                            nodeA = source; 
                            nodeB = i; 
                        }
                        pathPair = new Pair<>(nodeA, nodeB);
                        //Check if we have already computed the path. 
                        if (pathPairs.contains(pathPair)) {
                            System.out.printf("Removed path pair %s\n", pathPair);
                            pathPairs.remove(pathPair);
                            //Build the path.
                            path = buildPartialPath(minDistNode.node, dist.previousEdge, true);
                            dist2 = nodeDistances[source];
                            path.addAll(buildPartialPath(dist2.source, dist2.previousEdge, false));
                            shortestPaths.add(path);
                        } 
                    }
                }
                visitedSources.add(i);
                commonVisitedNodes.put(minDistNode.node, visitedSources);
                
                dist.visitedNodes.add(minDistNode.node);
            }
        }
        return shortestPaths; 
    }
    
    private static List<Edge> buildPartialPath(Long startingNode, Map<Long,Edge> previousEdge, boolean reverse) {
        List<Edge> edges  = new ArrayList<>(); 
        Edge prev = previousEdge.get(startingNode);
        while (prev != null) {
            edges.add(prev);
            prev = previousEdge.get(prev.getSource() == startingNode.longValue()? prev.getDestination() : prev.getSource());
        }
        if (reverse) {
            Collections.reverse(edges);
        }
        return edges; 
    }
    
            
}
