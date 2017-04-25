/*
 * Copyright (C) 2016 Matteo Lissandrini <ml@disi.unitn.eu>
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
package eu.unitn.disi.db.grava.graphs.comparators;

import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.WeightedEdge;
import java.util.Comparator;
import java.util.Map;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class EdgeWeightComparator  implements Comparator<Edge> {
    
    
    private final Map<Edge, Double> edgeWeights;
    private boolean inverse = false;
    
    public EdgeWeightComparator(){
        this.edgeWeights = null;                
    }
    
    public EdgeWeightComparator( boolean inverse){
        this.edgeWeights = null;        
        this.inverse = inverse;
    }
    
    public EdgeWeightComparator(Map<Edge, Double> edgeWeights){
        this.edgeWeights = edgeWeights;        
    }
    
    public EdgeWeightComparator(Map<Edge, Double> edgeWeights, boolean inverse){
        this.edgeWeights = edgeWeights;        
        this.inverse = inverse;
    }
    
    @Override
    public int compare(Edge o1, Edge o2) {
           if(edgeWeights == null && (o1 instanceof WeightedEdge) && (o2 instanceof WeightedEdge)){
               return  (inverse ? -1 : 1) *Double.compare(((WeightedEdge)o1).getWeight(), ((WeightedEdge)o2).getWeight()); 
           }
           if(edgeWeights==null){
               throw new NullPointerException("No Edge weight is defined and edges are not of type WeightedEdge");
           }
           
           
           return (inverse ? -1 : 1) * edgeWeights.get(o1).compareTo(edgeWeights.get(o2));
    }
    
    
}
