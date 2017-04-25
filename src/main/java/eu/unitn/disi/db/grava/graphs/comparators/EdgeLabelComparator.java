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
import java.util.Comparator;
import java.util.Map;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class EdgeLabelComparator implements Comparator<Edge>{

    private final Map<Long, Double> labelWeights;
    private boolean inverse = false;
    
    public EdgeLabelComparator(Map<Long, Double> labelWeights){
        this.labelWeights = labelWeights;        
    }
    
    public EdgeLabelComparator(Map<Long, Double> labelWeights, boolean inverse){
        this.labelWeights = labelWeights;        
        this.inverse = inverse;
    }
    
    @Override
    public int compare(Edge o1, Edge o2) {
           return (inverse ? -1 : 1) * labelWeights.get(o1.getLabel()).compareTo(labelWeights.get(o2.getLabel()));
    }
    
}
