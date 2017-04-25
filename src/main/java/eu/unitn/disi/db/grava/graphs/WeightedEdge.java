/*
 * Copyright (C) 2016 Matteo Lissandrini
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

/**
 *
 * @author Matteo Lissandrini
 */
public class WeightedEdge extends Edge{
    
    private double weight = 0.0;
    
    public WeightedEdge(long src, long dest, long rel) {
        this(src, dest, rel, 0.0);
        
    }
    
    public WeightedEdge(long src, long dest, long rel, double weight) {
        super(src, dest, rel);
        this.weight = weight;
    }

    
    /**
     * 
     * @return  the weight of the edge
     */
    public double getWeight() {
        return weight;
    }

    
    /**
     * 
     * @param weight  change the weight of the edge
     */
    public void setWeight(double weight) {
        this.weight = weight;
    }
    
    
    
    @Override
    public String toString() {
        return this.getSource() + "-[" + this.getLabel() + " : " + this.weight+"]->" + this.getDestination();
    }
    
    @Override
    public WeightedEdge reversedEdge() {
        return new WeightedEdge(this.getDestination(), this.getSource(), this.getLabel(), weight);
    }

}
    

