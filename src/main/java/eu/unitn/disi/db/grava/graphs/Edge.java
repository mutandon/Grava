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

/**
 * This is an unmodifiable class that stores informations about an edge.
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class Edge {
    /**
     * A generic label used in case of matching none. 
     */
    public static long GENERIC_EDGE_LABEL = -1;
    /*
     * Source node
     */
    private final long src;
    /*
     * Destination node
     */
    private final long dest;
    /*
     * Relationship
     */
    private final long rel;
    //Pre-cache the hashing. 
    private int hash = -1;

    /**
     * Construct an edge specifying source, destination and relationship
     * @param src Source node
     * @param dest Destination node
     * @param rel Relationship node
     */
    public Edge(long src, long dest, long rel) {
        this.src = src;
        this.dest = dest;
        this.rel = rel;
        hash = 7;
        hash = 59 * hash + (int) (this.src ^ (this.src >>> 32));
        hash = 59 * hash + (int) (this.dest ^ (this.dest >>> 32));
        hash = 59 * hash + (int) (this.rel ^ (this.rel >>> 32));
    }
    
    public Long getLabel() {
        return rel;
    }
    
    public Long getSource() {
        return src;
    }
    
    public Long getDestination() {
        return dest;
    }

    
    public String getId(){
        return src + "|" + dest + "|" + rel;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Edge other = (Edge) obj;
        if (this.src != other.src) {
            return false;
        }
        if (this.dest != other.dest) {
            return false;
        }
        if (this.rel != other.rel) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return src + "-[" + rel + "]->" + dest;
    }
    
    
}