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

import java.util.Comparator;

/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class PairFirstComparator implements Comparator<Pair<? extends Comparable,?>> {
    private final boolean asc; 

    public PairFirstComparator(boolean asc) {
        this.asc = asc;
    }

    public PairFirstComparator() {
        this.asc = true; 
    }
    
    
    @Override
    public int compare(Pair<? extends Comparable, ?> o1, Pair<? extends Comparable, ?> o2) {
        return asc? o1.getFirst().compareTo(o2.getFirst()) : -o1.getFirst().compareTo(o2.getFirst());
    }
}
