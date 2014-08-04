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

package eu.unitn.disi.db.grava.graphs.collections;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Partitioned Set containes a set of sets, in order to keep objects into 
 * different partitions. 
 * 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class PartitionedSet<E> 
    extends AbstractPartitionedCollection<E> 
    implements Serializable, Set<E>
{
    private Set<E>[] sets;
    
    private static final int DEFAULT_INITIAL_CAPACITY = 16;
    private static final float DEFAULT_LOAD_FACTOR = 0.75F;
    private static final int DEFAULT_NUM_PARTITIONS = 2;
    
    public PartitionedSet(int initialCapacity, float loadFactor, int numPartitions) {
        if (numPartitions < 1) {
            throw new IllegalArgumentException("Number of partitions must be > 1");
        }
        partitions = numPartitions;
        sets = new Set[partitions];
        for (int i = 0; i < partitions; i++) {
            sets[i] = new HashSet<E>(initialCapacity, loadFactor);
        }
    }

    public PartitionedSet(Collection<? extends E> c) {
        this();
        Iterator<? extends E> it = c.iterator();
        E key; 
        while (it.hasNext()) {
            key = it.next();
            addEntry(key);
        }
    }

    public PartitionedSet(int initialCapacity, float loadFactor) {
        this(initialCapacity, loadFactor, DEFAULT_NUM_PARTITIONS);
    }   

    public PartitionedSet() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public PartitionedSet(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }
    
    PartitionedSet(Set<E>[] sets) {
        this.sets = sets;
        partitions = sets.length;
    }
    
    private boolean addEntry(E el) {
        int index = getIndexPartition(el);
        return sets[index].add(el);
    }
    
    @Override
    protected Collection<E>[] getPartitions() {
        return sets;
    }
    
}
