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

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Represents an a collection that is partitioned (i.e. it contains and array
 * of collections) ad it is managed using a hash on the input elements. 
 * 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public abstract class AbstractPartitionedCollection<E> 
    extends AbstractCollection<E>
    implements PartitionableCollection<E> 
{
    protected int partitions; 

    protected abstract Collection<E>[] getPartitions();

    /**
     * Compute the index of the partition in which the object is stored. It uses
     * a simple module operation to get the index. 
     * @param key The object to know which partition it belongs
     * @return The partition index of the input object
     */
    protected int getIndexPartition(Object key) {
        return Math.abs(key.hashCode()) % partitions;
    }
    
    @Override
    public Iterator<E> iterator() {
        return new PartitionIterator<E>();
    }

    protected class PartitionIterator<E> implements Iterator<E> {
        private int currentPartition; 
        private Iterator<E> currentIterator;
        
        @Override
        public boolean hasNext() {
            return currentIterator.hasNext() || currentPartition < partitions; 
        }

        @Override
        public E next() {
            if (currentIterator.hasNext()) {
                return currentIterator.next();
            }
            currentPartition++;
            if (currentPartition < partitions) {
                currentIterator = (Iterator<E>) getPartitions()[currentPartition].iterator();
                return currentIterator.next();
            }
            throw new NoSuchElementException("Cannot iteratoe over the set");
        }

        @Override
        public void remove() {
            currentIterator.remove();
        }
    }

    @Override
    public Collection<E> getPartition(int i) {
        if (i < 0 || i > size() - 1) {
            throw new IllegalArgumentException("i cannot be less than 0 or greter than num partitions");
        }
        return getPartitions()[i];

    }
    
    @Override
    public int size() {
        int size = 0;
        Collection<E>[] part = getPartitions();
        for (int i = 0; i < partitions; i++) 
        {
            size += part[i].size();
        }
        return size;
    }               

    @Override
    public boolean add(E e) {
        int index = getIndexPartition(e);
        return getPartitions()[index].add(e);
    }
    
    @Override
    public boolean remove(Object o) {
        int index = getIndexPartition(o);
        return getPartitions()[index].remove(o);        
    }

    
    @Override
    public int getNumPartitions() {
        return partitions;
    }

    @Override
    public int getPartitionSize(int i) {
        return getPartition(i).size();
    }   

    @Override
    public boolean contains(Object o) {
        int index = getIndexPartition(o);
        return getPartitions()[index].contains(o);
    }
    
    @Override
    public void clear() {
        Collection<E>[] part = getPartitions();
        for (int i = 0; i < part.length; i++) {
            part[i].clear();
        }
    }
}
