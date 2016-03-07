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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;

/**
 * A partitioned list contains an array of {@link ArrayList{, that are indexed using 
 * a hashing funtion. This will allow to load more elements in the same list. 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class PartitionedList<E> 
    extends AbstractPartitionedCollection<E> 
    implements List<E>, Serializable, RandomAccess, Cloneable
{
    private ArrayList<E>[] lists; 
    private int sizes[];

    private static final int DEFAULT_INITIAL_CAPACITY = 10;
    private static final int DEFAULT_NUM_PARTITIONS = 2;

    
    public PartitionedList(int initialCapacity, int numPartitions) {
        if (numPartitions < 1) {
            throw new IllegalArgumentException("Number of partitions must be > 1");
        }
        partitions = numPartitions;
        lists = new ArrayList[partitions]; 
        for (int i = 0; i < partitions; i++) {
            lists[i] = new ArrayList<E>(initialCapacity);
        }
    }
    
    public PartitionedList(int initialCapacity) {
        this(initialCapacity, DEFAULT_NUM_PARTITIONS);
    }
    
    public PartitionedList() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_NUM_PARTITIONS);
    }
    
    @Override
    protected Collection<E>[] getPartitions() {
        return lists;
    }
    
    
    @Override
    public E get(int index) {
        int sum = 0, size = 0;
        if (index < 0) {
            throw new ArrayIndexOutOfBoundsException("Index is less than 0");
        }
            
        for (int i = 0; i < partitions; i++) {
            sum += lists[i].size();
            if (sum > index) {
                return lists[i].get(index - size);
            }
            size = sum;
        }
        throw new ArrayIndexOutOfBoundsException("Input index is greater than the size of the list");
    }

    @Override
    public E set(int index, E element) {
        int sum = 0, size = 0;
        if (index < 0) {
            throw new ArrayIndexOutOfBoundsException("Index is less than 0");
        }
            
        for (int i = 0; i < partitions; i++) {
            sum += lists[i].size();
            if (sum > index) {
                return lists[i].set(index - size, element);
            }
            size = sum;
        }
        throw new ArrayIndexOutOfBoundsException("Input index is greater than the size of the list");
    }

    @Override
    public void add(int index, E element) {
        throw new UnsupportedOperationException(
                "This operation is not permitted because it violates the partitioning rule.");
    }

    @Override
    public E remove(int index) {
        int sum = 0, size = 0;
        if (index < 0) {
            throw new ArrayIndexOutOfBoundsException("Index is less than 0");
        }
            
        for (int i = 0; i < partitions; i++) {
            sum += lists[i].size();
            if (sum > index) {
                return lists[i].remove(index - size);
            }
            size = sum;
        }
        throw new ArrayIndexOutOfBoundsException("Input index is greater than the size of the list");

    }

    @Override
    public int indexOf(Object o) {
        int index;
        int sumSizes = 0;
        for (int i = 0; i < partitions; i++) {
            index = lists[i].indexOf(o);
            if (index != -1) {
                return index + sumSizes;
            }
            sumSizes += lists[i].size();
        }
        return -1;
    }

    @Override
    public int lastIndexOf(Object o) {
        int index;
        int sumSizes = 0;
        for (int i = partitions - 1; i >= 0; i--) {
            index = lists[i].lastIndexOf(o);
            sumSizes += lists[i].size();
            if (index != -1) {
                return index + (size() - sumSizes);
            }
        }
        return -1;
    }
    
    //TODO: I have to test the new collections before continuing in this crazyness
    protected class PartitionListIterator<E> 
        extends PartitionIterator<E>
        implements ListIterator<E> 
    {
        private int currentPartition; 
        private Iterator<E> currentIterator;
        
        @Override
        public boolean hasPrevious() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public E previous() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int nextIndex() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int previousIndex() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void set(E e) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void add(E e) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }


    @Override
    public ListIterator<E> listIterator() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        throw new UnsupportedOperationException(
                "This operation is not permitted because it violates the partitioning rule.");
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        PartitionedList<E> newList = new PartitionedList<E>(DEFAULT_INITIAL_CAPACITY, partitions);
        for (int i = 0; i < partitions; i++) {
            newList.lists[i] = (ArrayList<E>) lists[i].clone();
        }
        return newList;
    }
}
