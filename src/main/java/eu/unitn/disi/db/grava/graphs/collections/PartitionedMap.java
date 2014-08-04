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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A partionedmap contains an array of hashmaps ({@link HashMap}), that are indexed using 
 * a hashing funtion. This will allow to load more elements in the same map. 
 * 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class PartitionedMap<K,V> implements PartitionableMap<K, V>, Serializable {
    private Map<K,V>[] maps;
    private final int partitions;
    
    private static final int DEFAULT_INITIAL_CAPACITY = 16;
    private static final float DEFAULT_LOAD_FACTOR = 0.75F;
    private static final int DEFAULT_NUM_PARTITIONS = 2;
    

    public PartitionedMap(int initialCapacity, float loadFactor, int numPartitions) {
        if (numPartitions < 1) {
            throw new IllegalArgumentException("Number of partitions must be > 1");
        }
        partitions = numPartitions;
        maps = new Map[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            maps[i] = new HashMap<K, V>(initialCapacity, loadFactor);
        }
        
    }
    
    public PartitionedMap(int initialCapacity, float loadFactor) {
        this(initialCapacity, loadFactor, DEFAULT_NUM_PARTITIONS);
    }

    public PartitionedMap(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    public PartitionedMap() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    private int getIndexPartition(Object key) {
        return Math.abs(key.hashCode()) % partitions;
    }
    
    private static int hash(int h) {
      // This function ensures that hashCodes that differ only by
      // constant multiples at each bit position have a bounded
      // number of collisions (approximately 8 at default load factor).
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    public PartitionedMap(Map<? extends K, ? extends V> m) {
        this();
        Iterator<K> it = (Iterator<K>) m.keySet().iterator();
        K key; 
        while (it.hasNext()) {
            key = it.next();
            addEntry(key, m.get(key));
        }
    }

    private V addEntry(K key, V value) {
        int index = getIndexPartition(key);
        return maps[index].put(key, value);
    }
    
    @Override
    public int size() {
        int size = 0;
        for (int i = 0; i < partitions; i++) {
            size += maps[i].size();
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size() != 0;
    }

    @Override
    public boolean containsKey(Object key) {
        int index = getIndexPartition(key);
        return maps[index].containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        for (int i = 0; i < partitions; i++) {
            if (maps[i].containsValue(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public V get(Object key) {
        int index = getIndexPartition(key);
        return maps[index].get(key);
    }

    @Override
    public V put(K key, V value) {
        return addEntry(key, value);
    }

    @Override
    public V remove(Object key) {
        int index = getIndexPartition(key);
        return maps[index].remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        Iterator<K> it = (Iterator<K>) m.keySet().iterator();
        K key; 
        while (it.hasNext()) {
            key = it.next();
            addEntry(key, m.get(key));
        }
    }

    @Override
    public void clear() {
        for (int i = 0; i < partitions; i++) {
            maps[i].clear();
        }
    }

    @Override
    public Set<K> keySet() {
        Set<K>[] keySets = new Set[partitions];
        for (int i = 0; i < partitions; i++) {
            keySets[i] = maps[i].keySet();
        }
        return new PartitionedSet<K>(keySets);
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<K, V> getPartition(int i) {
        if (i < 0 || i > size() - 1) {
            throw new IllegalArgumentException("i cannot be less than 0 or greter than num partitions");
        }
        return maps[i];
    }

    @Override
    public int getNumPartitions() {
        return partitions;
    }

    @Override
    public int getPartitionSize(int i) {
        return getPartition(i).size();
    }

}
