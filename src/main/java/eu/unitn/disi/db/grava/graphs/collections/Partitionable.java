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

/**
 * A partitionable object is a kind of object that contains a set of indeaxable
 * partitions. These kind of objects are typically slower than the normal but
 * allows to load more information into memory, splitting them into chunks.
 * 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public interface Partitionable {
    /**
     * Returns the total number of partitions in the object
     * @return The number of partitions
     */
    public int getNumPartitions();
    /**
     * Returns the size of the i-th partition. Partition index start by 0, like
     * a normal array. If the index is out of bounds an {@link IllegalArgumentException} 
     * is raised. 
     * @param i The index of the partition to get the size
     * @return The size of the i-th partition
     * @throws IllegalArgumentException if the index of the partition is less 
     * than 0 or greater then the number of partitions. 
     */
    public int getPartitionSize(int i);
}
