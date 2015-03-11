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
package eu.unitn.disi.db.grava.utils;

import eu.unitn.disi.db.grava.exceptions.ParseException;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Flushable;
import java.io.IOException;
import java.io.InvalidClassException;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * This is a utility class with fast methods to access files, split strings and
 * print maps and other things. 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public final class Utilities {
    public static final int PAGE_SIZE = 1024;

    private static class TableComparator implements Comparator<long[]> {
        @Override
        public int compare(long[] o1, long[] o2) {
            if (o1 == null) return -1; 
            if (o2 == null) return 1;
            if (o1[0] > o2[0]) return 1;
            if (o1[0] < o2[0]) return -1;
            return 0;
        }
    }

    private Utilities() {
    }

    public static <A, B> String mapToString(Map<A, B> m) {
        StringBuilder sb = new StringBuilder();
        Set<A> keys = m.keySet();
        sb.append("{");
        for (A key : keys) {
            sb.append("(").append(key).append(",").append(m.get(key)).append(")");
        }
        sb.append("}");
        return sb.toString();
    }
    
    public static <T> int intersectionSize(Set<T> checkCollection, Collection<T> inputCollection) {
        int count = 0;
        for (T t : inputCollection) {
            if (checkCollection.contains(t)) {
                count++;
            }
        }
        return count;
    }
    
    public static <T> boolean intersectionNotEmpty(Set<T> checkCollection, Collection<T> inputCollection) {
        for (T t : inputCollection) {
            if (checkCollection.contains(t)) {
                return true;
            }
        }
        return false;
    }

    public static void closeAll(Closeable... cls) {
        for (Closeable c : cls) {
            close(c);
        }
    }

    public static void close(Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException ex) {
            }
        }
    }

    public static void flushAll(Flushable... fs) throws IOException {
        for (Flushable f : fs) {
            flush(f);
        }
    }

    public static void flush(Flushable f) throws IOException {
        if (f != null) {
            f.flush();
        }
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception ex) {
            }
        }
    }

    public static void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (Exception ex) {
            }
        }
    }

    public static void closeAllStatements(Statement... stmts) {
        for (Statement s : stmts) {
            closeStatement(s);
        }
    }

    public static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception ex) {
            }
        }
    }
    
    /**
     * Count the number of lines in a file in a very fast way. </br>
     * <i>This method may have a problem with non-ascii files</i>
     * @param file The input file for which you want to know the number of lines
     * @return The number of lines
     * @throws IOException If an error occurs while reading the file
     */
    public static int countLines(String file) throws IOException {
        final byte nl = 0xA;
        BufferedInputStream in = null;
        int count = 0;
        byte[] buf = new byte[1024];
        int len, i;
        
        try {
            in = new BufferedInputStream(new FileInputStream(file));
            while ((len = in.read(buf)) != -1) {
                for (i = 0; i < len; i++) {
                    if (buf[i] == nl) {
                        count++;
                    }
                }
            }
        } catch (IOException ex) {
            throw new IOException(ex);
        } finally  {
            close(in);
        }
        return count;
    }
    
    /**
     * Method to fast Split a string using a character as separator
     * @param line
     * @param split
     * @param numberOfChunks
     * @return
     */
    public static String[] fastSplit(String line, char split, int numberOfChunks) {
        String[] temp = new String[numberOfChunks + 1];
        String chunk = "";
        int wordCount = 0;
        int i = 0;
        int j = line.indexOf(split);  // First substring
        while (j >= 0) {
            chunk = line.substring(i, j);
            if(chunk.trim().length()>0){
                temp[wordCount++] = chunk;
            }
            i = j + 1;
            j = line.indexOf(split, i);   // Rest of substrings
        }
        temp[wordCount++] = line.substring(i); // Last substring
        String[] result = new String[wordCount];
        System.arraycopy(temp, 0, result, 0, wordCount);
        return result;
    }

    /**
     * Read from file using optimized functions to get data as fast as possible
     * @param file The file to be loaded.
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static String readFile(String file) throws IOException {
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
        ByteBuffer buffer = new ByteBuffer();
        byte[] buf = new byte[PAGE_SIZE];
        int len;
        while ((len = in.read(buf)) != -1) {
            buffer.put(buf, len);
        }
        in.close();
        return new String(buffer.buffer, 0, buffer.write);
    }

    static class ByteBuffer {
        public byte[] buffer = new byte[256];
        public int write;

        public void put(byte[] buf, int len) {
            ensure(len);
            System.arraycopy(buf, 0, buffer, write, len);
            write += len;
        }

        private void ensure(int amt) {
            int req = write + amt;
            if (buffer.length <= req) {
                byte[] temp = new byte[req * 2];
                System.arraycopy(buffer, 0, temp, 0, write);
                buffer = temp;
            }
        }
    }

    public static void shutdownAndAwaitTermination(ExecutorService pool) {
        if (pool != null) {
            pool.shutdown(); // Disable new tasks from being submitted
            try {
                // Wait a while for existing tasks to terminate
                if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                    pool.shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                        throw new InterruptedException("Pool did not terminate");
                    }
                }
            } catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                pool.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }
    
    /**
     * Sort a bidimensional array using the first value of each array 
     * @param table The multidimensional array to be sorted
     */
    public static void binaryTableSort(long[][] table) {
        Arrays.sort(table, new TableComparator());
    }

    /**
     * Sort a bidimensional array using a parallelized version of merge sort
     * @param table 
     * @param numThreads
     */
    public static void fastBinaryTableSort(long[][] table, int numThreads) {
        int chunkSize = (int) Math.round(table.length /(double)numThreads + 0.5); 
        List<Future<double[][]>> lists = new ArrayList<>();
        
       

        ////////////////////// USE 1 THREAD
        //chunkSize =  graphNodes.size();
        ////////////////////// USE 1 THREAD


//        for (Long node : graphNodes) {
//            //if (nodesSimilarity(queryConcept, node) > MIN_SIMILARITY) {
//            if (count % chunkSize == 0) {
//                tmpChunk = new LinkedList<>();
//                nodesChunks.add(tmpChunk);
//
//            }
//
//            tmpChunk.add(node);
//            count++;
//
//            //} else {
//            //     loggable.error("Similarity not satisfied..");
//            //}
//        }
        
        Arrays.sort(table, new TableComparator());
    }

    
    public static int binaryTableSearch(long[][] table, long vertex) {
        return binaryTableSearch(table, 0, vertex);
    }

    public static int binaryTableSearch(long[][] table, int searchField, long vertex) {
        return binaryTableSearch0(table, searchField, 0, table.length, vertex);
    }

    
    public static int binaryTableSearch0(long[][]a, int searchField, int fromIndex, int toIndex, long key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = a[mid][searchField]; //Take the first

            if (midVal < key) {
                low = mid + 1;
            } else if (midVal > key) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return -(low + 1);  // key not found.
    }
    
    public static <K,V> boolean emptyIntersection(Map<K,V> map) {
        Set<K> keys = map.keySet();
        for (K k : keys) {
            if (k.equals(map.get(k))) {
                return false;
            }
        }
        return true;
    }
    
    
    /**
     * Read a file into a collection of strings
     * @param file The input file to be read
     * @param collection The collection of elements to be populated
     * @throws NullPointerException If some of the inputs is null
     * @throws IOException If the file is not readable
     */
    public static void readFileIntoCollection(String file, Collection<String> collection)
            throws NullPointerException, IOException
    
    {
        if (collection == null || file == null) {
            throw new NullPointerException("Input cannot be null");
        }
        BufferedReader fileReader;
        String line; 
        int lineNo = 0;
        
        fileReader = new BufferedReader(new FileReader(file));
        while ((line = fileReader.readLine()) != null) {
            lineNo++;
            if (!"".equals(line.trim())) {
                collection.add(line);
            }
        }
    }
    
    /**
     * Read a file into a collection of numbers
     * @param <T> The type into which converting the string in the lines
     * @param file The input file (each line represents a record in the collection)
     * @param collection The collection of elements to be populated
     * @param castType The type into which casting the lines
     * @throws IOException If the file is not readable
     * @throws NullPointerException If some of the inputs is null
     * @throws InvalidClassException If the input type does not allow a string constructor
     * @throws ParseException If the line is not of the correct type
     */
    public static <T extends Number> void 
        readFileIntoCollection(String file, Collection<T> collection, Class<T> castType)  
        throws IOException, NullPointerException, InvalidClassException, ParseException 
    {
        if (collection == null || file == null || castType == null) {
            throw new NullPointerException("Input cannot be null");
        }
        Constructor<T> numConstructor;
        BufferedReader fileReader;
        String line; 
        int lineNo = 0;
        
        try {
            numConstructor = castType.getConstructor(String.class);
            fileReader = new BufferedReader(new FileReader(file));
            while ((line = fileReader.readLine()) != null) {
                lineNo++;
                if (!"".equals(line.trim())) {
                    collection.add(numConstructor.newInstance(line));
                }
            }
        } catch (NoSuchMethodException ex) {
            throw new InvalidClassException(String.format("The input class %s cannot be parsed since it does not contain a constructor that takes in input a String", castType.getCanonicalName()));
        } catch (Exception ex) {
            throw new ParseException(String.format("Cannot convert line %d into class %s", lineNo, castType.getCanonicalName()), ex);
        }
    }
    
    /**
     * Read a file into a key-value map, the keys and values can be cast to any 
     * arbitrary class
     * @param <K> The class of the keys
     * @param <V> The class of the values
     * @param file The input file to be stored in a map
     * @param separator The sepator used to identify fields
     * @param map The map to be populated
     * @param keyCastType The class of the keys to be casted
     * @param valueCastType The class of the values to be casted
     * @throws IOException If the file is not readable
     * @throws NullPointerException If some of the inputs is null
     * @throws InvalidClassException If the input type does not allow a string constructor
     * @throws ParseException If the line is not of the correct type
     */
    public static <K extends Number,V extends Number> void 
            readFileIntoMap(String file, String separator, Map<K,V> map, Class<K> keyCastType, Class<V> valueCastType) 
            throws IOException, NullPointerException, InvalidClassException, ParseException 
    {
        if (map == null || file == null || keyCastType == null || valueCastType == null || separator == null) {
            throw new NullPointerException("Input cannot be null");
        }
        Constructor<K> keyConstructor;
        Constructor<V> valueConstructor;
        BufferedReader fileReader;
        String line;
        String[] splittedLine; 
        int lineNo = 0;
        
        try {
            keyConstructor = keyCastType.getConstructor(String.class);
            valueConstructor = valueCastType.getConstructor(String.class);
            fileReader = new BufferedReader(new FileReader(file));
            while ((line = fileReader.readLine()) != null) {
                lineNo++;
                if (!"".equals(line.trim())) {
                    splittedLine = line.split(separator);
                    if (splittedLine.length != 2) {
                        throw new ParseException("Line %d has an invalid format", lineNo);
                    }
                    try {
                        map.put(keyConstructor.newInstance(splittedLine[0]), valueConstructor.newInstance(splittedLine[1]));
                    } catch (Exception ex) {
                        throw new ParseException(String.format("Cannot convert line %d into classes %s and %s", lineNo, keyCastType.getCanonicalName()), valueCastType.getCanonicalName(), ex);
                    }
                }
            }
        } catch (NoSuchMethodException ex) {
            throw new InvalidClassException(String.format("The input class %s or %s cannot be parsed since it does not contain a constructor that takes in input a String", keyCastType.getCanonicalName(), valueCastType.getCanonicalName()));
        }        
    }
}
