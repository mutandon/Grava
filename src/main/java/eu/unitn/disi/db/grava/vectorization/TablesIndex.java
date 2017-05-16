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
package eu.unitn.disi.db.grava.vectorization;

import eu.unitn.disi.db.mutilities.LoggableObject;
import eu.unitn.disi.db.mutilities.ThreadUtilities;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class TablesIndex extends LoggableObject {

    protected static final int DEFAULT_KEY_LENGTH = 3;
    protected static final int INITIAL_SIZE = 1000;

    protected final int k;

    private final String indexPath;
    private final boolean readOnly;
    private final boolean caching;
    private final int keyFileNameSize;

    private static final int ALPHABET_SIZE = 25;
    private final MemoryNeighborTables cachedTables;

    /**
     * Instantiate an index for MemoryNeighborTables that is read only and has
     * caching disactivated
     *
     * @param path
     * @param k depth of the memory table
     * @throws IOException
     */
    public TablesIndex(String path, int k) throws IOException {
        this(path, k, true, false, DEFAULT_KEY_LENGTH);
    }

    /**
     * Instantiate an index for MemoryNeighborTables that has caching
     * disactivated
     *
     * @param path
     * @param k depth of the memory table
     * @param readOnly
     * @throws IOException
     */
    public TablesIndex(String path, int k, boolean readOnly) throws IOException {
        this(path, k, readOnly, false, DEFAULT_KEY_LENGTH);
    }

    /**
     *
     * @param path
     * @param k depth of the memory table
     * @param readOnly
     * @param caching
     * @param keyFileSize number of characters in the file size name
     * @throws IOException
     */
    public TablesIndex(String path, int k, boolean readOnly, boolean caching, int keyFileSize) throws IOException {
        this.indexPath = path;
        this.k = k;
        this.readOnly = readOnly;
        this.caching = caching;
        this.cachedTables = new MemoryNeighborTables(k);
        this.keyFileNameSize = keyFileSize * 2;
        File savingDir = new File(path);
        // First, make sure the path exists
        // This will tell you if it is a directory
        if (!savingDir.exists() && !readOnly) {
            if (!savingDir.mkdirs()) {
                throw new IOException("Could not instantiate index at directory path");
            }
        } else if (!savingDir.exists() || !savingDir.isDirectory() || !savingDir.canWrite()) {
            throw new IOException("Illegal directory path " + path);
        }
        //debug("Instantiated Table in %s with K:%s and KeyfileSize:%s", path, k, keyFileSize);
    }

    /**
     * Computes the hashing value for the long node TODO: IMPROVE HASHING
     * FUNCTION
     *
     * @param decimal
     * @return
     * @throws NullPointerException
     * @throws IndexOutOfBoundsException
     */
    public String generateFileName(long decimal) throws NullPointerException, IndexOutOfBoundsException {
        StringBuilder mid = new StringBuilder();

        //(decimal*5381)%(9973*9973) + "";
        String decimalString = (decimal) + "";

        int lenght = decimalString.length();
        if (lenght < keyFileNameSize) {
            while (lenght < keyFileNameSize) {
                decimalString += "0";
                lenght++;
            }
        } else if (lenght > keyFileNameSize) {
            decimalString = decimalString.substring(0, keyFileNameSize);
        }

        for (int i = 0; i < decimalString.length(); i += 2) {
            mid.append((char) (97 + (Integer.parseInt(decimalString.substring(i, i + 2))) % ALPHABET_SIZE));
        }

        return "k" + this.k + "_" + mid.toString().toLowerCase() + ".idx";
    }

    /**
     * Save this memory table on file, updating in case existing files
     *
     * IF CACHING IS ACTIVE THIS METHOD UPDATES THE CACHE AND RETURNS THE ENTIRE
     *
     * @param tb the table to store
     * @return true if new files were created
     * @throws IOException
     */
    public boolean storeTable(MemoryNeighborTables tb) throws IOException {
        if (readOnly) {
            throw new IllegalStateException("This index is in Read Only MODE");
        }
        if (tb.getMaxLevel() != this.k) {
            throw new IllegalArgumentException("Could not save a table of size " + tb.getMaxLevel() + "  within a index of size " + this.k);
        }

        return save(tb.getTables());

    }

    /**
     * Load the memory tables for the node
     *
     * IF CACHING IS ACTIVE THIS METHOD UPDATES THE CACHE AND RETURNS THE ENTIRE
     * CACHE
     *
     * @param n the nodes for which loading the table
     * @return true if new files were created
     * @throws IOException
     */
    public MemoryNeighborTables loadTable(long n) throws IOException {
        List<Long> wrap = new ArrayList<>(1);
        wrap.add(n);
        return loadTable(wrap);
    }

    /**
     * Load the memory tables for all the nodes in the collection
     *
     * IF CACHING IS ACTIVE THIS METHOD UPDATES THE CACHE TODO: Parallelize
     *
     * @param nodes
     * @return the table containing the required nodes
     * @throws IOException
     */
    public MemoryNeighborTables loadTable(Collection<Long> nodes) throws IOException {

        Collection<Long> toLoad;

        if (caching) {
            toLoad = new ArrayList<>(nodes.size());
            for (Long node : nodes) {
                if (!this.cachedTables.contains(node)) {
                    toLoad.add(node);
                }
            }
        } else {
            toLoad = nodes;
        }

        return load(toLoad);
    }

    /**
     * Save the Map Table minimizing the file writes
     *
     * IF CACHING IS ACTIVE THIS METHOD UPDATES THE CACHE
     *
     * TODO: Parallelize
     *
     * @param table
     * @return true if new files were created
     */
    private boolean save(Map<Long, List<Map<Long, Integer>>> table) throws IOException {
        if (this.readOnly) {
            throw new IllegalStateException("This table is in Read Only MODE");
        }

        //UPDATE CACHE
        if (caching) {
            this.cachedTables.putAll(table);
        }

        //UPDATE FILES
        int initSize = (int) Math.ceil(Math.pow(ALPHABET_SIZE, this.keyFileNameSize) * 4 / 3);
        HashMap<String, List<Long>> keys = new HashMap<>(initSize);
        Map<Long, List<Map<Long, Integer>>> loadedMap;

        //First compute minimum set of files
        List<Long> tmpNodes;
        String key;
        boolean ret = false;
        for (Long n : table.keySet()) {
            key = generateFileName(n);
            tmpNodes = keys.get(key);
            if (tmpNodes == null) {
                tmpNodes = new ArrayList<>(table.size() / (ALPHABET_SIZE * this.keyFileNameSize));
                keys.put(key, tmpNodes);
            }
            tmpNodes.add(n);
        }

        //debug("Will load %s files for %s nodes", keys.size(), table.size());
        //Then update file after file
        for (Map.Entry<String, List<Long>> entry : keys.entrySet()) {
            key = entry.getKey();
            List<Long> nodes = entry.getValue();
            try {
                loadedMap = load(key, this.indexPath);
                if (loadedMap == null) {
                    ret = true; // New file will be generated
                    loadedMap = new HashMap<>(Math.max(nodes.size() * 4 / 3, INITIAL_SIZE));
                }
                for (long n : nodes) {
                    MemoryNeighborTables.put(n, table.get(n), loadedMap);
                }
                save(key, loadedMap);
            } catch (ClassNotFoundException ex) {
                fatal("Could not load index file for %s - Corrupted Index", ex, key);
            }
        }

        return ret;

    }

    /**
     * save a map rewriting the index file if needed
     *
     *
     * THIS METHOD NEVER UPDATES THE CACHE
     *
     * @param node
     * @param loadedLevelTables
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private boolean save(String key, Map<Long, List<Map<Long, Integer>>> toSave) throws IOException, ClassNotFoundException {
        if (this.readOnly) {
            throw new IllegalStateException("This index is in Read Only MODE");
        }

        File toFile = Paths.get(indexPath, key).toFile();
        toFile.getParentFile().mkdirs();
        boolean isUpdate = toFile.exists();

        try (
                OutputStream file = new FileOutputStream(toFile);
                OutputStream buffer = new BufferedOutputStream(file);
                ObjectOutput output = new ObjectOutputStream(buffer);) {

            output.writeObject(toSave);

        } catch (IOException ex) {
            throw new IOException("Could Not Serialize Table Nodes File", ex);
        }

        //debug("%s %s", isUpdate ? "Saved on" : "Updated ", toFile);
        return isUpdate;
    }

    /**
     * Load the memory tables for all the nodes in the collection
     *
     * IF CACHING IS ACTIVE THIS METHOD UPDATES THE CACHE TODO: Parallelize
     *
     * @param nodes
     * @return the MemoryNeighborTables for th
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private MemoryNeighborTables load2(Collection<Long> nodes) throws IOException {
        Set<String> keys = new HashSet<>(nodes.size());

        MemoryNeighborTables tables = this.caching ? this.cachedTables : new MemoryNeighborTables(k);

        //First compute minimum set of files
        nodes.parallelStream().map(n -> {
            return generateFileName(n);
        }).sequential().forEach(s -> {
            keys.add(s);
        });

        //debug("Will load %s files for %s nodes", keys.size(), nodes.size());
        keys.parallelStream().map(key -> {
            try {
                return TablesIndex.loadMapData(key, this.indexPath);
            } catch (IOException ex) {
                fatal("Could not load index file for %s - Corrupted Index", ex, key);
            }
            return null;

        }).map(data -> {
            try {
                return TablesIndex.parseMap(data);
            } catch (ClassNotFoundException | IOException ex) {
                fatal("Could not parse index file- Corrupted Index", ex);
            }
            return null;
        }).sequential().forEach(loaded -> {
            //Map<Long, List<Map<Long, Integer>>> loaded
            if (loaded != null) {
                tables.putAll(loaded);
            }
        });

        return tables;

    }

    /**
     * Load Map associated with a single node, all nodes in th same key are
     * loaded
     *
     * THIS METHOD NEVER UPDATES THE CACHE
     *
     * @param node
     * @return the map of all the nodes stored with this node, or null if none
     * exists
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private Map<Long, List<Map<Long, Integer>>> load(long node) throws IOException, ClassNotFoundException {
        return load(generateFileName(node), this.indexPath);
    }

    /**
     * Load Map associated with a single Key
     *
     * THIS METHOD NEVER UPDATES THE CACHE
     *
     * @param key
     * @return the map for they key or null if non exists
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private static Map<Long, List<Map<Long, Integer>>> load(String key, String indexPath) throws IOException, ClassNotFoundException {
        File fileName = Paths.get(indexPath, key).toFile();
        if (!fileName.exists()) {
            return null;
        }

        byte[] data = Files.readAllBytes(fileName.toPath());
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(data))) {

            @SuppressWarnings("unchecked")
            Map<Long, List<Map<Long, Integer>>> loadedLevelTables = (Map<Long, List<Map<Long, Integer>>>) input.readObject();
            //if (loadedLevelTables.gesize() != this.k) {
            //    throw new IllegalStateException("Could not save a table of size " + loadedLevelTables.size() + "  with tables of size " + this.k);
            //}
            return loadedLevelTables;

        } catch (ClassNotFoundException ex) {
            //fatal("Could not load index file for %s - Corrupted Index", ex, key);
            throw ex;
        }

    }

    private static byte[] loadMapData(String key, String indexPath) throws IOException {
        File fileName = Paths.get(indexPath, key).toFile();
        if (!fileName.exists()) {
            return null;
        }

        return Files.readAllBytes(fileName.toPath());
    }

    private static Map<Long, List<Map<Long, Integer>>> parseMap(byte[] data) throws IOException, ClassNotFoundException {
        if (data == null) {
            return null;
        }
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(data))) {

            @SuppressWarnings("unchecked")
            Map<Long, List<Map<Long, Integer>>> loadedLevelTables = (Map<Long, List<Map<Long, Integer>>>) input.readObject();
            //if (loadedLevelTables.gesize() != this.k) {
            //    throw new IllegalStateException("Could not save a table of size " + loadedLevelTables.size() + "  with tables of size " + this.k);
            //}
            return loadedLevelTables;

        } catch (ClassNotFoundException ex) {
            //fatal("Could not load index file for %s - Corrupted Index", ex, key);
            throw ex;
        }

    }

    /**
     * Load the memory tables for all the nodes in the collection
     *
     * IF CACHING IS ACTIVE THIS METHOD UPDATES THE CACHE TODO: Parallelize
     *
     * @param nodes
     * @return the MemoryNeighborTables for th
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private MemoryNeighborTables load(Collection<Long> nodes) throws IOException {
        Set<String> keys = new HashSet<>(nodes.size());

        MemoryNeighborTables tables = this.caching ? this.cachedTables : new MemoryNeighborTables(k);

        //First compute minimum set of files
        nodes.parallelStream().map(n -> {
            return generateFileName(n);
        }).sequential().forEach(s -> {
            keys.add(s);
        });

        ExecutorService loaderPool = Executors.newFixedThreadPool(30);
        List<Future<Map<Long, List<Map<Long, Integer>>>>> tableNodeFuture = new ArrayList<>();
        debug("Will load %s files for %s nodes", keys.size(), nodes.size());

        for (String key : keys) {
            tableNodeFuture.add(loaderPool.submit(new LoadMapFile(key, this.indexPath)));
        }

        try {
            for (Future<Map<Long, List<Map<Long, Integer>>>> future : tableNodeFuture) {

                Map<Long, List<Map<Long, Integer>>> loaded =  future.get();
                if(loaded != null){
                    tables.putAll(loaded);
                }

            }
        } catch (ExecutionException | InterruptedException ex) {
            fatal("Could not load index file - Corrupted Index", ex);
        } finally {
            ThreadUtilities.shutdownAndAwaitTermination(loaderPool);
        }
        //loaderPool.shutdownNow();
        //loaderPool = null;
        return tables;

    }

    private class LoadMapFile implements Callable<Map<Long, List<Map<Long, Integer>>>> {

        private final String key;
        private final String indexPath;

        public LoadMapFile(String key, String indexPath) {
            this.key = key;
            this.indexPath = indexPath;

        }

        @Override
        public Map<Long, List<Map<Long, Integer>>> call() throws Exception {
            File fileName = Paths.get(indexPath, key).toFile();
            if (!fileName.exists()) {
                fatal("File index for %s %s does not exists!! ", indexPath, key);
                return null;
            }

            byte[] data = Files.readAllBytes(fileName.toPath());

            try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(data))) {

                @SuppressWarnings("unchecked")
                Map<Long, List<Map<Long, Integer>>> loadedLevelTables = (Map<Long, List<Map<Long, Integer>>>) input.readObject();
                //if (loadedLevelTables.gesize() != this.k) {
                //    throw new IllegalStateException("Could not save a table of size " + loadedLevelTables.size() + "  with tables of size " + this.k);
                //}
                return loadedLevelTables;

            } catch (ClassNotFoundException ex) {
                //fatal("Could not load index file for %s - Corrupted Index", ex, key);
                throw ex;
            }

        }

    }

}
