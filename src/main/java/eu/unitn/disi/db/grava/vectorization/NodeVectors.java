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
package eu.unitn.disi.db.grava.vectorization;

import eu.unitn.disi.db.grava.exceptions.DataException;
import eu.unitn.disi.db.grava.utils.Utilities;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Represens the node vectors used in the computation of the node similarity.
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 * @see NeighborhoodPruningAlgorithm
 * @see GenerateNeighborTables
 */
@PrimitiveObject
public class NodeVectors implements Closeable {

    //private File indexDirectory;
    private RandomAccessFile indexFile;
    private RandomAccessFile vectorFile;
    private int numberOfNodes;
    private long lastRead = -1;
    private int k;
    protected Map<Long, Map<Long, Integer>[]> levelTables = new LinkedHashMap<Long, Map<Long, Integer>[]>(); //Preserve the order
    private Map<Long, Double> labelFrequencies;
    private long[][] index = null;
    //private int edgeNumber;
    private Mode mode;

    public enum Mode {

        WRITE("rw"),
        READ("r");
        private String m;

        Mode(String m) {
            this.m = m;
        }

        @Override
        public String toString() {
            return m;
        }
    }
    //private Map<Long, StringBuilder[]> labelIndex;
    //private static final int PAGE_SIZE = 1024;
    //private int minFrequency;
    //private int k;


    public NodeVectors(String index, String vector, File labelFrequency, int numberOfNodes, int k, Mode mode) throws IOException {
        int frequency;
        int numberOfEdges = 0;
        this.k = k;
        //this.indexDirectory = indexDirectory;
        this.mode = mode;
        this.numberOfNodes = numberOfNodes;
        //Remove the files if they already exists
        levelTables = new HashMap<>();
        Set<Long> labels = new HashSet<>();
//        edgeNumber = 0;

        indexFile = null;
        vectorFile = null;
        //Reade frequencies
        labelFrequencies = new HashMap<>();
        BufferedReader reader = null;

        if (mode == Mode.WRITE) {
            String line;
            String[] splittedLine;
            try {
                reader = new BufferedReader(new FileReader(labelFrequency));
                while ((line = reader.readLine()) != null) {
                    if (!"".equals(line)) {
                        splittedLine = Utilities.fastSplit(line, ' ', 2);
                        frequency = Integer.parseInt(splittedLine[1]);
        //                edgeNumber += frequency;
                        labels.add(Long.parseLong(splittedLine[0]));
                        labelFrequencies.put(Long.parseLong(splittedLine[0]), (double) frequency);
                        numberOfEdges += frequency;
                    }
                }
            } catch (IOException ex) {
                throw ex;
            } finally {
                Utilities.close(reader);
            }
        }


        for (Long label : labels) {
            labelFrequencies.put(label, Math.log10(numberOfEdges / (labelFrequencies.get(label))) / Math.log10(2));
        }
        File iFile = new File(index);
        File vFile = new File(vector);

        indexFile = new RandomAccessFile(iFile, mode.toString());
        vectorFile = new RandomAccessFile(vFile, mode.toString());
        //while (vectorFile.readLine() != nul`l); //Go to end
        vectorFile.seek(vFile.length());
        lastRead = vectorFile.getFilePointer();

        if (mode == Mode.READ) {
            loadIndex();
        }
        indexFile.seek(iFile.length());

    }
    
    public NodeVectors(File indexDirectory, File labelFrequency, int numberOfNodes, int k, Mode mode) throws IOException {
        this(indexDirectory.getAbsolutePath() + File.separator + "vectors.index", indexDirectory.getAbsolutePath() + File.separator + "vectors.dat", labelFrequency, numberOfNodes, k, mode);
    }

//    private NodeVectors(int k) throws IOException {
//        this(new File(System.getProperty("user.dir")), new File("label-frequency.txt"), k, Mode.READ);
//    }
    @Override
    public void close() throws IOException {
        indexFile.close();
        vectorFile.close();
    }

    public boolean addNode(Map<Long, Integer>[] nodeTable, long node) {
        return levelTables.put(node, nodeTable) != null;
    }

    public boolean addNodeVector(Map<Long, Integer> levelNodeTable, long node, short level) {
        Map<Long, Integer>[] nodeTable = levelTables.get(node);
        if (nodeTable == null) {
            nodeTable = new Map[k];
        }
        nodeTable[level] = levelNodeTable;
        return levelTables.put(node, nodeTable) != null;
    }

    public Map<Long, Double> getVector(long node) throws DataException {
        Map<Long, Double> vector = null;
        String line;
        String[] splittedLine;
        int position;
        try {
            assert index != null;
            position = Utilities.binaryTableSearch(index, node);
            if (position >= 0) {
                vectorFile.seek(index[position][1]);
                //debug("Found the correct position");
                line = vectorFile.readLine();
                //debug("First line is %s", line);
                if (line != null) {
                    vector = new HashMap<Long, Double>();
                    line = line.substring(1); //remove %
                    while (line != null && !line.startsWith("%")) {
                        splittedLine = Utilities.fastSplit(line, ' ', 2);
                        vector.put(Long.parseLong(splittedLine[0]), Double.parseDouble(splittedLine[1]));
                        line = vectorFile.readLine();
                    }
                }
            }
        } catch (IOException ex) {
            throw new DataException(ex);
        }
        return vector;
    }

    public static void normalize(Map<Long, Double> vector) {
        Set<Long> keys = vector.keySet();
        double sum = 0;
        for (Long key : keys) {
            sum += vector.get(key) * vector.get(key);
        }
        sum = Math.sqrt(sum);
        for (Long key : keys) {
            vector.put(key, vector.get(key)/sum);
        }
    }

    private void loadIndex() throws IOException {
        if (mode != Mode.READ) {
            throw new IOException("Cannot read the index if the mode is write");
        }
        String line;
        String[] splittedLine;
        int count = 0;
        index = new long[numberOfNodes][];

        while ((line = indexFile.readLine()) != null) {
            if (!"".equals(line)) {
                splittedLine = Utilities.fastSplit(line, ' ', 2);
                index[count] = new long[2];
                index[count][0] = Long.parseLong(splittedLine[0]);
                index[count][1] = Long.parseLong(splittedLine[1]);
            }
            if (++count % 1000000 == 0) {
                System.out.printf("Loaded %d nodes in the index", count);
            }
        }
        Arrays.sort(index, new Comparator<long[]>(){
            @Override
            public int compare(long[] o1, long[] o2) {
                if (o1[0] > o2[0]) 
                    return 1; 
                else if (o1[0] < o2[0])
                    return -1; 
                return 0;
            }
        });
        System.out.printf("Sorted %d nodes in the index", index.length);
    }

    /**
     * Computes a similarity score of the two input vectors.
     * @param vector1 The first node vector
     * @param vector2 The second node vector
     * @param normalize If you want to normalize vectors or not.
     * @return The final score, that is a double between 0 and 11
     */
    public double score(Map<Long, Double> vector1, Map<Long,Double> vector2, boolean normalize)
    {
        if (vector1 == null || vector2 == null) {
            return 0.0;
        }
        if (normalize) {
            normalize(vector1);
            normalize(vector2);
        }
        //We use cosine as a metric
        Set<Long> labels = vector1.keySet();
        double intersection = 0, v1SqNorm = 0, v2SqNorm = 0, value;
        for (Long l : labels) {
            value = vector1.get(l);
            v1SqNorm += value * value;
            if (vector2.containsKey(l)) {
                intersection += value * vector2.get(l);
            }
        }
        labels = vector2.keySet();
        for (Long l : labels) {
            value = vector2.get(l);
            v2SqNorm += value * value;
        }

        return intersection / (Math.sqrt(v1SqNorm * v2SqNorm)/* - intersection*/);
    }

    /**
     * Computes a similarity score of the two input nodes.
     * @param node1
     * @param node2
     * @param normalize
     * @return
     * @throws DataException
     */
    public double score(long node1, long node2, boolean normalize) throws DataException {
        Map<Long, Double> vector1 = getVector(node1);
        Map<Long, Double> vector2 = getVector(node2);

        return score(vector1, vector2, normalize);
    }

    public boolean serialize() throws DataException {
        Set<Long> nodeTables = levelTables.keySet();
        Map<Long, Integer>[] nodeTable;
        Map<Long, Double> vector;
        Map<Long, Integer> levelLabelVector;
        Set<Long> labels;
        Double value;

        try {
            vectorFile.seek(lastRead);

            for (Long node : nodeTables) {
                vector = new HashMap<>();
                nodeTable = levelTables.get(node);
                for (int i = 0; i < nodeTable.length; i++) {
                    levelLabelVector = nodeTable[i];
                    labels = levelLabelVector.keySet();
                    for (Long label : labels) {
                        value = vector.get(label);
                        if (value == null) {
                            value = 0.0;
                        }
                        value += getCoefficient(label, levelLabelVector.get(label), i);
                        vector.put(label, value);
                    }
                }
                //Write to file
                if (!vector.isEmpty()) {
                    labels = vector.keySet();
                    indexFile.writeBytes(node + " " + lastRead + "\n");
                    vectorFile.writeBytes("%");
                    for (Long label : labels) {
                        vectorFile.writeBytes(label + " " + vector.get(label) + "\n");
                    }
                    lastRead = vectorFile.getFilePointer();
                } else {
                    System.out.printf("Vector for node %d is empty", node);
                }
                //labelDirectory = new File(indexDirectory, label.toString());
//                if (labelDirectory.mkdir()) {
//                    debug("Label directory %s created, for label %d", labelDirectory.getCanonicalPath(), label);
//                }
//                nodes = labelIndex.get(label);
//                for (i = 0; i < nodes.length; i++) {
//                    if (nodes[i] != null) {
//                        //labelDirectory = new File(labelDirectory, (i + 1) + "");
//                        //labelDirectory.mkdir();
//                        writer = new BufferedWriter(new FileWriter(labelDirectory.getCanonicalPath() + File.separator + (i + 1), true), PAGE_SIZE * PAGE_SIZE);
//                        writer.append(nodes[i].toString());
//                        Utilities.close(writer);
//                    }
//                }
            }
            levelTables = null;
            //Once you serialize remove the tables from the memory
            levelTables = new HashMap<Long, Map<Long, Integer>[]>();

        } catch (IOException ex) {  
            System.out.printf("Some exception occurred while writing into the file", ex);
            Utilities.close(indexFile);
            Utilities.close(vectorFile);
        } 
        return true;
    }

    //Transform to probabilities
    private double getCoefficient(long label, int nLabelFrequency, int level) {
        assert labelFrequencies.get(label) != null : "We don't have frequency information for label " + label;
        return nLabelFrequency * ((double) labelFrequencies.get(label) * Math.pow(2.0, level));
    }

    @Override
    public String toString() {
        return "";
    }
}
