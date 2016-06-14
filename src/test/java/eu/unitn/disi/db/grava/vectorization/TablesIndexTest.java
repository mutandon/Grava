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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.TestCase;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class TablesIndexTest extends TestCase {

    private static final String ROOT = "/tmp/test";
    private static final String PATH = ROOT + "/test_TablesIndexTest";
    private static final int K = 2;

    private final File tmpDir;
    private final Path tmpDirPath;
    private final int keySize = 2;
    private static boolean isDir = false;

    public static void prepare() {
        File root = new File(ROOT);
        if (!isDir || !root.exists()) {
            isDir = root.mkdir();
        }
    }

    public TablesIndexTest(String testName) {
        super(testName);
        TablesIndexTest.prepare();
        this.tmpDir = new File(PATH);
        this.tmpDirPath = tmpDir.toPath();

    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        // attempt to create the directory here
        boolean successful = tmpDir.mkdir();
        if (!successful) {
            throw new IOException("Didnt managed to create the direcotry for " + this.getName() + " in " + TablesIndexTest.PATH);
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        String[] entries = tmpDir.list();
        File currentFile;
        System.out.println("Cleaning directory of " + entries.length + " files");
        for (String s : entries) {
            currentFile = new File(tmpDir.getPath(), s);
            currentFile.delete();
        }
        Files.deleteIfExists(tmpDirPath);
    }

    private static int getNodeLabelLevelFreq(long node, long label, int level) {
        int value = (int) ((node + label) % (3 * (level + 1)));
        return value;
    }

    public MemoryNeighborTables getMemTable(long[] nodes, long[] labels) {
        MemoryNeighborTables tb = new MemoryNeighborTables(K);
        Map<Long, Integer> levelValues;

        for (long node : nodes) {
            for (short level = 0; level < K; level++) {
                levelValues = new HashMap<>();
                for (long label : labels) {
                    levelValues.put(label, getNodeLabelLevelFreq(node, label, level));
                }
                tb.addNodeLevelTable(levelValues, node, level);

            }
        }

        return tb;
    }

    /**
     * Test of storeTable method, of class TablesIndex.
     *
     * @throws java.io.IOException
     */
    public void testStoreTableNoCache() throws IOException {
        try {
            System.out.println("TablesIndexTest storeTable NO CACHING!!");

            TablesIndex instance = new TablesIndex(PATH, K, false, false, keySize);
            MemoryNeighborTables tb = getMemTable(NODE_LONGS, LABEL_LONGS);

            boolean expResult = true;
            boolean result = instance.storeTable(tb);
            assertEquals(expResult, result);
            System.out.println("The directory " + PATH + " has total size of " + size(tmpDirPath) + "Kb");
            // TODO review the generated test code and remove the default call to fail.
            //fail("The test case is a prototype.");
        } catch (IOException ex) {
            Logger.getLogger(TablesIndexTest.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        }
    }

    /**
     * Test of storeTable method, of class TablesIndex.
     *
     * @throws java.io.IOException
     */
    public void testStoreTableCache() throws IOException {
        try {
            System.out.println("TablesIndexTest storeTable WITH CACHING!!");

            TablesIndex instance = new TablesIndex(PATH, K, false, true, keySize);
            MemoryNeighborTables tb = getMemTable(NODE_LONGS, LABEL_LONGS);

            boolean expResult = true;
            boolean result = instance.storeTable(tb);
            assertEquals(expResult, result);
            System.out.println("The directory " + PATH + " has total size of " + size(tmpDirPath) + "Kb");
            // TODO review the generated test code and remove the default call to fail.
            //fail("The test case is a prototype.");
        } catch (IOException ex) {
            Logger.getLogger(TablesIndexTest.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        }
    }

    /**
     * Test of loadTable method, of class TablesIndex.
     *
     * @throws java.io.IOException
     */
    public void testLoadTableNoCache_long() throws IOException {
        try {
            System.out.println("TablesIndexTest LOAD Table NO CACHING!! Long");

            TablesIndex instance = new TablesIndex(PATH, K, false, false, keySize);
            MemoryNeighborTables tb = getMemTable(NODE_LONGS, LABEL_LONGS);

            boolean expResult = true;
            boolean result = instance.storeTable(tb);
            assertEquals(expResult, result);
            System.out.println("The directory " + PATH + " has total size of " + size(tmpDirPath) + "Kb");

            TablesIndex instance2 = new TablesIndex(PATH, K, false, false, keySize);
            long selectNode = TablesIndexTest.NODE_LONGS[0];
            long selectLabelExists = TablesIndexTest.LABEL_LONGS[0];
            long selectLabelNonExists = 1l;

            for (int i = 0; i < K; i++) {
                assertEquals("Existing labels with equals", tb.getCountForNodeLabel(selectNode, selectLabelExists, i), instance2.loadTable(selectNode).getCountForNodeLabel(selectNode, selectLabelExists, i));
                assertEquals("NON Existing labels with equals", tb.getCountForNodeLabel(selectNode, selectLabelNonExists, i), instance2.loadTable(selectNode).getCountForNodeLabel(selectNode, selectLabelNonExists, i));
            }

            //MemoryNeighborTables result = instance.loadTable(n);
            //assertEquals(expResult, result);
            // TODO review the generated test code and remove the default call to fail.
            //fail("The test case is a prototype.");
        } catch (IOException ex) {
            Logger.getLogger(TablesIndexTest.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        }
    }

    /**
     * Test of loadTable method, of class TablesIndex.
     *
     * @throws java.io.IOException
     */
    public void testLoadTable_Collection() throws IOException {
        try {
            System.out.println("TablesIndexTest LOAD Table NO CACHING!! Collection");

            TablesIndex instance = new TablesIndex(PATH, K, false, false, keySize);
            MemoryNeighborTables tb = getMemTable(NODE_LONGS, LABEL_LONGS);

            boolean expResult = true;
            boolean result = instance.storeTable(tb);
            assertEquals(expResult, result);
            System.out.println("The directory " + PATH + " has total size of " + size(tmpDirPath) + "Kb");

            TablesIndex instance2 = new TablesIndex(PATH, K, false, false, keySize);
            List<Long> selectNodes = new ArrayList<>(NODE_LONGS.length);
            for (Long selectNode : NODE_LONGS) {
                selectNodes.add(selectNode);
            }

            long selectLabelExists = TablesIndexTest.LABEL_LONGS[0];
            long selectLabelNonExists = 1l;

            MemoryNeighborTables loaded = instance2.loadTable(selectNodes);
            for (long node : NODE_LONGS) {
                for (int i = 0; i < K; i++) {
                    assertEquals(tb.getCountForNodeLabel(node, selectLabelExists, i), loaded.getCountForNodeLabel(node, selectLabelExists, i));
                    assertEquals(tb.getCountForNodeLabel(node, selectLabelNonExists, i), loaded.getCountForNodeLabel(node, selectLabelNonExists, i));
                }
            }

            //MemoryNeighborTables result = instance.loadTable(n);
            //assertEquals(expResult, result);
            // TODO review the generated test code and remove the default call to fail.
            //fail("The test case is a prototype.");
        } catch (IOException ex) {
            Logger.getLogger(TablesIndexTest.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        }
    }

    /**
     * Test of loadTable method, of class TablesIndex.
     *
     * @throws java.io.IOException
     */
    public void testUpdateTable() throws IOException {
        try {
            System.out.println("TablesIndexTest UPDATE Table NO CACHING!! Collection");

            TablesIndex instance = new TablesIndex(PATH, K, false, false, keySize);
            long[] nodes1 = Arrays.copyOfRange(NODE_LONGS, 0, NODE_LONGS.length / 2);
            long[] nodes2 = Arrays.copyOfRange(NODE_LONGS, NODE_LONGS.length / 2, NODE_LONGS.length);
            long[] labels1 = Arrays.copyOfRange(LABEL_LONGS, 0, LABEL_LONGS.length / 2);
            long[] labels2 = Arrays.copyOfRange(LABEL_LONGS, LABEL_LONGS.length / 2, LABEL_LONGS.length);
            MemoryNeighborTables tb1 = getMemTable(nodes1, labels1);
            MemoryNeighborTables tb2 = getMemTable(nodes2, labels1);

            MemoryNeighborTables tb3 = getMemTable(nodes1, labels2);
            MemoryNeighborTables tb4 = getMemTable(nodes2, labels2);

            //MemoryNeighborTables groundTruth;

            boolean expResult = true;
            boolean result = instance.storeTable(tb1);   // Store first part
            System.out.println("The directory " + PATH + " has total size of " + size(tmpDirPath) + "Kb");

            //groundTruth = getMemTable(nodes1, labels1);  // Update Ground Truth

            assertEquals(expResult, result);

            instance = new TablesIndex(PATH, K, false, false, keySize); // Reset                                               
            expResult = false;
            result = instance.storeTable(tb3); // Store Second part
            //groundTruth.merge(tb3);            // Update Ground Truth
            System.out.println("The directory " + PATH + " has total size of " + size(tmpDirPath) + "Kb");

            assertEquals(expResult, result); // NO new Files

            instance = new TablesIndex(PATH, K, false, false, keySize); // Reset Object

            instance.storeTable(tb2);
            instance.storeTable(tb4);
            //groundTruth.merge(tb2);            // Update Ground Truth

            List<Long> selectNodes = new ArrayList<>(NODE_LONGS.length);
            for (Long selectNode : NODE_LONGS) {
                selectNodes.add(selectNode);
            }
                        

            MemoryNeighborTables loaded = instance.loadTable(selectNodes);
            for (long selectLabelExists   : LABEL_LONGS) {                            
                for (Long node : selectNodes) {
                    for (int i = 0; i < K; i++) {
                        assertEquals(getNodeLabelLevelFreq(node, selectLabelExists, i), loaded.getCountForNodeLabel(node, selectLabelExists, i));                        
                    }
                }
            }
            //MemoryNeighborTables result = instance.loadTable(n);
            //assertEquals(expResult, result);
            // TODO review the generated test code and remove the default call to fail.
            //fail("The test case is a prototype.");
        } catch (IOException ex) {
            Logger.getLogger(TablesIndexTest.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        }
    }

    /**
     * Attempts to calculate the size of a file or directory.
     *
     * <p>
     * Since the operation is non-atomic, the returned value may be inaccurate.
     * However, this method is quick and does its best.
     *
     * @param path
     * @return size of Path in KyloBytes
     */
    public static long size(Path path) {
        final long KB = 1024l;
        final AtomicLong size = new AtomicLong(0);

        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult
                        visitFile(Path file, BasicFileAttributes attrs) {

                    size.addAndGet(attrs.size());
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult
                        visitFileFailed(Path file, IOException exc) {

                    System.out.println("skipped: " + file + " (" + exc + ")");
                    // Skip folders that can't be traversed
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult
                        postVisitDirectory(Path dir, IOException exc) {

                    if (exc != null) {
                        System.out.println("had trouble traversing: " + dir + " (" + exc + ")");
                    }
                    // Ignore errors traversing a folder
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new AssertionError("walkFileTree will not throw IOException if the FileVisitor does not");
        }

        return size.get() / KB;
    }

    private final static long[] LABEL_LONGS = {
        6848l, // 100437970 // ISA
        88957580537448l, // 13859361   
        55487680537448l, // 13859361
        89517680537448l, // 13853463  

        80487680537448l, // 13853463
        747186807548l, // 10841280
        705386807548l, // 10841280
        81847667785348l, // 9415984
        71517476884948l, // 9415984
        6648l, // 7829914     

        90568048708148l, // 2748973
        70484970688048l, // 2748973
        785568837448l, // 2104295
        86578387895148l, // 1863243
        68578387895148l, // 1863243
        78675551884948l, // 1723260
        71705551884948l,
        95844948l,
        82874948l,
        70887650535048l,
        88875554725048l,
        55715554725048l,
        88849082884948l,
        50499582884948l,
        76499582884948l,
        52869082884948l,
        896771837448l,
        895571837448l,
        805771837448l,
        525671837448l,
        81517476884948l,
        51775095495048l,
        95525570905148l,
        89767652515248l // SIZE 1104538 EDGES
    };

    private final static long[] NODE_LONGS = {
        81758384755248l,
        8752784948l,
        52527876688248l,
        5457905348l,
        74568089885048l,
        7682728048l,
        6880785448l,
        7671834948l,
        8070554948l,
        84575050816748l,
        66677755905048l,
        6873798571786576l,
        8071817248l,
        7257745748l,
        5053675548l,
        84778648l,
        78764884725048l,
        8849705548l,
        775389764948l,
        569071564948l,
        8470675048l,
        687865768373l,
        90575050816748l,
        774881717148l,
        76899068755348l,
        8375485348l,
        559088665148l,
        875588715048l,
        5749905148l,
        685789745148l,
        5553895448l,
        9048714948l,
        87815566535048l,
        865254664948l,
        7152757148l,
        84537450894948l,
        665384704948l,
        8882495248l,
        95525286725048l,
        70958681725048l,
        48545268905048l,
        496757954948l,
        66778767755248l,
        5482744948l,
        5188785148l,
        5072485648l,
        4882678148l,
        5572814948l,
        8171505648l,
        78877450535048l,
        77875268565748l,
        75815552725048l,
        55877688885448l,
        8056667248l,};

}
