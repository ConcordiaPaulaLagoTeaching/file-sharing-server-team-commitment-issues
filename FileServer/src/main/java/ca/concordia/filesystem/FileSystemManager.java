package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileSystemManager {

    private final int MAXFILES = 5;
    private final int MAXBLOCKS = 10;
    private static final int BLOCK_SIZE = 128;

    private FEntry[] inodeTable;
    private boolean[] freeBlockList;
    private int[] fnodeNext;
    private int[] fnodeBlockIndex;

    private RandomAccessFile disk;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private static final int METADATA_SIZE = 165;

    public FileSystemManager(String filename, int totalSize) {
        try {
            disk = new RandomAccessFile(filename, "rw");

            long minSize = METADATA_SIZE + (long) MAXBLOCKS * BLOCK_SIZE;

            if (disk.length() == 0) {
                disk.setLength(Math.max(minSize, totalSize));
                initializeMetadata();
                saveMetadata();
            } else {
                initializeMetadata();
                loadMetadata();
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize FileSystemManager", e);
        }
    }

    //This is the validation part of the file system manager

    private void validateFilename(String filename) {
        if (filename == null || filename.isEmpty())
            throw new IllegalArgumentException("ERROR: invalid filename");

        if (filename.length() > 11)
            throw new IllegalArgumentException("ERROR: invalid filename");

        if (filename.contains(" "))
            throw new IllegalArgumentException("ERROR: invalid filename");

        for (char c : filename.toCharArray()) {
            if (c < 32 || c > 126)
                throw new IllegalArgumentException("ERROR: invalid filename");
        }
    }

    //This is for the metadata fucntion

    private void initializeMetadata() {
        inodeTable = new FEntry[MAXFILES];
        freeBlockList = new boolean[MAXBLOCKS];
        fnodeNext = new int[MAXBLOCKS];
        fnodeBlockIndex = new int[MAXBLOCKS];

        for (int i = 0; i < MAXFILES; i++) {
            inodeTable[i] = new FEntry();
        }

        Arrays.fill(fnodeNext, -1);
        Arrays.fill(fnodeBlockIndex, -1);
        Arrays.fill(freeBlockList, true);
    }

    private void saveMetadata() throws IOException {
        disk.seek(0);

        for (int i = 0; i < MAXFILES; i++) {
            FEntry fe = inodeTable[i];

            byte[] nameBytes = new byte[11];
            if (fe.getFilename() != null) {
                byte[] raw = fe.getFilename().getBytes();
                System.arraycopy(raw, 0, nameBytes, 0, Math.min(raw.length, 11));
            }
            disk.write(nameBytes);

            disk.writeShort(fe.getFilesize());
            disk.writeShort(fe.getFirstBlock());
        }

        for (int i = 0; i < MAXBLOCKS; i++)
            disk.writeInt(fnodeBlockIndex[i]);

        for (int i = 0; i < MAXBLOCKS; i++)
            disk.writeInt(fnodeNext[i]);

        for (int i = 0; i < MAXBLOCKS; i++)
            disk.writeBoolean(freeBlockList[i]);
    }

    private void loadMetadata() throws IOException {
        disk.seek(0);

        for (int i = 0; i < MAXFILES; i++) {
            byte[] nameBytes = new byte[11];
            disk.readFully(nameBytes);
            String name = new String(nameBytes).trim();

            short size = disk.readShort();
            short first = disk.readShort();

            if (name.isEmpty()) {
                inodeTable[i] = new FEntry();
            } else {
                inodeTable[i] = new FEntry(name, size, first);
            }
        }

        for (int i = 0; i < MAXBLOCKS; i++)
            fnodeBlockIndex[i] = disk.readInt();

        for (int i = 0; i < MAXBLOCKS; i++)
            fnodeNext[i] = disk.readInt();

        for (int i = 0; i < MAXBLOCKS; i++)
            freeBlockList[i] = disk.readBoolean();
    }

    private int getBlockOffset(int blockIndex) {
        return METADATA_SIZE + blockIndex * BLOCK_SIZE;
    }

    private int findInodeByName(String filename) {
        for (int i = 0; i < MAXFILES; i++) {
            String n = inodeTable[i].getFilename();
            if (n != null && n.equals(filename)) {
                return i;
            }
        }
        return -1;
    }

    private int findFreeInode() {
        for (int i = 0; i < MAXFILES; i++) {
            if (inodeTable[i].getFilename() == null) {
                return i;
            }
        }
        return -1;
    }

    private int findFreeBlock() {
        for (int i = 0; i < MAXBLOCKS; i++) {
            if (freeBlockList[i]) return i;
        }
        return -1;
    }

    private int findFreeFNode() {
        for (int i = 0; i < MAXBLOCKS; i++) {
            if (fnodeBlockIndex[i] == -1) return i;
        }
        return -1;
    }

    private void wipeBlock(int blockIndex) throws IOException {
        disk.seek(getBlockOffset(blockIndex));
        byte[] zero = new byte[BLOCK_SIZE];
        disk.write(zero);
    }

    private int countNodesForFile(FEntry fe) {
        int count = 0;
        int node = fe.getFirstBlock();
        while (node != -1) {
            count++;
            node = fnodeNext[node];
        }
        return count;
    }

    //This is the file operation of the file system

    public void createFile(String filename) throws Exception {
        lock.writeLock().lock();
        try {
            validateFilename(filename);

            if (findInodeByName(filename) != -1)
                throw new IllegalArgumentException("ERROR: file " + filename + " already exists");

            int idx = findFreeInode();
            if (idx == -1)
                throw new IllegalStateException("ERROR: no space for more files");

            FEntry fe = inodeTable[idx];
            fe.setFilename(filename);
            fe.setFilesize((short) 0);
            fe.setFirstBlock((short) -1);

            saveMetadata();

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void writeFile(String filename, byte[] contents) throws Exception {
        lock.writeLock().lock();
        try {
            validateFilename(filename);

            int inodeIndex = findInodeByName(filename);
            if (inodeIndex == -1)
                throw new IllegalArgumentException("ERROR: file " + filename + " does not exist");

            FEntry fe = inodeTable[inodeIndex];

            int bytesNeeded = contents.length;
            int blocksNeeded = (bytesNeeded == 0) ? 0 :
                    (bytesNeeded + BLOCK_SIZE - 1) / BLOCK_SIZE;
            int nodesNeeded = blocksNeeded;

            int usedNodes = countNodesForFile(fe);

            int freeBlocks = 0, freeNodes = 0;

            for (int i = 0; i < MAXBLOCKS; i++) {
                if (freeBlockList[i]) freeBlocks++;
                if (fnodeBlockIndex[i] == -1) freeNodes++;
            }

            int availableBlocks = freeBlocks + usedNodes;
            int availableNodes = freeNodes + usedNodes;

            if (blocksNeeded > availableBlocks || nodesNeeded > availableNodes)
                throw new IllegalStateException("ERROR: file too large");

            int cursor = fe.getFirstBlock();
            while (cursor != -1) {
                int blockIdx = fnodeBlockIndex[cursor];
                if (blockIdx >= 0) freeBlockList[blockIdx] = true;
                int next = fnodeNext[cursor];
                fnodeBlockIndex[cursor] = -1;
                fnodeNext[cursor] = -1;
                cursor = next;
            }

            fe.setFilesize((short) 0);
            fe.setFirstBlock((short) -1);

            int bytesLeft = contents.length;
            int offset = 0;
            int prevNode = -1;
            int firstNode = -1;

            while (bytesLeft > 0) {
                int node = findFreeFNode();
                int block = findFreeBlock();

                freeBlockList[block] = false;
                fnodeBlockIndex[node] = block;

                int writeSize = Math.min(BLOCK_SIZE, bytesLeft);
                disk.seek(getBlockOffset(block));
                disk.write(contents, offset, writeSize);

                offset += writeSize;
                bytesLeft -= writeSize;

                if (prevNode != -1)
                    fnodeNext[prevNode] = node;

                prevNode = node;
                if (firstNode == -1)
                    firstNode = node;
            }

            if (prevNode != -1)
                fnodeNext[prevNode] = -1;

            fe.setFilesize((short) contents.length);
            fe.setFirstBlock((short) firstNode);

            saveMetadata();

        } finally {
            lock.writeLock().unlock();
        }
    }

    public byte[] readFile(String filename) throws Exception {
        lock.readLock().lock();
        try {
            validateFilename(filename);

            int inodeIndex = findInodeByName(filename);
            if (inodeIndex == -1)
                throw new IllegalArgumentException("ERROR: file " + filename + " does not exist");

            FEntry fe = inodeTable[inodeIndex];
            int size = fe.getFilesize();

            if (size == 0)
                return new byte[0];

            byte[] result = new byte[size];
            int bytesRead = 0;
            int node = fe.getFirstBlock();

            while (node != -1 && bytesRead < size) {
                int block = fnodeBlockIndex[node];

                disk.seek(getBlockOffset(block));

                int bytesToRead = Math.min(BLOCK_SIZE, size - bytesRead);
                disk.readFully(result, bytesRead, bytesToRead);

                bytesRead += bytesToRead;
                node = fnodeNext[node];
            }

            return result;

        } finally {
            lock.readLock().unlock();
        }
    }

    public void deleteFile(String filename) throws Exception {
        lock.writeLock().lock();
        try {
            validateFilename(filename);

            int inodeIndex = findInodeByName(filename);
            if (inodeIndex == -1)
                throw new IllegalArgumentException("ERROR: file " + filename + " does not exist");

            FEntry fe = inodeTable[inodeIndex];

            int node = fe.getFirstBlock();
            while (node != -1) {
                int block = fnodeBlockIndex[node];
                int next = fnodeNext[node];

                if (block != -1) {
                    wipeBlock(block);
                    freeBlockList[block] = true;
                }

                fnodeBlockIndex[node] = -1;
                fnodeNext[node] = -1;

                node = next;
            }

            fe.setFilename(null);
            fe.setFilesize((short) 0);
            fe.setFirstBlock((short) -1);

            saveMetadata();

        } finally {
            lock.writeLock().unlock();
        }
    }

    public String[] listFiles() {
        lock.readLock().lock();
        try {
            java.util.List<String> files = new java.util.ArrayList<>();

            for (int i = 0; i < MAXFILES; i++) {
                String name = inodeTable[i].getFilename();
                if (name != null)
                    files.add(name);
            }

            return files.toArray(new String[0]);

        } finally {
            lock.readLock().unlock();
        }
    }
}
