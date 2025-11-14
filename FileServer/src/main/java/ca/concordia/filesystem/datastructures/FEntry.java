package ca.concordia.filesystem.datastructures;

public class FEntry {

    // filename == null  → this inode slot is free
    private String filename;
    private short filesize;
    private short firstBlock;  // index into fnode array, -1 if none

    public FEntry() {
        this.filename = null;
        this.filesize = 0;
        this.firstBlock = -1;
    }

    public FEntry(String filename, short filesize, short firstBlock) {
        setFilename(filename);
        setFilesize(filesize);
        this.firstBlock = firstBlock;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        // allow null (used to mark free inode)
        if (filename != null && filename.length() > 11) {
            throw new IllegalArgumentException("ERROR: filename too large");
        }
        this.filename = filename;
    }

    public short getFilesize() {
        return filesize;
    }

    public void setFilesize(short filesize) {
        if (filesize < 0) {
            throw new IllegalArgumentException("ERROR: filesize cannot be negative");
        }
        this.filesize = filesize;
    }

    public short getFirstBlock() {
        return firstBlock;
    }

    public void setFirstBlock(short firstBlock) {
        this.firstBlock = firstBlock;
    }
}
