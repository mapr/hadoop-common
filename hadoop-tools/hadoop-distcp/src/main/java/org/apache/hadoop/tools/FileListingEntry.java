package org.apache.hadoop.tools;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.LinkedList;

public class FileListingEntry {
    private FileListingEntry parent;
    private FileStatus sourceRealPath;
    private Path sourceLinkPath;
    private boolean keepLink;
    private LinkedList<CopyListingFileStatus> copyListingFileStatus;

    public FileListingEntry() {
    }


    public boolean isSymlink() {
        return sourceLinkPath != null;
    }

    public FileListingEntry getParent() {
        return parent;
    }

    public FileListingEntry setParent(FileListingEntry parent) {
        this.parent = parent;
        return this;
    }

    public FileListingEntry setSourceRealPath(FileStatus sourceRealPath) {
        this.sourceRealPath = sourceRealPath;
        return this;
    }

    public FileListingEntry setSourceLinkPath(Path sourceLinkPath) {
        this.sourceLinkPath = sourceLinkPath;
        return this;
    }

    public FileListingEntry setKeepLink(boolean keepLink) {
        this.keepLink = keepLink;
        return this;
    }

    public FileListingEntry setCopyListingFileStatus(LinkedList<CopyListingFileStatus> copyListingFileStatus) {
        this.copyListingFileStatus = copyListingFileStatus;
        return this;
    }

    public LinkedList<CopyListingFileStatus> getCopyListingFileStatus() {
        return copyListingFileStatus;
    }

    public FileStatus getSourceRealPath() {
        return sourceRealPath;
    }

    public Path getSourceLinkPath() {
        return sourceLinkPath;
    }

    public boolean isKeepLink() {
        return keepLink;
    }
}