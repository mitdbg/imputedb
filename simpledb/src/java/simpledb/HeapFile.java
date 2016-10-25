package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	private final int id;
	private final File file;
	private final TupleDesc schema;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        schema = td;
        id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return schema;
    }
    
    public Page readPage(PageId pid) {    	
    	try (RandomAccessFile f = new RandomAccessFile(file, "r")) {
    		int pageSize = BufferPool.getPageSize();
			int pageOffset = pid.getPageNumber() * pageSize;
			byte[] buf = new byte[pageSize];

			f.seek(pageOffset);
			f.readFully(buf);
			return new HeapPage(new HeapPageId(pid), buf);
    	} catch (IOException e) {
		}
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	try (RandomAccessFile f = new RandomAccessFile(file, "rw")) {
    		int pageSize = BufferPool.getPageSize();
			int pageOffset = page.getId().getPageNumber() * pageSize;
			f.seek(pageOffset);
			f.write(page.getPageData());
    	}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	BufferPool bp = Database.getBufferPool();
    	HeapPage insertPage = null;
    
    	// Find a page with empty slots.
    	for (int pageNum = 0; pageNum < numPages(); pageNum++) {
    		HeapPageId pid = new HeapPageId(id, pageNum);
    		HeapPage page = (HeapPage)bp.getPage(tid, pid, Permissions.READ_WRITE);
    		if (page.getNumEmptySlots() > 0) {
    			insertPage = page;
    			break;
    		}
    		bp.releasePage(tid, pid);
    	}
    	
    	// If we didn't find a page with empty slots, add a new page to the end
    	// of the file.
    	if (insertPage == null) {
    		try (RandomAccessFile fc = new RandomAccessFile(file, "rw")) {
    			int pageNum = numPages();
        		int pageSize = BufferPool.getPageSize();
    			int pageOffset = pageNum * pageSize;
    			
    			byte[] buf = HeapPage.createEmptyPageData();
    			fc.seek(pageOffset);
    			fc.write(buf);
    			
    			insertPage = (HeapPage)bp.getPage(tid, new HeapPageId(id, pageNum), Permissions.READ_WRITE);
    		}
    	}
    	
    	insertPage.insertTuple(t);
    	
    	ArrayList<Page> ret = new ArrayList<Page>();
        ret.add(insertPage);
        return ret;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	RecordId rid = t.getRecordId();
    	if (rid == null) {
    		throw new DbException("Tuple not stored in this file.");
    	}
    	
    	BufferPool bp = Database.getBufferPool();
        HeapPage page = (HeapPage)bp.getPage(tid, rid.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
                
        ArrayList<Page> ret = new ArrayList<Page>();
        ret.add(page);
        return ret;
    }
    
    private class PageIterator {
    	private int pageNum;
    	private final TransactionId tid;
    	private final BufferPool bp = Database.getBufferPool();
    	
    	public PageIterator(TransactionId tid) {
    		this.tid = tid;
    	}
		
		public boolean hasNext() {
			return pageNum < numPages();
		}

		public HeapPage next() throws DbException, TransactionAbortedException {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			
			HeapPage p = (HeapPage)bp.getPage(tid, new HeapPageId(id, pageNum), Permissions.READ_ONLY);
			pageNum++;
			return p;
		}
    }
    
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
        	private PageIterator pages;
        	private HeapPage page;
        	private Iterator<Tuple> tuplesInPage;
        	private final BufferPool bp = Database.getBufferPool();
        	        	
			@Override
			public void open() throws DbException, TransactionAbortedException {
				pages = new PageIterator(tid);
				tuplesInPage = null;
			}

			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				// Iterator is closed.
				if (pages == null) {
					return false;
				}
				
				// Iterator is open but we haven't read a page, 
				// or iterator is open and we've just finished reading a page.
				while (true) {
					if (tuplesInPage == null || !tuplesInPage.hasNext()) {
						if (pages.hasNext()) {
							// Release previous page before getting a new one.
							if (page != null) {
								bp.releasePage(tid, page.pid);
							}
							page = pages.next();
							tuplesInPage = page.iterator();
						} else {
							return false;
						}
					}
					if (tuplesInPage.hasNext()) {
						return true;
					} else {
						tuplesInPage = null;
					}
				}
			}

			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				return tuplesInPage.next();
			}

			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				open();
			}

			@Override
			public void close() {
				pages = null;
				tuplesInPage = null;
			}
        };
    }
}

