package simpledb;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import static java.nio.file.StandardOpenOption.READ;
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
    	try (FileChannel fc = FileChannel.open(file.toPath(), READ)) {
    		int pageSize = BufferPool.getPageSize();
			int pageOffset = pid.getPageNumber() * pageSize;
			
			ByteBuffer buf = ByteBuffer.allocate(pageSize);
			fc.read(buf, pageOffset);
			return new HeapPage(new HeapPageId(pid), buf.array());
    	} catch (IOException e) {
			e.printStackTrace();
		}
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
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
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }
    
    private class PageIterator {
    	private int pageNum;
    	private final TransactionId tid;
    	
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
			
			BufferPool bp = Database.getBufferPool();
			HeapPage p = (HeapPage)bp.getPage(tid, new HeapPageId(id, pageNum), Permissions.READ_ONLY);
			pageNum++;
			return p;
		}
    }
    
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
        	private PageIterator pages;
        	private Iterator<Tuple> tuplesInPage;
        	        	
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
				if (tuplesInPage == null || !tuplesInPage.hasNext()) {
					if (pages.hasNext()) {
						tuplesInPage = pages.next().iterator();
					} else {
						return false;
					}
				}
				return tuplesInPage.hasNext();
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

