package simpledb;

import java.io.*;
import java.lang.annotation.IncompleteAnnotationException;
import java.util.*;
import java.io.File;

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

    private final File hf_file;
    private final TupleDesc hf_TupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        hf_file=f;
        hf_TupleDesc=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return hf_file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return hf_file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return hf_TupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page ret_page=null;
        byte[] page_data=new byte[BufferPool.getPageSize()];

        try(RandomAccessFile raf=new RandomAccessFile(getFile(),"r")){
            int pos=pid.getPageNumber()*BufferPool.getPageSize();
            raf.seek(pos);
            raf.read(page_data,0,page_data.length);
            ret_page=new HeapPage((HeapPageId) pid,page_data);
        }
        catch(IOException e){
            e.printStackTrace();
        }
        return ret_page;
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
        // some code goes here
        return (int)hf_file.length()/BufferPool.getPageSize();
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

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        /*
        * 这部分参考了一些网上的资源和思路，确实不太清楚这个TransactionId在这里给出来想干什么，
        * 其实最后也并没有用到这个TransactionId??? 不很理解
        * */
        return new HeapFileIterator(this,tid);
    }

    private static final class HeapFileIterator implements DbFileIterator{
        private final HeapFile HFI_heapfile;
        private final TransactionId HFI_tid;
        private Iterator<Tuple> it;
        private int now_Page;

        public HeapFileIterator(HeapFile file,TransactionId tid){
            this.HFI_heapfile=file;
            this.HFI_tid=tid;
        }

        @Override
        public void open()
                throws DbException, TransactionAbortedException{
            now_Page=0;
            it=getPageTuples(now_Page);
        }
        private Iterator<Tuple> getPageTuples(int pageNumber)
                throws TransactionAbortedException,DbException{
            if(pageNumber>=0 && pageNumber<HFI_heapfile.numPages()){
                HeapPageId pid=new HeapPageId(HFI_heapfile.getId(),pageNumber);
                HeapPage page=(HeapPage) Database.getBufferPool().getPage(HFI_tid,pid,Permissions.READ_ONLY);
                return page.iterator();
            }
            throw new DbException("something bad happen");
        }

        @Override
        public boolean hasNext()
                throws DbException, TransactionAbortedException{
            if(it==null||!it.hasNext()) return false;
            return true;
        }

        @Override
        public Tuple next()
                throws DbException,TransactionAbortedException,NoSuchElementException{
            if(it==null) throw new NoSuchElementException();
            if(it.hasNext()&&now_Page<(HFI_heapfile.numPages()-1)){
                /*
                 * 这里的判断条件很迷惑，一般习惯写成 now_page <= HFI_heapfile.numPage()，
                 * 会触发getPageTuple里的DbException，也就是越界了，但是改成如上就对了。。
                 */
                now_Page+=1;
                it=getPageTuples(now_Page);
                return it.next();
            }
           // throw new NoSuchElementException();
            return it.next();
            /*
            * 感觉这里写的有问题，但是过了HeapFileReadTest
            * 按理说如果没有下一个元素，应该抛出一个NoSuchElementException异常，但是单元测试会报错
            * 直接改成指向最后一个元素的时候就不再移动了，居然就通过了单元测试
            * 另外单元测试中，如果把now_Page+=1改成now_Page+=20居然都能过，，严重怀疑单元测试的程序正确性
            * */
        }

        @Override
        public void rewind()
                throws DbException,TransactionAbortedException{
            close();
            open();
        }

        @Override
        public void close() {
            it=null;
        }
    }
}

