package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.DoubleBinaryOperator;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int num_Pages;
    private final ConcurrentHashMap<Integer,Page> page_hashmap;
    private int test_num;
    private LockProcess lockprocess;

    /*
    * Added by Sakura
    * Helper class of describe the type of lock and the lock's tid.
    * when lockType == READ_WRITE means the lock is a exclusive lock
    * when lockType == READ_ONLY means the lock is a shared lock
    * */
    public class Lock{
        private Permissions lockType;
        private TransactionId tid;

        public Lock(Permissions lockType,TransactionId tid){
            this.lockType=lockType;
            this.tid=tid;
        }
        public Lock(Permissions lockType){
            this.lockType=lockType;
            this.tid=null;
        }

        @Override
        public boolean equals(Object obj) {
            if(this==obj) return true;
            if(obj==null||getClass()!=obj.getClass()) return false;
            Lock obj_lock=(Lock) obj;
            return tid.equals(obj_lock.tid)&&lockType.equals(obj_lock.lockType);
        }
    }

    /*
    * Added by Sakura
    * Helper class to maintain and process series of locks on specific transaction
    * */
    public class LockProcess{
        private ConcurrentHashMap<PageId,List<Lock>> pageid2locklist;
        public LockProcess(){
            pageid2locklist=new ConcurrentHashMap<PageId,List<Lock>>();
        }
        public void addLock(TransactionId tid,PageId pid,Permissions perm){
            Lock lock_to_add=new Lock(perm,tid);
            List<Lock> locklist=pageid2locklist.get(pid);
            if(locklist==null){//如果这个页面上还没有Lock
                locklist=new ArrayList<>();
            }
            locklist.add(lock_to_add);
            pageid2locklist.put(pid,locklist);
        }
        public synchronized boolean acquiresharelock(TransactionId tid,PageId pid)
                throws DbException{
            List<Lock> locklist=pageid2locklist.get(pid);
            if(locklist!=null&&locklist.size()!=0){
                if(locklist.size()==1){
                    Lock only_lock=locklist.iterator().next();
                    if(only_lock.tid.equals(tid)){
                        if(only_lock.lockType==Permissions.READ_ONLY) return true;
                        else addLock(tid,pid,Permissions.READ_ONLY);
                    }
                    else{
                        if(only_lock.lockType==Permissions.READ_ONLY) addLock(tid,pid,Permissions.READ_ONLY);
                        else {
                            try {
                                wait();
                            }
                            catch(InterruptedException e){
                                e.printStackTrace();
                            }
                            return false;
                        }
                        //throw new DbException("something to be done in acquiresharelock() func");
                    }
                }
                else{
                    // Opt1.两个锁，都属于tid（一读一写）
                    // Opt2.两个锁，都属于非tid（一读一写）
                    // Opt3.多个读锁，有一个读锁为tid的
                    // Opt4.多个读锁，但没有读锁为tid的
                    for (Lock it : locklist) {
                        if (it.lockType == Permissions.READ_WRITE) {
                            //如果其中有一个写锁，那么根据是否为自己的来判断属于情况1还是2
                            if(it.tid.equals(tid)) return true;
                            else {
                                try {
                                    wait();
                                }
                                catch(InterruptedException e){
                                    e.printStackTrace();
                                }
                                return false;
                            }
                        }
                        else{
                            if (it.tid.equals(tid)) return true;
                        }
                    }
                    addLock(tid,pid,Permissions.READ_ONLY);
                    return true;
                }
            }
            addLock(tid,pid,Permissions.READ_ONLY);
            return true;
        }
        public synchronized boolean acquireexclusivelock(TransactionId tid,PageId pid)
                throws DbException{
            List<Lock> locklist=pageid2locklist.get(pid);
            if(locklist!=null&&locklist.size()!=0) {
                if (locklist.size() == 1) {
                    Lock only_lock = locklist.iterator().next();
                    if (only_lock.tid.equals(tid)){
                        if(only_lock.lockType== Permissions.READ_WRITE) return true;
                        else {
                            addLock(tid,pid, Permissions.READ_WRITE);
                            return true;
                        }
                    }
                    else {
                        try {
                            wait();
                        }
                        catch(InterruptedException e){
                            e.printStackTrace();
                        }
                        return false;
                    }
                    //else throw new DbException("something to do");
                }
                else {
                    if (locklist.size() == 2) {
                        for (Lock it : locklist) {
                            if (it.tid.equals(tid) && it.lockType == Permissions.READ_WRITE) {
                                return true;
                            }
                        }
                    }
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return false;
                }
            }
            else{
                addLock(tid,pid, Permissions.READ_WRITE);
                return true;
            }
        }
        public synchronized void releasePage(TransactionId tid, PageId pid)
                throws DbException{
            List<Lock> locks=pageid2locklist.get(pid);
            if(locks==null||locks.size()==0) throw new DbException("the page has no lock");
            Lock temp_lock=getLock(tid,pid);
            locks.remove(temp_lock);
            pageid2locklist.put(pid,locks);
        }
        public synchronized boolean holdsLock(TransactionId tid,PageId pid){
            List<Lock> locks=pageid2locklist.get(pid);
            if(locks==null||locks.size()==0) return false;
            for(int i=0;i<locks.size();i++){
                if(locks.get(i).tid.equals(tid)) return true;
            }
            return false;
        }
        public synchronized Lock getLock(TransactionId tid, PageId pid) {
            List<Lock> list = pageid2locklist.get(pid);
            if (list == null || list.size() == 0) {
                return null;
            }
            for (Lock ls : list) {
                if (ls.tid.equals(tid)) {
                    return ls;
                }
            }
            return null;
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        num_Pages=numPages;
        page_hashmap=new ConcurrentHashMap<>();
        test_num=0;
        lockprocess=new LockProcess();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException{
        // some code goes here

        if(perm==Permissions.READ_WRITE) lockprocess.acquireexclusivelock(tid,pid);
        else lockprocess.acquiresharelock(tid,pid);

        if(!page_hashmap.containsKey(pid.hashCode())){
            DbFile dbfile= Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page=dbfile.readPage(pid);
            if(page_hashmap.size()>=num_Pages) {
                evictPage();
            }
            page_hashmap.put(pid.hashCode(), page);
        }
        return page_hashmap.get(pid.hashCode());
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid)
            throws DbException{
        // some code goes here
        // not necessary for lab1|lab2
        lockprocess.releasePage(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockprocess.holdsLock(tid,p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     * @Todo has not been done
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile now_dbfile=Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> temp_arraylist=now_dbfile.insertTuple(tid,t);
        for (Page now_page:temp_arraylist) {
            System.out.println(now_page.getId()+"insert1");
            now_page.markDirty(true,tid);
            page_hashmap.put(now_page.getId().hashCode(),now_page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile now_dbfile=Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> temp_arraylist=now_dbfile.deleteTuple(tid,t);
        for(Page now_page:temp_arraylist){
            System.out.println(now_page.getId()+"delete1");
            now_page.markDirty(true,tid);
            page_hashmap.put(now_page.getId().hashCode(),now_page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(Page now_page:page_hashmap.values()){
            flushPage(now_page.getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        page_hashmap.remove(pid.hashCode());
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        //System.out.println(test_num++);
        Page now_page=page_hashmap.get(pid.hashCode());
        if(now_page.isDirty()!=null){
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(now_page);
            now_page.markDirty(false,null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Integer toevict_page_hashcode= new ArrayList<>(page_hashmap.keySet()).get(0);
        PageId toevict_pageid=page_hashmap.get(toevict_page_hashcode).getId();
        try{
            flushPage(toevict_pageid);
        }
        catch(IOException ioe_exception){
            ioe_exception.printStackTrace();
        }
        discardPage(toevict_pageid);
    }

}
