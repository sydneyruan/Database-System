package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
 
import java.util.*;
 
/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();
 
    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();
 
        // TODO(hw4_part1): You may add helper methods here if you wish
 
        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }
 
    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();
 
    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }
 
    // TODO(hw4_part1): You may add helper methods here if you wish
 
    // Indicate if LockRequest should be placed at the FRONT or BACK of the queue
    private int front = 0;
    private int back = 1;
 
    // Check if the new lock is compatible with another transaction's lock on the resource.
    public boolean checkCompatibility(ResourceName name, TransactionContext transaction, LockType lockType) {
        for (Lock l : getLocks(name)) {
            if (!LockType.compatible(lockType, l.lockType) && (!l.equals(getLock(name, transaction)))) {
                return false;
            }
        }
        return true;
    }
 
    // If error(s) exist, the transaction is blocked and the request is placed at the front/back of ITEM's queue.
    public void ifError (TransactionContext transaction, ResourceName name, LockType lockType, int position) {
        transaction.prepareBlock();
        Lock newLock = new Lock(name, lockType, transaction.getTransNum());
        LockRequest newRequest = new LockRequest(transaction, newLock);
        if (position == front)
            this.getResourceEntry(name).waitingQueue.addFirst(newRequest);
        else if (position == back)
            this.getResourceEntry(name).waitingQueue.addLast(newRequest);
    }

    private void acquireHelper(ResourceName name, Long transNum, Lock newLock) {
        if (transactionLocks.containsKey(transNum)) {
            this.transactionLocks.get(transNum).add(newLock);
            this.getResourceEntry(name).locks.add(newLock);
        }
        else {
            List<Lock> newLockList = new ArrayList<>();
            newLockList.add(newLock);
            transactionLocks.put(transNum, newLockList);
            this.getResourceEntry(name).locks.add(newLock);
        }
    }

    public void upgrade (TransactionContext transaction, ResourceName name, LockType newLockType) {
        Long transNum = transaction.getTransNum();
        for (Lock lk : getResourceEntry(name).locks) {
            if (lk.transactionNum == transNum) {
                lk.lockType = newLockType;
                break;
            }
        }
        for (Lock lk : transactionLocks.get(transNum)) {
            if (lk.name == name) {
                lk.lockType = newLockType;
                break;
            }
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.

        Long transNum = transaction.getTransNum();
        boolean blocked = false;
        Lock newLock = new Lock(name, lockType, transaction.getTransNum());
 
        synchronized (this) {
            // Checking if compatible
            boolean isCompatible = checkCompatibility(name, transaction, lockType);
            blocked = !isCompatible;
            if (blocked)
                ifError(transaction, name, lockType, front);
            for (Lock l : getLocks(transaction)) {
                if (getLocks(name).contains(l) && !releaseLocks.contains(l.name))
                    throw new DuplicateLockRequestException("Duplicate lock request!");
            }
            
            if (!blocked) {
                // Promote
                if (releaseLocks.contains(name)) {
                    Lock oldLock = getLock(name, transaction);
                    if (oldLock != null)
                        if (LockType.substitutable(lockType, oldLock.lockType))
                            upgrade(transaction, name, lockType);
                }
                 // Acquire
                acquireHelper(name, transNum, newLock);

                // Release
                Lock check = getLock(name, transaction);
                for (ResourceName rn : releaseLocks) {
                    release(transaction, rn);
                    System.out.println(rn);
                }
            }
        }
        if (blocked)
            transaction.block(); 
    }
 
    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        Long transNum = transaction.getTransNum();
        boolean blocked = false;
        Lock newLock = new Lock(name, lockType, transaction.getTransNum());
 
        synchronized (this) {
            // Checking if compatible
            boolean isCompatible = checkCompatibility(name, transaction, lockType);
            blocked = !isCompatible || !getResourceEntry(name).waitingQueue.isEmpty();
            // If the new lock is not compatible with another transaction's lock on the resource,
            // or if there are other transaction in queue for the resource,
            // the transaction is blocked and the request is placed at the **back** of NAME's queue.
            if (blocked)
                ifError(transaction, name, lockType, back);
            
            // Check if a lock on NAME is held by TRANSACTION
            if (getLock(name, transaction) != null)
                throw new DuplicateLockRequestException("Duplicate lock request!");

            // acquire
            if (!blocked) {
                acquireHelper(name, transNum, newLock);
            }
        }
        if (blocked)
            transaction.block();
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method.
        Long transNum = transaction.getTransNum();
 
        synchronized (this) {
            ResourceEntry entry = this.getResourceEntry(name);
            Lock l = getLock(name, transaction);
            if (l == null)
                throw new NoLockHeldException("No lock held!");
            transactionLocks.get(transNum).remove(l);
            entry.locks.remove(l);

            // Process queue
            while (!entry.waitingQueue.isEmpty()) {
                LockRequest request = entry.waitingQueue.peek();
                Lock lock = request.lock;
                boolean isCompatible = checkCompatibility(name, request.transaction, lock.lockType);
                boolean promote = false;
                Lock oldLock = getLock(name, request.transaction);

                // check promote
                if (oldLock != null) {
                    if (LockType.substitutable(lock.lockType, oldLock.lockType))
                        promote = true;
                }

                if (promote) {
                    request.transaction.unblock();
                    upgrade(request.transaction, name, lock.lockType);
                    entry.waitingQueue.removeFirst();
                    for (Lock rl : request.releasedLocks) {
                        release(request.transaction, rl.name);
                    }
                }
                else if (isCompatible) {
                    request.transaction.unblock();
                        acquireHelper(name, request.transaction.getTransNum(), lock);
                        entry.waitingQueue.removeFirst();
                        for (Lock rl : request.releasedLocks) {
                            release(request.transaction, rl.name);
                        }
                }
                else
                    break;
            }
            transaction.unblock();
        }
    }
 
 
    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method.
        boolean blocked = false;
        //Long transNum = transaction.getTransNum();
 
        synchronized (this) {
            boolean isCompatible = checkCompatibility(name, transaction, newLockType);
            blocked = !isCompatible;
            LockType required = getLockType(transaction, name);
            Lock l = getLock(name, transaction);

            // If the new lock is not compatible with another transaction's
            // lock on the resource, the transaction is blocked and the
            // request is placed at the **front** of ITEM's queue.
            if (blocked) {
                ifError(transaction, name, newLockType, front);
            }
 
            // Check if TRANSACTION already has a NEWLOCKTYPE lock on NAME
            if (required.equals(newLockType))
                throw new DuplicateLockRequestException("Duplicate lock request!");
 
            // Check if requested lock type is not a promotion
            if (!LockType.substitutable(newLockType, required)) {
                //System.out.println("lockman");
                //System.out.println(newLockType);
                //System.out.println(required);
                throw new InvalidLockException("Invalid lock!");
            }
            if (l == null || newLockType == LockType.NL)
                throw new NoLockHeldException("No lock held!");
 
            // set to new lock type
            if (!blocked) {
                upgrade(transaction, name, newLockType);
            }
        }
        if (blocked)
            transaction.block();
    }
 
    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(hw4_part1): implement
        Lock l = getLock(name, transaction);
        if (l != null)
            return l.lockType;
        return LockType.NL;
    }
 
    // Returns the lock held on NAME by TRANSACTION, throws exception if no lock found.
    public synchronized Lock getLock(ResourceName name, TransactionContext transaction) {
        for (Lock l : getLocks(transaction)) {
            if (l.name.equals(name))
                return new Lock(name, l.lockType, transaction.getTransNum());
        }  
        return null;
    }
    
    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }
 
    /**
     * Returns the list of locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }
 
    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }
 
    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
 

