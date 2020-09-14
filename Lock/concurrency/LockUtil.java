package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */

    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(hw4_part2): implement

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null) {
            return;
        }

        if (lockContext.parent != null
                && lockContext.parent.saturation(transaction) >= .2
                && lockContext.parent.capacity() >= 10) {
            lockContext.parent.escalate(transaction);
            return;
        }

        if (LockType.substitutable(lockContext.getEffectiveLockType(transaction), lockType)) {
            return;
        }

        checkLocks(transaction, lockContext, lockType);
    }

    private static void checkLocks(TransactionContext transaction, LockContext lockContext,
                                   LockType lockType) {
        if (lockContext == null) {
            return;
        }
        LockType lock = lockContext.lockman.getLockType(transaction, lockContext.name);

        if (LockType.substitutable(lock, lockType)) {
            return;
        }

        checkLocks(transaction, lockContext.parent, LockType.parentLock(lockType));
        LockType eff = lockContext.getEffectiveLockType(transaction);
        LockType exp = lockContext.getExplicitLockType(transaction);
            if (lockContext.getExplicitLockType(transaction).equals(LockType.NL))
                lockContext.acquire(transaction, lockType);
            else if (LockType.substitutable(lockType, eff) && LockType.substitutable(lockType, exp) || lockType == LockType.SIX) {
                //System.out.println("lockutil");
                //System.out.println(lockType);
                //System.out.println(eff);
                //System.out.println(LockType.substitutable(lockType, curr));
                lockContext.promote(transaction, lockType);
            }
            else {
                //System.out.println("escalate");
                lockContext.escalate(transaction);
            }
    }

    // TODO(hw4_part2): add helper methods as you see fit

}
