package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;

class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    BNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getWorkMemSize();

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().getStats().getNumPages();
        int numRightPages = getRightSource().getStats().getNumPages();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               numLeftPages;
    }

    /**
     * BNLJ: Block Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Page> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Page> rightIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftRecordIterator = null;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordIterator = null;
        // The current record on the left page
        private Record leftRecord = null;
        private Record rightRecord = null;
        // The next record to return
        private Record nextRecord = null;

        private BNLJIterator() {
            super();

            this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
            fetchNextLeftBlock();

            this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
            this.rightIterator.markNext();
            fetchNextRightPage();

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        private void resetRightRecord() {
            this.rightRecordIterator.reset();
            assert(rightRecordIterator.hasNext());
            rightRecord = rightRecordIterator.next();
        }

        private void resetLeftRecord() {
            this.leftRecordIterator.reset();
            assert(leftRecordIterator.hasNext());
            leftRecord = leftRecordIterator.next();
        }

        /**
         * Fetch the next non-empty block of B - 2 pages from the left relation. leftRecordIterator
         * should be set to a record iterator over the next B - 2 pages of the left relation that
         * have a record in them, and leftRecord should be set to the first record in this block.
         *
         * If there are no more pages in the left relation with records, both leftRecordIterator
         * and leftRecord should be set to null.
         */
        private void fetchNextLeftBlock() {
            // TODO(hw3_part1): implement
            
            do {
                this.leftRecordIterator = getBlockIterator(this.getLeftTableName(), leftIterator, numBuffers - 2);
                this.leftRecordIterator.markNext();
                this.leftRecord = this.leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
                
            } while (!this.leftRecordIterator.hasNext());

            // No pages in the left relation with records
            if (!this.leftRecordIterator.hasNext()) {
                this.leftRecordIterator = null;
                this.leftRecord = null;
            }
        }

        /**
         * Fetch the next non-empty page from the right relation. rightRecordIterator
         * should be set to a record iterator over the next page of the right relation that
         * has a record in it.
         *
         * If there are no more pages in the right relation with records, rightRecordIterator
         * should be set to null.
         */
        private void fetchNextRightPage() {
            // TODO(hw3_part1): implement
            do {
                this.rightRecordIterator = getBlockIterator(this.getRightTableName(), rightIterator, 1);
                this.rightRecordIterator.markNext();
                this.rightRecord = this.rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;          
            } while (!this.rightRecordIterator.hasNext());
            
            // No pages in the right relation with records
            if (!this.rightRecordIterator.hasNext()) {
                this.rightRecordIterator = null;
            }
        }

        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         *
         * @throws NoSuchElementException if there are no more Records to yield
         */
        private void fetchNextRecord() {
            // TODO(hw3_part1): implement
            // our stopping condition for retrieving a right page is when the left 
            // side has no more tuples to evaluate because the left side is only 
            // looped through once in the BNLJ
            this.nextRecord = null;
            do {
                if (rightRecord != null) {
                    DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                    DataBox rightJoinValue = this.rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue))
                        this.nextRecord = joinRecords(leftRecord, rightRecord);
                    this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                }

                // end of right page, move on to the next right page
                else if (this.leftRecordIterator.hasNext()) {
                    leftRecord = leftRecordIterator.next();
                    resetRightRecord();
                }

                // left block and right page both ended
                else if (this.rightIterator.hasNext()) {
                    fetchNextRightPage();
                    resetLeftRecord();
                }

                // end of current left block and all right pages, move on to new left block
                else if (this.leftIterator.hasNext()) {
                    fetchNextLeftBlock();
                    fetchNextRightPage();
                }
            
                // No new records
                else
                    { throw new NoSuchElementException("No new record to fetch"); }
            } while (!hasNext());
        }

        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
