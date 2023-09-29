/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.streaming;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.JVMStabilityInspector;
<<<<<<< HEAD
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.dht.*;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.service.StorageService;
=======

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.ExecutorUtils.awaitTermination;
import static org.apache.cassandra.utils.ExecutorUtils.shutdown;
>>>>>>> cassandra-5

/**
 * Task that manages receiving files for the session for certain ColumnFamily.
 */
public class StreamReceiveTask extends StreamTask
{
    private static final Logger logger = LoggerFactory.getLogger(StreamReceiveTask.class);

    private static final ExecutorService executor = executorFactory().pooled("StreamReceiveTask", Integer.MAX_VALUE);

    private final StreamReceiver receiver;

    // number of streams to receive
    private final int totalStreams;

    // total size of streams to receive
    private final long totalSize;

    // true if task is done (either completed or aborted)
    private volatile boolean done = false;

    private int remoteStreamsReceived = 0;
    private long bytesReceived = 0;

<<<<<<< HEAD
    private int remoteSSTablesReceived = 0;

    public int migrationFlag = 0;

    public StreamReceiveTask(StreamSession session, UUID cfId, int totalFiles, long totalSize)
=======
    public StreamReceiveTask(StreamSession session, TableId tableId, int totalStreams, long totalSize)
>>>>>>> cassandra-5
    {
        super(session, tableId);
        this.receiver = ColumnFamilyStore.getIfExists(tableId).getStreamManager().createStreamReceiver(session, totalStreams);
        this.totalStreams = totalStreams;
        this.totalSize = totalSize;
<<<<<<< HEAD
        // this is an "offline" transaction, as we currently manually expose the sstables once done;
        // this should be revisited at a later date, so that LifecycleTransaction manages all sstable state changes
        this.txn = LifecycleTransaction.offline(OperationType.STREAM);
        this.sstables = new ArrayList<>(totalFiles);

        this.migrationFlag = 0;
=======
>>>>>>> cassandra-5
    }

    /**
     * Process received stream.
     *
     * @param stream Stream received.
     */
<<<<<<< HEAD
    public synchronized void received(SSTableMultiWriter sstable, int migration)
    {

    if(migration==0){
=======
    public synchronized void received(IncomingStream stream)
    {
        Preconditions.checkState(!session.isPreview(), "we should never receive sstables when previewing");

>>>>>>> cassandra-5
        if (done)
        {
            logger.warn("[{}] Received stream {} on already finished stream received task. Aborting stream.", session.planId(),
                        stream.getName());
            receiver.discardStream(stream);
            return;
        }
        migrationFlag = migration;

        remoteStreamsReceived += stream.getNumFiles();
        bytesReceived += stream.getSize();
        Preconditions.checkArgument(tableId.equals(stream.getTableId()));
        logger.debug("received {} of {} total files, {} of total bytes {}", remoteStreamsReceived, totalStreams,
                     bytesReceived, stream.getSize());

<<<<<<< HEAD
        Collection<SSTableReader> finished = null;
        try
        {
            finished = sstable.finish(true);
        }
        catch (Throwable t)
        {
            Throwables.maybeFail(sstable.abort(t));
        }
        logger.debug("[{}] Received sstable {} from:{}", session.planId(), sstable.getFilename(), session.peer);
        txn.update(finished, false);
        sstables.addAll(finished);
=======
        receiver.received(stream);
>>>>>>> cassandra-5

        if (remoteStreamsReceived == totalStreams)
        {
            done = true;
            executor.submit(new OnCompletionRunnable(this));
        }
    }else{
        if (done)
        {
            logger.debug("[{}] Received replicaFile  on already finished stream received task. Aborting sstable.", session.planId());
            Throwables.maybeFail(sstable.abort(null));
            return;
        }
        remoteSSTablesReceived++;
        Collection<SSTableReader> finished = null;
        try
        {
            finished = sstable.finish(true);
        }
        catch (Throwable t)
        {
            Throwables.maybeFail(sstable.abort(t));
        }
        logger.debug("[{}] Received sstable {} from:{}, finished size:{}", session.planId(), sstable.getFilename(), session.peer, finished.size());
        txn.update(finished, false);
        if (remoteSSTablesReceived == totalFiles){
            done = true;
            logger.debug("after recieve repaired SSTable and write to replica copy!");
            executor.submit(new OnCompletionRunnable(this));
        }
    }

    }

    public synchronized void receivedReplica()
    {
        if (done)
        {
            logger.debug("[{}] Received replicaFile  on already finished stream received task. Aborting sstable.", session.planId());
            //Throwables.maybeFail(sstable.abort(null));
            return;
        }
        remoteSSTablesReceived++;
        //migrationFlag = migration;
        logger.debug("in receivedReplica, remoteSSTablesReceived:{}, totalFiles:{}", remoteSSTablesReceived, totalFiles);
        if (remoteSSTablesReceived == totalFiles){
            done = true;
            executor.submit(new OnCompletionRunnable(this));
        }
    }

    public int getTotalNumberOfFiles()
    {
        return totalStreams;
    }

    public long getTotalSize()
    {
        return totalSize;
    }

    public synchronized StreamReceiver getReceiver()
    {
        if (done)
            throw new RuntimeException(String.format("Stream receive task %s of cf %s already finished.", session.planId(), tableId));
        return receiver;
    }

    private static class OnCompletionRunnable implements Runnable
    {
        private final StreamReceiveTask task;

        public OnCompletionRunnable(StreamReceiveTask task)
        {
            this.task = task;
        }

        public void run()
        {
<<<<<<< HEAD
            boolean hasViews = false;
            boolean hasCDC = false;
            ColumnFamilyStore cfs = null;
            logger.debug("OnCompletionRunnable migrationFlag:{}", task.migrationFlag);
=======
>>>>>>> cassandra-5
            try
            {
                if (ColumnFamilyStore.getIfExists(task.tableId) == null)
                {
                    // schema was dropped during streaming
                    task.receiver.abort();
                    task.session.taskCompleted(task);
                    return;
                }

<<<<<<< HEAD
                Collection<SSTableReader> readers = task.sstables;
        if(readers!=null && readers.size()>0){
                try (Refs<SSTableReader> refs = Refs.ref(readers))
                {

                    Collection<Descriptor> migratedReadersDes = new ArrayList();
                    for(SSTableReader reader : readers){
                        migratedReadersDes.add(reader.descriptor);
                    }
                    StorageService.instance.replayMigratedSSTablesDes.put(migratedReadersDes);
                    StorageService.instance.migartedCFS = cfs;
                    /*
                     * We have a special path for views and for CDC.
                     *
                     * For views, since the view requires cleaning up any pre-existing state, we must put all partitions
                     * through the same write path as normal mutations. This also ensures any 2is are also updated.
                     *
                     * For CDC-enabled tables, we want to ensure that the mutations are run through the CommitLog so they
                     * can be archived by the CDC process on discard.
                     */
                    if (hasViews || hasCDC)
                    {
                        logger.debug("@@@@[Stream #{}] Received {} sstables from {} ({})", task.session.planId(), readers.size(), task.session.peer, readers);   
                        for (SSTableReader reader : readers)
                        {
                            Keyspace ks = Keyspace.open(reader.getKeyspaceName());
                            try (ISSTableScanner scanner = reader.getScanner())
                            {
                                while (scanner.hasNext())
                                {
                                    try (UnfilteredRowIterator rowIterator = scanner.next())
                                    {
                                        Mutation m = new Mutation(PartitionUpdate.fromIterator(rowIterator, ColumnFilter.all(cfs.metadata)));

                                        // MV *can* be applied unsafe if there's no CDC on the CFS as we flush below
                                        // before transaction is done.
                                        //
                                        // If the CFS has CDC, however, these updates need to be written to the CommitLog
                                        // so they get archived into the cdc_raw folder
                                        ks.apply(m, hasCDC, true, false);
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        task.finishTransaction();
                        logger.debug("------[Stream #{}] Received {} sstables from {} ({})", task.session.planId(), readers.size(), task.session.peer, readers);
                        // add sstables and build secondary indexes
                        cfs.addSSTables(readers);
                        cfs.indexManager.buildAllIndexesBlocking(readers);

                        //invalidate row and counter cache
                        if (cfs.isRowCacheEnabled() || cfs.metadata.isCounter())
                        {
                            List<Bounds<Token>> boundsToInvalidate = new ArrayList<>(readers.size());
                            readers.forEach(sstable -> boundsToInvalidate.add(new Bounds<Token>(sstable.first.getToken(), sstable.last.getToken())));
                            Set<Bounds<Token>> nonOverlappingBounds = Bounds.getNonOverlappingBounds(boundsToInvalidate);

                            if (cfs.isRowCacheEnabled())
                            {
                                int invalidatedKeys = cfs.invalidateRowCache(nonOverlappingBounds);
                                if (invalidatedKeys > 0)
                                    logger.debug("[Stream #{}] Invalidated {} row cache entries on table {}.{} after stream " +
                                                 "receive task completed.", task.session.planId(), invalidatedKeys,
                                                 cfs.keyspace.getName(), cfs.getTableName());
                            }

                            if (cfs.metadata.isCounter())
                            {
                                int invalidatedKeys = cfs.invalidateCounterCache(nonOverlappingBounds);
                                if (invalidatedKeys > 0)
                                    logger.debug("[Stream #{}] Invalidated {} counter cache entries on table {}.{} after stream " +
                                                 "receive task completed.", task.session.planId(), invalidatedKeys,
                                                 cfs.keyspace.getName(), cfs.getTableName());
                            }
                        }
                    }
                }
        }else{
            logger.debug("------before task.finishTransaction, [Stream #{}] Received replica File from {}", task.session.planId(), task.session.peer);                    
            task.finishTransaction();
        }

=======
                task.receiver.finished();
>>>>>>> cassandra-5
                task.session.taskCompleted(task);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                task.session.onError(t);
            }
            finally
            {
                task.receiver.cleanup();
            }
        }
    }

    /**
     * Abort this task.
     * If the task already received all files and
     * {@link org.apache.cassandra.streaming.StreamReceiveTask.OnCompletionRunnable} task is submitted,
     * then task cannot be aborted.
     */
    public synchronized void abort()
    {
        if (done)
            return;

        done = true;
        receiver.abort();
    }

    @VisibleForTesting
    public static void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        shutdown(executor);
        awaitTermination(timeout, unit, executor);
    }
}
