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
package org.apache.cassandra.db.compaction;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.service.StorageService;

import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata.ChecksumValidator;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import org.apache.cassandra.gms.Gossiper;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.cassandra.io.erasurecode.*;
import org.apache.cassandra.io.sstable.format.*;
import java.util.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.dht.*;
import java.nio.ByteBuffer;
import java.io.*;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.db.partitions.PartitionUpdate;

import org.iq80.twoLayerLog.Options;
import org.iq80.twoLayerLog.ReadOptions;
import org.iq80.twoLayerLog.WriteOptions;
import org.iq80.twoLayerLog.impl.DbImpl;
import static org.iq80.twoLayerLog.impl.DbImpl.CompactionState;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;


import static org.apache.cassandra.db.compaction.CompactionHistoryTabularData.COMPACTION_TYPE_PROPERTY;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.FBUtilities.now;

public class CompactionTask extends AbstractCompactionTask
{
    protected static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);
    protected final long gcBefore;
    protected final boolean keepOriginals;
    protected static long totalBytesCompacted = 0;
    private ActiveCompactionsTracker activeCompactions;

    public CompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, long gcBefore)
    {
        this(cfs, txn, gcBefore, false);
    }

    public CompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, long gcBefore, boolean keepOriginals)
    {
        super(cfs, txn);
        this.gcBefore = gcBefore;
        this.keepOriginals = keepOriginals;
    }

    public static synchronized long addToTotalBytesCompacted(long bytesCompacted)
    {
        return totalBytesCompacted += bytesCompacted;
    }

    protected int executeInternal(ActiveCompactionsTracker activeCompactions)
    {
        this.activeCompactions = activeCompactions == null ? ActiveCompactionsTracker.NOOP : activeCompactions;
        run();
        return transaction.originals().size();
    }

    public boolean reduceScopeForLimitedSpace(Set<SSTableReader> nonExpiredSSTables, long expectedSize)
    {
        if (partialCompactionsAcceptable() && transaction.originals().size() > 1)
        {
            // Try again w/o the largest one.
            SSTableReader removedSSTable = cfs.getMaxSizeFile(nonExpiredSSTables);
            logger.warn("insufficient space to compact all requested files. {}MiB required, {} for compaction {} - removing largest SSTable: {}",
                        (float) expectedSize / 1024 / 1024,
                        StringUtils.join(transaction.originals(), ", "),
                        transaction.opId(),
                        removedSSTable);
            // Note that we have removed files that are still marked as compacting.
            // This suboptimal but ok since the caller will unmark all the sstables at the end.
            transaction.cancel(removedSSTable);
            return true;
        }
        return false;
    }

    /**
     * For internal use and testing only.  The rest of the system should go through the submit* methods,
     * which are properly serialized.
     * Caller is in charge of marking/unmarking the sstables as compacting.
     */
    protected void runMayThrow() throws Exception
    {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        assert transaction != null;

        if (transaction.originals().isEmpty())
            return;

        // Note that the current compaction strategy, is not necessarily the one this task was created under.
        // This should be harmless; see comments to CFS.maybeReloadCompactionStrategy.
        CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();

        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
        {
            Instant creationTime = now();
            cfs.snapshotWithoutMemtable(creationTime.toEpochMilli() + "-compact-" + cfs.name, creationTime);
        }

        try (CompactionController controller = getCompactionController(transaction.originals()))
        {
            final Set<SSTableReader> fullyExpiredSSTables = controller.getFullyExpiredSSTables();

            TimeUUID taskId = transaction.opId();
            // select SSTables to compact based on available disk space.
            if (!buildCompactionCandidatesForAvailableDiskSpace(fullyExpiredSSTables, taskId))
            {
                // The set of sstables has changed (one or more were excluded due to limited available disk space).
                // We need to recompute the overlaps between sstables.
                controller.refreshOverlaps();
            }

            // sanity check: all sstables must belong to the same cfs
            assert !Iterables.any(transaction.originals(), new Predicate<SSTableReader>()
            {
                @Override
                public boolean apply(SSTableReader sstable)
                {
                    return !sstable.descriptor.cfname.equals(cfs.name);
                }
            });

            // new sstables from flush can be added during a compaction, but only the compaction can remove them,
            // so in our single-threaded compaction world this is a valid way of determining if we're compacting
            // all the sstables (that existed when we started)
            StringBuilder ssTableLoggerMsg = new StringBuilder("[");
            for (SSTableReader sstr : transaction.originals())
            {
                ssTableLoggerMsg.append(String.format("%s:level=%d, ", sstr.getFilename(), sstr.getSSTableLevel()));
            }
            ssTableLoggerMsg.append("]");

            logger.info("Compacting ({}) {}", taskId, ssTableLoggerMsg);

            RateLimiter limiter = CompactionManager.instance.getRateLimiter();
            long start = nanoTime();
            long startTime = currentTimeMillis();
            long totalKeysWritten = 0;
            long estimatedKeys = 0;
            long inputSizeBytes;
            long timeSpentWritingKeys;

            Set<SSTableReader> actuallyCompact = null;
            actuallyCompact = Sets.difference(transaction.originals(), fullyExpiredSSTables);
            Collection<SSTableReader> newSStables;
            logger.debug("transaction.originals size:{}, actuallyCompact size:{}", transaction.originals().size(), actuallyCompact.size());

            long[] mergedRowCounts;
            long totalSourceCQLRows;

            long nowInSec = FBUtilities.nowInSeconds();
            try (Refs<SSTableReader> refs = Refs.ref(actuallyCompact);
                 AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(actuallyCompact);
                 CompactionIterator ci = new CompactionIterator(compactionType, scanners.scanners, controller, nowInSec, taskId))
            {
                long lastCheckObsoletion = start;
                inputSizeBytes = scanners.getTotalCompressedSize();
                double compressionRatio = scanners.getCompressionRatio();
                if (compressionRatio == MetadataCollector.NO_COMPRESSION_RATIO)
                    compressionRatio = 1.0;

                long lastBytesScanned = 0;

                activeCompactions.beginCompaction(ci);
                try (CompactionAwareWriter writer = getCompactionAwareWriter(cfs, getDirectories(), transaction, actuallyCompact))
                {
                    // Note that we need to re-check this flag after calling beginCompaction above to avoid a window
                    // where the compaction does not exist in activeCompactions but the CSM gets paused.
                    // We already have the sstables marked compacting here so CompactionManager#waitForCessation will
                    // block until the below exception is thrown and the transaction is cancelled.
                    if (!controller.cfs.getCompactionStrategyManager().isActive())
                        throw new CompactionInterruptedException(ci.getCompactionInfo());
                    estimatedKeys = writer.estimatedKeys();
                    ///////////////////////////////////////////////////////
                    logger.debug("during compaction, cfs.name:{}, getLevel:{}",cfs.name, getLevel());
                    if(cfs.name.equals("globalReplicaTable") && getLevel()>0 ){
                        
                        StorageService.instance.doingGlobalSplit = true;
                        //StorageService.instance.db.mutex.lock();   
                        CompactionState compactionState = new CompactionState(null);////
                        logger.debug("######actuallyCompact.size:{}, output size:{}", actuallyCompact.size(), compactionState.outputs.size()); 
                        
                        DataOutputBuffer dob = new DataOutputBuffer();                      
                        while (ci.hasNext())
                        {
                            if(StorageService.instance.groupCountDownMap.size() > 5) {
                                for (Map.Entry<String,CountDownLatch> latchEntry: StorageService.instance.groupCountDownMap.entrySet()) {
                                    //String groupID =  latchEntry.getKey();		
                                    try {		
                                        CountDownLatch curLatch = latchEntry.getValue();
                                        if(curLatch!=null) {
                                            curLatch.await();
                                            //groupCountDownMap.remove(groupID);
                                        }					
                                    } catch (InterruptedException e1) {
                                        // TODO Auto-generated catch block
                                        e1.printStackTrace();
                                    }
                                }
                                StorageService.instance.groupCountDownMap.clear();
                            }                                            
                            dob.clear();      
                               
                            if (ci.isStopRequested())
                                throw new CompactionInterruptedException(ci.getCompactionInfo());

                            UnfilteredRowIterator rowIterator = ci.next();
                            Mutation mutation = new Mutation(PartitionUpdate.fromIterator(rowIterator, ColumnFilter.all(cfs.metadata)));
                            try{
                                Mutation.serializer.serializeToValue(mutation, dob, MessagingService.current_version);   
                            } catch(Throwable e){
                                logger.debug("in splitToRangeGroups, Mutation.serializer.deserialize failed, strToken:{}!!", mutation.key().getToken());
                            } 
                            String strToken = StorageService.instance.getTokenFactory().toString(mutation.key().getToken());         
                            String groupID = StorageService.instance.findBoundTokenAccordingTokeny(strToken);                                          

                            try {		
                                CountDownLatch curLatch = StorageService.instance.groupCountDownMap.get(groupID);
                                if(curLatch!=null) {
                                    curLatch.await();
                                    StorageService.instance.groupCountDownMap.remove(groupID);
                                    //logger.debug("after await, curLatch size:{}", curLatch.getCount());
                                }					
                            } catch (InterruptedException e1) {
                                // TODO Auto-generated catch block
                                e1.printStackTrace();
                            }

                            CountDownLatch latch = new CountDownLatch(1);
                            StorageService.instance.groupCountDownMap.put(groupID,latch);
                            byte[] value = Arrays.copyOfRange(dob.toByteArray(), 0, dob.toByteArray().length);
                            Runnable runThread = new Runnable() {
                                public void run() {
                                    //StorageService.instance.db.splitToRangeGroups(strToken.getBytes(), dob.toByteArray(), groupID, compactionState);
                                    StorageService.instance.db.splitToRangeGroups(strToken.getBytes(), value, groupID, compactionState);
                                    CountDownLatch myLatch = StorageService.instance.groupCountDownMap.get(groupID);
                                    if(myLatch!=null) myLatch.countDown();                   	
                                }
                            };
                            StorageService.instance.splitExecutor.submit(runThread);

                            totalKeysWritten++;

                            long bytesScanned = scanners.getTotalBytesScanned();

                            //Rate limit the scanners, and account for compression
                            CompactionManager.compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio);

                            lastBytesScanned = bytesScanned;

                            if (nanoTime() - lastCheckObsoletion > TimeUnit.MINUTES.toNanos(1L))
                            {
                                controller.maybeRefreshOverlaps();
                                lastCheckObsoletion = nanoTime();
                            }
                        }

                        for (Map.Entry<String,CountDownLatch> latchEntry: StorageService.instance.groupCountDownMap.entrySet()) {                                
                            try {		
                                CountDownLatch curLatch = latchEntry.getValue();
                                if(curLatch!=null) {
                                    curLatch.await();                                         
                                }					
                            } catch (InterruptedException e1) {
                                // TODO Auto-generated catch block
                                e1.printStackTrace();
                            }
                        }
                        StorageService.instance.groupCountDownMap.clear();

                        logger.debug("------actuallyCompact.size:{}, output size:{}", actuallyCompact.size(), compactionState.outputs.size());
                        if(actuallyCompact.size()>0 && compactionState.currentFileNumberMap.size()>0){
                            logger.debug("------before installSplitResults, compactionState.currentFileNumber:{}", compactionState.currentFileNumber);
                            StorageService.instance.db.installSplitResults(compactionState);
                        }
                        //StorageService.instance.db.mutex.unlock();
                        StorageService.instance.doingGlobalSplit = false;      
                        StorageService.instance.mergeSort += System.currentTimeMillis() - startTime;
                        StorageService.instance.db.performGroupMerge();    

                    }else{/////////////////////////////////////////////

                        while (ci.hasNext())
                        {
                            if (ci.isStopRequested())
                                throw new CompactionInterruptedException(ci.getCompactionInfo());

                            if (writer.append(ci.next()))
                                totalKeysWritten++;


                            long bytesScanned = scanners.getTotalBytesScanned();

                            //Rate limit the scanners, and account for compression
                            CompactionManager.compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio);

                            lastBytesScanned = bytesScanned;

                            if (nanoTime() - lastCheckObsoletion > TimeUnit.MINUTES.toNanos(1L))
                            {
                                controller.maybeRefreshOverlaps();
                                lastCheckObsoletion = nanoTime();
                            }
                        }

                        StorageService.instance.compaction += System.currentTimeMillis() - startTime;//////
                    
                    }
                    timeSpentWritingKeys = TimeUnit.NANOSECONDS.toMillis(nanoTime() - start);

                    // point of no return
                    newSStables = writer.finish();
                }
                finally
                {
                    activeCompactions.finishCompaction(ci);
                    mergedRowCounts = ci.getMergedRowCounts();
                    totalSourceCQLRows = ci.getTotalSourceCQLRows();
                }
            }

            for (SSTableReader sstable : newSStables){
                int level = sstable.getSSTableLevel();
                long size = sstable.bytesOnDisk();
                StorageService.instance.totalLevelWrite[level]+= size/1048576;
            }

            if (transaction.isOffline())
                return;

            // log a bunch of statistics about the result and save to system table compaction_history
            long durationInNano = nanoTime() - start;
            long dTime = TimeUnit.NANOSECONDS.toMillis(durationInNano);
            long startsize = inputSizeBytes;
            long endsize = SSTableReader.getTotalBytes(newSStables);
            double ratio = (double) endsize / (double) startsize;

            StringBuilder newSSTableNames = new StringBuilder();
            for (SSTableReader reader : newSStables)
                newSSTableNames.append(reader.descriptor.baseFile()).append(",");
            long totalSourceRows = 0;
            for (int i = 0; i < mergedRowCounts.length; i++)
                totalSourceRows += mergedRowCounts[i] * (i + 1);

            String mergeSummary = updateCompactionHistory(taskId, cfs.getKeyspaceName(), cfs.getTableName(), mergedRowCounts, startsize, endsize,
                                                          ImmutableMap.of(COMPACTION_TYPE_PROPERTY, compactionType.type));

            logger.info(String.format("Compacted (%s) %d sstables to [%s] to level=%d.  %s to %s (~%d%% of original) in %,dms.  Read Throughput = %s, Write Throughput = %s, Row Throughput = ~%,d/s.  %,d total partitions merged to %,d.  Partition merge counts were {%s}. Time spent writing keys = %,dms",
                                       taskId,
                                       transaction.originals().size(),
                                       newSSTableNames.toString(),
                                       getLevel(),
                                       FBUtilities.prettyPrintMemory(startsize),
                                       FBUtilities.prettyPrintMemory(endsize),
                                       (int) (ratio * 100),
                                       dTime,
                                       FBUtilities.prettyPrintMemoryPerSecond(startsize, durationInNano),
                                       FBUtilities.prettyPrintMemoryPerSecond(endsize, durationInNano),
                                       (int) totalSourceCQLRows / (TimeUnit.NANOSECONDS.toSeconds(durationInNano) + 1),
                                       totalSourceRows,
                                       totalKeysWritten,
                                       mergeSummary,
                                       timeSpentWritingKeys));
            if (logger.isTraceEnabled())
            {
                Refs.release(Refs.selfRefs(newSStables));
            }
            else
            {
                // log a bunch of statistics about the result and save to system table compaction_history

                long durationInNano = nanoTime() - start;
                long dTime = TimeUnit.NANOSECONDS.toMillis(durationInNano);
                long startsize = inputSizeBytes;
                long endsize = SSTableReader.getTotalBytes(newSStables);
                double ratio = (double) endsize / (double) startsize;

                StorageService.instance.totalCompactWrite+= endsize/1048576;//////

                StringBuilder newSSTableNames = new StringBuilder();
                for (SSTableReader reader : newSStables)
                    newSSTableNames.append(reader.descriptor.baseFilename()).append(",");
                long totalSourceRows = 0;
                for (int i = 0; i < mergedRowCounts.length; i++)
                    totalSourceRows += mergedRowCounts[i] * (i + 1);

                String mergeSummary = updateCompactionHistory(cfs.keyspace.getName(), cfs.getTableName(), mergedRowCounts, startsize, endsize);
                logger.debug(String.format("Compacted (%s) %d sstables to [%s] to level=%d.  %s to %s (~%d%% of original) in %,dms.  Read Throughput = %s, Write Throughput = %s, Row Throughput = ~%,d/s.  %,d total partitions merged to %,d.  Partition merge counts were {%s}",
                                           taskId,
                                           transaction.originals().size(),
                                           newSSTableNames.toString(),
                                           getLevel(),
                                           FBUtilities.prettyPrintMemory(startsize),
                                           FBUtilities.prettyPrintMemory(endsize),
                                           (int) (ratio * 100),
                                           dTime,
                                           FBUtilities.prettyPrintMemoryPerSecond(startsize, durationInNano),
                                           FBUtilities.prettyPrintMemoryPerSecond(endsize, durationInNano),
                                           (int) totalSourceCQLRows / (TimeUnit.NANOSECONDS.toSeconds(durationInNano) + 1),
                                           totalSourceRows,
                                           totalKeysWritten,
                                           mergeSummary));
                logger.debug("######insertMemTable:{} s, insertLog:{} s, flushMemTable:{} s, compaction:{} s, mergeSort:{} s, mergeSortNum:{}", StorageService.instance.insertMemTable/1000, StorageService.instance.insertLog/1000, StorageService.instance.flushMemTable/1000, StorageService.instance.compaction/1000, StorageService.instance.mergeSort/1000, StorageService.instance.mergeSortNum);
                logger.debug("######readMemTable:{} ms, readRowCache:{} ms, readKeyCache:{} ms, readIndexBlock:{} ms, readSSTables:{} ms", StorageService.instance.readMemTable, StorageService.instance.readRowCache, StorageService.instance.readKeyCache, StorageService.instance.readIndexBlock, StorageService.instance.readSSTables);

                logger.debug("------totalCompactWrite:{} MB, readCommandNum:{}, totalReadBytes:{} totalSSTablesChecked:{}, totalSSTablesView:{}", StorageService.instance.totalCompactWrite, StorageService.instance.readCommandNum, StorageService.instance.totalReadBytes, StorageService.instance.totalSSTablesChecked, StorageService.instance.totalSSTablesView);
                logger.debug("------ReadGroupBytes:{} , writeGroupBytes:{}", StorageService.instance.ReadGroupBytes, StorageService.instance.writeGroupBytes);
                logger.debug("------DifferentiationTime:{} ms, DifferentiationNaTime:{} ns", StorageService.instance.DifferentiationTime, StorageService.instance.DifferentiationNaTime);
                
                strategy.getScanners(newSStables);
                logger.trace("CF Total Bytes Compacted: {}", FBUtilities.prettyPrintMemory(CompactionTask.addToTotalBytesCompacted(endsize)));
                logger.trace("Actual #keys: {}, Estimated #keys:{}, Err%: {}", totalKeysWritten, estimatedKeys, ((double)(totalKeysWritten - estimatedKeys)/totalKeysWritten));
            }

            cfs.getCompactionStrategyManager().compactionLogger.compaction(startTime, transaction.originals(), currentTimeMillis(), newSStables);

            // update the metrics
            cfs.metric.compactionBytesWritten.inc(endsize);
        }
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                          Directories directories,
                                                          LifecycleTransaction transaction,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        return new DefaultCompactionWriter(cfs, directories, transaction, nonExpiredSSTables, keepOriginals, getLevel());
    }

    public static String updateCompactionHistory(TimeUUID taskId, String keyspaceName, String columnFamilyName, long[] mergedRowCounts, long startSize, long endSize, Map<String, String> compactionProperties)
    {
        StringBuilder mergeSummary = new StringBuilder(mergedRowCounts.length * 10);
        Map<Integer, Long> mergedRows = new HashMap<>();
        for (int i = 0; i < mergedRowCounts.length; i++)
        {
            long count = mergedRowCounts[i];
            if (count == 0)
                continue;

            int rows = i + 1;
            mergeSummary.append(String.format("%d:%d, ", rows, count));
            mergedRows.put(rows, count);
        }
        SystemKeyspace.updateCompactionHistory(taskId, keyspaceName, columnFamilyName, currentTimeMillis(), startSize, endSize, mergedRows, compactionProperties);
        return mergeSummary.toString();
    }

    protected Directories getDirectories()
    {
        return cfs.getDirectories();
    }

    public static long getMinRepairedAt(Set<SSTableReader> actuallyCompact)
    {
        long minRepairedAt= Long.MAX_VALUE;
        for (SSTableReader sstable : actuallyCompact)
            minRepairedAt = Math.min(minRepairedAt, sstable.getSSTableMetadata().repairedAt);
        if (minRepairedAt == Long.MAX_VALUE)
            return ActiveRepairService.UNREPAIRED_SSTABLE;
        return minRepairedAt;
    }

    public static TimeUUID getPendingRepair(Set<SSTableReader> sstables)
    {
        if (sstables.isEmpty())
        {
            return ActiveRepairService.NO_PENDING_REPAIR;
        }
        Set<TimeUUID> ids = new HashSet<>();
        for (SSTableReader sstable: sstables)
            ids.add(sstable.getSSTableMetadata().pendingRepair);

        if (ids.size() != 1)
            throw new RuntimeException(String.format("Attempting to compact pending repair sstables with sstables from other repair, or sstables not pending repair: %s", ids));

        return ids.iterator().next();
    }

    public static boolean getIsTransient(Set<SSTableReader> sstables)
    {
        if (sstables.isEmpty())
        {
            return false;
        }

        boolean isTransient = sstables.iterator().next().isTransient();

        if (!Iterables.all(sstables, sstable -> sstable.isTransient() == isTransient))
        {
            throw new RuntimeException("Attempting to compact transient sstables with non transient sstables");
        }

        return isTransient;
    }


    /*
     * Checks if we have enough disk space to execute the compaction.  Drops the largest sstable out of the Task until
     * there's enough space (in theory) to handle the compaction.
     *
     * @return true if there is enough disk space to execute the complete compaction, false if some sstables are excluded.
     */
    protected boolean buildCompactionCandidatesForAvailableDiskSpace(final Set<SSTableReader> fullyExpiredSSTables, TimeUUID taskId)
    {
        if(!cfs.isCompactionDiskSpaceCheckEnabled() && compactionType == OperationType.COMPACTION)
        {
            logger.info("Compaction space check is disabled - trying to compact all sstables");
            return true;
        }

        final Set<SSTableReader> nonExpiredSSTables = Sets.difference(transaction.originals(), fullyExpiredSSTables);
        CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
        int sstablesRemoved = 0;

        while(!nonExpiredSSTables.isEmpty())
        {
            // Only consider write size of non expired SSTables
            long writeSize;
            try
            {
                writeSize = cfs.getExpectedCompactedFileSize(nonExpiredSSTables, compactionType);
                Map<File, Long> expectedNewWriteSize = new HashMap<>();
                List<File> newCompactionDatadirs = cfs.getDirectoriesForFiles(nonExpiredSSTables);
                long writeSizePerOutputDatadir = writeSize / Math.max(newCompactionDatadirs.size(), 1);
                for (File directory : newCompactionDatadirs)
                    expectedNewWriteSize.put(directory, writeSizePerOutputDatadir);

                Map<File, Long> expectedWriteSize = CompactionManager.instance.active.estimatedRemainingWriteBytes();

                // todo: abort streams if they block compactions
                if (cfs.getDirectories().hasDiskSpaceForCompactionsAndStreams(expectedNewWriteSize, expectedWriteSize))
                    break;
            }
            catch (Exception e)
            {
                logger.error("Could not check if there is enough disk space for compaction {}", taskId, e);
                break;
            }

            if (!reduceScopeForLimitedSpace(nonExpiredSSTables, writeSize))
            {
                // we end up here if we can't take any more sstables out of the compaction.
                // usually means we've run out of disk space

                // but we can still compact expired SSTables
                if(partialCompactionsAcceptable() && fullyExpiredSSTables.size() > 0 )
                {
                    // sanity check to make sure we compact only fully expired SSTables.
                    assert transaction.originals().equals(fullyExpiredSSTables);
                    break;
                }

                String msg = String.format("Not enough space for compaction (%s) of %s.%s, estimated sstables = %d, expected write size = %d",
                                           taskId,
                                           cfs.getKeyspaceName(),
                                           cfs.name,
                                           Math.max(1, writeSize / strategy.getMaxSSTableBytes()),
                                           writeSize);
                logger.warn(msg);
                CompactionManager.instance.incrementAborted();
                throw new RuntimeException(msg);
            }

            sstablesRemoved++;
            logger.warn("Not enough space for compaction {}, {}MiB estimated. Reducing scope.",
                        taskId, (float) writeSize / 1024 / 1024);
        }

        if(sstablesRemoved > 0)
        {
            CompactionManager.instance.incrementCompactionsReduced();
            CompactionManager.instance.incrementSstablesDropppedFromCompactions(sstablesRemoved);
            return false;
        }
        return true;
    }

    protected int getLevel()
    {
        return 0;
    }

    protected CompactionController getCompactionController(Set<SSTableReader> toCompact)
    {
        return new CompactionController(cfs, toCompact, gcBefore);
    }

    protected boolean partialCompactionsAcceptable()
    {
        return !isUserDefined;
    }

    public static long getMaxDataAge(Collection<SSTableReader> sstables)
    {
        long max = 0;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.maxDataAge > max)
                max = sstable.maxDataAge;
        }
        return max;
    }
}
