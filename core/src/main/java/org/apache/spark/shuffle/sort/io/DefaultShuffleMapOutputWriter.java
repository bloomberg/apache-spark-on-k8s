/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.sort.io;

import java.io.*;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.api.shuffle.ShuffleMapOutputWriter;
import org.apache.spark.api.shuffle.ShufflePartitionWriter;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.internal.config.package$;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.storage.TimeTrackingOutputStream;
import org.apache.spark.util.Utils;

public class DefaultShuffleMapOutputWriter implements ShuffleMapOutputWriter {

  private static final Logger log =
    LoggerFactory.getLogger(DefaultShuffleMapOutputWriter.class);

  private final int shuffleId;
  private final int mapId;
  private final ShuffleWriteMetricsReporter metrics;
  private final IndexShuffleBlockResolver blockResolver;
  private final long[] partitionLengths;
  private final int bufferSize;
  private int currPartitionId = 0;
  private long currChannelPosition;

  private final File outputFile;
  private final File outputTempFile;
  private FileOutputStream outputFileStream;
  private FileChannel outputFileChannel;
  private TimeTrackingOutputStream ts;
  private BufferedOutputStream outputBufferedFileStream;

  public DefaultShuffleMapOutputWriter(
      int shuffleId,
      int mapId,
      int numPartitions,
      ShuffleWriteMetricsReporter metrics,
      IndexShuffleBlockResolver blockResolver,
      SparkConf sparkConf) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.metrics = metrics;
    this.blockResolver = blockResolver;
    this.bufferSize =
      (int) (long) sparkConf.get(
        package$.MODULE$.SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE()) * 1024;
    this.partitionLengths = new long[numPartitions];
    this.outputFile = blockResolver.getDataFile(shuffleId, mapId);
    this.outputTempFile = Utils.tempFileWith(outputFile);
  }

  @Override
  public ShufflePartitionWriter getNextPartitionWriter() {
    return new DefaultShufflePartitionWriter(currPartitionId++);
  }

  @Override
  public void commitAllPartitions() throws IOException {
    cleanUp();
    blockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, outputTempFile);
    if (!outputFile.exists()) {
      if (!outputFile.getParentFile().isDirectory() && !outputFile.getParentFile().mkdirs()) {
        throw new IOException(
          String.format(
            "Failed to create shuffle file directory at %s.",
            outputFile.getParentFile().getAbsolutePath()));
      }
      if (!outputFile.isFile() && !outputFile.createNewFile()) {
        throw new IOException(
          String.format(
            "Failed to create empty shuffle file at %s.", outputFile.getAbsolutePath()));
      }
    }
  }

  @Override
  public void abort(Throwable error) {
    try {
      cleanUp();
    } catch (Exception e) {
      log.error("Unable to close appropriate underlying file stream", e);
    }
    if (!outputTempFile.delete() && outputTempFile.exists()) {
      log.warn("Failed to delete temporary shuffle file at {}", outputTempFile.getAbsolutePath());
    }
    if (!outputFile.delete() && outputFile.exists()) {
      log.warn("Failed to delete output shuffle file at {}", outputFile.getAbsolutePath());
    }
  }

  private void cleanUp() throws IOException {
    if (outputBufferedFileStream != null) {
      outputBufferedFileStream.close();
    }

    if (outputFileChannel != null) {
      outputFileChannel.close();
    }

    if (outputFileStream != null) {
      outputFileStream.close();
    }
  }

  private void initStream() throws IOException {
    if (outputFileStream == null) {
      outputFileStream = new FileOutputStream(outputTempFile, true);
      ts = new TimeTrackingOutputStream(metrics, outputFileStream);
    }
    if (outputBufferedFileStream == null) {
      outputBufferedFileStream = new BufferedOutputStream(ts, bufferSize);
    }
  }

  private void initChannel() throws IOException {
    if (outputFileStream == null) {
      outputFileStream = new FileOutputStream(outputTempFile, true);
    }
    if (outputFileChannel == null) {
      outputFileChannel = outputFileStream.getChannel();
    }
  }

  private class DefaultShufflePartitionWriter implements ShufflePartitionWriter {

    private final int partitionId;
    private PartitionWriterStream stream = null;

    private DefaultShufflePartitionWriter(int partitionId) {
      this.partitionId = partitionId;
    }

    @Override
    public OutputStream toStream() throws IOException {
      initStream();
      stream = new PartitionWriterStream();
      return stream;
    }

    @Override
    public FileChannel toChannel() throws IOException {
      initChannel();
      currChannelPosition = outputFileChannel.position();
      return outputFileChannel;
    }

    @Override
    public long getNumBytesWritten() {
      if (outputFileChannel != null && stream == null) {
        try {
          long newPosition = outputFileChannel.position();
          return newPosition - currChannelPosition;
        } catch (Exception e) {
          log.error("The currPartition is: {}", partitionId, e);
          throw new IllegalStateException("Failed to calculate position of file channel", e);
        }
      } else if (stream != null) {
        return stream.getCount();
      } else {
        return 0;
      }
    }

    @Override
    public void close() throws IOException {
      if (stream != null) {
        stream.close();
      }
      partitionLengths[partitionId] = getNumBytesWritten();
    }
  }

  private class PartitionWriterStream extends OutputStream {
    private int count = 0;
    private boolean isClosed = false;

    public int getCount() {
      return count;
    }

    @Override
    public void write(int b) throws IOException {
      if (isClosed) {
        throw new IllegalStateException("Attempting to write to a closed block output stream.");
      }
      outputBufferedFileStream.write(b);
      count++;
    }

    @Override
    public void close() throws IOException {
      flush();
      isClosed = true;
    }

    @Override
    public void flush() throws IOException {
      if (!isClosed) {
        outputBufferedFileStream.flush();
      }
    }
  }
}