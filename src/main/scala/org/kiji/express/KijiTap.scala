/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.express

import java.util.UUID

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeCollector
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator
import cascading.tuple.TupleEntryCollector
import cascading.tuple.TupleEntryIterator
import com.google.common.base.Objects

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.lib.NullOutputFormat

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.Resources.doAndRelease
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI

/**
 * A Kiji-specific implementation of a Cascading `Tap`, which defines the location of a Kiji table.
 *
 * KijiTap is responsible for configuring a MapReduce job with the correct input format for reading
 * from a Kiji table.
 *
 * KijiTap must be used with [[org.kiji.express.KijiScheme]] to perform decoding of cells in a
 * Kiji table. [[org.kiji.express.KijiSource]] handles the creation of both KijiScheme and
 * KijiTap in KijiExpress.
 *
 * @param uri of the Kiji table to read or write from.
 * @param scheme that will convert data read from Kiji into Cascading's tuple model.
 */
@ApiAudience.Framework
@ApiStability.Experimental
private[express] class KijiTap(
    // This is not a val because KijiTap needs to be serializable and KijiURI is not.
    uri: KijiURI,
    private val scheme: KijiScheme)
    extends Tap[JobConf, RecordReader[KijiKey, KijiValue], OutputCollector[_, _]](
        scheme.asInstanceOf[Scheme[JobConf, RecordReader[KijiKey, KijiValue],
            OutputCollector[_, _], _, _]]) {

  /** Address of the table to read from or write to. */
  private val tableUri: String = uri.toString()

  /** Unique identifier for this KijiTap instance. */
  private val id: String = UUID.randomUUID().toString()

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that reads from a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param conf to which we will add the table uri.
   */
  override def sourceConfInit(flow: FlowProcess[JobConf], conf: JobConf) {
    // Configure the job's input format.
    conf.setInputFormat(classOf[KijiInputFormat])

    // Store the input table.
    conf.set(KijiConfKeys.KIJI_INPUT_TABLE_URI, tableUri)

    super.sourceConfInit(flow, conf)
  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that writes to a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param conf to which we will add the table uri.
   */
  override def sinkConfInit(flow: FlowProcess[JobConf], conf: JobConf) {
    // TODO(CHOP-35): Use an output format that writes to HFiles.
    // Configure the job's output format.
    conf.setOutputFormat(classOf[NullOutputFormat[_, _]])

    // Store the output table.
    conf.set(KijiConfKeys.KIJI_OUTPUT_TABLE_URI, tableUri)

    super.sinkConfInit(flow, conf)
  }

  /**
   * Provides a string representing the resource this `Tap` instance represents.
   *
   * @return a java UUID representing this KijiTap instance. Note: This does not return the uri of
   *     the Kiji table being used by this tap to allow jobs that read from or write to the same
   *     table to have different data request options.
   */
  override def getIdentifier(): String = id

  /**
   * Opens any resources required to read from a Kiji table.
   *
   * @param flow being run.
   * @param recordReader that will read from the desired Kiji table.
   * @return an iterator that reads rows from the desired Kiji table.
   */
  override def openForRead(
      flow: FlowProcess[JobConf],
      recordReader: RecordReader[KijiKey, KijiValue]): TupleEntryIterator = {
    new HadoopTupleEntrySchemeIterator(
        flow,
        this.asInstanceOf[Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]],
        recordReader)
  }

  /**
   * Opens any resources required to write from a Kiji table.
   *
   * @param flow being run.
   * @param outputCollector that will write to the desired Kiji table. Note: This is ignored
   *     currently since writing to a Kiji table is currently implemented without using an output
   *     format by writing to the table directly from [[org.kiji.express.KijiScheme]].
   * @return a collector that writes tuples to the desired Kiji table.
   */
  override def openForWrite(
      flow: FlowProcess[JobConf],
      outputCollector: OutputCollector[_, _]): TupleEntryCollector = {
    new HadoopTupleEntrySchemeCollector(
        flow,
        this.asInstanceOf[Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]],
        outputCollector)
  }

  /**
   * Builds any resources required to read from or write to a Kiji table.
   *
   * Note: KijiExpress currently does not support automatic creation of Kiji tables.
   *
   * @param conf containing settings for this flow.
   * @return true if required resources were created successfully.
   * @throws UnsupportedOperationException always.
   */
  override def createResource(conf: JobConf): Boolean = {
    throw new UnsupportedOperationException("KijiTap does not support creating tables for you.")
  }

  /**
   * Deletes any unnecessary resources used to read from or write to a Kiji table.
   *
   * Note: KijiExpress currently does not support automatic deletion of Kiji tables.
   *
   * @param conf containing settings for this flow.
   * @return true if superfluous resources were deleted successfully.
   * @throws UnsupportedOperationException always.
   */
  override def deleteResource(conf: JobConf): Boolean = {
    throw new UnsupportedOperationException("KijiTap does not support deleting tables for you.")
  }

  /**
   * Determines if the Kiji table this `Tap` instance points to exists.
   *
   * @param conf containing settings for this flow.
   * @return true if the target Kiji table exists.
   */
  override def resourceExists(conf: JobConf): Boolean = {
    val uri: KijiURI = KijiURI.newBuilder(tableUri).build()

    doAndRelease(Kiji.Factory.open(uri)) { kiji: Kiji =>
      kiji.getTableNames().contains(uri.getTable())
    }
  }

  /**
   * Gets the time that the target Kiji table was last modified.
   *
   * Note: This will always return the current timestamp.
   *
   * @param conf containing settings for this flow.
   * @return the current time.
   */
  override def getModifiedTime(conf: JobConf): Long = System.currentTimeMillis()

  override def equals(other: Any): Boolean = {
    other match {
      case tap: KijiTap => (tableUri == tap.tableUri) && (scheme == tap.scheme) && (id == tap.id)
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(tableUri, scheme, id)
}
