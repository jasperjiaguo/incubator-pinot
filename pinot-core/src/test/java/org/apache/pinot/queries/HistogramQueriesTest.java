/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.queries;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOrderByOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Queries test for histogramSV/histogramMV queries.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class HistogramQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "HistogramQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final Random RANDOM = new Random();

  private static final int NUM_RECORDS = 2000;
  private static final int MAX_VALUE = 3000;

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG).addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    // NOTE: This is a match all filter
    return " WHERE intColumn >=  500";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      int value = RANDOM.nextInt(MAX_VALUE);
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, i);
      record.putValue(LONG_COLUMN, (long) i - NUM_RECORDS / 2);
      record.putValue(FLOAT_COLUMN, (float) i * 0.5);
      record.putValue(DOUBLE_COLUMN, (double) i);
      records.add(record);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @Test
  public void testAggregationOnlyVer1() {
    String query = "SELECT HISTOGRAMSV(intColumn,ARRAY[0,1,10,100,1000,10000]) FROM testTable";

    // Inner segment
    Object operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getAggregationResult();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(), new double[]{1, 9, 90, 900, 999});

    operator = getOperatorWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), 1500, 0, 1500,
        NUM_RECORDS);
    aggregationResult = resultsBlock.getAggregationResult();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(), new double[]{0, 0, 0, 501, 999});

    // Inter segment
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.get(0)[0], new double[]{4, 36, 360, 3600, 3996});
  }

  @Test
  public void testAggregationOnlyVer2() {
    String query = "SELECT HISTOGRAMSV(intColumn,0,1000,10) FROM testTable";

    // Inner segment
    Object operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getAggregationResult();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(),
        new double[]{100, 100, 100, 100, 100, 100, 100, 100, 100, 100});

    operator = getOperatorWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), 1500, 0, 1500,
        NUM_RECORDS);
    aggregationResult = resultsBlock.getAggregationResult();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(),
        new double[]{0, 0, 0, 0, 1, 100, 100, 100, 100, 100});

    // Inter segment
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.get(0)[0], new double[]{400, 400, 400, 400, 400, 400, 400, 400, 400, 400});
  }

  @Test
  public void testAggregationGroupBy() {
    String query = "SELECT HISTOGRAMSV(doubleColumn,0,2000,20) FROM testTable GROUP BY CEIL(DIV(intColumn, 400)) "
        + "ORDER BY CEIL(DIV(intColumn, 400))";

    // Inner segment
    Object operator = getOperator(query);
    assertTrue(operator instanceof AggregationGroupByOrderByOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationGroupByOrderByOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 2, NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    assertEquals(((DoubleArrayList) aggregationGroupByResult.getResultForGroupId(0, 0)).elements(),
        new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    assertEquals(((DoubleArrayList) aggregationGroupByResult.getResultForGroupId(0, 1)).elements(),
        new double[]{100, 100, 100, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    assertEquals(((DoubleArrayList) aggregationGroupByResult.getResultForGroupId(0, 2)).elements(),
        new double[]{0, 0, 0, 0, 100, 100, 100, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    assertEquals(((DoubleArrayList) aggregationGroupByResult.getResultForGroupId(0, 3)).elements(),
        new double[]{0, 0, 0, 0, 0, 0, 0, 0, 100, 100, 100, 100, 0, 0, 0, 0, 0, 0, 0, 0});
    assertEquals(((DoubleArrayList) aggregationGroupByResult.getResultForGroupId(0, 4)).elements(),
        new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100, 100, 100, 100, 0, 0, 0, 0});
    assertEquals(((DoubleArrayList) aggregationGroupByResult.getResultForGroupId(0, 5)).elements(),
        new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100, 100, 100, 99});

    // Inter segment
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.get(0)[0], new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    assertEquals(rows.get(1)[0],
        new double[]{400, 400, 400, 400, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    assertEquals(rows.get(2)[0],
        new double[]{0, 0, 0, 0, 400, 400, 400, 400, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    assertEquals(rows.get(3)[0],
        new double[]{0, 0, 0, 0, 0, 0, 0, 0, 400, 400, 400, 400, 0, 0, 0, 0, 0, 0, 0, 0});
    assertEquals(rows.get(4)[0],
        new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 400, 400, 400, 400, 0, 0, 0, 0});
    assertEquals(rows.get(5)[0],
        new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 400, 400, 400, 396});
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
