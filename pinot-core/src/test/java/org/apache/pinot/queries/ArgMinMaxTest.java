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

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.query.utils.rewriter.ResultRewriterFactory;
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
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;


/**
 * Queries test for argMin/argMax functions.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ArgMinMaxTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "HistogramQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_RECORDS = 2000;

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String MV_DOUBLE_COLUMN = "mvDoubleColumn";
  private static final String MV_INT_COLUMN = "mvIntColumn";
  private static final String MV_BYTES_COLUMN = "mvBytesColumn";
  private static final String MV_STRING_COLUMN = "mvStringColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String GROUP_BY_INT_COLUMN = "groupByIntColumn";
  private static final String GROUP_BY_MV_INT_COLUMN = "groupByMVIntColumn";
  private static final String GROUP_BY_INT_COLUMN2 = "groupByIntColumn2";
  private static final String BIG_DECIMAL_COLUMN = "bigDecimalColumn";
  private static final String TIMESTAMP_COLUMN = "timestampColumn";
  private static final String BOOLEAN_COLUMN = "booleanColumn";

  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG).addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE).addMultiValueDimension(MV_INT_COLUMN, DataType.INT)
      .addMultiValueDimension(MV_BYTES_COLUMN, DataType.BYTES)
      .addMultiValueDimension(MV_STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(GROUP_BY_INT_COLUMN, DataType.INT)
      .addMultiValueDimension(GROUP_BY_MV_INT_COLUMN, DataType.INT)
      .addSingleValueDimension(GROUP_BY_INT_COLUMN2, DataType.INT)
      .addSingleValueDimension(BIG_DECIMAL_COLUMN, DataType.BIG_DECIMAL)
      .addSingleValueDimension(TIMESTAMP_COLUMN, DataType.TIMESTAMP)
      .addSingleValueDimension(BOOLEAN_COLUMN, DataType.BOOLEAN)
      .addMultiValueDimension(MV_DOUBLE_COLUMN, DataType.DOUBLE)
      .build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
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
    String[] stringSVVals = new String[]{"a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a11", "a22"};
    int j = 1;
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, i);
      record.putValue(LONG_COLUMN, (long) i - NUM_RECORDS / 2);
      record.putValue(FLOAT_COLUMN, (float) i * 0.5);
      record.putValue(DOUBLE_COLUMN, (double) i);
      record.putValue(MV_INT_COLUMN, Arrays.asList(i, i + 1, i + 2));
      record.putValue(MV_BYTES_COLUMN, Arrays.asList(String.valueOf(i).getBytes(), String.valueOf(i + 1).getBytes(),
          String.valueOf(i + 2).getBytes()));
      record.putValue(MV_STRING_COLUMN, Arrays.asList("a" + i, "a" + i + 1, "a" + i + 2));
      if (i < 20) {
        record.putValue(STRING_COLUMN, stringSVVals[i % stringSVVals.length]);
      } else {
        record.putValue(STRING_COLUMN, "a33");
      }
      record.putValue(GROUP_BY_INT_COLUMN, i % 5);
      record.putValue(GROUP_BY_MV_INT_COLUMN, Arrays.asList(i % 10, (i + 1) % 10));
      if (i == j) {
        j *= 2;
      }
      record.putValue(GROUP_BY_INT_COLUMN2, j);
      record.putValue(BIG_DECIMAL_COLUMN, new BigDecimal(-i * i + 1200 * i));
      record.putValue(TIMESTAMP_COLUMN, 1683138373879L - i);
      record.putValue(BOOLEAN_COLUMN, i % 2);
      record.putValue(MV_DOUBLE_COLUMN, Arrays.asList((double) i, (double) i * i, (double) i * i * i));
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

    QueryRewriterFactory.init(String.join(",", QueryRewriterFactory.DEFAULT_QUERY_REWRITERS_CLASS_NAMES)
        + ",org.apache.pinot.sql.parsers.rewriter.ArgMinMaxRewriter");
    ResultRewriterFactory
        .init("org.apache.pinot.core.query.utils.rewriter.ParentAggregationResultRewriter");
  }

  @Test
  public void testAggregationInterSegment() {
    // Simple inter segment aggregation test
    String query = "SELECT arg_max(intColumn, longColumn) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();

    assertEquals(rows.get(0)[0], 999L);
    assertEquals(rows.get(1)[0], 999L);
    assertEquals(rows.size(), 2);

    // Inter segment data type test
    query = "SELECT arg_max(intColumn, longColumn), arg_max(intColumn, floatColumn), "
        + "arg_max(intColumn, doubleColumn), arg_min(intColumn, mvIntColumn), "
        + "arg_min(intColumn, mvStringColumn), arg_min(intColumn, intColumn), "
        + "arg_max(bigDecimalColumn, bigDecimalColumn), arg_max(bigDecimalColumn, doubleColumn),"
        + "arg_min(timestampColumn, timestampColumn), arg_max(bigDecimalColumn, mvDoubleColumn)"
        + " FROM testTable";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(resultTable.getDataSchema().getColumnName(0), "argmax(intColumn,longColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(1), "argmax(intColumn,floatColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(2), "argmax(intColumn,doubleColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(3), "argmin(intColumn,mvIntColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(4), "argmin(intColumn,mvStringColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(5), "argmin(intColumn,intColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(6), "argmax(bigDecimalColumn,bigDecimalColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(7), "argmax(bigDecimalColumn,doubleColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(8), "argmin(timestampColumn,timestampColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(9), "argmax(bigDecimalColumn,mvDoubleColumn)");

    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0)[0], 999L);
    assertEquals(rows.get(1)[0], 999L);
    assertEquals(rows.get(0)[1], 999.5F);
    assertEquals(rows.get(1)[1], 999.5F);
    assertEquals(rows.get(0)[2], 1999D);
    assertEquals(rows.get(1)[2], 1999D);
    assertEquals(rows.get(0)[3], new Integer[]{0, 1, 2});
    assertEquals(rows.get(1)[3], new Integer[]{0, 1, 2});
    assertEquals(rows.get(0)[4], new String[]{"a0", "a01", "a02"});
    assertEquals(rows.get(1)[4], new String[]{"a0", "a01", "a02"});
    assertEquals(rows.get(0)[5], 0);
    assertEquals(rows.get(1)[5], 0);
    assertEquals(rows.get(0)[6], "360000");
    assertEquals(rows.get(1)[6], "360000");
    assertEquals(rows.get(0)[7], 600D);
    assertEquals(rows.get(1)[7], 600D);
    assertEquals(rows.get(0)[8], 1683138373879L - 1999L);
    assertEquals(rows.get(1)[8], 1683138373879L - 1999L);
    assertEquals(rows.get(0)[9], new Double[]{600D, 600D * 600D, 600D * 600D * 600D});
    assertEquals(rows.get(1)[9], new Double[]{600D, 600D * 600D, 600D * 600D * 600D});

    // Inter segment data type test for boolean column
    query = "SELECT arg_max(booleanColumn, booleanColumn) FROM testTable";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(rows.size(), 2000);
    for (int i = 0; i < 2000; i++) {
      assertEquals(rows.get(i)[0], 1);
    }

    // Inter segment mix aggregation function with different result length
    // Inter segment string column comparison test, with dedupe
    query = "SELECT sum(intColumn), argmin(stringColumn, doubleColumn), argmin(stringColumn, stringColumn), "
        + "argmin(stringColumn, doubleColumn, doubleColumn) FROM testTable";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(rows.size(), 4);

    assertEquals(rows.get(0)[0], 7996000D);
    assertEquals(rows.get(0)[1], 8D);
    assertEquals(rows.get(0)[2], "a11");
    assertEquals(rows.get(0)[3], 8D);

    assertNull(rows.get(1)[0]);
    assertEquals(rows.get(1)[1], 18D);
    assertEquals(rows.get(1)[2], "a11");
    assertEquals(rows.get(1)[3], 8D);

    assertNull(rows.get(2)[0]);
    assertEquals(rows.get(2)[1], 8D);
    assertEquals(rows.get(2)[2], "a11");
    assertNull(rows.get(2)[3]);

    assertNull(rows.get(3)[0]);
    assertEquals(rows.get(3)[1], 18D);
    assertEquals(rows.get(3)[2], "a11");
    assertNull(rows.get(3)[3]);

    // Inter segment mix aggregation function with CASE statement
    query = "SELECT argmin(CASE WHEN stringColumn = 'a33' THEN 'b' WHEN stringColumn = 'a22' THEN 'a' ELSE 'c' END"
        + ", stringColumn), argmin(CASE WHEN stringColumn = 'a33' THEN 'b' WHEN stringColumn = 'a22' THEN 'a' "
        + "ELSE 'c' END, CASE WHEN stringColumn = 'a33' THEN 'b' WHEN stringColumn = 'a22' THEN 'a' ELSE 'c' END) "
        + "FROM testTable";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(rows.size(), 4);

    assertEquals(rows.get(0)[0], "a22");
    assertEquals(rows.get(0)[1], "a");
    assertEquals(rows.get(1)[0], "a22");
    assertEquals(rows.get(1)[1], "a");

    //   TODO: The following query throws an exception, requires support for multi-value bytes column
    //   TODO: serialization in DataBlock
    query = "SELECT arg_min(intColumn, mvBytesColumn) FROM testTable";

    try {
      brokerResponse = getBrokerResponse(query);
      fail("remove this test case, now mvBytesColumn works correctly in serialization");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage()
          .contains("java.lang.IllegalArgumentException: Unsupported type of value: byte[][]"));
    }
  }

  @Test
  public void testAggregationDedupe() {
    // Inter segment dedupe test1 without dedupe
    String query = "SELECT  "
        + "argmin(booleanColumn, bigDecimalColumn, intColumn) FROM testTable WHERE doubleColumn <= 1200";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();

    assertEquals(rows.size(), 4);

    assertEquals(rows.get(0)[0], 0);
    assertEquals(rows.get(1)[0], 1200);
    assertEquals(rows.get(2)[0], 0);
    assertEquals(rows.get(3)[0], 1200);
    // test1, with dedupe
    query = "SELECT  "
        + "argmin(booleanColumn, bigDecimalColumn, doubleColumn, intColumn) FROM testTable WHERE doubleColumn <= 1200";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(rows.size(), 2);

    assertEquals(rows.get(0)[0], 0);
    assertEquals(rows.get(1)[0], 0);
  }

  @Test
  public void testEmptyAggregation() {
    // Inter segment mix aggregation with no documents after filtering
    String query =
        "SELECT arg_max(intColumn, longColumn), argmin(CASE WHEN stringColumn = 'a33' THEN 'b' "
            + "WHEN stringColumn = 'a22' THEN 'a' ELSE 'c' END"
            + ", stringColumn) FROM testTable where intColumn > 10000";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();
    assertNull(rows.get(0)[0]);
    assertNull(rows.get(0)[1]);
    assertEquals(resultTable.getDataSchema().getColumnName(0), "argmax(intColumn,longColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(1),
        "argmin(case(equals(stringColumn,'a33'),equals(stringColumn,'a22'),'b','a','c'),stringColumn)");
  }

  @Test
  public void testGroupByInterSegment() {
    // Simple inter segment group by
    String query = "SELECT groupByIntColumn, arg_max(intColumn, longColumn) FROM testTable GROUP BY groupByIntColumn";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();

    assertEquals(rows.size(), 10);

    assertEquals(rows.get(0)[0], 1);
    assertEquals(rows.get(0)[1], 996L);

    assertNull(rows.get(1)[0]);
    assertEquals(rows.get(1)[1], 996L);

    assertNull(rows.get(9)[0]);
    assertEquals(rows.get(9)[1], 995L);

    assertEquals(rows.get(6)[0], 4);
    assertEquals(rows.get(6)[1], 999L);

    // Simple inter segment group by with limit
    query =
        "SELECT groupByIntColumn2, arg_max(longColumn, doubleColumn) FROM testTable GROUP BY groupByIntColumn2 ORDER "
            + "BY groupByIntColumn2 LIMIT 15";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(rows.size(), 24);

    assertEquals(rows.get(0)[0], 1);
    assertEquals(rows.get(0)[1], 0D);

    assertNull(rows.get(1)[0]);
    assertEquals(rows.get(1)[1], 0D);

    assertEquals(rows.get(22)[0], 2048);
    assertEquals(rows.get(22)[1], 1999D);

    assertNull(rows.get(23)[0]);
    assertEquals(rows.get(23)[1], 1999D);

    // MV inter segment group by
    query = "SELECT groupByMVIntColumn, arg_min(intColumn, doubleColumn) FROM testTable GROUP BY groupByMVIntColumn";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(rows.size(), 20);

    assertEquals(rows.get(0)[0], 1);
    assertEquals(rows.get(0)[1], 0D);

    assertNull(rows.get(1)[0]);
    assertEquals(rows.get(1)[1], 0D);

    assertEquals(rows.get(2)[0], 2);
    assertEquals(rows.get(2)[1], 1D);

    assertNull(rows.get(3)[0]);
    assertEquals(rows.get(3)[1], 1D);

    assertEquals(rows.get(16)[0], 9);
    assertEquals(rows.get(16)[1], 8D);

    assertNull(rows.get(17)[0]);
    assertEquals(rows.get(17)[1], 8D);

    assertEquals(rows.get(18)[0], 0);
    assertEquals(rows.get(18)[1], 0D);

    assertNull(rows.get(19)[0]);
    assertEquals(rows.get(19)[1], 0D);
  }

  @Test
  public void testEmptyGroupByInterSegment() {
    // Simple inter segment group by with no documents after filtering
    String query = "SELECT groupByIntColumn, arg_max(intColumn, longColumn) FROM testTable "
        + " where intColumn > 10000 GROUP BY groupByIntColumn";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();

    assertEquals(rows.size(), 0);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
