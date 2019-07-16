package org.apache.pinot.tools.tuner.meta.manager;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math.fraction.BigFraction;
import org.apache.pinot.common.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JsonFileMetaManagerImpl implements MetaManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonFileMetaManagerImpl.class);

  /*Meta data type:
   COL_META: Aggregated (sum and weighted sum of metadata)
   SEGMENT_META: Individually stored metadata of each segment
   */
  public static final Boolean IGNORE_EXISTING_INDEX=false;
  public static final Boolean USE_EXISTING_INDEX=true;

  private static final String COL_META = "col_meta";
  private static final String SEGMENT_META = "segment_meta";
  private static final String TYPE_REALTIME="_REALTIME";
  private static final String TYPE_OFFLINE="_OFFLINE";
  private static final String TYPE_HYBRID="_HYBRID";

  private String _path;
  private Boolean _use_existing_index;
  private HashMap<String, HashSet<String>> _additional_masking_cols;

  private JsonNode _aggregatedMap = null;
  private JsonNode _segmentMap = null;

  private JsonFileMetaManagerImpl(Builder builder) {
    _path = builder._path;
    _use_existing_index = builder._use_existing_index;
    _additional_masking_cols = builder._additional_masking_cols;
  }

  public static final class Builder {
    private String _path;
    private Boolean _use_existing_index = USE_EXISTING_INDEX;
    private HashMap<String, HashSet<String>> _additional_masking_cols=new HashMap<>();

    public Builder() {
    }

    @Nonnull
    public Builder _path(@Nonnull String val) {
      _path = val;
      return this;
    }

    @Nonnull
    public Builder _use_existing_index(@Nonnull Boolean val) {
      _use_existing_index = val;
      return this;
    }

    @Nonnull
    public Builder _additional_masking_cols(@Nonnull HashMap<String, HashSet<String>> val) {
      _additional_masking_cols = val;
      return this;
    }

    @Nonnull
    public JsonFileMetaManagerImpl build() {
      return new JsonFileMetaManagerImpl(this).cache();
    }
  }

  public JsonFileMetaManagerImpl cache() {
    File file = new File(this._path);
    String metaBytes = "";
    try {
      metaBytes = FileUtils.readFileToString(file);
    } catch (IOException e) {
      LOGGER.error(e.toString());
      System.exit(1);
    }
    try {
      JsonNode jnode = JsonUtils.stringToJsonNode(metaBytes);
      _aggregatedMap = jnode.get(COL_META);
      _segmentMap = jnode.get(SEGMENT_META);
    } catch (IOException e) {
      LOGGER.error("Can not parse Json file!");
      System.exit(1);
    }
    return this;
  }

  public BigFraction getAverageCardinality(String tableNameWithType, String columnName) {
    if(tableNameWithType.endsWith(TYPE_OFFLINE) || tableNameWithType.endsWith(TYPE_REALTIME) || tableNameWithType.endsWith(TYPE_HYBRID)){
      tableNameWithType=tableNameWithType.replace(TYPE_OFFLINE,"");
      tableNameWithType=tableNameWithType.replace(TYPE_REALTIME,"");
      tableNameWithType=tableNameWithType.replace(TYPE_HYBRID,"");
    }
    LOGGER.debug("Getting card from: {} {}",tableNameWithType,columnName);
    if(_use_existing_index){
      if (_additional_masking_cols.getOrDefault(tableNameWithType, new HashSet<>()).contains(columnName)){
        return BigFraction.ONE;
      }
      String _numHasInv=getColField(tableNameWithType,columnName,NUM_SEGMENTS_HAS_INVERTED_INDEX);
      if (_numHasInv==null || Integer.parseInt(_numHasInv)>0){
        return BigFraction.ONE;
      }
    }

    String nSortedNuemrator=getColField(tableNameWithType, columnName, NUM_SEGMENTS_SORTED);
    String nSortedDenominator=getColField(tableNameWithType, columnName, NUM_SEGMENTS_COUNT);
    String cardNumerator = getColField(tableNameWithType, columnName, WEIGHTED_SUM_CARDINALITY);
    String cardDenominator = getColField(tableNameWithType, columnName, SUM_DOCS);

    LOGGER.debug("Cardinality table:{} column:{} card: {}/{}, sort: {}/{}",
        tableNameWithType, columnName, cardNumerator,
        cardDenominator, nSortedNuemrator, nSortedDenominator
    );

    if (cardNumerator == null || cardDenominator == null) {
      LOGGER.error("{} {}'s cardinality does not exist!", tableNameWithType, columnName);
      return BigFraction.ONE;
    }

    BigFraction sorted_ratio;
    if (nSortedNuemrator == null || nSortedDenominator==null){
      LOGGER.error("{} {}'s sort info does not exist!", tableNameWithType, columnName);
      sorted_ratio=BigFraction.ONE;
    }
    else if(nSortedNuemrator.equals(nSortedDenominator)){
      return BigFraction.ONE;
    }
    else{
      sorted_ratio = new BigFraction(new BigInteger(nSortedNuemrator), new BigInteger(nSortedDenominator));
    }

    sorted_ratio=BigFraction.ONE.subtract(sorted_ratio);
    BigFraction averageCard= new BigFraction(new BigInteger(cardNumerator), new BigInteger(cardDenominator));
    BigFraction ret=averageCard.multiply(sorted_ratio);

//    LOGGER.debug("Cardinality: table:{} column:{} card: {}/{}, sort: {}/{}, final {}",
//        tableNameWithType, columnName, cardNumerator,
//        cardDenominator, nSortedNuemrator, nSortedDenominator, ret
//        );

    return ret;
  }

  public String getSegmentField(String tableNameWithType, String columnName, String segmentName, String fieldName) {
    JsonNode ret = _aggregatedMap.get(tableNameWithType).get(columnName).get(segmentName).get(fieldName);
    if (ret == null) {
      LOGGER.error("tableNameWithType:{} columnName:{} segmentName:{} fieldName:{} Does not exist!", tableNameWithType, columnName, segmentName, fieldName);
      return null;
    }
    return ret.asText();
  }

  public String getColField(String tableNameWithType, String columnName, String fieldName) {
    JsonNode ret=null;
    try {
      ret = _aggregatedMap.get(tableNameWithType).get(columnName).get(fieldName);
    }
    catch (NullPointerException e){
      LOGGER.debug("tableNameWithType:{} columnName:{} Does not exist!", tableNameWithType, columnName);
      return null;
    }
    if (ret == null) {
      LOGGER.debug("tableNameWithType:{} columnName:{} fieldName:{} Does not exist!", tableNameWithType, columnName, fieldName);
      return null;
    }
    return ret.asText();
  }
}
