package com.atguigu.spark.utils

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.mutable.ListBuffer


/**
 * Elasticsearch工具类
 */
object MyESUtils {

  var esClient: RestHighLevelClient = build()

  /**
   * 批量数据幂等写入:通过指定id实现幂等
   */
  def bulkSaveIdempotent(sourceList: List[(String, AnyRef)], indexName: String): Unit = {
    if (sourceList != null && sourceList.nonEmpty) {
      // BulkRequest实际上就是多个单条IndexRequest的组合
      val bulkRequest = new BulkRequest()

      for ((docId, sourceObj) <- sourceList) {
        val indexRequest = new IndexRequest(indexName)
        val movieJsonStr: String =
          JSON.toJSONString(sourceObj, new SerializeConfig(true))
        indexRequest.source(movieJsonStr, XContentType.JSON)
        indexRequest.id(docId)
        bulkRequest.add(indexRequest)
      }

      esClient.bulk(bulkRequest, RequestOptions.DEFAULT)
    }

  }

  /**
   * 批量数据写入
   */
  def bulkSave(sourceList: List[AnyRef], indexName: String): Unit = {
    val bulkRequest = new BulkRequest()

    for (source <- sourceList) {
      val indexRequest = new IndexRequest(indexName)
      val movieJsonStr: String =
        JSON.toJSONString(source, new SerializeConfig(true))
      indexRequest.source(movieJsonStr, XContentType.JSON)
      bulkRequest.add(indexRequest)
    }

    esClient.bulk(bulkRequest, RequestOptions.DEFAULT)
  }

  /**
   * 单条数据幂等写入：通过指定id实现幂等
   */
  def saveIdempotent(source: (String, AnyRef), indexName: String): Unit = {
    val indexRequest = new IndexRequest()
    indexRequest.index(indexName)
    val movieJsonStr: String =
      JSON.toJSONString(source._2, new SerializeConfig(true))
    indexRequest.source(movieJsonStr, XContentType.JSON)
    indexRequest.id(source._1)
    esClient.index(indexRequest, RequestOptions.DEFAULT)
  }

  /**
   * 单条数据写入
   */
  def save(source: AnyRef, indexName: String): Unit = {
    val indexRequest = new IndexRequest()
    indexRequest.index(indexName)
    val movieJsonStr: String =
      JSON.toJSONString(source, new SerializeConfig(true))
    indexRequest.source(movieJsonStr, XContentType.JSON)
    esClient.index(indexRequest, RequestOptions.DEFAULT)
  }

  /**
   * 销毁ES客户端
   */
  def destory(): Unit = {
    esClient.close()
    esClient = null
  }

  /**
   * 创建ES客户端
   */
  def build(): RestHighLevelClient = {
    val builder: RestClientBuilder =
      RestClient.builder(new HttpHost("hadoop102", 9200))
    val esClient = new RestHighLevelClient(builder)
    esClient
  }

  /**
   * 获取Elasticsearch客户端
   */
  def getClient(): RestHighLevelClient = {
    esClient
  }

  /**
   * 查询指定的字段
   */
  def searchField(indexName: String, fieldName: String): List[String] = {
    // 判断索引是否存在
    val getIndexRequest: GetIndexRequest =
      new GetIndexRequest(indexName)
    val isExists: Boolean =
      esClient.indices()
        .exists(getIndexRequest, RequestOptions.DEFAULT)
    if (!isExists) {
      return null
    }

    // 从ES中提取指定的字段
    val mids: ListBuffer[String] = ListBuffer[String]()
    val searchRequest: SearchRequest = new SearchRequest(indexName)
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.fetchSource(fieldName, null).size(100000)
    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse =
      esClient.search(searchRequest, RequestOptions.DEFAULT)
    val hits: Array[SearchHit] = searchResponse.getHits.getHits

    for (hit <- hits) {
      val sourceMap: java.util.Map[String, AnyRef] = hit.getSourceAsMap
      val mid: String = sourceMap.get(fieldName).toString
      mids.append(mid)
    }

    mids.toList
  }

}
