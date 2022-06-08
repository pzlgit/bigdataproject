package com.atguigu.spark.es

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.text.Text
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, QueryBuilders, RangeQueryBuilder}
import org.elasticsearch.index.reindex.UpdateByQueryRequest
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.bucket.terms.{ParsedStringTerms, Terms, TermsAggregationBuilder}
import org.elasticsearch.search.aggregations.metrics.{AvgAggregationBuilder, ParsedAvg}
import org.elasticsearch.search.aggregations.{AggregationBuilders, Aggregations, BucketOrder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.{HighlightBuilder, HighlightField}
import org.elasticsearch.search.sort.SortOrder

import java.util

/**
 * Elasticsearch客户端操作
 *
 * @author pangzl
 * @create 2022-06-06 18:56
 */
object ElasticsearchClient {

  def main(args: Array[String]): Unit = {
    println(esClient)
    //save(("1001", Movie("1001", "功夫")), "movie_index_api")
    //    bulkSave(
    //      List(
    //        Movie("1002","速度与激情"),
    //        Movie("1003","八佰"),
    //      ),
    //      "movie_index_api"
    //    )
    //updateByDocId(Movie("1001", "Java"), "movie_index_api")
    //updateByQuery("movieName", "java", "shangguigu", "movie_index_api")
    //deleteByDocId("1001", "movie_index_api")
    /// getByDocId("1002", "movie_index_api")
    //search()
    searchAgg()
    destory()
  }

  /**
   * 3）聚合查询 :查询每位演员参演的电影的平均分，倒叙排序
   */
  def searchAgg(): Unit = {
    val searchRequest = new SearchRequest("movie_index")
    val searchSourceBuilder = new SearchSourceBuilder()

    // 聚合分组
    val termsAggregationBuilder: TermsAggregationBuilder =
      AggregationBuilders.
        terms("groupByActor")
        .field("actorList.name.keyword")
        .size(100)
        .order(BucketOrder.aggregation("avg_score", false))

    // avg
    val avgAggregationBuilder: AvgAggregationBuilder =
      AggregationBuilders
        .avg("avg_score")
        .field("doubanScore")

    termsAggregationBuilder.subAggregation(avgAggregationBuilder)
    searchSourceBuilder.aggregation(termsAggregationBuilder)

    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse =
      esClient.search(searchRequest, RequestOptions.DEFAULT)

    // 处理结果
    val groupByActorTerms: ParsedStringTerms = searchResponse.
      getAggregations.get[ParsedStringTerms]("groupByActor")

    val buckets: util.List[_ <: Terms.Bucket] =
      groupByActorTerms.getBuckets

    import scala.collection.JavaConverters._
    for (bucket <- buckets.asScala) {
      val actorName: AnyRef = bucket.getKey
      val movieCount: Long = bucket.getDocCount
      val aggregations: Aggregations = bucket.getAggregations

      val parsedAvg: ParsedAvg =
        aggregations.get[ParsedAvg]("avg_score")
      val avgScore: Double = parsedAvg.getValue

      println(s"$actorName 共参演了 $movieCount 部电影， 平均评分为 $avgScore")
    }
  }


  /**
   * 6.2.5 查询数据  2）条件查询
   * 查询doubanScore>=5.0关键词搜索red sea
   * 关键词高亮显示
   * 显示第一页，每页20条
   * 按doubanScore从大到小排序
   */
  def search(): Unit = {
    val searchRequest = new SearchRequest()
    searchRequest.indices("movie_index")
    val searchSourceBuilder = new SearchSourceBuilder()
    // 过滤匹配
    val boolQueryBuilder = new BoolQueryBuilder()
    val rangeQueryBuilder: RangeQueryBuilder =
      new RangeQueryBuilder("doubanScore").gte("5.0")
    boolQueryBuilder.filter(rangeQueryBuilder)

    val matchQueryBuilder = new MatchQueryBuilder("name", "red sea")
    boolQueryBuilder.must(matchQueryBuilder)
    searchSourceBuilder.query(boolQueryBuilder)
    // 高亮
    val nameHighlightBuilder: HighlightBuilder = new HighlightBuilder().field("name")
    searchSourceBuilder.highlighter(nameHighlightBuilder)
    // 分页
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(20)
    // 排序
    searchSourceBuilder.sort("doubanScore", SortOrder.DESC)

    searchRequest.source(searchSourceBuilder)
    val response: SearchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT)

    // Response返回值处理
    val total: Long = response.getHits.getTotalHits.value
    println(s"共查询到 $total 条数据")
    // 数据明细
    val hits: Array[SearchHit] = response.getHits.getHits
    for (searchHit <- hits) {
      val sourceJson: String = searchHit.getSourceAsString
      // 高亮
      val fields: util.Map[String, HighlightField] =
        searchHit.getHighlightFields
      val field: HighlightField = fields.get("name")
      val fragments: Array[Text] = field.getFragments
      val highlightText: Text = fragments(0)
      println(sourceJson)
      println(highlightText)
    }

  }


  /**
   * 6.2.5 查询数据  1）DocId查询
   */
  def getByDocId(docId: String, indexName: String): Unit = {
    val request = new GetRequest(indexName, docId)
    val response: GetResponse = esClient.get(request, RequestOptions.DEFAULT)
    println(response.getId)
    println(response.getSource.get("movieName"))
  }

  /**
   * 6.2.4 删除数据
   */
  def deleteByDocId(docId: String, indexName: String): Unit = {
    val request = new DeleteRequest(indexName, docId)
    esClient.delete(request, RequestOptions.DEFAULT)
  }

  /**
   * 6.2.3 修改数据  1）根据docid更新字段
   */
  def updateByDocId(movie: Movie, indexName: String): Unit = {
    val request = new UpdateRequest(indexName, movie.id)
    request.doc(
      JSON.toJSONString(movie, new SerializeConfig(true)),
      XContentType.JSON
    )
    esClient.update(request, RequestOptions.DEFAULT)
  }

  /**
   * 2）根据查询条件修改:将指定字段中包含有srcValue的统一修改为newValue
   */
  def updateByQuery(fieldName: String, srcValue: String, newValue: String, indexName: String): Unit = {
    val request = new UpdateByQueryRequest(indexName)
    val matchQueryBuilder: MatchQueryBuilder =
      QueryBuilders.matchQuery(fieldName, srcValue)
    val map = new util.HashMap[String, Object]()
    map.put("newName", newValue)
    val script = new Script(
      ScriptType.INLINE,
      Script.DEFAULT_SCRIPT_LANG,
      "ctx._source['movieName']=params.newName",
      map
    )
    request.setQuery(matchQueryBuilder)
    request.setScript(script)
    esClient.updateByQuery(request, RequestOptions.DEFAULT)
  }

  /**
   * 4）批量数据写入(非幂等)(幂等)
   */
  def bulkSave(sourceList: List[Movie], indexName: String): Unit = {
    val bulkRequest = new BulkRequest(indexName)
    for (elem <- sourceList) {
      val request = new IndexRequest()
      // 幂等与非幂等的区别就是是否指定id
      request.id(elem.id)
      request.source(
        JSON.toJSONString(elem, new SerializeConfig(true)),
        XContentType.JSON
      )
      bulkRequest.add(request)
    }
    esClient.bulk(bulkRequest, RequestOptions.DEFAULT)
  }

  /**
   * 3）单条数据幂等写入
   */
  def save(source: (String, Movie), indexName: String): Unit = {
    val request = new IndexRequest(indexName)
    request.id(source._1)
    request.source(JSON.toJSONString(source._2, new SerializeConfig(true)), XContentType.JSON)
    esClient.index(request, RequestOptions.DEFAULT)
  }

  case class Movie(id: String, movieName: String)

  // 声明Elasticsearch客户端
  var esClient: RestHighLevelClient = build()

  /**
   * 创建Elasticsearch客户端
   */
  def build(): RestHighLevelClient = {
    val httpHost = new HttpHost("hadoop102", 9200)
    val builder: RestClientBuilder = RestClient.builder(httpHost)
    val esClient = new RestHighLevelClient(builder)
    esClient
  }

  /**
   * 销毁对象
   */
  def destory(): Unit = {
    esClient.close()
    esClient = null
  }

}