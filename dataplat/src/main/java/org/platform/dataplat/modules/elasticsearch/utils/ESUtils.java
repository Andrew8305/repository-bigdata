package org.platform.dataplat.modules.elasticsearch.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.platform.dataplat.modules.elasticsearch.client.ESClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESUtils {
	
	private static final Logger LOG = LoggerFactory.getLogger(ESUtils.class);
	
	public static void createIndex(String index, String shardsNum, String replicasNum) {
		Client client = ESClient.getInstance().getClient();
		try {
			XContentBuilder builder = XContentFactory
			            .jsonBuilder()
			            .startObject()
		                    .field("number_of_shards", shardsNum)
		                    .field("number_of_replicas", replicasNum)
				        .endObject();
			CreateIndexResponse response = client.admin().indices()
					.prepareCreate(index).setSettings(builder).execute().actionGet();
			System.out.println(response.isAcknowledged());
		} catch (Exception e) {
			LOG.error("create index error.", e);
		} finally {
			ESClient.getInstance().closeClient(client);
		}
	}
	
	public static void deleteIndex(String index) {
		Client client = ESClient.getInstance().getClient();
		try {
			DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
			ActionFuture<DeleteIndexResponse> response = 
					client.admin().indices().delete(deleteIndexRequest);
			System.out.println(response.get().isAcknowledged());
		} catch (Exception e) {
			LOG.error("delete index error.", e);
		} finally {
			ESClient.getInstance().closeClient(client);
		}
	}
	
	public static void createIndexType(String index, String type, XContentBuilder builder) {
		Client client = ESClient.getInstance().getClient();
		PutMappingRequest mapping = Requests.putMappingRequest(index).type(type).source(builder);
		PutMappingResponse response = client.admin().indices().putMapping(mapping).actionGet();
		System.out.println(response.isAcknowledged());
		ESClient.getInstance().closeClient(client);
	}
	
	public static void createIndexType(String index, String type, String fileName) {
		Client client = ESClient.getInstance().getClient114();
		PutMappingRequest mapping = Requests.putMappingRequest(index)
				.type(type).source(readSource(fileName));
		PutMappingResponse response = client.admin().indices().putMapping(mapping).actionGet();
		System.out.println(response.isAcknowledged());
		ESClient.getInstance().closeClient(client);
	}
	
	private static String readSource(String fileName) {
		InputStream in = null;
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
		try {
			in = ESUtils.class.getClassLoader().getResourceAsStream("mapping/" + fileName);
			br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while (null != (line = br.readLine())) {
				sb.append(line);
			}
		} catch (Exception e) {
			LOG.error("read source error.", e);
		} finally {
			try {
				if (null != br) {
					br.close();
				}
				if (null != in) {
					in.close();
				}
			} catch (Exception e) {
				LOG.error("close reader or stream error.", e);
			}
		}
		return sb.toString();
	}
	
	public static void createIndexTypeData(String index, String type, String id, String source) {
		Client client = ESClient.getInstance().getClient();
		IndexRequestBuilder irb = client.prepareIndex(index, type, id).setSource(source);
		IndexResponse response = irb.execute().actionGet();
		System.out.println(response.isCreated());
		ESClient.getInstance().closeClient(client);
	}
	
	public static void createIndexTypeData(String index, String type, String id, Map<String, Object> source) {
		Client client = ESClient.getInstance().getClient();
		IndexRequestBuilder irb = client.prepareIndex(index, type, id).setSource(source);
		IndexResponse response = irb.execute().actionGet();
		System.out.println(response.isCreated());
		ESClient.getInstance().closeClient(client);
	}
	
	public static void createIndexTypeDatas(String index, String type, List<Map<String, Object>> sources) {
		Client client = ESClient.getInstance().getClient();
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		IndexRequestBuilder irb = null;
		Map<String, Object> source = null;
		for (int i = 0, len = sources.size(); i < len; i++) {
			source = sources.get(i);
			if (source.containsKey("_id")) {
				irb = client.prepareIndex(index, type, (String) source.get("_id"));
				source.remove("_id");
				irb.setSource(source);
			} else {
				irb = client.prepareIndex(index, type).setSource(source);
			}
			bulkRequestBuilder.add(irb);
		}
		BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			LOG.info(bulkResponse.buildFailureMessage());
		}
		ESClient.getInstance().closeClient(client);
	}
	
	public static void deleteIndexType(String index, String type) {
		Client client = ESClient.getInstance().getClient();
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
		searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
		searchRequestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		searchRequestBuilder.setScroll(TimeValue.timeValueMinutes(3));
		searchRequestBuilder.setSize(1000).setExplain(false);
		SearchResponse response = searchRequestBuilder.execute().actionGet();
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		while (true) {
			SearchHit[] hitArray = response.getHits().getHits();
			SearchHit hit = null;
			for (int i = 0, len = hitArray.length; i < len; i++) {
				hit = hitArray[i];
				DeleteRequestBuilder request = client.prepareDelete(index, type, hit.getId());
				bulkRequest.add(request);
			}
			bulkRequest.execute().actionGet();
			if (hitArray.length == 0) break;
			response = client.prepareSearchScroll(response.getScrollId())
							.setScroll(new TimeValue(60000)).execute().actionGet();
		}
		ESClient.getInstance().closeClient(client);
	}
	
	public static void deleteIndexTypeDataById(String index, String type, String id) {
		Client client = ESClient.getInstance().getClient();
		DeleteResponse response = client.prepareDelete().setIndex(index)
				.setType(type).setId(id).execute().actionGet();
		System.out.println(response.isFound());
		ESClient.getInstance().closeClient(client);
	}
	
	public static void deleteIndexTypeDatasByQuery(String index, String type, String name, String query) {
		Client client = ESClient.getInstance().getClient();
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
		searchRequestBuilder.setQuery(QueryBuilders.wildcardQuery(name, query));
		searchRequestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		searchRequestBuilder.setScroll(TimeValue.timeValueMinutes(3));
		searchRequestBuilder.setSize(1000).setExplain(false);
		SearchResponse response = searchRequestBuilder.execute().actionGet();
		System.out.println("total hits: " + response.getHits().getTotalHits());
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		while (true) {
			SearchHit[] hitArray = response.getHits().getHits();
			SearchHit hit = null;
			for (int i = 0, len = hitArray.length; i < len; i++) {
				hit = hitArray[i];
				System.out.println(hit.getSource());
				DeleteRequestBuilder request = client.prepareDelete(index, type, hit.getId());
				bulkRequest.add(request);
			}
			bulkRequest.execute().actionGet();
			if (hitArray.length == 0) break;
			response = client.prepareSearchScroll(response.getScrollId())
							.setScroll(new TimeValue(60000)).execute().actionGet();
		}
		ESClient.getInstance().closeClient(client);
	}
	
	public static void readIndexTypeDatasWithFrom(String query, int from, int size) {
//		readIndexTypeDatasWithFrom("102", query, from, size);
//		readIndexTypeDatasWithFrom("105", query, from, size);
		readIndexTypeDatasWithFrom("107", query, from, size);
	}
	
	public static void readIndexTypeDatasWithFrom(String cluster, String query, int from, int size) {
		Client client = null;
		String[] indices = null;
		String[] types = null;
		if ("102".equals(cluster)) {
			client = ESClient.getInstance().getClient21();
			indices = new String[]{"financial", "financial_new", "operator", "qq", "email", "trip", "work"};
			types = new String[]{"logistics", "business", "telecom", "qqdata", "mailbox", "airplane", 
					"hotel", "resume", "socialSecurity"};
		} else if ("105".equals(cluster)) {
			client = ESClient.getInstance().getClient105();
			indices = new String[]{"financial", "financial_new", "operator", "other", "trip", "work", "qq", "email"};
			types = new String[]{"logistics", "business", "finance", "hotel", "house", "car", "telecom", "contact", 
					"Internet", "qqdata", "mailbox", "qualification", "student", "cybercafe", "hospital", "resume", "socialSecurity"};
		} else if ("107".equals(cluster)) {
			client = ESClient.getInstance().getClient20();
			indices = new String[]{"financial", "financial_new", "operator", "other", "work", "qq", "email"};
			types = new String[]{"logistics", "business", "finance", "house", "car", "telecom", "contact", 
					"Internet", "qqdata", "mailbox", "qualification", "student", "cybercafe", "hospital"};
		}
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indices).setTypes(types);
//		searchRequestBuilder.setQuery(QueryBuilders.termQuery("_all", query));
		searchRequestBuilder.setQuery(QueryBuilders.wildcardQuery("name", query));
		searchRequestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		searchRequestBuilder.setExplain(false);
		searchRequestBuilder.setFrom(from).setSize(size);
		SearchResponse response = searchRequestBuilder.execute().actionGet();
		System.out.println("total hits: " + response.getHits().getTotalHits());
		SearchHit[] hitArray = response.getHits().getHits();
		for (int i = 0, len = hitArray.length; i < len; i++) {
			System.out.println(hitArray[i].getSource());
		}
	}
	
	public static void readIndexTypeDatasWithScroll(String query) {
		readIndexTypeDatasWithScroll("102", query);
		readIndexTypeDatasWithScroll("105", query);
		readIndexTypeDatasWithScroll("107", query);
	}
	
	public static void readIndexTypeDatasWithScroll(String cluster, String query) {
		Client client = null;
		String[] indices = null;
		String[] types = null;
		if ("102".equals(cluster)) {
			client = ESClient.getInstance().getClient21();
			indices = new String[]{"financial", "financial_new", "operator", "qq", "email", "trip", "work"};
			types = new String[]{"logistics", "business", "telecom", "qqdata", "mailbox", "airplane", 
					"hotel", "resume", "socialSecurity"};
		} else if ("105".equals(cluster)) {
			client = ESClient.getInstance().getClient105();
			indices = new String[]{"financial", "financial_new", "operator", "other", "trip", "work", "qq", "email"};
			types = new String[]{"logistics", "business", "finance", "hotel", "house", "car", "telecom", "contact", 
					"Internet", "qqdata", "mailbox", "qualification", "student", "cybercafe", "hospital", "resume", "socialSecurity"};
		} else if ("107".equals(cluster)) {
			client = ESClient.getInstance().getClient20();
			indices = new String[]{"financial", "financial_new", "operator", "other", "work", "qq", "email"};
			types = new String[]{"logistics", "business", "finance", "house", "car", "telecom", "contact", 
					"Internet", "qqdata", "mailbox", "qualification", "student", "cybercafe", "hospital"};
		}
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indices).setTypes(types);
//		searchRequestBuilder.setQuery(QueryBuilders.wildcardQuery("_all", query));
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		bqb.should(QueryBuilders.termQuery("companyPhone", query));
		searchRequestBuilder.setQuery(bqb);
		searchRequestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		searchRequestBuilder.setScroll(TimeValue.timeValueMinutes(3));
		searchRequestBuilder.setExplain(false);
		SearchResponse response = searchRequestBuilder.execute().actionGet();
		System.out.println("total hits: " + response.getHits().getTotalHits());
		while (true) {
			SearchHit[] hitArray = response.getHits().getHits();
			for (int i = 0, len = hitArray.length; i < len; i++) {
				System.out.println(hitArray[i].getSource());
			}
			if (hitArray.length == 0) break;
			response = client.prepareSearchScroll(response.getScrollId())
							.setScroll(new TimeValue(60000)).execute().actionGet();
		}
	}
	
	public static void readIndexTypeDatasWithFrom(String index, String type, String name, String query, int from, int size) {
		Client client = ESClient.getInstance().getClient();
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
//		searchRequestBuilder.setQuery(QueryBuilders.wildcardQuery(name, query));
//		QueryStringQueryBuilder qb = new QueryStringQueryBuilder(query).field(name);
		QueryStringQueryBuilder qb = new QueryStringQueryBuilder(query);
		searchRequestBuilder.setQuery(qb);
		searchRequestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		searchRequestBuilder.setScroll(TimeValue.timeValueMinutes(3));
		searchRequestBuilder.setFrom(from).setSize(size).setExplain(true);
		SearchResponse response = searchRequestBuilder.execute().actionGet();
		System.out.println("total hits: " + response.getHits().getTotalHits());
		SearchHit[] hitArray = response.getHits().getHits();
		SearchHit hit = null;
		Map<String, Object> source = null;
		for (int i = 0, len = hitArray.length; i < len; i++) {
			hit = hitArray[i];
			source = hit.getSource();
			System.out.println(source);
		}
	}
	
	public static String readIndexTypeDatasWithScroll(String index, String type, String name, String query, 
			String scrollId, int size) {
		Client client = ESClient.getInstance().getClient();
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
		searchRequestBuilder.setQuery(QueryBuilders.wildcardQuery(name, query));
		searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);
		searchRequestBuilder.setScroll(TimeValue.timeValueMinutes(3));
		searchRequestBuilder.setSize(size).setExplain(false);
		SearchResponse response = searchRequestBuilder.execute().actionGet();
		if (StringUtils.isNotBlank(scrollId)) {
			response = client.prepareSearchScroll(scrollId).setScroll(
					TimeValue.timeValueMinutes(3)).execute().actionGet();
		} 
		System.out.println("total hits: " + response.getHits().getTotalHits());
		SearchHit[] hitArray = response.getHits().getHits();
		SearchHit hit = null;
		Map<String, Object> source = null;
		for (int i = 0, len = hitArray.length; i < len; i++) {
			hit = hitArray[i];
			source = hit.getSource();
			System.out.println(source);
		}
		System.out.println("scroll id: " + response.getScrollId());
		return response.getScrollId();
	}
	
	public static void readIndexTypeDatasWithScroll(String index, String type, String name, String query) {
		Client client = ESClient.getInstance().getClient();
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
		searchRequestBuilder.setQuery(QueryBuilders.wildcardQuery(name, query));
		searchRequestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		searchRequestBuilder.setScroll(TimeValue.timeValueMinutes(3));
		searchRequestBuilder.setExplain(false);
		SearchResponse response = searchRequestBuilder.execute().actionGet();
		System.out.println("total hits: " + response.getHits().getTotalHits());
		while (true) {
			SearchHit[] hitArray = response.getHits().getHits();
			for (int i = 0, len = hitArray.length; i < len; i++) {
				System.out.println(hitArray[i].getSource());
			}
			if (hitArray.length == 0) break;
			response = client.prepareSearchScroll(response.getScrollId())
							.setScroll(new TimeValue(60000)).execute().actionGet();
		}
		ESClient.getInstance().closeClient(client);
	}
	
	public static void readIndexTypeDatasGroupInfo(String index, String type, String groupFieldName) {
		String groupFieldAgg = groupFieldName + "Agg";
		Client client = ESClient.getInstance().getClient();
		TermsBuilder termsBuilder = AggregationBuilders.terms(groupFieldAgg)
				.size(Integer.MAX_VALUE).field(groupFieldName);
		SearchResponse response = client.prepareSearch(index).setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery())
					.addAggregation(termsBuilder).execute().actionGet();
		Terms terms = response.getAggregations().get(groupFieldAgg);
		if (null != terms) {
			List<Bucket> buckets = terms.getBuckets();
			for (int i = 0, len = buckets.size(); i < len; i++) {
				Bucket bucket = buckets.get(i);
				System.out.println(bucket.getKey() + " - " + bucket.getDocCount());
			}
		}
	}
	
	public static void readIndexTypeDatasGroupInfo(String index, String type, String groupFieldName,
			String subGroupFieldName) {
		String groupFieldAgg = groupFieldName + "Agg";
		String subGroupFieldAgg = subGroupFieldName + "Agg";
		Client client = ESClient.getInstance().getClient();
		TermsBuilder subTermsBuilder = AggregationBuilders.terms(subGroupFieldAgg)
				.size(Integer.MAX_VALUE).field(subGroupFieldName);
		TermsBuilder termsBuilder = AggregationBuilders.terms(groupFieldAgg)
				.size(Integer.MAX_VALUE).field(groupFieldName).subAggregation(subTermsBuilder);
		SearchResponse response = client.prepareSearch(index).setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery())
					.addAggregation(termsBuilder).execute().actionGet();
		Terms terms = response.getAggregations().get(groupFieldAgg);
		if (null != terms) {
			List<Bucket> buckets = terms.getBuckets();
			for (int i = 0, len = buckets.size(); i < len; i++) {
				Bucket bucket = buckets.get(i);
				System.out.println(bucket.getKey() + " - " + bucket.getDocCount());
				Terms subTerms = bucket.getAggregations().get(subGroupFieldAgg);
				if (null != subTerms) {
					List<Bucket> subBuckets = subTerms.getBuckets();
					for (int j = 0, slen = subBuckets.size(); j < slen; j++) {
						Bucket subBucket = subBuckets.get(j);
						System.out.println(subBucket.getKey() + " - " + subBucket.getDocCount());
					}
				}
			}
		}
	}
	
	public static void analyzer(String index, String text) {
		Client client = ESClient.getInstance().getClient();
		AnalyzeRequestBuilder request = new AnalyzeRequestBuilder(client, AnalyzeAction.INSTANCE, index, text);
		// request.setAnalyzer("ik");
		request.setTokenizer("ik_smart");
		// Analyzer（分析器）、Tokenizer（分词器）
		List<AnalyzeToken> analyzeTokens = request.execute().actionGet().getTokens();
		for (int i = 0, len = analyzeTokens.size(); i < len; i++) {
			AnalyzeToken analyzeToken = analyzeTokens.get(i);
			System.out.println(analyzeToken.getTerm());
		}
	}
	
	public static void main(String[] args) {
//		List<Map<String, Object>> sources = new ArrayList<Map<String, Object>>();
//		Map<String, Object> source = new HashMap<String, Object>();
//		source.put("expressId", "8656257105");
//		source.put("begainTime", "2014-05-20 13:12:09");
//		source.put("linkName", "深圳市");
//		source.put("linkCall", "18620376327");
//		source.put("linkAddress", "河南省郑州市");
//		source.put("sendCompany", "TB孟星星");
//		source.put("name", "钱圣龙");
//		source.put("phone", "051086830332");
//		source.put("address", "江苏省无锡市江阴市暨阳路19号");
//		source.put("_id", "113");
//		sources.add(source);
//		createIndexTypeDatas("data", "logistics", sources);
//		createIndexTypeData("data", "logistics", "111", source);
//		String sourceJson = "{ \"expressId\": \"8656257105\", \"begainTime\": \"2014-05-20 13:12:09\", \"linkName\": \"深圳市\", \"linkCall\": "
//				+ "\"18620376327\", \"linkAddress\": \"河南省郑州市\", \"sendCompany\": \"TB孟星星\", \"name\": \"钱圣龙\", \"phone\": \"051086830332\","
//				+ " \"address\": \"江苏省无锡市江阴市暨阳路19号\", \"price\": 0, \"good\": \"物品\"}";
//		createIndexTypeData("data", "logistics", "112", sourceJson);
		readIndexTypeDatasWithScroll("何毅");
	}
	
}
