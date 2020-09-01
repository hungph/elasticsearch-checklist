package org.example;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedAutoDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;

public class Application {
    public static void main(String arg[]) throws IOException {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "brique0901"));

        RestClientBuilder builder = RestClient.builder(new HttpHost("ba-es1.brique.kr", 9200))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient esClient = new RestHighLevelClient(builder);

        SearchRequest esSearchRequest = new SearchRequest("metricbeat-7.8.0");
        SearchSourceBuilder esSourceBuilder = new SearchSourceBuilder();


        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        BoolQueryBuilder boolModuleQueryBuilder = QueryBuilders.boolQuery();
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("event.module", "redis");
        boolModuleQueryBuilder.filter(matchQueryBuilder);

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("@timestamp");
        rangeQueryBuilder.format("strict_date_optional_time");
        rangeQueryBuilder.from("2020-08-02T06:24:44.881Z", true);
        rangeQueryBuilder.to("2020-09-01T06:24:44.881Z", true);

        boolQueryBuilder.filter(QueryBuilders.matchAllQuery());
        boolQueryBuilder.filter(boolModuleQueryBuilder);
        boolQueryBuilder.filter(rangeQueryBuilder);

        esSourceBuilder.query(boolQueryBuilder);
        esSourceBuilder.size(0);

        AvgAggregationBuilder avgAggregationBuilder = AggregationBuilders.avg("1");
        avgAggregationBuilder.field("redis.keyspace.keys");

        BucketOrder orderTerms = BucketOrder.aggregation("1", false);
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("3");
        termsAggregationBuilder.field("redis.keyspace.id");
        termsAggregationBuilder.order(orderTerms);
        termsAggregationBuilder.size(5);
        termsAggregationBuilder.subAggregation(avgAggregationBuilder);

        DateHistogramAggregationBuilder dateHistogramAggBuilder = AggregationBuilders.dateHistogram("2");
        dateHistogramAggBuilder.field("@timestamp");
        dateHistogramAggBuilder.fixedInterval(DateHistogramInterval.hours(12));
        dateHistogramAggBuilder.timeZone(ZoneId.of("Asia/Seoul"));
        dateHistogramAggBuilder.minDocCount(1L);
        dateHistogramAggBuilder.subAggregation(termsAggregationBuilder);

        esSourceBuilder.aggregation(dateHistogramAggBuilder);
        esSearchRequest.source(esSourceBuilder);

        SearchResponse esSearchResponse = esClient.search(esSearchRequest, RequestOptions.DEFAULT);

        for (Map.Entry<String, Aggregation> item : esSearchResponse.getAggregations().getAsMap().entrySet()) {
            System.out.println("Agg Name: " + item.getKey());
            ParsedDateHistogram aggReturned = (ParsedDateHistogram)item.getValue(); //Have to debug to know which class type of this item

            if (aggReturned.getBuckets() != null && aggReturned.getBuckets().size() > 0) {
                for (Bucket curBucket : aggReturned.getBuckets()) {
                    ParsedDateHistogram.ParsedBucket parsedBucket = (ParsedDateHistogram.ParsedBucket)curBucket; //Have to debug to know which class type of this item
                    System.out.println("->-> Sub Agg Name: " + parsedBucket.getKey());

                    for (Map.Entry<String, Aggregation> subItem : parsedBucket.getAggregations().getAsMap().entrySet()) {
                        System.out.println("->->->-> Sub Sub Name: " + subItem.getKey());
                        ///... Debug and debug
                    }
                }
            }
        }

        esClient.close();
    }
}
