package com.streams.kafka;

import com.mitchseymour.kafka.serialization.avro.AvroSerdes;
import com.streams.kafka.model.FilterRes;
import com.streams.kafka.topic.Tweet;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class CryptoTopology {

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<byte[], Tweet> stream =
                streamsBuilder.stream("tweets", Consumed.with(Serdes.ByteArray(), Tweet.serdes()));

        KStream<byte[], Tweet> filtered = stream.filterNot((key, tweet) -> tweet.isRetweet());

        KStream<byte[], FilterRes> res = filtered.flatMapValues(
                (tweet) -> {
                    List<FilterRes> filterResList = new ArrayList<>();
                    if("pass".equals(tweet.getText())) {
                        FilterRes filterRes = new FilterRes();
                        filterRes.setRetweet(false);
                        filterRes.setText("success");
                        filterResList.add(filterRes);
                    }
                    return filterResList;
                }
        );
        res.to(
                "result",
                Produced.with(
                        Serdes.ByteArray(),
                       AvroSerdes.get(FilterRes.class)));
    }
}
