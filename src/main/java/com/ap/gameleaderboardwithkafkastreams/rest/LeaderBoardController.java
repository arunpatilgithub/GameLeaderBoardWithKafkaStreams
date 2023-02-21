package com.ap.gameleaderboardwithkafkastreams.rest;

import com.ap.gameleaderboardwithkafkastreams.HighScores;
import com.ap.gameleaderboardwithkafkastreams.model.join.Enriched;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/leaderboard")
public class LeaderBoardController {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    private static final Logger log =
            LoggerFactory.getLogger(LeaderBoardController.class);

    public LeaderBoardController(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    @GetMapping
    public Map<String, List<Enriched>> getLeaderBoard() {

        Map<String, List<Enriched>> leaderboard = new HashMap<>();

        KeyValueIterator<String, HighScores> range = getStore().all();
        while (range.hasNext()) {
            KeyValue<String, HighScores> next = range.next();
            String game = next.key;
            HighScores highScores = next.value;
            leaderboard.put(game, highScores.toList());
        }

        return leaderboard;
    }

    ReadOnlyKeyValueStore<String, HighScores> getStore() {
        return streamsBuilderFactoryBean.getKafkaStreams().store(
                StoreQueryParameters.fromNameAndType(
                        "leader-boards",
                        QueryableStoreTypes.keyValueStore()));
    }

}
