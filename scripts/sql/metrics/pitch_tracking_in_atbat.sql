SELECT *
FROM dbo.play_by_play t1
LEFT JOIN dbo.pitches t2
    ON t1.game_pk = t2.game_pk
    AND t1.pitcher_id = t2.pitcher_id
    AND t1.batter_id = t2.batter_id
    AND t1.at_bat_index = t2.at_bat_index
