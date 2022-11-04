SELECT *
FROM dbo.pitches t1
INNER JOIN dbo.tracking t2 
    ON t1.game_pk = t2.game_pk
    AND t1.pitcher_id = t2.pitcherid
    AND t1.batter_id = t2.batterid
    AND t1.pitch_number = t2.pitchno
LEFT JOIN dbo.play_by_play t3 
    ON t1.game_pk = t3.game_pk
    AND t1.pitcher_id = t3.pitcher_id
    AND t1.batter_id = t3.batter_id
    AND t1.at_bat_index = t3.at_bat_index
