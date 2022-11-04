#agg avarage pitching stuff
SELECT pitcherid,
  pitcher,
  avg(relspeed) as AverageReleaseSpeed,
  avg(spinrate) as AverageSpinRate,
  avg(relheight) as AverageReleHeight,
  avg(relside) as AverageRelSide,
  avg(extension) as AverageExtension,
  avg(vertbreak) as AverageVertBreak,
  avg(inducedvertbreak) as AverageInducedVartBreak,
  avg(horzbreak) as AverageHorzBreak
FROM (SELECT *, row_number() over (partition BY pitcherid) AS seqnum,
             COUNT(*) over (partition BY pitcherid) AS cnt
      FROM test.tracking
     ) s
group by pitcherid, pitcher;

#join to play by play to see result
select *
from dbo.pitches t1
inner join dbo.pitch_tracking t2 
    on t1.game_pk = t2.game_pk
    and t1.pitcher_id = t2.pitcherid
    and t1.batter_id = t2.batterid
    and t1.pitch_number = t2.pitchno
left join dbo.play_by_play t3 
    on t1.game_pk = t2.game_pk
    and t1.pitcher_id = t2.pitcherid
    and t1.batter_id = t2.batterid
    and t1.at_bat_index = t2.at_bat_index

#join to pitches to see v/h_score
