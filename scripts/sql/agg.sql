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

#join to pitches to see v/h_score
