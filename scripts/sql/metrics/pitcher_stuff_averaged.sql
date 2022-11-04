-- Average stuff speed, spin, break, etc. per pitcher.

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
