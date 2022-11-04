select pitcherid, pitcher, avg(spinrate) as AverageSpinRate
from (select *, row_number() over (partition by pitcherid) as seqnum,
             count(*) over (partition by pitcherid) as cnt
      from test.tracking
     ) s
group by pitcherid, pitcher;
