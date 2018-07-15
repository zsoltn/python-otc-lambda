--- create time window to all name
--insert into lambda_data_markt
SELECT d.messageid, 
       d.metrics, 
       d.name, 
       d.value, 
       w.datewindows 
from 
    (
    select from_unixtime( unix_timestamp(date_sub(current_date(), 1) ) +  (a.a + (10 * b.a) + (100 * c.a) ) * 300  ) AS datewindows
    from (select 0 as a union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as a
    cross join (select 0 as a union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as b
    cross join (select 0 as a union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as c
    order by datewindows   
    ) w
left outer join
    ( select 
        messageid, 
        from_unixtime( unix_timestamp( messagedate )  - pmod(  unix_timestamp( messagedate ),300 ) ) as windowmessagedate,
        metrics,
        name,
        value  
      from 
        inboxbatchlambda) d
on  d.windowmessagedate = w.datewindows
--where messageid is not null
;

