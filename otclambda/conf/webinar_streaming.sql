select 
    s.name,
    m.metrics,
    m.value 
from
   ( select
        name,
        metrics,
        value,
        messagedate,
        messageid 
     from 
        inboxtemp
    ) s
left outer join
   ( select 
        metrics,
        value,
        mod
     from 
        meta_lambda_alert
    )  m 
on
    s.metrics=m.metrics
where
    ( (m.mod like  "%gt%") and ( int(s.value) > int( m.value ) ) ) or
    ( (m.mod like  "%lt%") and ( int(s.value) < int( m.value ) ) ) or
   ( (m.mod ==  "eq") and ( int(s.value) ==  int( m.value ) ) )    
