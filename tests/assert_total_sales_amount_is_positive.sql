select
orderId,
sum(linesalesamount) as sales
from
{{ref('fct_orders')}}
group by OrderId
having sales <0