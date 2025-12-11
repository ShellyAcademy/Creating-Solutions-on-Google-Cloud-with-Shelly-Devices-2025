-- moving average on act_power
select data.params.`em1:0`.act_power,
  avg(float64(data.params.`em1:0`.act_power)) over
  (order by float64(data.params.ts) rows between 5 preceding and current row) smooth_act_power
FROM `creating-solutions-gcp-shelly.creating_solutions_with_shelly.ingress_table` 
where 
  JSON_VALUE(data.src) like "%em%" and
  data.params.`em1:0` is not null
order by FLOAT64(data.param.ts)