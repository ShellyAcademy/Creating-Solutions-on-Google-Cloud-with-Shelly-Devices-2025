SELECT JSON_VALUE(data.src) as device_id, TIMESTAMP_TRUNC(TIMESTAMP_SECONDS(LAX_INT64(data.params.ts)), HOUR) as timestamp, 
       sum(LAX_FLOAT64(data.params.`switch:0`.aenergy.by_minute[0])) energy
FROM `creating-solutions-gcp-shelly.creating_solutions_with_shelly.ingress_table` 
where 
  JSON_VALUE(data.src) = "shelly2pmg3-e4b3233f64d0"
group by device_id, timestamp
order by device_id, timestamp