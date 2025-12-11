SELECT
  data.src as device_id,
  TIMESTAMP_SECONDS(LAX_INT64(data.params.ts)) as timestamp,
  DATETIME(TIMESTAMP_SECONDS(LAX_INT64(data.params.ts)), "CET") as event_time,
  data.params.`switch:0`.apower
FROM `creating-solutions-gcp-shelly.shelly_dataset.device_status`
where data.params.`switch:0`.apower is not null
ORDER BY event_time DESC
LIMIT 100