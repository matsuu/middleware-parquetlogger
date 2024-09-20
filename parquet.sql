--
-- $ cat parquet.sql | duckdb -cmd "SET VARIABLE path = '/tmp/log.parquet'" > result.md
--
CREATE OR REPLACE TABLE logs AS FROM read_parquet(getvariable('path'));

.headers off
.mode column
SELECT '# ' || strftime(min(StartTime), '%Y-%m-%d %H:%M:%S') || ' - ' || strftime(max(StartTime + to_microseconds((Latency/1e3)::INTEGER)), '%Y-%m-%d %H:%M:%S') FROM logs;

.headers on
.mode markdown

.print "\n## By Count\n"

SELECT
  (100 * count(Pattern) / sum(count(Pattern)) OVER ())::DECIMAL AS 'cum%',
  count(Pattern) AS cnt,
  count(CASE WHEN Status BETWEEN 100 AND 199 THEN 1 END) AS '1xx',
  count(CASE WHEN Status BETWEEN 200 AND 299 THEN 1 END) AS '2xx',
  count(CASE WHEN Status BETWEEN 300 AND 399 THEN 1 END) AS '3xx',
  count(CASE WHEN Status BETWEEN 400 AND 499 THEN 1 END) AS '4xx',
  count(CASE WHEN Status BETWEEN 500 AND 599 THEN 1 END) AS '5xx',
  count(CASE WHEN Status NOT BETWEEN 100 AND 599 THEN 1 END) AS 'other',
  Method, Pattern
FROM logs GROUP BY ALL ORDER BY cnt DESC LIMIT 40;

.print "\n## By Latency\n"

SELECT
  (100 * sum(Latency) / sum(sum(Latency)) OVER ())::DECIMAL AS 'cum%',
  count(Pattern) AS cnt,
  (sum(Latency)/1e9)::DECIMAL AS sum,
  (min(Latency)/1e9)::DECIMAL AS min,
  (avg(Latency)/1e9)::DECIMAL AS avg,
  (quantile_disc(Latency,0.5)/1e9)::DECIMAL AS p50,
  (quantile_disc(Latency,0.99)/1e9)::DECIMAL AS p99,
  (max(Latency)/1e9)::DECIMAL AS max,
  Method, Pattern
FROM logs GROUP BY ALL ORDER BY sum DESC LIMIT 40;

.print "\n## By Upload Bytes\n"

SELECT
  (100 * sum(RequestSize) / sum(sum(RequestSize)) OVER ())::DECIMAL AS 'cum%',
  count(Pattern) AS cnt,
  sum(RequestSize) AS sum,
  min(RequestSize) AS min,
  cast(avg(RequestSize) as BIGINT) AS avg,
  quantile_disc(RequestSize,0.5) AS p50,
  quantile_disc(RequestSize,0.99) AS p99,
  max(RequestSize) AS max,
  Method, Pattern
FROM logs WHERE RequestSize IS NOT NULL GROUP BY ALL ORDER BY sum DESC LIMIT 40;

.print "\n## By Download Bytes\n"

SELECT
  (100 * sum(ResponseSize) / sum(sum(ResponseSize)) OVER ())::DECIMAL AS 'cum%',
  count(Pattern) AS cnt,
  sum(ResponseSize) AS sum,
  min(ResponseSize) AS min,
  cast(avg(ResponseSize) as BIGINT) AS avg,
  quantile_disc(ResponseSize,0.5) AS p50,
  quantile_disc(ResponseSize,0.99) AS p99,
  max(ResponseSize) AS max,
  Method, Pattern
FROM logs GROUP BY ALL ORDER BY sum DESC LIMIT 40;

.print "\n## Top Protocols\n"

SELECT
  (100 * count(*) / sum(count(*)) OVER ())::DECIMAL AS 'cum%',
  count(*) AS cnt,
  Protocol
FROM logs GROUP BY ALL ORDER BY cnt DESC, Protocol ASC LIMIT 40;

.print "\n## Top RemoteAddr\n"

SELECT
  (100 * count(*) / sum(count(*)) OVER ())::DECIMAL AS 'cum%',
  count(*) AS cnt,
  RemoteAddr
FROM logs GROUP BY ALL ORDER BY cnt DESC, RemoteAddr ASC LIMIT 40;

.print "\n## Top Host\n"

SELECT
  (100 * count(*) / sum(count(*)) OVER ())::DECIMAL AS 'cum%',
  count(*) AS cnt,
  Host
FROM logs GROUP BY ALL ORDER BY cnt DESC, Host ASC LIMIT 40;

.print "\n## Top Method\n"

SELECT
  (100 * count(*) / sum(count(*)) OVER ())::DECIMAL AS 'cum%',
  count(*) AS cnt,
  Method
FROM logs GROUP BY ALL ORDER BY cnt DESC, Method ASC LIMIT 40;

.print "\n## Top Status\n"

SELECT
  (100 * count(*) / sum(count(*)) OVER ())::DECIMAL AS 'cum%',
  count(*) AS cnt,
  Status
FROM logs GROUP BY ALL ORDER BY cnt DESC, Status ASC LIMIT 40;

.print "\n## Top Latency\n"

SELECT
  (100 * Latency / sum(Latency) OVER ())::DECIMAL AS 'cum%',
  (Latency/1e9)::DECIMAL AS Latency,
  Method,
  Host,
  URL
FROM logs ORDER BY Latency DESC LIMIT 40;

.print "\n## Request Headers\n"

SELECT *,count(*) AS cnt FROM (
  SELECT Method, Pattern, list_sort(map_keys(RequestHeaders)) as headers from logs
) GROUP BY ALL ORDER BY Pattern, Method, cnt DESC, headers;

.print "\n## Request Headers Analysis\n"

SELECT
  (100 * count(s.key)/ (SELECT count(*) FROM logs))::DECIMAL AS 'cum%',
  s.key,
  count(s.key) AS cnt,
  count(DISTINCT s.value) AS uniqCnt,
  entropy(s.value)::DECIMAL AS entropy,
  mode(s.value) AS mode
FROM (
  SELECT unnest(list_transform(map_entries(RequestHeaders), s -> {'key':lower(s.key), 'value':s.value})) AS s FROM logs
) GROUP BY ALL ORDER BY s.key, cnt DESC, uniqCnt DESC;

.print "\n## Cookies Count\n"

SELECT
  cnt AS visitCnt,
  count(cnt) AS uniqCnt
FROM (
  SELECT count(*) AS cnt FROM logs GROUP BY map_extract(RequestHeaders, 'Cookie')
) GROUP BY cnt ORDER BY visitCnt DESC, uniqCnt DESC;

.print "\n## Response Headers\n"

SELECT *,count(*) AS cnt FROM (
  SELECT Method, Pattern, list_sort(map_keys(ResponseHeaders)) as headers from logs
) GROUP BY ALL ORDER BY Pattern, Method, cnt DESC, headers;

.print "\n## Response Headers Analysis\n"

SELECT
  (100 * count(s.key)/ (SELECT count(*) FROM logs))::DECIMAL AS 'cum%',
  s.key,
  count(s.key) AS cnt,
  count(DISTINCT s.value) AS uniqCnt,
  entropy(s.value)::DECIMAL AS entropy,
  mode(s.value) AS mode
FROM (
  SELECT unnest(list_transform(map_entries(ResponseHeaders), s -> {'key':lower(s.key), 'value':s.value})) AS s FROM logs
) GROUP BY ALL ORDER BY s.key, cnt DESC, uniqCnt DESC;

.print "\n## All Errors\n"

SELECT
  StartTime,
  (Latency/1e9)::DECIMAL AS Latency,
  Status,
  Host,
  URL,
  Error
FROM logs WHERE Status >= 400 OR Error IS NOT NULL ORDER BY StartTime;
