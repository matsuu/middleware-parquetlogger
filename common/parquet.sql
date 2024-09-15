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
  (100 * count(Pattern) / sum(count(Pattern)) OVER ())::DECIMAL(6,3) AS 'cum%',
  count(Pattern) AS cnt,
  count(CASE WHEN Status BETWEEN 200 AND 299 THEN 1 END) AS '2xx',
  count(CASE WHEN Status BETWEEN 300 AND 399 THEN 1 END) AS '3xx',
  count(CASE WHEN Status BETWEEN 400 AND 499 THEN 1 END) AS '4xx',
  count(CASE WHEN Status BETWEEN 500 AND 599 THEN 1 END) AS '5xx',
  Method, Pattern
FROM logs GROUP BY ALL ORDER BY cnt DESC;

.print "\n## By Latency\n"

SELECT
  (100 * sum(Latency) / sum(sum(Latency)) OVER ())::DECIMAL(6,3) AS 'cum%',
  count(Pattern) AS cnt,
  (sum(Latency)/1e9)::DECIMAL(38,6) AS sum,
  (min(Latency)/1e9)::DECIMAL(38,6) AS min,
  (avg(Latency)/1e9)::DECIMAL(38,6) AS avg,
  (quantile_disc(Latency,0.5)/1e9)::DECIMAL(38,6) AS p50,
  (quantile_disc(Latency,0.99)/1e9)::DECIMAL(38,6) AS p99,
  (max(Latency)/1e9)::DECIMAL(38,6) AS max,
  Method, Pattern
FROM logs GROUP BY ALL ORDER BY sum DESC;

.print "\n## By Upload Bytes\n"

SELECT
  (100 * sum(ContentLength) / sum(sum(ContentLength)) OVER ())::DECIMAL(6,3) AS 'cum%',
  count(Pattern) AS cnt,
  sum(ContentLength) AS sum,
  min(ContentLength) AS min,
  cast(avg(ContentLength) as BIGINT) AS avg,
  quantile_disc(ContentLength,0.5) AS p50,
  quantile_disc(ContentLength,0.99) AS p99,
  max(ContentLength) AS max,
  Method, Pattern
FROM logs WHERE ContentLength IS NOT NULL GROUP BY ALL ORDER BY sum DESC;

.print "\n## By Download Bytes\n"

SELECT
  (100 * sum(ResponseSize) / sum(sum(ResponseSize)) OVER ())::DECIMAL(6,3) AS 'cum%',
  count(Pattern) AS cnt,
  sum(ResponseSize) AS sum,
  min(ResponseSize) AS min,
  cast(avg(ResponseSize) as BIGINT) AS avg,
  quantile_disc(ResponseSize,0.5) AS p50,
  quantile_disc(ResponseSize,0.99) AS p99,
  max(ResponseSize) AS max,
  Method, Pattern
FROM logs GROUP BY ALL ORDER BY sum DESC;

.print "\n## Top Protocols\n"

SELECT
  (100 * count(*) / sum(count(*)) OVER ())::DECIMAL(6,3) AS 'cum%',
  count(*) AS cnt,
  Protocol
FROM logs GROUP BY ALL ORDER BY cnt DESC, Protocol ASC LIMIT 40;

.print "\n## Top RemoteAddr\n"

SELECT
  (100 * count(*) / sum(count(*)) OVER ())::DECIMAL(6,3) AS 'cum%',
  count(*) AS cnt,
  RemoteAddr
FROM logs GROUP BY ALL ORDER BY cnt DESC, RemoteAddr ASC LIMIT 40;

.print "\n## Top Host\n"

SELECT
  (100 * count(*) / sum(count(*)) OVER ())::DECIMAL(6,3) AS 'cum%',
  count(*) AS cnt,
  Host
FROM logs GROUP BY ALL ORDER BY cnt DESC, Host ASC LIMIT 40;

.print "\n## Top Method\n"

SELECT
  (100 * count(*) / sum(count(*)) OVER ())::DECIMAL(6,3) AS 'cum%',
  count(*) AS cnt,
  Method
FROM logs GROUP BY ALL ORDER BY cnt DESC, Method ASC LIMIT 40;

.print "\n## Top Referer\n"

SELECT
  (100 * count(*) / sum(count(*)) OVER ())::DECIMAL(6,3) AS 'cum%',
  count(*) AS cnt,
  Referer
FROM logs GROUP BY ALL ORDER BY cnt DESC, Referer ASC LIMIT 40;

.print "\n## Top UserAgent\n"

SELECT
  (100 * count(*) / sum(count(*)) OVER ())::DECIMAL(6,3) AS 'cum%',
  count(*) AS cnt,
  UserAgent
FROM logs GROUP BY ALL ORDER BY cnt DESC, UserAgent ASC LIMIT 40;

.print "\n## Top Status\n"

SELECT
  (100 * count(*) / sum(count(*)) OVER ())::DECIMAL(6,3) AS 'cum%',
  count(*) AS cnt,
  Status
FROM logs GROUP BY ALL ORDER BY cnt DESC, Status ASC LIMIT 40;

.print "\n## Top Latency\n"

SELECT
  (100 * Latency / sum(Latency) OVER ())::DECIMAL(6,3) AS 'cum%',
  (Latency/1e9)::DECIMAL(38,6) AS Latency,
  Method,
  Host,
  URL
FROM logs ORDER BY Latency DESC LIMIT 40;

.print "\n## Request Headers\n"

SELECT *,count(*) AS cnt FROM (
  SELECT Method, Pattern,list_sort(map_keys(RequestHeader)) as headers from logs
) GROUP BY ALL ORDER BY Pattern, Method, cnt DESC, headers;

.print "\n## Cookies Count\n"

SELECT
  cnt AS visit_cnt,
  count(cnt) AS unique_cnt
FROM (
  SELECT count(*) AS cnt FROM logs GROUP BY map_extract(RequestHeader, 'Cookie')
) GROUP BY cnt ORDER BY visit_cnt DESC, unique_cnt DESC;

.print "\n## All Errors\n"

SELECT
  StartTime,
  (Latency/1e9)::DECIMAL(38,6) AS Latency,
  Status,
  Host,
  URL,
FROM logs WHERE Status >= 400 ORDER BY StartTime;
