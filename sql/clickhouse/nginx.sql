--
-- $ cat /var/log/nginx/access.log | clickhouse > nginx.md
--

-- CREATE VIEW IF NOT EXISTS logs AS SELECT * FROM file('/var/log/nginx/access.log', 'JSONEachRow');

CREATE TABLE IF NOT EXISTS logs (
  StartTime DateTime64(3),
  Latency BIGINT,
  Protocol VARCHAR,
  RemoteAddr VARCHAR,
  Host VARCHAR,
  Method VARCHAR,
  URL VARCHAR,
  Pattern VARCHAR,
  Status INTEGER,
  Error Nullable(VARCHAR),
  RequestSize BIGINT,
  ResponseSize BIGINT,
  RequestHeaders Map(String, Array(String)),
  ResponseHeaders Map(String, Array(String)),
  SSL Map(VARCHAR, VARCHAR)
) ENGINE = MergeTree()
ORDER BY StartTime;
INSERT INTO logs SELECT
  cast(StartTime as DateTime64(3)) - toIntervalMillisecond(toFloat64(Latency)*1e3),
  toInt64(toFloat64(Latency) * 1e9),
  Protocol,
  RemoteAddr,
  Host,
  Method,
  URL,
  Pattern,
  Status,
  Error,
  RequestSize,
  ResponseSize,
  tupleToNameValuePairs(RequestHeaders),
  tupleToNameValuePairs(ResponseHeaders),
  tupleToNameValuePairs(SSL)
FROM file('/var/log/nginx/access.log', 'JSONEachRow');

SELECT '# ' || min(StartTime) || ' - ' || max(StartTime + Latency/1e9) FROM logs FORMAT LineAsString;

SELECT '\n## By Count\n' FORMAT LineAsString;

SELECT
  round(100 * count(Pattern) / sum(count(Pattern)) OVER (), 3) AS "cum%",
  count(Pattern) AS cnt,
  count(CASE WHEN Status BETWEEN 100 AND 199 THEN 1 END) AS 1xx,
  count(CASE WHEN Status BETWEEN 200 AND 299 THEN 1 END) AS 2xx,
  count(CASE WHEN Status BETWEEN 300 AND 399 THEN 1 END) AS 3xx,
  count(CASE WHEN Status BETWEEN 400 AND 499 THEN 1 END) AS 4xx,
  count(CASE WHEN Status BETWEEN 500 AND 599 THEN 1 END) AS 5xx,
  count(CASE WHEN Status NOT BETWEEN 100 AND 599 THEN 1 END) AS other,
  Method, Pattern
FROM logs GROUP BY ALL ORDER BY cnt DESC LIMIT 40 FORMAT Markdown;

SELECT '\n## By Latency\n' FORMAT LineAsString;

SELECT
  round(100 * sum(Latency) / sum(sum(Latency)) OVER (), 3) AS "cum%",
  count(Pattern) AS cnt,
  round(sum(Latency)/1e9, 3) AS sum,
  round(min(Latency)/1e9, 3) AS min,
  round(avg(Latency)/1e9, 3) AS avg,
  round(quantile(0.5)(Latency)/1e9, 3) AS p50,
  round(quantile(0.99)(Latency)/1e9, 3) AS p99,
  round(max(Latency)/1e9, 3) AS max,
  Method, Pattern
FROM logs GROUP BY ALL ORDER BY sum DESC LIMIT 40 FORMAT Markdown;

SELECT '\n## By Upload Bytes\n' FORMAT LineAsString;

SELECT
  round(100 * sum(RequestSize) / sum(sum(RequestSize)) OVER (), 3) AS "cum%",
  count(Pattern) AS cnt,
  sum(RequestSize) AS sum,
  min(RequestSize) AS min,
  cast(avg(RequestSize) as BIGINT) AS avg,
  quantile(0.5)(RequestSize) AS p50,
  quantile(0.99)(RequestSize) AS p99,
  max(RequestSize) AS max,
  Method, Pattern
FROM logs WHERE RequestSize IS NOT NULL GROUP BY ALL ORDER BY sum DESC LIMIT 40 FORMAT Markdown;

SELECT '\n## By Download Bytes\n' FORMAT LineAsString;

SELECT
  round(100 * sum(ResponseSize) / sum(sum(ResponseSize)) OVER (), 3) AS "cum%",
  count(Pattern) AS cnt,
  sum(ResponseSize) AS sum,
  min(ResponseSize) AS min,
  cast(avg(ResponseSize) as BIGINT) AS avg,
  quantile(0.5)(ResponseSize) AS p50,
  quantile(0.99)(ResponseSize) AS p99,
  max(ResponseSize) AS max,
  Method, Pattern
FROM logs GROUP BY ALL ORDER BY sum DESC LIMIT 40 FORMAT Markdown;

SELECT '\n## Top Protocols\n' FORMAT LineAsString;

SELECT
  round(100 * count(*) / sum(count(*)) OVER (), 3) AS "cum%",
  count(*) AS cnt,
  Protocol
FROM logs GROUP BY ALL ORDER BY cnt DESC, Protocol ASC LIMIT 40 FORMAT Markdown;

SELECT '\n## Top RemoteAddr\n' FORMAT LineAsString;

SELECT
  round(100 * count(*) / sum(count(*)) OVER (), 3) AS "cum%",
  count(*) AS cnt,
  RemoteAddr
FROM logs GROUP BY ALL ORDER BY cnt DESC, RemoteAddr ASC LIMIT 40 FORMAT Markdown;

SELECT '\n## Top Host\n' FORMAT LineAsString;

SELECT
  round(100 * count(*) / sum(count(*)) OVER (), 3) AS "cum%",
  count(*) AS cnt,
  Host
FROM logs GROUP BY ALL ORDER BY cnt DESC, Host ASC LIMIT 40 FORMAT Markdown;

SELECT '\n## Top Method\n' FORMAT LineAsString;

SELECT
  round(100 * count(*) / sum(count(*)) OVER (), 3) AS "cum%",
  count(*) AS cnt,
  Method
FROM logs GROUP BY ALL ORDER BY cnt DESC, Method ASC LIMIT 40 FORMAT Markdown;

SELECT '\n## Top Status\n' FORMAT LineAsString;

SELECT
  round(100 * count(*) / sum(count(*)) OVER (), 3) AS "cum%",
  count(*) AS cnt,
  Status
FROM logs GROUP BY ALL ORDER BY cnt DESC, Status ASC LIMIT 40 FORMAT Markdown;

SELECT '\n## Top Latency\n' FORMAT LineAsString;

SELECT
  round(100 * Latency / sum(Latency) OVER (), 3) AS "cum%",
  round(Latency/1e9, 3) AS Latency,
  Method,
  Host,
  URL
FROM logs ORDER BY Latency DESC LIMIT 40 FORMAT Markdown;

SELECT '\n## Request Headers\n' FORMAT LineAsString;

SELECT *,count(*) AS cnt FROM (
  SELECT Method, Pattern, arraySort(mapKeys(RequestHeaders)) as headers from logs
) GROUP BY ALL ORDER BY Pattern, Method, cnt DESC, headers LIMIT 40 FORMAT Markdown;

SELECT '\n## Request Headers Analysis\n' FORMAT LineAsString;

SELECT
  round(100 * count(lower(key))/ (SELECT count() FROM logs), 3) AS "cum%",
  lower(key) AS key,
  count(lower(key)) AS cnt,
  count(DISTINCT value) AS uniqCnt,
  round(entropy(value), 3) AS entropy,
  topK(1)(value) AS mode
FROM (
  SELECT arrayJoin(mapKeys(RequestHeaders)) as key, arrayJoin(RequestHeaders[key]) as value FROM logs
) GROUP BY ALL ORDER BY key, cnt DESC, uniqCnt DESC FORMAT Markdown;

SELECT '\n## Cookies Count\n' FORMAT LineAsString;

SELECT
  cnt AS visitCnt,
  count(cnt) AS uniqCnt
FROM (
  SELECT count(*) AS cnt FROM logs GROUP BY RequestHeaders['Cookie']
) GROUP BY cnt ORDER BY visitCnt DESC, uniqCnt DESC FORMAT Markdown;

SELECT '\n## Response Headers\n' FORMAT LineAsString;

SELECT *,count(*) AS cnt FROM (
  SELECT Method, Pattern, arraySort(mapKeys(ResponseHeaders)) as headers from logs
) GROUP BY ALL ORDER BY Pattern, Method, cnt DESC, headers LIMIT 40 FORMAT Markdown;

SELECT '\n## Response Headers Analysis\n' FORMAT LineAsString;

SELECT
  round(100 * count(lower(key))/ (SELECT count() FROM logs), 3) AS "cum%",
  lower(key) AS key,
  count(lower(key)) AS cnt,
  count(DISTINCT value) AS uniqCnt,
  round(entropy(value), 3) AS entropy,
  topK(1)(value) AS mode
FROM (
  SELECT arrayJoin(mapKeys(ResponseHeaders)) AS key, arrayJoin(ResponseHeaders[key]) AS value FROM logs
) GROUP BY ALL ORDER BY key, cnt DESC, uniqCnt DESC FORMAT Markdown;

SELECT '\n## All SSL\n' FORMAT LineAsString;

SELECT
  SSL.*,
  count(SSL) AS cnt
FROM logs GROUP BY ALL ORDER BY cnt DESC LIMIT 40 FORMAT Markdown;

SELECT '\n## All Errors\n' FORMAT LineAsString;

SELECT
  StartTime,
  round(Latency/1e9, 3) AS Latency,
  Status,
  Host,
  URL,
  Error
FROM logs WHERE Status >= 400 OR Error IS NOT NULL ORDER BY StartTime FORMAT Markdown;
