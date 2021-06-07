create TABLE wtst."IBKR_EOD_DATA" AS 
SELECT 'RELIANCE' AS "IBKR_SYMBOL",date('5-JUN-2021') AS "Date", 234.5 AS "Open", 249.9 AS "High", 233.5 AS "Low", 
245.8 AS "Close", 123456678 AS "Volume", 244.7 AS "Average", 1233456 AS "BarCount" 


(''IBKR_SYMBOL'',''Date'', ''Open'', ''High'', ''Low'', ''Close'', ''Volume'', ''Average'', ''BarCount'') VALUES (%s, %(date)s, %(double precision)s, %(double precision)s, %(double precision)s, %(double precision)s, %(bigint)s, %(double precision)s, %(bigint)s)''"


(IBKR_SYMBOL,Date, Open, High, Low, Close, Volume, Average, BarCount) VALUES (%s, %(date)s, %(double precision)s, %(double precision)s, %(double precision)s, %(double precision)s, %(bigint)s, %(double precision)s, %(bigint)s)

// ACTIVE STOCKS BASELINE QUERY

INSERT INTO wtst."ACTIVE_STOCKS_BASELINE"("SYMBOL", "COMPANY_NAME","AVERAGETRADEVALUE")
SELECT NED."SYMBOL", ESD."COMPANY_NAME", AVG(NED."TRADEVALUE") AS AVERAGETRADEVALUE
FROM wtst."NSE_EOD_DATA" NED, wtst."EXCHANGE_SYMBOLS_EQUITY" ESD
WHERE NED."SYMBOL" = ESD."SYMBOL" AND "DATE" >= '01-Jan-2020'
GROUP BY NED."SYMBOL", ESD."COMPANY_NAME"
ORDER BY AVERAGETRADEVALUE desc
LIMIT 100
ON CONFLICT("SYMBOL") DO NOTHING





