create TABLE wtst."IBKR_EOD_DATA" AS 
SELECT 'RELIANCE' AS "IBKR_SYMBOL",date('5-JUN-2021') AS "Date", 234.5 AS "Open", 249.9 AS "High", 233.5 AS "Low", 
245.8 AS "Close", 123456678 AS "Volume", 244.7 AS "Average", 1233456 AS "BarCount" 


(''IBKR_SYMBOL'',''Date'', ''Open'', ''High'', ''Low'', ''Close'', ''Volume'', ''Average'', ''BarCount'') VALUES (%s, %(date)s, %(double precision)s, %(double precision)s, %(double precision)s, %(double precision)s, %(bigint)s, %(double precision)s, %(bigint)s)''"


(IBKR_SYMBOL,Date, Open, High, Low, Close, Volume, Average, BarCount) VALUES (%s, %(date)s, %(double precision)s, %(double precision)s, %(double precision)s, %(double precision)s, %(bigint)s, %(double precision)s, %(bigint)s)

// ACTIVE STOCKS BASELINE QUERY


DELETE FROM wtst.active_stocks_baseline

INSERT INTO wtst.active_stocks_baseline(symbol,company_name,averagetradevalue)
SELECT NED."SYMBOL", ESD."COMPANY_NAME", AVG(NED."TRADEVALUE") AS averagetradevalue
FROM wtst."NSE_EOD_DATA" NED, wtst."EXCHANGE_SYMBOLS_EQUITY" ESD
WHERE NED."SYMBOL" = ESD."SYMBOL" AND "DATE" >= '01-Jan-2020'
GROUP BY NED."SYMBOL", ESD."COMPANY_NAME"
ORDER BY AVERAGETRADEVALUE desc
LIMIT 250
ON CONFLICT(symbol) DO NOTHING


// FOCUS Stocks query
delete from wtst.focus_stocks

insert into wtst.focus_stocks 
select ise."SYMBOL", asb.company_name, asb.averagetradevalue, asb.score 
from wtst.active_stocks_baseline asb, wtst."IBKR_SYMBOLS_EQUITY" ise
where asb.symbol = ise."SYMBOL"





