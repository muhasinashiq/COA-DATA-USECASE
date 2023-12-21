SELECT c.currency_code, c.name, f.currency_date, f.exchange_rate
FROM `unique-atom-406411.currencyupdated.currency_details` c
JOIN `unique-atom-406411.currencyupdated.forex_update_DB` f
ON c.currency_code = f.foreign_currency
WHERE (c.currency_code = 'USD' OR c.currency_code = 'EUR' OR c.currency_code = 'GBP')
     AND   _PARTITIONTIME >= TIMESTAMP("2023-12-18") 
    AND _PARTITIONTIME < TIMESTAMP("2023-12-19") 
  AND EXTRACT(MONTH FROM f.currency_date) = 10;
