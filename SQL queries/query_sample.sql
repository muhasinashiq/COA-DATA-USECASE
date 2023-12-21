SELECT 
    MAX(fd.exchange_rate) AS max_forex_value,
    MIN(fd.exchange_rate) AS min_forex_value,
    AVG(fd.exchange_rate) AS avg_forex_value,
    cd.name,
    fd.foreign_currency,
    fd.base_currency
FROM 
    `unique-atom-406411.currencyupdated.forex_update_DB` fd
JOIN 
    `unique-atom-406411.currencyupdated.currency_details` cd 
    ON fd.foreign_currency = cd.currency_code
WHERE 
    _PARTITIONTIME >= TIMESTAMP("2023-12-18") 
    AND _PARTITIONTIME < TIMESTAMP("2023-12-19")
GROUP BY 
    cd.name, fd.foreign_currency, fd.base_currency;