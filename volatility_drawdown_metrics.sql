CREATE OR REPLACE PROCEDURE `volatility_drawdown_metrics`()
BEGIN
	 -- выполняем логирование процедуры
    SET @startDaily := NOW();
    SET @nameDaily := 'volatility_drawdown_metrics';
    INSERT INTO _procedure_calls (name, start, finish) 
    VALUES (@nameDaily, @startDaily, NULL);
   	-- очищаем изменяемые значения
    TRUNCATE TABLE company_volatility_drawdown_metrics;
    -- Отключаем строгие проверки (при необходимости)
    SET @original_sql_mode := @@sql_mode;
    SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES';
    
	INSERT INTO company_volatility_drawdown_metrics(company_id ,
													price,
													price_change,
												    percentage_1d_change,
												    percentage_1w_change,
												    percentage_1m_change,
												    percentage_3m_change,
												    percentage_6m_change,
												    percentage_year_change,
												    percentage_1year_change,
												    percentage_3year_change,
												    percentage_5year_change,
												    percentage_10year_change,
												    percentage_max_change,
												    percentage_52weeks_change,
												    volume,
												    volume_change,
												    percentage_volume_1d_change)	
	SELECT fp.company_id,
		   MAX(CASE WHEN period = 'current' THEN `close` END) as price,
		   ROUND((MAX(CASE WHEN period = 'current' THEN `close` END) - 
	         MAX(CASE WHEN period = 'day' THEN `close` END)), 2) AS price_change,
	       ROUND(((MAX(CASE WHEN period = 'current' THEN `close` END) - 
	         MAX(CASE WHEN period = 'day' THEN `close` END)) /
	         MAX(CASE WHEN period = 'day' THEN `close` END)) * 100, 2) AS percentage_1d_change,
	       ROUND(((MAX(CASE WHEN period = 'current' THEN `close` END) - 
	         MAX(CASE WHEN period = 'week' THEN `close` END)) /
	         MAX(CASE WHEN period = 'week' THEN `close` END)) * 100, 2) AS percentage_1w_change,
	       ROUND(((MAX(CASE WHEN period = 'current' THEN `close` END) - 
	         MAX(CASE WHEN period = 'month' THEN `close` END)) /
	         MAX(CASE WHEN period = 'month' THEN `close` END)) * 100, 2) AS percentage_1m_change,
	       ROUND(((MAX(CASE WHEN period = 'current' THEN `close` END) - 
	         MAX(CASE WHEN period = '3month' THEN `close` END)) /
	         MAX(CASE WHEN period = '3month' THEN `close` END)) * 100, 2) AS percentage_3m_change,
	       ROUND(((MAX(CASE WHEN period = 'current' THEN `close` END) - 
	         MAX(CASE WHEN period = '6month' THEN `close` END)) /
	         MAX(CASE WHEN period = '6month' THEN `close` END)) * 100, 2) AS percentage_6m_change,
	       ROUND(((MAX(CASE WHEN period = 'current' THEN `close` END) - 
	         MAX(CASE WHEN period = 'year' THEN `close` END)) /
	         MAX(CASE WHEN period = 'year' THEN `close` END)) * 100, 2) AS percentage_year_change,
	       ROUND(((MAX(CASE WHEN period = 'current' THEN `close` END) - 
	         MAX(CASE WHEN period = '1year' THEN `close` END)) /
	         MAX(CASE WHEN period = '1year' THEN `close` END)) * 100, 2) AS percentage_1year_change,
	       ROUND(((MAX(CASE WHEN period = 'current' THEN `close` END) - 
	         MAX(CASE WHEN period = '3year' THEN `close` END)) /
	         MAX(CASE WHEN period = '3year' THEN `close` END)) * 100, 2) AS percentage_3year_change,
	       ROUND(((MAX(CASE WHEN period = 'current' THEN `close` END) - 
	         MAX(CASE WHEN period = '5year' THEN `close` END)) /
	         MAX(CASE WHEN period = '5year' THEN `close` END)) * 100, 2) AS percentage_5year_change,
	       ROUND(((MAX(CASE WHEN period = 'current' THEN `close` END) - 
	         MAX(CASE WHEN period = '10year' THEN `close` END)) /
	         MAX(CASE WHEN period = '10year' THEN `close` END)) * 100, 2) AS percentage_10year_change,
	       ROUND(((MAX(CASE WHEN period = 'current' THEN `close` END) - 
	         MAX(CASE WHEN period = 'max' THEN `close` END)) /
	         MAX(CASE WHEN period = 'max' THEN `close` END)) * 100, 2) AS percentage_max_change,
	       ROUND(((MAX(CASE WHEN period = 'current' THEN `close` END) - 
	         MAX(CASE WHEN period = '52weeks' THEN `close` END)) /
	         MAX(CASE WHEN period = '52weeks' THEN `close` END)) * 100, 2) AS percentage_52weeks_change,
	       MAX(CASE WHEN period = 'current' THEN volume END) as volume,
	       ROUND((MAX(CASE WHEN period = 'current' THEN volume END) - 
	         MAX(CASE WHEN period = 'day' THEN volume END)), 2) AS volume_change,
	       ROUND(((MAX(CASE WHEN period = 'current' THEN volume END) - 
	         MAX(CASE WHEN period = 'day' THEN volume END)) /
	         MAX(CASE WHEN period = 'day' THEN volume END)) * 100, 2) AS percentage_volume_1d_change
	FROM fs_prices fp
	JOIN companies c on fp.company_id = c.id
	GROUP BY company_id
	ORDER BY company_id
	ON DUPLICATE KEY UPDATE 
						price = VALUES(price),
						price_change = VALUES(price_change),
						percentage_1d_change = VALUES(percentage_1d_change),
				        percentage_1w_change = VALUES(percentage_1w_change),
				        percentage_1m_change = VALUES(percentage_1m_change),
				        percentage_3m_change = VALUES(percentage_3m_change),
				        percentage_6m_change = VALUES(percentage_6m_change),
				        percentage_year_change = VALUES(percentage_year_change),
				        percentage_1year_change = VALUES(percentage_1year_change),
				        percentage_3year_change = VALUES(percentage_3year_change),
				        percentage_5year_change = VALUES(percentage_5year_change),
				        percentage_10year_change = VALUES(percentage_10year_change),
				        percentage_max_change = VALUES(percentage_max_change),
				        percentage_52weeks_change = VALUES(percentage_52weeks_change),
				        volume = VALUES(volume),
					    volume_change = VALUES(volume_change),
					    percentage_volume_1d_change = VALUES(percentage_volume_1d_change);
	COMMIT;
				       		   
	UPDATE company_volatility_drawdown_metrics AS cvdm
	JOIN (SELECT company_id, MIN(adj_close) as min_price, MAX(adj_close) as max_price
			FROM prices p 
			JOIN companies c on p.company_id = c.id
			GROUP BY company_id ) AS sub ON cvdm.company_id= sub.company_id 
	SET cvdm.min_price = sub.min_price,
		cvdm.max_price = sub.max_price;
  
	UPDATE company_volatility_drawdown_metrics AS cvdm
	JOIN (SELECT company_id,  ROUND(STDDEV_POP(stddev_data)*100, 2) as stddev_1w_change
			FROM(
				SELECT company_id, date, 
					(adj_close - lag(adj_close) over(PARTITION by company_id order by date))/lag(adj_close) over(PARTITION by company_id order by date) as stddev_data
				FROM prices p 
				JOIN companies c on p.company_id = c.id
				WHERE p.`date`>= CURDATE() - INTERVAL 1 WEEK) t
			GROUP BY company_id) AS sub ON cvdm.company_id= sub.company_id 
	SET cvdm.stddev_1w_change = sub.stddev_1w_change;
	
	UPDATE company_volatility_drawdown_metrics AS cvdm
	JOIN (SELECT company_id, ROUND(STDDEV_POP(stddev_data)*100, 2) as stddev_1m_change
			FROM(
				SELECT company_id, date, adj_close, lag(adj_close) over(PARTITION by company_id order by date), 
					(adj_close - lag(adj_close) over(PARTITION by company_id order by date))/lag(adj_close) over(PARTITION by company_id order by date) as stddev_data
				FROM prices p 
				JOIN companies c on p.company_id = c.id
				WHERE p.`date`>= CURDATE() - INTERVAL 1 MONTH) t
			GROUP BY company_id) AS sub ON cvdm.company_id= sub.company_id 
	SET cvdm.stddev_1m_change = sub.stddev_1m_change;

	UPDATE company_volatility_drawdown_metrics AS cvdm
	JOIN (SELECT company_id, MIN(adj_close) as min_52weeks, MAX(adj_close) as max_52weeks
			FROM prices p 
			JOIN companies c on p.company_id = c.id
			WHERE p.`date`>= CURDATE() - INTERVAL 52 WEEK
			group by company_id) AS sub ON cvdm.company_id= sub.company_id 
	SET cvdm.min_52weeks = sub.min_52weeks,
		cvdm.max_52weeks = sub.max_52weeks;

	UPDATE company_volatility_drawdown_metrics AS cvdm
	JOIN (SELECT company_id, ROUND(((MAX(adj_close) - MIN(adj_close))/ MAX(adj_close)) * 100, 2) as max_drawdown_1m
			FROM prices p 
			JOIN companies c on p.company_id = c.id
			WHERE p.`date`>= CURDATE() - INTERVAL 1 MONTH 
			GROUP BY company_id) AS sub ON cvdm.company_id= sub.company_id 
	SET cvdm.max_drawdown_1m = sub.max_drawdown_1m;

	UPDATE company_volatility_drawdown_metrics AS cvdm
	JOIN (SELECT company_id, ROUND(((MAX(adj_close) - MIN(adj_close))/ MAX(adj_close)) * 100, 2) as max_drawdown_3m
			FROM prices p 
			JOIN companies c on p.company_id = c.id
			WHERE p.`date`>= CURDATE() - INTERVAL 3 MONTH 
			GROUP BY company_id) AS sub ON cvdm.company_id= sub.company_id 
	SET cvdm.max_drawdown_3m = sub.max_drawdown_3m;
	
	UPDATE company_volatility_drawdown_metrics AS cvdm
	JOIN (SELECT company_id, ROUND(((MAX(adj_close) - MIN(adj_close))/ MAX(adj_close)) * 100, 2) as max_drawdown_6m
			FROM prices p 
			JOIN companies c on p.company_id = c.id
			WHERE p.`date`>= CURDATE() - INTERVAL 6 MONTH 
			GROUP BY company_id) AS sub ON cvdm.company_id= sub.company_id 
	SET cvdm.max_drawdown_6m = sub.max_drawdown_6m;

	UPDATE company_volatility_drawdown_metrics AS cvdm
	JOIN (SELECT company_id, ROUND(((MAX(adj_close) - MIN(adj_close))/ MAX(adj_close)) * 100, 2) as max_drawdown_start_year
			FROM prices p 
			JOIN companies c on p.company_id = c.id
			WHERE p.`date`>= MAKEDATE(YEAR(CURDATE()), 1) 
			GROUP BY company_id) AS sub ON cvdm.company_id= sub.company_id 
	SET cvdm.max_drawdown_start_year = sub.max_drawdown_start_year;

	UPDATE company_volatility_drawdown_metrics AS cvdm
	JOIN (SELECT company_id, ROUND(((MAX(adj_close) - MIN(adj_close))/ MAX(adj_close)) * 100, 2) as max_drawdown_1y
			FROM prices p 
			JOIN companies c on p.company_id = c.id
			WHERE p.`date`>= CURDATE() - INTERVAL 1 YEAR 
			GROUP BY company_id) AS sub ON cvdm.company_id= sub.company_id 
	SET cvdm.max_drawdown_1y = sub.max_drawdown_1y;

	UPDATE company_volatility_drawdown_metrics AS cvdm
	JOIN (SELECT company_id, ROUND(((MAX(adj_close) - MIN(adj_close))/ MAX(adj_close)) * 100, 2) as max_drawdown_3y
			FROM prices p 
			JOIN companies c on p.company_id = c.id
			WHERE p.`date`>= CURDATE() - INTERVAL 3 YEAR 
			GROUP BY company_id) AS sub ON cvdm.company_id= sub.company_id 
	SET cvdm.max_drawdown_3y = sub.max_drawdown_3y;
	
	UPDATE company_volatility_drawdown_metrics AS cvdm
	JOIN (SELECT company_id, ROUND(((MAX(adj_close) - MIN(adj_close))/ MAX(adj_close)) * 100, 2) as max_drawdown_5y
			FROM prices p 
			JOIN companies c on p.company_id = c.id
			WHERE p.`date`>= CURDATE() - INTERVAL 5 YEAR 
			GROUP BY company_id) AS sub ON cvdm.company_id= sub.company_id 
	SET cvdm.max_drawdown_5y = sub.max_drawdown_5y;
	
	COMMIT;
	
	UPDATE company_volatility_drawdown_metrics AS cvdm
	JOIN (
		SELECT fp.company_id,
	       ROUND((MAX(CASE WHEN period = 'current' THEN fp.close END) /  MAX(CASE WHEN period = 'week' THEN fp.close END)) /
	             (MAX(CASE WHEN period = 'current' THEN ip.close END) / MAX(CASE WHEN period = 'week' THEN ip.close END)), 4) AS rs_1w_change,
	       ROUND((MAX(CASE WHEN period = 'current' THEN fp.close END) /  MAX(CASE WHEN period = 'month' THEN fp.close END)) /
	             (MAX(CASE WHEN period = 'current' THEN ip.close END) / MAX(CASE WHEN period = 'month' THEN ip.close END)), 4) AS rs_1m_change,
	       ROUND((MAX(CASE WHEN period = 'current' THEN fp.close END) /  MAX(CASE WHEN period = '3month' THEN fp.close END)) /
	             (MAX(CASE WHEN period = 'current' THEN ip.close END) / MAX(CASE WHEN period = '3month' THEN ip.close END)), 4) AS rs_3m_change,  
	       ROUND((MAX(CASE WHEN period = 'current' THEN fp.close END) /  MAX(CASE WHEN period = '6month' THEN fp.close END)) /
	             (MAX(CASE WHEN period = 'current' THEN ip.close END) / MAX(CASE WHEN period = '6month' THEN ip.close END)), 4) AS rs_6m_change,
	       ROUND((MAX(CASE WHEN period = 'current' THEN fp.close END) /  
	              MAX(CASE WHEN period = 'year' THEN fp.close END)) /
	             (MAX(CASE WHEN period = 'current' THEN ip.close END) / 
	              MAX(CASE WHEN period = 'year' THEN ip.close END)), 4) AS rs_year_change,
	       ROUND((MAX(CASE WHEN period = 'current' THEN fp.close END) /  
	              MAX(CASE WHEN period = '1year' THEN fp.close END)) /
	             (MAX(CASE WHEN period = 'current' THEN ip.close END) / 
	              MAX(CASE WHEN period = '1year' THEN ip.close END)), 4) AS rs_1year_change
		FROM fs_prices fp 
		JOIN companies c ON c.id = fp.company_id 
		LEFT JOIN stock_indexes si ON c.country = si.country 
		LEFT JOIN indexes_prices ip ON ip.index_id = si.id 
		  AND ip.`date` = CASE 
		                WHEN fp.`date` = CONCAT_WS('-', YEAR(CURDATE()) - 1, '12', '31') THEN 
		                    (SELECT MAX(ip1.date) 
		                     FROM indexes_prices ip1 
		                     WHERE YEAR(ip1.date) = YEAR(CURDATE()) - 1 
		                     AND ip1.index_id = si.id) 
		                ELSE fp.`date`
		                  END
		GROUP BY fp.company_id) AS sub ON cvdm.company_id= sub.company_id 
	SET cvdm.rs_1w_change = sub.rs_1w_change,
		cvdm.rs_1m_change = sub.rs_1m_change,
		cvdm.rs_3m_change = sub.rs_3m_change,
		cvdm.rs_6m_change = sub.rs_6m_change,
		cvdm.rs_year_change = sub.rs_year_change,
		cvdm.rs_1year_change = sub.rs_1year_change;

	SET SESSION sql_mode = @original_sql_mode;	

	-- обновляем запись, когда процедура завершена
	UPDATE _procedure_calls 
    SET finish = NOW() 
    WHERE name = @nameDaily AND start = @startDaily;
END;


call volatility_drawdown_metrics();


truncate table company_volatility_drawdown_metrics;