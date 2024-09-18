CREATE OR REPLACE PROCEDURE `median_industry_dividends`()
BEGIN
	-- выполняем логирование процедуры
	set @startDaily := NOW();
    set @nameDaily := 'median_industry_dividends';
    insert into _procedure_calls (name, start, finish) value (@nameDaily, @startDaily, null);
   
	-- Отключаем строгие проверки (при необходимости)
    SET @original_sql_mode := @@sql_mode;
    SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES';
    
    -- заносим данные в таблицу 
    UPDATE median_industry_metrics AS mim
    JOIN ( SELECT t.*
    	   FROM(
    	   		WITH ranked_data AS (                                           
						SELECT ham.company_id, c.industry_id, c.exchange_id, 
								revenue_growth_1y,
								revenue_growth_3y,
								revenue_growth_5y,
								revenue_growth_10y,
								revenue_cagr_3y,
								revenue_cagr_5y,
								revenue_cagr_10y,
								ebitda_growth_1y,
								ebitda_growth_3y,
								ebitda_growth_5y,
								ebitda_growth_10y,
								ebitda_cagr_3y,
								ebitda_cagr_5y,
								ebitda_cagr_10y,
								operatingIncome_growth_1y,
								operatingIncome_growth_3y,
								operatingIncome_growth_5y,
								operatingIncome_growth_10y,
								operatingIncome_cagr_3y,
								operatingIncome_cagr_5y,
								operatingIncome_cagr_10y,
								netIncome_growth_1y,
								netIncome_growth_3y,
								netIncome_growth_5y,
								netIncome_growth_10y,
								netIncome_cagr_3y,
								netIncome_cagr_5y,
								netIncome_cagr_10y,
								eps_growth_1y,
								eps_growth_3y,
								eps_growth_5y,
								eps_growth_10y,
								eps_cagr_3y,
								eps_cagr_5y,
								eps_cagr_10y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by revenue_growth_1y) as rn_revenue_growth_1y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by revenue_growth_3y) as rn_revenue_growth_3y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by revenue_growth_5y) as rn_revenue_growth_5y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by revenue_growth_10y) as rn_revenue_growth_10y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by revenue_cagr_3y) as rn_revenue_cagr_3y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by revenue_cagr_5y) as rn_revenue_cagr_5y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by revenue_cagr_10y) as rn_revenue_cagr_10y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by ebitda_growth_1y) as rn_ebitda_growth_1y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by ebitda_growth_3y) as rn_ebitda_growth_3y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by ebitda_growth_5y) as rn_ebitda_growth_5y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by ebitda_growth_10y) as rn_ebitda_growth_10y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by ebitda_cagr_3y) as rn_ebitda_cagr_3y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by ebitda_cagr_5y) as rn_ebitda_cagr_5y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by ebitda_cagr_10y) as rn_ebitda_cagr_10y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by operatingIncome_growth_1y) as rn_operatingIncome_growth_1y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by operatingIncome_growth_3y) as rn_operatingIncome_growth_3y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by operatingIncome_growth_5y) as rn_operatingIncome_growth_5y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by operatingIncome_growth_10y) as rn_operatingIncome_growth_10y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by operatingIncome_cagr_3y) as rn_operatingIncome_cagr_3y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by operatingIncome_cagr_5y) as rn_operatingIncome_cagr_5y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by operatingIncome_cagr_10y) as rn_operatingIncome_cagr_10y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by netIncome_growth_1y) as rn_netIncome_growth_1y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by netIncome_growth_3y) as rn_netIncome_growth_3y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by netIncome_growth_5y) as rn_netIncome_growth_5y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by netIncome_growth_10y) as rn_netIncome_growth_10y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by netIncome_cagr_3y) as rn_netIncome_cagr_3y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by netIncome_cagr_5y) as rn_netIncome_cagr_5y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by netIncome_cagr_10y) as rn_netIncome_cagr_10y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by eps_growth_1y) as rn_eps_growth_1y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by eps_growth_3y) as rn_eps_growth_3y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by eps_growth_5y) as rn_eps_growth_5y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by eps_growth_10y) as rn_eps_growth_10y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by eps_cagr_3y) as rn_eps_cagr_3y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by eps_cagr_5y) as rn_eps_cagr_5y,
								ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by eps_cagr_10y) as rn_eps_cagr_10y,
								COUNT(*) OVER (PARTITION BY c.industry_id, c.exchange_id) AS total_rows
						from historical_annual_metrics ham 
						join companies c on c.id = ham.company_id
						where ham.period_end_date = (SELECT MAX(period_end_date) FROM historical_annual_metrics ham2 where ham.period_end_date = ham2.period_end_date))
					SELECT industry_id, exchange_id,
									ROUND(AVG(CASE WHEN rn_revenue_growth_1y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_revenue_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN revenue_growth_1y END), 2) AS revenue_growth_1y_median_industry,
									ROUND(AVG(CASE WHEN rn_revenue_growth_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_revenue_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN revenue_growth_3y END), 2) AS revenue_growth_3y_median_industry,
									ROUND(AVG(CASE WHEN rn_revenue_growth_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_revenue_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN revenue_growth_5y END), 2) AS revenue_growth_5y_median_industry,
									ROUND(AVG(CASE WHEN rn_revenue_growth_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_revenue_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN revenue_growth_10y END), 2) AS revenue_growth_10y_median_industry,
									ROUND(AVG(CASE WHEN rn_revenue_cagr_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_revenue_cagr_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN revenue_cagr_3y END), 2) AS revenue_cagr_3y_median_industry,
									ROUND(AVG(CASE WHEN rn_revenue_cagr_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_revenue_cagr_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN revenue_cagr_5y END), 2) AS revenue_cagr_5y_median_industry,
									ROUND(AVG(CASE WHEN rn_revenue_cagr_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_revenue_cagr_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN revenue_cagr_10y END), 2) AS revenue_cagr_10y_median_industry,
									ROUND(AVG(CASE WHEN rn_ebitda_growth_1y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_ebitda_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN ebitda_growth_1y END), 2) AS ebitda_growth_1y_median_industry,
									ROUND(AVG(CASE WHEN rn_ebitda_growth_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_ebitda_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN ebitda_growth_3y END), 2) AS ebitda_growth_3y_median_industry,
									ROUND(AVG(CASE WHEN rn_ebitda_growth_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_ebitda_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN ebitda_growth_5y END), 2) AS ebitda_growth_5y_median_industry,
									ROUND(AVG(CASE WHEN rn_ebitda_growth_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_ebitda_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN ebitda_growth_10y END), 2) AS ebitda_growth_10y_median_industry,
									ROUND(AVG(CASE WHEN rn_ebitda_cagr_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_ebitda_cagr_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN ebitda_cagr_3y END), 2) AS ebitda_cagr_3y_median_industry,
									ROUND(AVG(CASE WHEN rn_ebitda_cagr_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_ebitda_cagr_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN ebitda_cagr_5y END), 2) AS ebitda_cagr_5y_median_industry,
									ROUND(AVG(CASE WHEN rn_ebitda_cagr_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_ebitda_cagr_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN ebitda_cagr_10y END), 2) AS ebitda_cagr_10y_median_industry,
									ROUND(AVG(CASE WHEN rn_operatingIncome_growth_1y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_operatingIncome_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN operatingIncome_growth_1y END), 2) AS operatingIncome_growth_1y_median_industry,
									ROUND(AVG(CASE WHEN rn_operatingIncome_growth_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_operatingIncome_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN operatingIncome_growth_3y END), 2) AS operatingIncome_growth_3y_median_industry,
									ROUND(AVG(CASE WHEN rn_operatingIncome_growth_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_operatingIncome_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN operatingIncome_growth_5y END), 2) AS operatingIncome_growth_5y_median_industry,
									ROUND(AVG(CASE WHEN rn_operatingIncome_growth_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_operatingIncome_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN operatingIncome_growth_10y END), 2) AS operatingIncome_growth_10y_median_industry,
									ROUND(AVG(CASE WHEN rn_operatingIncome_cagr_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_operatingIncome_cagr_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN operatingIncome_cagr_3y END), 2) AS operatingIncome_cagr_3y_median_industry,
									ROUND(AVG(CASE WHEN rn_operatingIncome_cagr_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_operatingIncome_cagr_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN operatingIncome_cagr_5y END), 2) AS operatingIncome_cagr_5y_median_industry,
									ROUND(AVG(CASE WHEN rn_operatingIncome_cagr_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_operatingIncome_cagr_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN operatingIncome_cagr_10y END), 2) AS operatingIncome_cagr_10y_median_industry,
									ROUND(AVG(CASE WHEN rn_netIncome_growth_1y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netIncome_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netIncome_growth_1y END), 2) AS netIncome_growth_1y_median_industry,
									ROUND(AVG(CASE WHEN rn_netIncome_growth_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netIncome_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netIncome_growth_3y END), 2) AS netIncome_growth_3y_median_industry,
									ROUND(AVG(CASE WHEN rn_netIncome_growth_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netIncome_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netIncome_growth_5y END), 2) AS netIncome_growth_5y_median_industry,
									ROUND(AVG(CASE WHEN rn_netIncome_growth_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netIncome_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netIncome_growth_10y END), 2) AS netIncome_growth_10y_median_industry,
									ROUND(AVG(CASE WHEN rn_netIncome_cagr_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netIncome_cagr_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netIncome_cagr_3y END), 2) AS netIncome_cagr_3y_median_industry,
									ROUND(AVG(CASE WHEN rn_netIncome_cagr_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netIncome_cagr_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netIncome_cagr_5y END), 2) AS netIncome_cagr_5y_median_industry,
									ROUND(AVG(CASE WHEN rn_netIncome_cagr_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netIncome_cagr_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netIncome_cagr_10y END), 2) AS netIncome_cagr_10y_median_industry,
									ROUND(AVG(CASE WHEN rn_eps_growth_1y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_eps_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN eps_growth_1y END), 2) AS eps_growth_1y_median_industry,
									ROUND(AVG(CASE WHEN rn_eps_growth_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_eps_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN eps_growth_3y END), 2) AS eps_growth_3y_median_industry,
									ROUND(AVG(CASE WHEN rn_eps_growth_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_eps_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN eps_growth_5y END), 2) AS eps_growth_5y_median_industry,
									ROUND(AVG(CASE WHEN rn_eps_growth_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_eps_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN eps_growth_10y END), 2) AS eps_growth_10y_median_industry,
									ROUND(AVG(CASE WHEN rn_eps_cagr_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_eps_cagr_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN eps_cagr_3y END), 2) AS eps_cagr_3y_median_industry,
									ROUND(AVG(CASE WHEN rn_eps_cagr_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_eps_cagr_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN eps_cagr_5y END), 2) AS eps_cagr_5y_median_industry,
									ROUND(AVG(CASE WHEN rn_eps_cagr_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_eps_cagr_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN eps_cagr_10y END), 2) AS eps_cagr_10y_median_industry
					FROM ranked_data
					GROUP BY industry_id, exchange_id
					ORDER BY industry_id, exchange_id) t ) AS sub on mim.industry_id = sub.industry_id AND mim.exchange_id = sub.exchange_id
		SET mim.revenue_growth_1y_median_industry = sub.revenue_growth_1y_median_industry,
			mim.revenue_growth_3y_median_industry = sub.revenue_growth_3y_median_industry,
			mim.revenue_growth_5y_median_industry = sub.revenue_growth_5y_median_industry,
			mim.revenue_growth_10y_median_industry = sub.revenue_growth_10y_median_industry,
			mim.revenue_cagr_3y_median_industry = sub.revenue_cagr_3y_median_industry,
			mim.revenue_cagr_5y_median_industry = sub.revenue_cagr_5y_median_industry,
			mim.revenue_cagr_10y_median_industry = sub.revenue_cagr_10y_median_industry,
			mim.ebitda_growth_1y_median_industry = sub.ebitda_growth_1y_median_industry,
			mim.ebitda_growth_3y_median_industry = sub.ebitda_growth_3y_median_industry,
			mim.ebitda_growth_5y_median_industry = sub.ebitda_growth_5y_median_industry,
			mim.ebitda_growth_10y_median_industry = sub.ebitda_growth_10y_median_industry,
			mim.ebitda_cagr_3y_median_industry = sub.ebitda_cagr_3y_median_industry,
			mim.ebitda_cagr_5y_median_industry = sub.ebitda_cagr_5y_median_industry,
			mim.ebitda_cagr_10y_median_industry = sub.ebitda_cagr_10y_median_industry,
			mim.operatingIncome_growth_1y_median_industry = sub.operatingIncome_growth_1y_median_industry,
			mim.operatingIncome_growth_3y_median_industry = sub.operatingIncome_growth_3y_median_industry,
			mim.operatingIncome_growth_5y_median_industry = sub.operatingIncome_growth_5y_median_industry,
			mim.operatingIncome_growth_10y_median_industry = sub.operatingIncome_growth_10y_median_industry,
			mim.operatingIncome_cagr_3y_median_industry = sub.operatingIncome_cagr_3y_median_industry,
			mim.operatingIncome_cagr_5y_median_industry = sub.operatingIncome_cagr_5y_median_industry,
			mim.operatingIncome_cagr_10y_median_industry = sub.operatingIncome_cagr_10y_median_industry,
			mim.netIncome_growth_1y_median_industry = sub.netIncome_growth_1y_median_industry,
			mim.netIncome_growth_3y_median_industry = sub.netIncome_growth_3y_median_industry,
			mim.netIncome_growth_5y_median_industry = sub.netIncome_growth_5y_median_industry,
			mim.netIncome_growth_10y_median_industry = sub.netIncome_growth_10y_median_industry,
			mim.netIncome_cagr_3y_median_industry = sub.netIncome_cagr_3y_median_industry,
			mim.netIncome_cagr_5y_median_industry = sub.netIncome_cagr_5y_median_industry,
			mim.netIncome_cagr_10y_median_industry = sub.netIncome_cagr_10y_median_industry,
			mim.eps_growth_1y_median_industry = sub.eps_growth_1y_median_industry,
			mim.eps_growth_3y_median_industry = sub.eps_growth_3y_median_industry,
			mim.eps_growth_5y_median_industry = sub.eps_growth_5y_median_industry,
			mim.eps_growth_10y_median_industry = sub.eps_growth_10y_median_industry,
			mim.eps_cagr_3y_median_industry = sub.eps_cagr_3y_median_industry,
			mim.eps_cagr_5y_median_industry = sub.eps_cagr_5y_median_industry,
			mim.eps_cagr_10y_median_industry = sub.eps_cagr_10y_median_industry;
		
	-- фиксируем обновление
    COMMIT;	
    -- заносим данные в таблицу 
    UPDATE median_industry_metrics AS mim
    JOIN ( SELECT t.*
    	   FROM(
                WITH ranked_data AS ( 
						SELECT t.*, 
								ROW_NUMBER() over(PARTITION by t.industry_id, t.exchange_id order by Div_yield_year) as rn_Div_yield_year,
								COUNT(*) OVER (PARTITION BY t.industry_id, t.exchange_id) AS total_rows
						FROM (
								SELECT ds.company_id, c.industry_id, c.exchange_id, ROUND(SUM(ds.dividend) / AVG(IF(p.adj_close is null, 0, p.adj_close)) * 100, 2) AS Div_yield_year
								from dividend_sources ds 
								join companies c on ds.company_id = c.id 
								LEFT JOIN prices p ON ds.company_id = p.company_id AND ds.t2Date = p.`date` 
								where ds.t2Date >= CURDATE() - INTERVAL 1 YEAR 
								GROUP BY ds.company_id) t)
				SELECT rd.industry_id, rd.exchange_id,
								ROUND(AVG(CASE WHEN rn_Div_yield_year = FLOOR((total_rows + 1) / 2) 
										                OR (total_rows % 2 = 0 AND rn_Div_yield_year IN (total_rows / 2, total_rows / 2 + 1)) 
										                THEN Div_yield_year END), 2) AS Div_yield_year_median_industry
				FROM ranked_data rd
				GROUP BY rd.industry_id, rd.exchange_id
				ORDER BY rd.industry_id, rd.exchange_id) t ) AS sub on mim.industry_id = sub.industry_id AND mim.exchange_id = sub.exchange_id
	SET mim.Div_yield_year_median_industry = sub.Div_yield_year_median_industry;

	-- фиксируем обновление
    COMMIT;	
   	 -- заносим данные в таблицу 
    UPDATE median_industry_metrics AS mim
    JOIN ( SELECT t.*
    	   FROM(
    	   		WITH ranked_data AS(
							SELECT dg.company_id,
									c.industry_id, c.exchange_id,
									dg.`year`,
									dg.DFCFR ,
									dg.DPR,
									dg.continuous_div_start_year,
									dg.continuous_div_grow_year,
									dg.dsi,
									dg.DGR,
									dg.DGR_3Y,
									dg.DGR_5Y,
									dg.DGR_10Y,
									ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by DFCFR) as rn_DFCFR,
									ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by DPR) as rn_DPR,
									ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by continuous_div_start_year) as rn_continuous_div_start_year,
									ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by continuous_div_grow_year) as rn_continuous_div_grow_year,
									ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by dsi) as rn_dsi,
									ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by DGR) as rn_DGR,
									ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by DGR_3Y) as rn_DGR_3Y,
									ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by DGR_5Y) as rn_DGR_5Y,
									ROW_NUMBER() over(PARTITION by c.industry_id, c.exchange_id order by DGR_10Y) as rn_DGR_10Y,
									COUNT(*) OVER (PARTITION BY c.industry_id, c.exchange_id) AS total_rows
							from dividend_graphs dg 
							join companies c on dg.company_id = c.id
							WHERE dg.`year` = YEAR(CURDATE()) - 1
							order by c.industry_id, c.exchange_id)
					SELECT rd.industry_id, rd.exchange_id,
									ROUND(AVG(CASE WHEN rn_DFCFR = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_DFCFR IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN DFCFR END), 2) AS DFCFR_median_industry,
									ROUND(AVG(CASE WHEN rn_DPR = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_DPR IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN DPR END), 2) AS DPR_median_industry,
									ROUND(AVG(CASE WHEN rn_continuous_div_start_year = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_continuous_div_start_year IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN continuous_div_start_year END), 2) AS continuous_div_start_year_median_industry,
									ROUND(AVG(CASE WHEN rn_continuous_div_grow_year = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_continuous_div_grow_year IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN continuous_div_grow_year END), 2) AS continuous_div_grow_year_median_industry,
									ROUND(AVG(CASE WHEN rn_dsi = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_dsi IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN dsi END), 2) AS dsi_median_industry,
									ROUND(AVG(CASE WHEN rn_DGR = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_DGR IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN DGR END), 2) AS DGR_median_industry,
									ROUND(AVG(CASE WHEN rn_DGR_3Y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_DGR_3Y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN DGR_3Y END), 2) AS DGR_3Y_median_industry,
									ROUND(AVG(CASE WHEN rn_DGR_5Y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_DGR_5Y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN DGR_5Y END), 2) AS DGR_5Y_median_industry,
									ROUND(AVG(CASE WHEN rn_DGR_10Y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_DGR_10Y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN DGR_10Y END), 2) AS DGR_10Y_median_industry
					FROM ranked_data rd
					GROUP BY rd.industry_id, rd.exchange_id
					ORDER BY rd.industry_id, rd.exchange_id) t ) AS sub on mim.industry_id = sub.industry_id AND mim.exchange_id = sub.exchange_id
	SET mim.DFCFR_median_industry = sub.DFCFR_median_industry,
		mim.DPR_median_industry = sub.DPR_median_industry,
		mim.continuous_div_start_year_median_industry = sub.continuous_div_start_year_median_industry,
		mim.continuous_div_grow_year_median_industry = sub.continuous_div_grow_year_median_industry,
		mim.dsi_median_industry = sub.dsi_median_industry,
		mim.DGR_median_industry = sub.DGR_median_industry,
		mim.DGR_3Y_median_industry = sub.DGR_3Y_median_industry,
		mim.DGR_5Y_median_industry = sub.DGR_5Y_median_industry,
		mim.DGR_10Y_median_industry = sub.DGR_10Y_median_industry;
					    
    	-- Обновление логирования процедуры после завершения
	UPDATE _procedure_calls SET finish = NOW() WHERE name = @nameDaily AND start = @startDaily;
END;