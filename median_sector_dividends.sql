CREATE OR REPLACE PROCEDURE `median_sector_dividends`()
BEGIN
	-- выполняем логирование процедуры
	set @startDaily := NOW();
    set @nameDaily := 'median_sector_dividends';
    insert into _procedure_calls (name, start, finish) value (@nameDaily, @startDaily, null);
   
	-- Отключаем строгие проверки (при необходимости)
    SET @original_sql_mode := @@sql_mode;
    SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES';
    
    -- заносим данные в таблицу 
    UPDATE median_sector_metrics AS mim
    JOIN ( SELECT t.*
    	   FROM(
    	   		WITH ranked_data AS (                                           
						SELECT ham.company_id, ham.period_end_date, c.sector_id, c.exchange_id, 
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
								debt_growth_1y,
								debt_growth_3y,
								debt_growth_5y,
								debt_growth_10y,
								netDebt_growth_1y,
								netDebt_growth_3y,
								netDebt_growth_5y,
								netDebt_growth_10y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by revenue_growth_1y) as rn_revenue_growth_1y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by revenue_growth_3y) as rn_revenue_growth_3y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by revenue_growth_5y) as rn_revenue_growth_5y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by revenue_growth_10y) as rn_revenue_growth_10y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by revenue_cagr_3y) as rn_revenue_cagr_3y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by revenue_cagr_5y) as rn_revenue_cagr_5y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by revenue_cagr_10y) as rn_revenue_cagr_10y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by ebitda_growth_1y) as rn_ebitda_growth_1y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by ebitda_growth_3y) as rn_ebitda_growth_3y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by ebitda_growth_5y) as rn_ebitda_growth_5y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by ebitda_growth_10y) as rn_ebitda_growth_10y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by ebitda_cagr_3y) as rn_ebitda_cagr_3y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by ebitda_cagr_5y) as rn_ebitda_cagr_5y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by ebitda_cagr_10y) as rn_ebitda_cagr_10y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by operatingIncome_growth_1y) as rn_operatingIncome_growth_1y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by operatingIncome_growth_3y) as rn_operatingIncome_growth_3y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by operatingIncome_growth_5y) as rn_operatingIncome_growth_5y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by operatingIncome_growth_10y) as rn_operatingIncome_growth_10y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by operatingIncome_cagr_3y) as rn_operatingIncome_cagr_3y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by operatingIncome_cagr_5y) as rn_operatingIncome_cagr_5y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by operatingIncome_cagr_10y) as rn_operatingIncome_cagr_10y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by netIncome_growth_1y) as rn_netIncome_growth_1y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by netIncome_growth_3y) as rn_netIncome_growth_3y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by netIncome_growth_5y) as rn_netIncome_growth_5y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by netIncome_growth_10y) as rn_netIncome_growth_10y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by netIncome_cagr_3y) as rn_netIncome_cagr_3y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by netIncome_cagr_5y) as rn_netIncome_cagr_5y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by netIncome_cagr_10y) as rn_netIncome_cagr_10y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by eps_growth_1y) as rn_eps_growth_1y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by eps_growth_3y) as rn_eps_growth_3y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by eps_growth_5y) as rn_eps_growth_5y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by eps_growth_10y) as rn_eps_growth_10y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by eps_cagr_3y) as rn_eps_cagr_3y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by eps_cagr_5y) as rn_eps_cagr_5y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by eps_cagr_10y) as rn_eps_cagr_10y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by debt_growth_1y) as rn_debt_growth_1y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by debt_growth_3y) as rn_debt_growth_3y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by debt_growth_5y) as rn_debt_growth_5y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by debt_growth_10y) as rn_debt_growth_10y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by netDebt_growth_1y) as rn_netDebt_growth_1y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by netDebt_growth_3y) as rn_netDebt_growth_3y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by netDebt_growth_5y) as rn_netDebt_growth_5y,
								ROW_NUMBER() over(PARTITION by period_end_date, c.sector_id, c.exchange_id order by netDebt_growth_10y) as rn_netDebt_growth_10y,
								COUNT(*) OVER (PARTITION BY period_end_date, c.sector_id, c.exchange_id) AS total_rows
						from historical_annual_metrics ham 
						join companies c on c.id = ham.company_id
						)
					SELECT period_end_date, sector_id, exchange_id,
									ROUND(AVG(CASE WHEN rn_revenue_growth_1y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_revenue_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN revenue_growth_1y END), 2) AS revenue_growth_1y_median_sector,
									ROUND(AVG(CASE WHEN rn_revenue_growth_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_revenue_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN revenue_growth_3y END), 2) AS revenue_growth_3y_median_sector,
									ROUND(AVG(CASE WHEN rn_revenue_growth_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_revenue_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN revenue_growth_5y END), 2) AS revenue_growth_5y_median_sector,
									ROUND(AVG(CASE WHEN rn_revenue_growth_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_revenue_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN revenue_growth_10y END), 2) AS revenue_growth_10y_median_sector,
									ROUND(AVG(CASE WHEN rn_revenue_cagr_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_revenue_cagr_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN revenue_cagr_3y END), 2) AS revenue_cagr_3y_median_sector,
									ROUND(AVG(CASE WHEN rn_revenue_cagr_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_revenue_cagr_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN revenue_cagr_5y END), 2) AS revenue_cagr_5y_median_sector,
									ROUND(AVG(CASE WHEN rn_revenue_cagr_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_revenue_cagr_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN revenue_cagr_10y END), 2) AS revenue_cagr_10y_median_sector,
									ROUND(AVG(CASE WHEN rn_ebitda_growth_1y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_ebitda_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN ebitda_growth_1y END), 2) AS ebitda_growth_1y_median_sector,
									ROUND(AVG(CASE WHEN rn_ebitda_growth_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_ebitda_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN ebitda_growth_3y END), 2) AS ebitda_growth_3y_median_sector,
									ROUND(AVG(CASE WHEN rn_ebitda_growth_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_ebitda_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN ebitda_growth_5y END), 2) AS ebitda_growth_5y_median_sector,
									ROUND(AVG(CASE WHEN rn_ebitda_growth_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_ebitda_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN ebitda_growth_10y END), 2) AS ebitda_growth_10y_median_sector,
									ROUND(AVG(CASE WHEN rn_ebitda_cagr_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_ebitda_cagr_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN ebitda_cagr_3y END), 2) AS ebitda_cagr_3y_median_sector,
									ROUND(AVG(CASE WHEN rn_ebitda_cagr_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_ebitda_cagr_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN ebitda_cagr_5y END), 2) AS ebitda_cagr_5y_median_sector,
									ROUND(AVG(CASE WHEN rn_ebitda_cagr_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_ebitda_cagr_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN ebitda_cagr_10y END), 2) AS ebitda_cagr_10y_median_sector,
									ROUND(AVG(CASE WHEN rn_operatingIncome_growth_1y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_operatingIncome_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN operatingIncome_growth_1y END), 2) AS operatingIncome_growth_1y_median_sector,
									ROUND(AVG(CASE WHEN rn_operatingIncome_growth_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_operatingIncome_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN operatingIncome_growth_3y END), 2) AS operatingIncome_growth_3y_median_sector,
									ROUND(AVG(CASE WHEN rn_operatingIncome_growth_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_operatingIncome_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN operatingIncome_growth_5y END), 2) AS operatingIncome_growth_5y_median_sector,
									ROUND(AVG(CASE WHEN rn_operatingIncome_growth_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_operatingIncome_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN operatingIncome_growth_10y END), 2) AS operatingIncome_growth_10y_median_sector,
									ROUND(AVG(CASE WHEN rn_operatingIncome_cagr_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_operatingIncome_cagr_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN operatingIncome_cagr_3y END), 2) AS operatingIncome_cagr_3y_median_sector,
									ROUND(AVG(CASE WHEN rn_operatingIncome_cagr_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_operatingIncome_cagr_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN operatingIncome_cagr_5y END), 2) AS operatingIncome_cagr_5y_median_sector,
									ROUND(AVG(CASE WHEN rn_operatingIncome_cagr_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_operatingIncome_cagr_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN operatingIncome_cagr_10y END), 2) AS operatingIncome_cagr_10y_median_sector,
									ROUND(AVG(CASE WHEN rn_netIncome_growth_1y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netIncome_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netIncome_growth_1y END), 2) AS netIncome_growth_1y_median_sector,
									ROUND(AVG(CASE WHEN rn_netIncome_growth_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netIncome_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netIncome_growth_3y END), 2) AS netIncome_growth_3y_median_sector,
									ROUND(AVG(CASE WHEN rn_netIncome_growth_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netIncome_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netIncome_growth_5y END), 2) AS netIncome_growth_5y_median_sector,
									ROUND(AVG(CASE WHEN rn_netIncome_growth_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netIncome_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netIncome_growth_10y END), 2) AS netIncome_growth_10y_median_sector,
									ROUND(AVG(CASE WHEN rn_netIncome_cagr_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netIncome_cagr_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netIncome_cagr_3y END), 2) AS netIncome_cagr_3y_median_sector,
									ROUND(AVG(CASE WHEN rn_netIncome_cagr_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netIncome_cagr_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netIncome_cagr_5y END), 2) AS netIncome_cagr_5y_median_sector,
									ROUND(AVG(CASE WHEN rn_netIncome_cagr_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netIncome_cagr_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netIncome_cagr_10y END), 2) AS netIncome_cagr_10y_median_sector,
									ROUND(AVG(CASE WHEN rn_eps_growth_1y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_eps_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN eps_growth_1y END), 2) AS eps_growth_1y_median_sector,
									ROUND(AVG(CASE WHEN rn_eps_growth_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_eps_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN eps_growth_3y END), 2) AS eps_growth_3y_median_sector,
									ROUND(AVG(CASE WHEN rn_eps_growth_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_eps_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN eps_growth_5y END), 2) AS eps_growth_5y_median_sector,
									ROUND(AVG(CASE WHEN rn_eps_growth_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_eps_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN eps_growth_10y END), 2) AS eps_growth_10y_median_sector,
									ROUND(AVG(CASE WHEN rn_eps_cagr_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_eps_cagr_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN eps_cagr_3y END), 2) AS eps_cagr_3y_median_sector,
									ROUND(AVG(CASE WHEN rn_eps_cagr_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_eps_cagr_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN eps_cagr_5y END), 2) AS eps_cagr_5y_median_sector,
									ROUND(AVG(CASE WHEN rn_eps_cagr_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_eps_cagr_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN eps_cagr_10y END), 2) AS eps_cagr_10y_median_sector,
						            ROUND(AVG(CASE WHEN rn_debt_growth_1y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_debt_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN debt_growth_1y END), 2) AS debt_growth_1y_median_sector,
						            ROUND(AVG(CASE WHEN rn_debt_growth_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_debt_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN debt_growth_3y END), 2) AS debt_growth_3y_median_sector,
					                ROUND(AVG(CASE WHEN rn_debt_growth_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_debt_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN debt_growth_5y END), 2) AS debt_growth_5y_median_sector,
					                ROUND(AVG(CASE WHEN rn_debt_growth_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_debt_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN debt_growth_10y END), 2) AS debt_growth_10y_median_sector,
					                ROUND(AVG(CASE WHEN rn_netDebt_growth_1y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netDebt_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netDebt_growth_1y END), 2) AS netDebt_growth_1y_median_sector,
					                ROUND(AVG(CASE WHEN rn_netDebt_growth_3y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netDebt_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netDebt_growth_3y END), 2) AS netDebt_growth_3y_median_sector,
					                ROUND(AVG(CASE WHEN rn_netDebt_growth_5y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netDebt_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netDebt_growth_5y END), 2) AS netDebt_growth_5y_median_sector,
					                ROUND(AVG(CASE WHEN rn_netDebt_growth_10y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_netDebt_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN netDebt_growth_10y END), 2) AS netDebt_growth_10y_median_sector
					FROM ranked_data
					GROUP BY sector_id, exchange_id, period_end_date
					ORDER BY sector_id, exchange_id, period_end_date) t ) AS sub on mim.sector_id = sub.sector_id
																				  AND mim.exchange_id = sub.exchange_id
																				  AND mim.period_end_date = sub.period_end_date
		SET mim.revenue_growth_1y_median_sector = sub.revenue_growth_1y_median_sector,
			mim.revenue_growth_3y_median_sector = sub.revenue_growth_3y_median_sector,
			mim.revenue_growth_5y_median_sector = sub.revenue_growth_5y_median_sector,
			mim.revenue_growth_10y_median_sector = sub.revenue_growth_10y_median_sector,
			mim.revenue_cagr_3y_median_sector = sub.revenue_cagr_3y_median_sector,
			mim.revenue_cagr_5y_median_sector = sub.revenue_cagr_5y_median_sector,
			mim.revenue_cagr_10y_median_sector = sub.revenue_cagr_10y_median_sector,
			mim.ebitda_growth_1y_median_sector = sub.ebitda_growth_1y_median_sector,
			mim.ebitda_growth_3y_median_sector = sub.ebitda_growth_3y_median_sector,
			mim.ebitda_growth_5y_median_sector = sub.ebitda_growth_5y_median_sector,
			mim.ebitda_growth_10y_median_sector = sub.ebitda_growth_10y_median_sector,
			mim.ebitda_cagr_3y_median_sector = sub.ebitda_cagr_3y_median_sector,
			mim.ebitda_cagr_5y_median_sector = sub.ebitda_cagr_5y_median_sector,
			mim.ebitda_cagr_10y_median_sector = sub.ebitda_cagr_10y_median_sector,
			mim.operatingIncome_growth_1y_median_sector = sub.operatingIncome_growth_1y_median_sector,
			mim.operatingIncome_growth_3y_median_sector = sub.operatingIncome_growth_3y_median_sector,
			mim.operatingIncome_growth_5y_median_sector = sub.operatingIncome_growth_5y_median_sector,
			mim.operatingIncome_growth_10y_median_sector = sub.operatingIncome_growth_10y_median_sector,
			mim.operatingIncome_cagr_3y_median_sector = sub.operatingIncome_cagr_3y_median_sector,
			mim.operatingIncome_cagr_5y_median_sector = sub.operatingIncome_cagr_5y_median_sector,
			mim.operatingIncome_cagr_10y_median_sector = sub.operatingIncome_cagr_10y_median_sector,
			mim.netIncome_growth_1y_median_sector = sub.netIncome_growth_1y_median_sector,
			mim.netIncome_growth_3y_median_sector = sub.netIncome_growth_3y_median_sector,
			mim.netIncome_growth_5y_median_sector = sub.netIncome_growth_5y_median_sector,
			mim.netIncome_growth_10y_median_sector = sub.netIncome_growth_10y_median_sector,
			mim.netIncome_cagr_3y_median_sector = sub.netIncome_cagr_3y_median_sector,
			mim.netIncome_cagr_5y_median_sector = sub.netIncome_cagr_5y_median_sector,
			mim.netIncome_cagr_10y_median_sector = sub.netIncome_cagr_10y_median_sector,
			mim.eps_growth_1y_median_sector = sub.eps_growth_1y_median_sector,
			mim.eps_growth_3y_median_sector = sub.eps_growth_3y_median_sector,
			mim.eps_growth_5y_median_sector = sub.eps_growth_5y_median_sector,
			mim.eps_growth_10y_median_sector = sub.eps_growth_10y_median_sector,
			mim.eps_cagr_3y_median_sector = sub.eps_cagr_3y_median_sector,
			mim.eps_cagr_5y_median_sector = sub.eps_cagr_5y_median_sector,
			mim.eps_cagr_10y_median_sector = sub.eps_cagr_10y_median_sector,
			mim.debt_growth_1y_median_sector = sub.debt_growth_1y_median_sector,
			mim.debt_growth_3y_median_sector = sub.debt_growth_3y_median_sector,
			mim.debt_growth_5y_median_sector = sub.debt_growth_5y_median_sector,
			mim.debt_growth_10y_median_sector = sub.debt_growth_10y_median_sector,
			mim.netDebt_growth_1y_median_sector = sub.netDebt_growth_1y_median_sector,
			mim.netDebt_growth_3y_median_sector = sub.netDebt_growth_3y_median_sector,
			mim.netDebt_growth_5y_median_sector = sub.netDebt_growth_5y_median_sector,
			mim.netDebt_growth_10y_median_sector = sub.netDebt_growth_10y_median_sector;
		
	-- фиксируем обновление
    COMMIT;	
	
   	INSERT INTO median_sector_metrics (period_end_date, sector_id, exchange_id,
									revenue_growth_1y_median_sector,
									revenue_growth_3y_median_sector,
									revenue_growth_5y_median_sector,
									revenue_growth_10y_median_sector,
									revenue_cagr_3y_median_sector,
									revenue_cagr_5y_median_sector,
									revenue_cagr_10y_median_sector,
									ebitda_growth_1y_median_sector,
									ebitda_growth_3y_median_sector,
									ebitda_growth_5y_median_sector,
									ebitda_growth_10y_median_sector,
									ebitda_cagr_3y_median_sector,
									ebitda_cagr_5y_median_sector,
									ebitda_cagr_10y_median_sector,
									operatingIncome_growth_1y_median_sector,
									operatingIncome_growth_3y_median_sector,
									operatingIncome_growth_5y_median_sector,
									operatingIncome_growth_10y_median_sector,
									operatingIncome_cagr_3y_median_sector,
									operatingIncome_cagr_5y_median_sector,
									operatingIncome_cagr_10y_median_sector,
									netIncome_growth_1y_median_sector,
									netIncome_growth_3y_median_sector,
									netIncome_growth_5y_median_sector,
									netIncome_growth_10y_median_sector,
									netIncome_cagr_3y_median_sector,
									netIncome_cagr_5y_median_sector,
									netIncome_cagr_10y_median_sector,
									eps_growth_1y_median_sector,
									eps_growth_3y_median_sector,
									eps_growth_5y_median_sector,
									eps_growth_10y_median_sector,
									eps_cagr_3y_median_sector,
									eps_cagr_5y_median_sector,
									eps_cagr_10y_median_sector,
									debt_growth_1y_median_sector,
									debt_growth_3y_median_sector,
									debt_growth_5y_median_sector,
									debt_growth_10y_median_sector,
									netDebt_growth_1y_median_sector,
									netDebt_growth_3y_median_sector,
									netDebt_growth_5y_median_sector,
									netDebt_growth_10y_median_sector)
	SELECT CONCAT_WS('-', YEAR(CURDATE()), '12', '31') as period_end_date, sector_id, exchange_id,
		    revenue_growth_1y_median_sector,
			revenue_growth_3y_median_sector,
			revenue_growth_5y_median_sector,
			revenue_growth_10y_median_sector,
			revenue_cagr_3y_median_sector,
			revenue_cagr_5y_median_sector,
			revenue_cagr_10y_median_sector,
			ebitda_growth_1y_median_sector,
			ebitda_growth_3y_median_sector,
			ebitda_growth_5y_median_sector,
			ebitda_growth_10y_median_sector,
			ebitda_cagr_3y_median_sector,
			ebitda_cagr_5y_median_sector,
			ebitda_cagr_10y_median_sector,
			operatingIncome_growth_1y_median_sector,
			operatingIncome_growth_3y_median_sector,
			operatingIncome_growth_5y_median_sector,
			operatingIncome_growth_10y_median_sector,
			operatingIncome_cagr_3y_median_sector,
			operatingIncome_cagr_5y_median_sector,
			operatingIncome_cagr_10y_median_sector,
			netIncome_growth_1y_median_sector,
			netIncome_growth_3y_median_sector,
			netIncome_growth_5y_median_sector,
			netIncome_growth_10y_median_sector,
			netIncome_cagr_3y_median_sector,
			netIncome_cagr_5y_median_sector,
			netIncome_cagr_10y_median_sector,
			eps_growth_1y_median_sector,
			eps_growth_3y_median_sector,
			eps_growth_5y_median_sector,
			eps_growth_10y_median_sector,
			eps_cagr_3y_median_sector,
			eps_cagr_5y_median_sector,
			eps_cagr_10y_median_sector,
			debt_growth_1y_median_sector,
			debt_growth_3y_median_sector,
			debt_growth_5y_median_sector,
			debt_growth_10y_median_sector,
			netDebt_growth_1y_median_sector,
			netDebt_growth_3y_median_sector,
			netDebt_growth_5y_median_sector,
			netDebt_growth_10y_median_sector
	FROM(
		WITH ranked_data AS (                                           
				SELECT lfi.company_id, lfi.end_date, c.sector_id, c.exchange_id, 
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
						debt_growth_1y,
						debt_growth_3y,
						debt_growth_5y,
						debt_growth_10y,
						netDebt_growth_1y,
						netDebt_growth_3y,
						netDebt_growth_5y,
						netDebt_growth_10y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by revenue_growth_1y) as rn_revenue_growth_1y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by revenue_growth_3y) as rn_revenue_growth_3y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by revenue_growth_5y) as rn_revenue_growth_5y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by revenue_growth_10y) as rn_revenue_growth_10y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by revenue_cagr_3y) as rn_revenue_cagr_3y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by revenue_cagr_5y) as rn_revenue_cagr_5y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by revenue_cagr_10y) as rn_revenue_cagr_10y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by ebitda_growth_1y) as rn_ebitda_growth_1y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by ebitda_growth_3y) as rn_ebitda_growth_3y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by ebitda_growth_5y) as rn_ebitda_growth_5y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by ebitda_growth_10y) as rn_ebitda_growth_10y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by ebitda_cagr_3y) as rn_ebitda_cagr_3y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by ebitda_cagr_5y) as rn_ebitda_cagr_5y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by ebitda_cagr_10y) as rn_ebitda_cagr_10y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by operatingIncome_growth_1y) as rn_operatingIncome_growth_1y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by operatingIncome_growth_3y) as rn_operatingIncome_growth_3y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by operatingIncome_growth_5y) as rn_operatingIncome_growth_5y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by operatingIncome_growth_10y) as rn_operatingIncome_growth_10y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by operatingIncome_cagr_3y) as rn_operatingIncome_cagr_3y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by operatingIncome_cagr_5y) as rn_operatingIncome_cagr_5y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by operatingIncome_cagr_10y) as rn_operatingIncome_cagr_10y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by netIncome_growth_1y) as rn_netIncome_growth_1y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by netIncome_growth_3y) as rn_netIncome_growth_3y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by netIncome_growth_5y) as rn_netIncome_growth_5y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by netIncome_growth_10y) as rn_netIncome_growth_10y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by netIncome_cagr_3y) as rn_netIncome_cagr_3y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by netIncome_cagr_5y) as rn_netIncome_cagr_5y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by netIncome_cagr_10y) as rn_netIncome_cagr_10y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by eps_growth_1y) as rn_eps_growth_1y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by eps_growth_3y) as rn_eps_growth_3y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by eps_growth_5y) as rn_eps_growth_5y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by eps_growth_10y) as rn_eps_growth_10y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by eps_cagr_3y) as rn_eps_cagr_3y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by eps_cagr_5y) as rn_eps_cagr_5y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by eps_cagr_10y) as rn_eps_cagr_10y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by debt_growth_1y) as rn_debt_growth_1y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by debt_growth_3y) as rn_debt_growth_3y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by debt_growth_5y) as rn_debt_growth_5y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by debt_growth_10y) as rn_debt_growth_10y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by netDebt_growth_1y) as rn_netDebt_growth_1y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by netDebt_growth_3y) as rn_netDebt_growth_3y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by netDebt_growth_5y) as rn_netDebt_growth_5y,
						ROW_NUMBER() over(PARTITION BY c.sector_id, c.exchange_id order by netDebt_growth_10y) as rn_netDebt_growth_10y,
						COUNT(*) OVER (PARTITION BY c.sector_id, c.exchange_id) AS total_rows
				from ltm_financial_indicators lfi
				join companies c on c.id = lfi.company_id
				where lfi.end_date = (SELECT MAX(lfi1.end_date) FROM ltm_financial_indicators lfi1 where lfi.company_id = lfi1.company_id group by lfi1.company_id)
				)
				SELECT sector_id, exchange_id,
							ROUND(AVG(CASE WHEN rn_revenue_growth_1y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_revenue_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN revenue_growth_1y END), 2) AS revenue_growth_1y_median_sector,
							ROUND(AVG(CASE WHEN rn_revenue_growth_3y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_revenue_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN revenue_growth_3y END), 2) AS revenue_growth_3y_median_sector,
							ROUND(AVG(CASE WHEN rn_revenue_growth_5y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_revenue_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN revenue_growth_5y END), 2) AS revenue_growth_5y_median_sector,
							ROUND(AVG(CASE WHEN rn_revenue_growth_10y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_revenue_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN revenue_growth_10y END), 2) AS revenue_growth_10y_median_sector,
							ROUND(AVG(CASE WHEN rn_revenue_cagr_3y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_revenue_cagr_3y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN revenue_cagr_3y END), 2) AS revenue_cagr_3y_median_sector,
							ROUND(AVG(CASE WHEN rn_revenue_cagr_5y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_revenue_cagr_5y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN revenue_cagr_5y END), 2) AS revenue_cagr_5y_median_sector,
							ROUND(AVG(CASE WHEN rn_revenue_cagr_10y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_revenue_cagr_10y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN revenue_cagr_10y END), 2) AS revenue_cagr_10y_median_sector,
							ROUND(AVG(CASE WHEN rn_ebitda_growth_1y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ebitda_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ebitda_growth_1y END), 2) AS ebitda_growth_1y_median_sector,
							ROUND(AVG(CASE WHEN rn_ebitda_growth_3y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ebitda_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ebitda_growth_3y END), 2) AS ebitda_growth_3y_median_sector,
							ROUND(AVG(CASE WHEN rn_ebitda_growth_5y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ebitda_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ebitda_growth_5y END), 2) AS ebitda_growth_5y_median_sector,
							ROUND(AVG(CASE WHEN rn_ebitda_growth_10y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ebitda_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ebitda_growth_10y END), 2) AS ebitda_growth_10y_median_sector,
							ROUND(AVG(CASE WHEN rn_ebitda_cagr_3y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ebitda_cagr_3y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ebitda_cagr_3y END), 2) AS ebitda_cagr_3y_median_sector,
							ROUND(AVG(CASE WHEN rn_ebitda_cagr_5y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ebitda_cagr_5y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ebitda_cagr_5y END), 2) AS ebitda_cagr_5y_median_sector,
							ROUND(AVG(CASE WHEN rn_ebitda_cagr_10y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ebitda_cagr_10y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ebitda_cagr_10y END), 2) AS ebitda_cagr_10y_median_sector,
							ROUND(AVG(CASE WHEN rn_operatingIncome_growth_1y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_operatingIncome_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN operatingIncome_growth_1y END), 2) AS operatingIncome_growth_1y_median_sector,
							ROUND(AVG(CASE WHEN rn_operatingIncome_growth_3y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_operatingIncome_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN operatingIncome_growth_3y END), 2) AS operatingIncome_growth_3y_median_sector,
							ROUND(AVG(CASE WHEN rn_operatingIncome_growth_5y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_operatingIncome_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN operatingIncome_growth_5y END), 2) AS operatingIncome_growth_5y_median_sector,
							ROUND(AVG(CASE WHEN rn_operatingIncome_growth_10y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_operatingIncome_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN operatingIncome_growth_10y END), 2) AS operatingIncome_growth_10y_median_sector,
							ROUND(AVG(CASE WHEN rn_operatingIncome_cagr_3y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_operatingIncome_cagr_3y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN operatingIncome_cagr_3y END), 2) AS operatingIncome_cagr_3y_median_sector,
							ROUND(AVG(CASE WHEN rn_operatingIncome_cagr_5y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_operatingIncome_cagr_5y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN operatingIncome_cagr_5y END), 2) AS operatingIncome_cagr_5y_median_sector,
							ROUND(AVG(CASE WHEN rn_operatingIncome_cagr_10y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_operatingIncome_cagr_10y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN operatingIncome_cagr_10y END), 2) AS operatingIncome_cagr_10y_median_sector,
							ROUND(AVG(CASE WHEN rn_netIncome_growth_1y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_netIncome_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN netIncome_growth_1y END), 2) AS netIncome_growth_1y_median_sector,
							ROUND(AVG(CASE WHEN rn_netIncome_growth_3y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_netIncome_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN netIncome_growth_3y END), 2) AS netIncome_growth_3y_median_sector,
							ROUND(AVG(CASE WHEN rn_netIncome_growth_5y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_netIncome_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN netIncome_growth_5y END), 2) AS netIncome_growth_5y_median_sector,
							ROUND(AVG(CASE WHEN rn_netIncome_growth_10y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_netIncome_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN netIncome_growth_10y END), 2) AS netIncome_growth_10y_median_sector,
							ROUND(AVG(CASE WHEN rn_netIncome_cagr_3y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_netIncome_cagr_3y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN netIncome_cagr_3y END), 2) AS netIncome_cagr_3y_median_sector,
							ROUND(AVG(CASE WHEN rn_netIncome_cagr_5y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_netIncome_cagr_5y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN netIncome_cagr_5y END), 2) AS netIncome_cagr_5y_median_sector,
							ROUND(AVG(CASE WHEN rn_netIncome_cagr_10y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_netIncome_cagr_10y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN netIncome_cagr_10y END), 2) AS netIncome_cagr_10y_median_sector,
							ROUND(AVG(CASE WHEN rn_eps_growth_1y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_eps_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN eps_growth_1y END), 2) AS eps_growth_1y_median_sector,
							ROUND(AVG(CASE WHEN rn_eps_growth_3y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_eps_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN eps_growth_3y END), 2) AS eps_growth_3y_median_sector,
							ROUND(AVG(CASE WHEN rn_eps_growth_5y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_eps_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN eps_growth_5y END), 2) AS eps_growth_5y_median_sector,
							ROUND(AVG(CASE WHEN rn_eps_growth_10y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_eps_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN eps_growth_10y END), 2) AS eps_growth_10y_median_sector,
							ROUND(AVG(CASE WHEN rn_eps_cagr_3y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_eps_cagr_3y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN eps_cagr_3y END), 2) AS eps_cagr_3y_median_sector,
							ROUND(AVG(CASE WHEN rn_eps_cagr_5y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_eps_cagr_5y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN eps_cagr_5y END), 2) AS eps_cagr_5y_median_sector,
							ROUND(AVG(CASE WHEN rn_eps_cagr_10y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_eps_cagr_10y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN eps_cagr_10y END), 2) AS eps_cagr_10y_median_sector,
				            ROUND(AVG(CASE WHEN rn_debt_growth_1y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_debt_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN debt_growth_1y END), 2) AS debt_growth_1y_median_sector,
				            ROUND(AVG(CASE WHEN rn_debt_growth_3y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_debt_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN debt_growth_3y END), 2) AS debt_growth_3y_median_sector,
			                ROUND(AVG(CASE WHEN rn_debt_growth_5y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_debt_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN debt_growth_5y END), 2) AS debt_growth_5y_median_sector,
			                ROUND(AVG(CASE WHEN rn_debt_growth_10y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_debt_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN debt_growth_10y END), 2) AS debt_growth_10y_median_sector,
			                ROUND(AVG(CASE WHEN rn_netDebt_growth_1y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_netDebt_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN netDebt_growth_1y END), 2) AS netDebt_growth_1y_median_sector,
			                ROUND(AVG(CASE WHEN rn_netDebt_growth_3y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_netDebt_growth_3y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN netDebt_growth_3y END), 2) AS netDebt_growth_3y_median_sector,
			                ROUND(AVG(CASE WHEN rn_netDebt_growth_5y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_netDebt_growth_5y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN netDebt_growth_5y END), 2) AS netDebt_growth_5y_median_sector,
			                ROUND(AVG(CASE WHEN rn_netDebt_growth_10y = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_netDebt_growth_10y IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN netDebt_growth_10y END), 2) AS netDebt_growth_10y_median_sector
			FROM ranked_data
			GROUP BY sector_id, exchange_id
			ORDER BY sector_id, exchange_id) t
			ON DUPLICATE KEY UPDATE revenue_growth_1y_median_sector = t.revenue_growth_1y_median_sector,
									revenue_growth_3y_median_sector = t.revenue_growth_3y_median_sector,
									revenue_growth_5y_median_sector = t.revenue_growth_5y_median_sector,
									revenue_growth_10y_median_sector = t.revenue_growth_10y_median_sector,
									revenue_cagr_3y_median_sector = t.revenue_cagr_3y_median_sector,
									revenue_cagr_5y_median_sector = t.revenue_cagr_5y_median_sector,
									revenue_cagr_10y_median_sector = t.revenue_cagr_10y_median_sector,
									ebitda_growth_1y_median_sector = t.ebitda_growth_1y_median_sector,
									ebitda_growth_3y_median_sector = t.ebitda_growth_3y_median_sector,
									ebitda_growth_5y_median_sector = t.ebitda_growth_5y_median_sector,
									ebitda_growth_10y_median_sector = t.ebitda_growth_10y_median_sector,
									ebitda_cagr_3y_median_sector = t.ebitda_cagr_3y_median_sector,
									ebitda_cagr_5y_median_sector = t.ebitda_cagr_5y_median_sector,
									ebitda_cagr_10y_median_sector = t.ebitda_cagr_10y_median_sector,
									operatingIncome_growth_1y_median_sector = t.operatingIncome_growth_1y_median_sector,
									operatingIncome_growth_3y_median_sector = t.operatingIncome_growth_3y_median_sector,
									operatingIncome_growth_5y_median_sector = t.operatingIncome_growth_5y_median_sector,
									operatingIncome_growth_10y_median_sector = t.operatingIncome_growth_10y_median_sector,
									operatingIncome_cagr_3y_median_sector = t.operatingIncome_cagr_3y_median_sector,
									operatingIncome_cagr_5y_median_sector = t.operatingIncome_cagr_5y_median_sector,
									operatingIncome_cagr_10y_median_sector = t.operatingIncome_cagr_10y_median_sector,
									netIncome_growth_1y_median_sector = t.netIncome_growth_1y_median_sector,
									netIncome_growth_3y_median_sector = t.netIncome_growth_3y_median_sector,
									netIncome_growth_5y_median_sector = t.netIncome_growth_5y_median_sector,
									netIncome_growth_10y_median_sector = t.netIncome_growth_10y_median_sector,
									netIncome_cagr_3y_median_sector = t.netIncome_cagr_3y_median_sector,
									netIncome_cagr_5y_median_sector = t.netIncome_cagr_5y_median_sector,
									netIncome_cagr_10y_median_sector = t.netIncome_cagr_10y_median_sector,
									eps_growth_1y_median_sector = t.eps_growth_1y_median_sector,
									eps_growth_3y_median_sector = t.eps_growth_3y_median_sector,
									eps_growth_5y_median_sector = t.eps_growth_5y_median_sector,
									eps_growth_10y_median_sector = t.eps_growth_10y_median_sector,
									eps_cagr_3y_median_sector = t.eps_cagr_3y_median_sector,
									eps_cagr_5y_median_sector = t.eps_cagr_5y_median_sector,
									eps_cagr_10y_median_sector = t.eps_cagr_10y_median_sector,
									debt_growth_1y_median_sector = t.debt_growth_1y_median_sector,
									debt_growth_3y_median_sector = t.debt_growth_3y_median_sector,
									debt_growth_5y_median_sector = t.debt_growth_5y_median_sector,
									debt_growth_10y_median_sector = t.debt_growth_10y_median_sector,
									netDebt_growth_1y_median_sector = t.netDebt_growth_1y_median_sector,
									netDebt_growth_3y_median_sector = t.netDebt_growth_3y_median_sector,
									netDebt_growth_5y_median_sector = t.netDebt_growth_5y_median_sector,
									netDebt_growth_10y_median_sector = t.netDebt_growth_10y_median_sector;
   	 -- заносим данные в таблицу 
    UPDATE median_sector_metrics AS mim
    JOIN ( SELECT t.*
    	   FROM(
    	   		WITH ranked_data AS(
							SELECT dg.company_id,
									c.sector_id, c.exchange_id,
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
									dg.Div_yield_year,
									dg.3y_Avg_Div_yield,
									dg.5y_Avg_Div_yield,
									dg.10y_Avg_Div_yield, 
									dg.Div_CAGR_3Y,
									dg.Div_CAGR_5Y,
									dg.Div_CAGR_10Y,
									Div_yield_year_ltm,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by DFCFR) as rn_DFCFR,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by DPR) as rn_DPR,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by continuous_div_start_year) as rn_continuous_div_start_year,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by continuous_div_grow_year) as rn_continuous_div_grow_year,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by dsi) as rn_dsi,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by DGR) as rn_DGR,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by DGR_3Y) as rn_DGR_3Y,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by DGR_5Y) as rn_DGR_5Y,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by DGR_10Y) as rn_DGR_10Y,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by Div_yield_year) as rn_Div_yield_year,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by 3y_Avg_Div_yield) as rn_3y_Avg_Div_yield,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by 5y_Avg_Div_yield) as rn_5y_Avg_Div_yield,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by 10y_Avg_Div_yield) as rn_10y_Avg_Div_yield,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by Div_CAGR_3Y) as rn_Div_CAGR_3Y,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by Div_CAGR_5Y) as rn_Div_CAGR_5Y,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by Div_CAGR_10Y) as rn_Div_CAGR_10Y,
									ROW_NUMBER() over(PARTITION by dg.year, c.sector_id, c.exchange_id order by Div_yield_year_ltm) as rn_Div_yield_year_ltm,
									COUNT(*) OVER (PARTITION BY dg.year, c.sector_id, c.exchange_id) AS total_rows
							from dividend_graphs dg 
							join companies c on dg.company_id = c.id
							order by dg.year, c.sector_id, c.exchange_id)
					SELECT CONCAT_WS('-', rd.year, '12', '31') as period_end_date, rd.sector_id, rd.exchange_id,
									ROUND(AVG(CASE WHEN rn_DFCFR = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_DFCFR IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN DFCFR END), 2) AS DFCFR_median_sector,
									ROUND(AVG(CASE WHEN rn_DPR = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_DPR IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN DPR END), 2) AS DPR_median_sector,
									ROUND(AVG(CASE WHEN rn_continuous_div_start_year = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_continuous_div_start_year IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN continuous_div_start_year END), 2) AS continuous_div_start_year_median_sector,
									ROUND(AVG(CASE WHEN rn_continuous_div_grow_year = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_continuous_div_grow_year IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN continuous_div_grow_year END), 2) AS continuous_div_grow_year_median_sector,
									ROUND(AVG(CASE WHEN rn_dsi = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_dsi IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN dsi END), 2) AS dsi_median_sector,
									ROUND(AVG(CASE WHEN rn_DGR = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_DGR IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN DGR END), 2) AS DGR_median_sector,
									ROUND(AVG(CASE WHEN rn_DGR_3Y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_DGR_3Y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN DGR_3Y END), 2) AS DGR_3Y_median_sector,
									ROUND(AVG(CASE WHEN rn_DGR_5Y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_DGR_5Y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN DGR_5Y END), 2) AS DGR_5Y_median_sector,
									ROUND(AVG(CASE WHEN rn_DGR_10Y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_DGR_10Y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN DGR_10Y END), 2) AS DGR_10Y_median_sector,
						            ROUND(AVG(CASE WHEN rn_Div_yield_year = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_Div_yield_year IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN Div_yield_year END), 2) AS Div_yield_year_median_sector,
							        ROUND(AVG(CASE WHEN rn_3y_Avg_Div_yield = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_3y_Avg_Div_yield IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN 3y_Avg_Div_yield END), 2) AS 3y_Avg_Div_yield_median_sector,
							        ROUND(AVG(CASE WHEN rn_5y_Avg_Div_yield = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_5y_Avg_Div_yield IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN 5y_Avg_Div_yield END), 2) AS 5y_Avg_Div_yield_median_sector,
							        ROUND(AVG(CASE WHEN rn_10y_Avg_Div_yield = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_10y_Avg_Div_yield IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN rn_10y_Avg_Div_yield END), 2) AS 10y_Avg_Div_yield_median_sector,
							        ROUND(AVG(CASE WHEN rn_Div_CAGR_3Y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_Div_CAGR_3Y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN Div_CAGR_3Y END), 2) AS Div_CAGR_3Y_median_sector,
							        ROUND(AVG(CASE WHEN rn_Div_CAGR_5Y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_Div_CAGR_5Y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN Div_CAGR_5Y END), 2) AS Div_CAGR_5Y_median_sector,
							        ROUND(AVG(CASE WHEN rn_Div_CAGR_10Y = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_Div_CAGR_10Y IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN Div_CAGR_10Y END), 2) AS Div_CAGR_10Y_median_sector,
					                ROUND(AVG(CASE WHEN rn_Div_yield_year_ltm = FLOOR((total_rows + 1) / 2) 
											                OR (total_rows % 2 = 0 AND rn_Div_yield_year_ltm IN (total_rows / 2, total_rows / 2 + 1)) 
											                THEN Div_yield_year_ltm END), 2) AS Div_yield_year_ltm_median_sector
					FROM ranked_data rd
					GROUP BY rd.year, rd.sector_id, rd.exchange_id
					ORDER BY rd.year, rd.sector_id, rd.exchange_id) t ) AS sub on mim.sector_id = sub.sector_id
																				AND mim.exchange_id = sub.exchange_id
																				AND mim.period_end_date = sub.period_end_date
	SET mim.DFCFR_median_sector = sub.DFCFR_median_sector,
		mim.DPR_median_sector = sub.DPR_median_sector,
		mim.continuous_div_start_year_median_sector = sub.continuous_div_start_year_median_sector,
		mim.continuous_div_grow_year_median_sector = sub.continuous_div_grow_year_median_sector,
		mim.dsi_median_sector = sub.dsi_median_sector,
		mim.DGR_median_sector = sub.DGR_median_sector,
		mim.DGR_3Y_median_sector = sub.DGR_3Y_median_sector,
		mim.DGR_5Y_median_sector = sub.DGR_5Y_median_sector,
		mim.DGR_10Y_median_sector = sub.DGR_10Y_median_sector,
		mim.Div_yield_year_median_sector = sub.Div_yield_year_median_sector,
		mim.3y_Avg_Div_yield_median_sector = sub.3y_Avg_Div_yield_median_sector,
		mim.5y_Avg_Div_yield_median_sector = sub.5y_Avg_Div_yield_median_sector,
		mim.10y_Avg_Div_yield_median_sector = sub.10y_Avg_Div_yield_median_sector,
		mim.Div_CAGR_3Y_median_sector = sub.Div_CAGR_3Y_median_sector,
		mim.Div_CAGR_5Y_median_sector = sub.Div_CAGR_5Y_median_sector,
		mim.Div_CAGR_10Y_median_sector = sub.Div_CAGR_10Y_median_sector,
		mim.Div_yield_year_ltm_median_sector = sub.Div_yield_year_ltm_median_sector;
					    
    	-- Обновление логирования процедуры после завершения
	UPDATE _procedure_calls SET finish = NOW() WHERE name = @nameDaily AND start = @startDaily;
END;

call median_sector_dividends();

ALTER TABLE median_sector_metrics
ADD COLUMN Div_yield_year_ltm_median_sector double;