median(mkt_cap) OVER (PARTITION BY company_id, period_end_date),
	PERCENTILE_CONT(0.5) WITHIN 
  			GROUP (ORDER BY mkt_cap) OVER (PARTITION BY company_id )
  			
  			
SELECT *
from (  			
	SELECT company_id, period_end_date, mkt_cap,
		ROW_NUMBER() over(PARTITION by company_id order by mkt_cap)	as rn
	FROM historical_annual_metrics ham
	WHERE company_id IN (5,6, 15, 198, 375)
	  AND YEAR(period_end_date) >= YEAR(CURDATE()) - 11
	order by company_id, mkt_cap
	 ) t 
where rn=6
 
WITH ranked_data AS (
    SELECT company_id, period_end_date, mkt_cap,
           ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY mkt_cap) AS rn,
           COUNT(*) OVER (PARTITION BY company_id) AS total_rows
    FROM historical_annual_metrics ham
    WHERE company_id IN (5, 6, 15, 198, 375, 1845)
      AND YEAR(period_end_date) >= YEAR(CURDATE()) - 11
)
SELECT company_id, period_end_date, AVG(mkt_cap)
FROM ranked_data
WHERE rn = FLOOR((total_rows + 1) / 2)  -- медиана для нечетного числа
   OR (total_rows % 2 = 0 AND rn IN (total_rows / 2, total_rows / 2 + 1))  -- медиана для четного числа
group by company_id
ORDER BY company_id, period_end_date;

call updateCompanyAverageMedian11Years();

TRUNCATE table company_average_metrics; 

SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE
FROM information_schema.ROUTINES
WHERE ROUTINE_DEFINITION LIKE '%company_average_metrics%';

drop table company_average_metrics;



CREATE OR REPLACE PROCEDURE `updateCompanyAverageMedian11Years`()
BEGIN

    set @start := NOW();
    set @name := 'updateCompanyAverageMedian11Years';
    insert into _procedure_calls (name, start, finish) value (@name, @start, null);

    insert into company_average_metrics(
    				 company_id,
                     mkt_cap,
                     p_e,
                     p_s,
                     p_bv,
                     ev_ebitda,
                     roe,
                     ev,
                     ev_ebit,
                     p_fcf,
                     ev_fcf,
                     ev_cfo,
                     ev_nopat,
                     ev_s,
                     net_debt_ebitda,
                     eps,
                     book_value_per_share,
                     dupont,
                     altman_model,
                     graham_number,
                     e_p,
                     free_cash_flow_yield,
                     roa,
                     roaa,
                     roae,
                     oroa,
                     rona,
                     rooa,
                     roic,
                     roce,
                     gross_margin,
                     ebitda_margin,
                     ebit_margin,
                     operating_margin,
                     pretax_margin,
                     net_margin,
                     div_yield,
                     div_fcf,
                     div_payout_ratio,
                     net_debt_ebit,
                     net_debt_net_income,
                     net_debt_mcap,
                     net_debt_asset,
                     debt_asset,
                     net_debt_equity,
                     debt_equity,
                     long_term_debt_to_equity,
                     long_term_debt_to_nopat,
                     free_cash_flow_debt,
                     net_debt_oibda,
                     debt_ratio,
                     debt_to_equity_ratio,
                     current_ratio,
                     cash_ratio,
                     quick_ratio,
                     solvency_ratio,
                     asset_coverage_ratio,
                     capitalization_ratio,
                     inventory_turnover,
                     receivable_turnover_ratio,
                     accounts_payable_turnover,
                     cap_turnover,
                     working_cap_turnover,
                     asset_turnover,
                     fixed_assets_turnover,
                     inv_turnover_p,
                     p_of_repayment,
                     payables_repayment_p,
                     cap_turnover_p,
                     asset_turnover_p,
                     operating_cycle,
                     p_ncf,
                     p_cfo,
                     p_nwc,
                     capex_fcf,
                     capex_revenue,
                     ev_bv,
                     p_c,
                     peg,
                     ev_ic,
                     pretax_roa,
                     pretax_roe,
                     graham,
                     oibda_margin,
                     fcf_margin,
                     croic,
					 croic_greenblatt,
					 ros,
					 reinvestment_rate,
					 debt_ebitda,
					 netDebt_FCF,
					 debt_FCF,
					 long_term_debt_to_asset,
					 interest_coverage_ratio,
					 ocfr,
					 capex_cfo,
					 capex_amortization,
					 revenue_growth_1y,
					 ebitda_growth_1y,
					 operatingIncome_growth_1y,
					 netIncome_growth_1y,
					 eps_growth_1y,
					 DGR
)
	    select t.*
	    from (
				WITH ranked_data AS (
				    SELECT ham.company_id, period_end_date,
				           mkt_cap,
				           p_e,  
				           p_s,
				           p_bv,
				           ev_ebitda,
				           roe,
				           ev,
				           ev_ebit,
				           p_fcf,
				           ev_fcf,
				           ev_cfo,
				           ev_nopat,
				           ev_s,
				           net_debt_ebitda,
				           eps,
				           book_value_per_share,
				           dupont,
				           altman_model,
				           graham_number,
				           e_p,
				           free_cash_flow_yield,
				           roa,
				           roaa,
				           roae,
				           oroa,
				           rona,
				           rooa,
				           roic,
				           roce,
				           gross_margin,
				           ebitda_margin,
				           ebit_margin,
				           operating_margin,
				           pretax_margin,
				           net_margin,
				           div_yield,
				           div_fcf,
				           div_payout_ratio,
				           net_debt_ebit,
				           net_debt_net_income,
				           net_debt_mcap,
				           net_debt_asset,
				           debt_asset,
				           net_debt_equity,
				           debt_equity,
				           long_term_debt_to_equity,
				           long_term_debt_to_nopat,
				           free_cash_flow_debt,
				           net_debt_oibda,
				           debt_ratio,
				           debt_to_equity_ratio,
				           current_ratio,
				           cash_ratio,
				           quick_ratio,
				           solvency_ratio,
				           asset_coverage_ratio,
				           capitalization_ratio,
				           inventory_turnover,
				           receivable_turnover_ratio,
				           accounts_payable_turnover,
				           cap_turnover,
				           working_cap_turnover,
				           asset_turnover,
				           fixed_assets_turnover,
				           inv_turnover_p,
				           p_of_repayment,
				           payables_repayment_p,
				           cap_turnover_p,
				           asset_turnover_p,
				           operating_cycle,
				           p_ncf,
				           p_cfo,
				           p_nwc,
				           capex_fcf,
				           capex_revenue,
				           ev_bv,
				           p_c,
				           peg,
				           ev_ic,
				           pretax_roa,
				           pretax_roe,
				           graham,
				           oibda_margin,
				           fcf_margin,
				           croic,
						   croic_greenblatt,
						   ros,
						   reinvestment_rate,
						   debt_ebitda,
						   netDebt_FCF,
						   debt_FCF,
						   long_term_debt_to_asset,
						   interest_coverage_ratio,
						   ocfr,
						   capex_cfo,
						   capex_amortization,
						   revenue_growth_1y,
						   ebitda_growth_1y,
						   operatingIncome_growth_1y,
						   netIncome_growth_1y,
						   eps_growth_1y,
						   DGR,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY mkt_cap) AS rn_mkt_cap,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY p_e) AS rn_p_e,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY p_s) AS rn_p_s,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY p_bv) AS rn_p_bv,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY ev_ebitda) AS rn_ev_ebitda,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY roe) AS rn_roe,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY ev) AS rn_ev,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY ev_ebit) AS rn_ev_ebit,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY p_fcf) AS rn_p_fcf,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY ev_fcf) AS rn_ev_fcf,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY ev_cfo) AS rn_ev_cfo,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY ev_nopat) AS rn_ev_nopat,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY ev_s) AS rn_ev_s,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY net_debt_ebitda) AS rn_net_debt_ebitda,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY eps) AS rn_eps,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY book_value_per_share) AS rn_book_value_per_share,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY dupont) AS rn_dupont,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY altman_model) AS rn_altman_model,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY graham_number) AS rn_graham_number,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY e_p) AS rn_e_p,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY free_cash_flow_yield) AS rn_free_cash_flow_yield,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY roa) AS rn_roa,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY roaa) AS rn_roaa,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY roae) AS rn_roae,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY oroa) AS rn_oroa,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY rona) AS rn_rona,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY rooa) AS rn_rooa,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY roic) AS rn_roic,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY roce) AS rn_roce,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY gross_margin) AS rn_gross_margin,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY ebitda_margin) AS rn_ebitda_margin,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY ebit_margin) AS rn_ebit_margin,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY operating_margin) AS rn_operating_margin,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY pretax_margin) AS rn_pretax_margin,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY net_margin) AS rn_net_margin,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY div_yield) AS rn_div_yield,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY div_fcf) AS rn_div_fcf,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY div_payout_ratio) AS rn_div_payout_ratio,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY net_debt_ebit) AS rn_net_debt_ebit,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY net_debt_net_income) AS rn_net_debt_net_income,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY net_debt_mcap) AS rn_net_debt_mcap,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY net_debt_asset) AS rn_net_debt_asset,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY debt_asset) AS rn_debt_asset,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY net_debt_equity) AS rn_net_debt_equity,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY debt_equity) AS rn_debt_equity,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY long_term_debt_to_equity) AS rn_long_term_debt_to_equity,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY long_term_debt_to_nopat) AS rn_long_term_debt_to_nopat,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY free_cash_flow_debt) AS rn_free_cash_flow_debt,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY net_debt_oibda) AS rn_net_debt_oibda,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY debt_ratio) AS rn_debt_ratio,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY debt_to_equity_ratio) AS rn_debt_to_equity_ratio,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY current_ratio) AS rn_current_ratio,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY cash_ratio) AS rn_cash_ratio,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY quick_ratio) AS rn_quick_ratio,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY solvency_ratio) AS rn_solvency_ratio,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY asset_coverage_ratio) AS rn_asset_coverage_ratio,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY capitalization_ratio) AS rn_capitalization_ratio,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY inventory_turnover) AS rn_inventory_turnover,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY receivable_turnover_ratio) AS rn_receivable_turnover_ratio,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY accounts_payable_turnover) AS rn_accounts_payable_turnover,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY cap_turnover) AS rn_cap_turnover,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY working_cap_turnover) AS rn_working_cap_turnover,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY asset_turnover) AS rn_asset_turnover,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY fixed_assets_turnover) AS rn_fixed_assets_turnover,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY inv_turnover_p) AS rn_inv_turnover_p,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY p_of_repayment) AS rn_p_of_repayment,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY payables_repayment_p) AS rn_payables_repayment_p,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY cap_turnover_p) AS rn_cap_turnover_p,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY asset_turnover_p) AS rn_asset_turnover_p,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY operating_cycle) AS rn_operating_cycle,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY p_ncf) AS rn_p_ncf,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY p_cfo) AS rn_p_cfo,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY p_nwc) AS rn_p_nwc,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY capex_fcf) AS rn_capex_fcf,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY capex_revenue) AS rn_capex_revenue,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY ev_bv) AS rn_ev_bv,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY p_c) AS rn_p_c,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY peg) AS rn_peg,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY ev_ic) AS rn_ev_ic,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY pretax_roa) AS rn_pretax_roa,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY pretax_roe) AS rn_pretax_roe,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY graham) AS rn_graham,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY oibda_margin) AS rn_oibda_margin,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY fcf_margin) AS rn_fcf_margin,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY croic) AS rn_croic,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY croic_greenblatt) AS rn_croic_greenblatt,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY ros) AS rn_ros,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY reinvestment_rate) AS rn_reinvestment_rate,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY debt_ebitda) AS rn_debt_ebitda,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY netDebt_FCF) AS rn_netDebt_FCF,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY debt_FCF) AS rn_debt_FCF,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY long_term_debt_to_asset) AS rn_long_term_debt_to_asset,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY interest_coverage_ratio) AS rn_interest_coverage_ratio,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY ocfr) AS rn_ocfr,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY capex_cfo) AS rn_capex_cfo,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY capex_amortization) AS rn_capex_amortization,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY revenue_growth_1y) AS rn_revenue_growth_1y,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY ebitda_growth_1y) AS rn_ebitda_growth_1y,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY operatingIncome_growth_1y) AS rn_operatingIncome_growth_1y,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY netIncome_growth_1y) AS rn_netIncome_growth_1y,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY eps_growth_1y) AS rn_eps_growth_1y,
				           ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY DGR) AS rn_DGR,
				           COUNT(*) OVER (PARTITION BY ham.company_id) AS total_rows
				    FROM historical_annual_metrics ham
				    LEFT JOIN dividend_graphs AS dg ON dg.company_id = ham.company_id AND dg.year = YEAR(ham.period_end_date)
				    WHERE YEAR(period_end_date) >= YEAR(CURDATE()) - 11
				)
				SELECT company_id,
				       AVG(CASE WHEN rn_mkt_cap = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_mkt_cap IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN mkt_cap END) AS mkt_cap_median,
				       AVG(CASE WHEN rn_p_e = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_p_e IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN p_e END) AS p_e_median,
				       AVG(CASE WHEN rn_p_s = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_p_s IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN p_s END) AS p_s_median,
				       AVG(CASE WHEN rn_p_bv = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_p_bv IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN p_bv END) AS p_bv_median,
				       AVG(CASE WHEN rn_ev_ebitda = FLOOR((total_rows + 1) / 2) 
							    OR (total_rows % 2 = 0 AND rn_ev_ebitda IN (total_rows / 2, total_rows / 2 + 1)) 
							    THEN ev_ebitda END) AS ev_ebitda_median,
					   AVG(CASE WHEN rn_roe = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_roe IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN roe END) AS roe_median,
				       AVG(CASE WHEN rn_ev = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_ev IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN ev END) AS ev_median,
				       AVG(CASE WHEN rn_ev_ebit = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_ev_ebit IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN ev_ebit END) AS ev_ebit_median,
				       AVG(CASE WHEN rn_p_fcf = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_p_fcf IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN p_fcf END) AS p_fcf_median,
				       AVG(CASE WHEN rn_ev_fcf = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_ev_fcf IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN ev_fcf END) AS ev_fcf_median,
				       AVG(CASE WHEN rn_ev_cfo = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_ev_cfo IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN ev_cfo END) AS ev_cfo_median,
				       AVG(CASE WHEN rn_ev_nopat = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_ev_nopat IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN ev_nopat END) AS ev_nopat_median,
				       AVG(CASE WHEN rn_ev_s = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_ev_s IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN ev_s END) AS ev_s_median,
				       AVG(CASE WHEN rn_net_debt_ebitda = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_net_debt_ebitda IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN net_debt_ebitda END) AS net_debt_ebitda_median,
				       AVG(CASE WHEN rn_eps = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_eps IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN eps END) AS eps_median,
				       AVG(CASE WHEN rn_book_value_per_share = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_book_value_per_share IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN book_value_per_share END) AS book_value_per_share_median,
				       AVG(CASE WHEN rn_dupont = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_dupont IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN dupont END) AS dupont_median,
				       AVG(CASE WHEN rn_altman_model = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_altman_model IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN altman_model END) AS altman_model_median,
				       AVG(CASE WHEN rn_graham_number = FLOOR((total_rows + 1) / 2) 
				                OR (total_rows % 2 = 0 AND rn_graham_number IN (total_rows / 2, total_rows / 2 + 1)) 
				                THEN graham_number END) AS graham_number_median,
				       AVG(CASE WHEN rn_e_p = FLOOR((total_rows + 1) / 2) 
						        OR (total_rows % 2 = 0 AND rn_e_p IN (total_rows / 2, total_rows / 2 + 1)) 
						        THEN e_p END) AS e_p_median,
					   AVG(CASE WHEN rn_free_cash_flow_yield = FLOOR((total_rows + 1) / 2) 
						        OR (total_rows % 2 = 0 AND rn_free_cash_flow_yield IN (total_rows / 2, total_rows / 2 + 1)) 
						        THEN free_cash_flow_yield END) AS free_cash_flow_yield_median,
					   AVG(CASE WHEN rn_roa = FLOOR((total_rows + 1) / 2) 
					            OR (total_rows % 2 = 0 AND rn_roa IN (total_rows / 2, total_rows / 2 + 1)) 
					            THEN roa END) AS roa_median,
					   AVG(CASE WHEN rn_roaa = FLOOR((total_rows + 1) / 2) 
					            OR (total_rows % 2 = 0 AND rn_roaa IN (total_rows / 2, total_rows / 2 + 1)) 
					            THEN roaa END) AS roaa_median,
					   AVG(CASE WHEN rn_roae = FLOOR((total_rows + 1) / 2) 
					            OR (total_rows % 2 = 0 AND rn_roae IN (total_rows / 2, total_rows / 2 + 1)) 
					            THEN roae END) AS roae_median,
					   AVG(CASE WHEN rn_oroa = FLOOR((total_rows + 1) / 2) 
					            OR (total_rows % 2 = 0 AND rn_oroa IN (total_rows / 2, total_rows / 2 + 1)) 
					            THEN oroa END) AS oroa_median,
					   AVG(CASE WHEN rn_rona = FLOOR((total_rows + 1) / 2) 
					            OR (total_rows % 2 = 0 AND rn_rona IN (total_rows / 2, total_rows / 2 + 1)) 
					            THEN rona END) AS rona_median,
					   AVG(CASE WHEN rn_rooa = FLOOR((total_rows + 1) / 2) 
					            OR (total_rows % 2 = 0 AND rn_rooa IN (total_rows / 2, total_rows / 2 + 1)) 
					            THEN rooa END) AS rooa_median,
					   AVG(CASE WHEN rn_roic = FLOOR((total_rows + 1) / 2) 
					            OR (total_rows % 2 = 0 AND rn_roic IN (total_rows / 2, total_rows / 2 + 1)) 
					            THEN roic END) AS roic_median,
					   AVG(CASE WHEN rn_roce = FLOOR((total_rows + 1) / 2) 
					            OR (total_rows % 2 = 0 AND rn_roce IN (total_rows / 2, total_rows / 2 + 1)) 
					            THEN roce END) AS roce_median,
					   AVG(CASE WHEN rn_gross_margin = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_gross_margin IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN gross_margin END) AS gross_margin_median,
					   AVG(CASE WHEN rn_ebitda_margin = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_ebitda_margin IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN ebitda_margin END) AS ebitda_margin_median,
					   AVG(CASE WHEN rn_ebit_margin = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_ebit_margin IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN ebit_margin END) AS ebit_margin_median,
					   AVG(CASE WHEN rn_operating_margin = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_operating_margin IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN operating_margin END) AS operating_margin_median,
					   AVG(CASE WHEN rn_pretax_margin = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_pretax_margin IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN pretax_margin END) AS pretax_margin_median,
					   AVG(CASE WHEN rn_net_margin = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_net_margin IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN net_margin END) AS net_margin_median,
					   AVG(CASE WHEN rn_div_yield = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_div_yield IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN div_yield END) AS div_yield_median,
					   AVG(CASE WHEN rn_div_fcf = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_div_fcf IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN div_fcf END) AS div_fcf_median,
					   AVG(CASE WHEN rn_div_payout_ratio = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_div_payout_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN div_payout_ratio END) AS div_payout_ratio_median,
					   AVG(CASE WHEN rn_net_debt_ebit = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_net_debt_ebit IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN net_debt_ebit END) AS net_debt_ebit_median,
					   AVG(CASE WHEN rn_net_debt_net_income = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_net_debt_net_income IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN net_debt_net_income END) AS net_debt_net_income_median,
					   AVG(CASE WHEN rn_net_debt_mcap = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_net_debt_mcap IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN net_debt_mcap END) AS net_debt_mcap_median,
					   AVG(CASE WHEN rn_net_debt_asset = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_net_debt_asset IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN net_debt_asset END) AS net_debt_asset_median,
					   AVG(CASE WHEN rn_debt_asset = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_debt_asset IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN debt_asset END) AS debt_asset_median,
					   AVG(CASE WHEN rn_net_debt_equity = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_net_debt_equity IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN net_debt_equity END) AS net_debt_equity_median,
					   AVG(CASE WHEN rn_debt_equity = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_debt_equity IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN debt_equity END) AS debt_equity_median,
					   AVG(CASE WHEN rn_long_term_debt_to_equity = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_long_term_debt_to_equity IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN long_term_debt_to_equity END) AS long_term_debt_to_equity_median,
					   AVG(CASE WHEN rn_long_term_debt_to_nopat = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_long_term_debt_to_nopat IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN long_term_debt_to_nopat END) AS long_term_debt_to_nopat_median,
				       AVG(CASE WHEN rn_free_cash_flow_debt = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_free_cash_flow_debt IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN free_cash_flow_debt END) AS free_cash_flow_debt_median,
					   AVG(CASE WHEN rn_net_debt_oibda = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_net_debt_oibda IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN net_debt_oibda END) AS net_debt_oibda_median,
				       AVG(CASE WHEN rn_debt_ratio = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_debt_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN debt_ratio END) AS debt_ratio_median,
				       AVG(CASE WHEN rn_debt_to_equity_ratio = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_debt_to_equity_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN debt_to_equity_ratio END) AS debt_to_equity_ratio_median,
				       AVG(CASE WHEN rn_current_ratio = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_current_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN current_ratio END) AS current_ratio_median,
				       AVG(CASE WHEN rn_cash_ratio = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_cash_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN cash_ratio END) AS cash_ratio_median,
				       AVG(CASE WHEN rn_quick_ratio = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_quick_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN quick_ratio END) AS quick_ratio_median,
				       AVG(CASE WHEN rn_solvency_ratio = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_solvency_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN solvency_ratio END) AS solvency_ratio_median,
				       AVG(CASE WHEN rn_asset_coverage_ratio = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_asset_coverage_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN asset_coverage_ratio END) AS asset_coverage_ratio_median,
				       AVG(CASE WHEN rn_capitalization_ratio = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_capitalization_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN capitalization_ratio END) AS capitalization_ratio_median,
				       AVG(CASE WHEN rn_inventory_turnover = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_inventory_turnover IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN inventory_turnover END) AS inventory_turnover_median,
				       AVG(CASE WHEN rn_receivable_turnover_ratio = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_receivable_turnover_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN receivable_turnover_ratio END) AS receivable_turnover_ratio_median,
				       AVG(CASE WHEN rn_accounts_payable_turnover = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_accounts_payable_turnover IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN accounts_payable_turnover END) AS accounts_payable_turnover_median,
				       AVG(CASE WHEN rn_cap_turnover = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_cap_turnover IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN cap_turnover END) AS cap_turnover_median,
				       AVG(CASE WHEN rn_working_cap_turnover = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_working_cap_turnover IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN working_cap_turnover END) AS working_cap_turnover_median,
				       AVG(CASE WHEN rn_asset_turnover = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_asset_turnover IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN asset_turnover END) AS asset_turnover_median,
				       AVG(CASE WHEN rn_fixed_assets_turnover = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_fixed_assets_turnover IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN fixed_assets_turnover END) AS fixed_assets_turnover_median,
				       AVG(CASE WHEN rn_inv_turnover_p = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_inv_turnover_p IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN inv_turnover_p END) AS inv_turnover_p_median,
				       AVG(CASE WHEN rn_p_of_repayment = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_p_of_repayment IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN p_of_repayment END) AS p_of_repayment_median,
				       AVG(CASE WHEN rn_payables_repayment_p = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_payables_repayment_p IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN payables_repayment_p END) AS payables_repayment_p_median,
				       AVG(CASE WHEN rn_cap_turnover_p = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_cap_turnover_p IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN cap_turnover_p END) AS cap_turnover_p_median,
				       AVG(CASE WHEN rn_asset_turnover_p = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_asset_turnover_p IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN asset_turnover_p END) AS asset_turnover_p_median,
				       AVG(CASE WHEN rn_operating_cycle = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_operating_cycle IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN operating_cycle END) AS operating_cycle_median,
				       AVG(CASE WHEN rn_p_ncf = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_p_ncf IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN p_ncf END) AS p_ncf_median,
				       AVG(CASE WHEN rn_p_cfo = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_p_cfo IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN p_cfo END) AS p_cfo_median,
				       AVG(CASE WHEN rn_p_nwc = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_p_nwc IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN p_nwc END) AS p_nwc_median,
				       AVG(CASE WHEN rn_capex_fcf = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_capex_fcf IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN capex_fcf END) AS capex_fcf_median,
				       AVG(CASE WHEN rn_capex_revenue = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_capex_revenue IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN capex_revenue END) AS capex_revenue_median,
				       AVG(CASE WHEN rn_ev_bv = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_ev_bv IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN ev_bv END) AS ev_bv_median,
					   AVG(CASE WHEN rn_p_c = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_p_c IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN p_c END) AS p_c_median,
					   AVG(CASE WHEN rn_peg = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_peg IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN peg END) AS peg_median,
					   AVG(CASE WHEN rn_ev_ic = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_ev_ic IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN ev_ic END) AS ev_ic_median,
					   AVG(CASE WHEN rn_pretax_roa = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_pretax_roa IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN pretax_roa END) AS pretax_roa_median, 
					   AVG(CASE WHEN rn_pretax_roe = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_pretax_roe IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN pretax_roe END) AS pretax_roe_median,
					   AVG(CASE WHEN rn_graham = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_graham IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN graham END) AS graham_median,
					   AVG(CASE WHEN rn_oibda_margin = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_oibda_margin IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN oibda_margin END) AS oibda_margin_median,
					   AVG(CASE WHEN rn_fcf_margin = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_fcf_margin IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN fcf_margin END) AS fcf_margin_median,
					   AVG(CASE WHEN rn_croic = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_croic IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN croic END) AS croic_median,
					   AVG(CASE WHEN rn_croic_greenblatt = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_croic_greenblatt IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN croic_greenblatt END) AS croic_greenblatt_median,
					   AVG(CASE WHEN rn_ros = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_ros IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN ros END) AS ros_median,
					   AVG(CASE WHEN rn_reinvestment_rate = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_reinvestment_rate IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN reinvestment_rate END) AS reinvestment_rate_median,
					   AVG(CASE WHEN rn_debt_ebitda = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_debt_ebitda IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN debt_ebitda END) AS debt_ebitda_median,
					   AVG(CASE WHEN rn_netDebt_FCF = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_netDebt_FCF IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN netDebt_FCF END) AS netDebt_FCF_median,
					   AVG(CASE WHEN rn_debt_FCF = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_debt_FCF IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN debt_FCF END) AS debt_FCF_median,
					   AVG(CASE WHEN rn_long_term_debt_to_asset = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_long_term_debt_to_asset IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN long_term_debt_to_asset END) AS long_term_debt_to_asset_median,
					   AVG(CASE WHEN rn_interest_coverage_ratio = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_interest_coverage_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN interest_coverage_ratio END) AS interest_coverage_ratio_median,
					   AVG(CASE WHEN rn_ocfr = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_ocfr IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN ocfr END) AS ocfr_median,
					   AVG(CASE WHEN rn_capex_cfo = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_capex_cfo IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN capex_cfo END) AS capex_cfo_median,
					   AVG(CASE WHEN rn_capex_amortization = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_capex_amortization IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN capex_amortization END) AS capex_amortization_median,
					   AVG(CASE WHEN rn_revenue_growth_1y = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_revenue_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN revenue_growth_1y END) AS revenue_growth_1y_median,
					   AVG(CASE WHEN rn_ebitda_growth_1y = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_ebitda_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN ebitda_growth_1y END) AS ebitda_growth_1y_median,
					   AVG(CASE WHEN rn_operatingIncome_growth_1y = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_operatingIncome_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN operatingIncome_growth_1y END) AS operatingIncome_growth_1y_median,
					   AVG(CASE WHEN rn_netIncome_growth_1y = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_netIncome_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN netIncome_growth_1y END) AS netIncome_growth_1y_median,
					   AVG(CASE WHEN rn_eps_growth_1y = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_eps_growth_1y IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN eps_growth_1y END) AS eps_growth_1y_median,
					   AVG(CASE WHEN rn_DGR = FLOOR((total_rows + 1) / 2) 
						         OR (total_rows % 2 = 0 AND rn_DGR IN (total_rows / 2, total_rows / 2 + 1)) 
						         THEN DGR END) AS DGR_median
				FROM ranked_data
				GROUP BY company_id
				ORDER BY company_id) t
			ON DUPLICATE KEY UPDATE 
									mkt_cap = t.mkt_cap_median,
									p_e = t.p_e_median,
									p_s = t.p_s_median,
									p_bv = t.p_bv_median,
									ev_ebitda = t.ev_ebitda_median,
									roe = t.roe_median,
									ev = t.ev_median,
									ev_ebit = t.ev_ebit_median,
									p_fcf = t.p_fcf_median,
									ev_fcf = t.ev_fcf_median,
									ev_cfo = t.ev_cfo_median,
									ev_nopat = t.ev_nopat_median,
									ev_s = t.ev_s_median,
									net_debt_ebitda = t.net_debt_ebitda_median,
									eps = t.eps_median,
									book_value_per_share = t.book_value_per_share_median,
									dupont = t.dupont_median,
									altman_model = t.altman_model_median,
									graham_number = t.graham_number_median,
									e_p = t.e_p_median,
									free_cash_flow_yield = t.free_cash_flow_yield_median,
									roa = t.roa_median,
									roaa = t.roaa_median,
									roae = t.roae_median,
									oroa = t.oroa_median,
									rona = t.rona_median,
									rooa = t.rooa_median,
									roic = t.roic_median,
									roce = t.roce_median,
									gross_margin = t.gross_margin_median,
									ebitda_margin = t.ebitda_margin_median,
									ebit_margin = t.ebit_margin_median,
									operating_margin = t.operating_margin_median,
									pretax_margin = t.pretax_margin_median,
									net_margin = t.net_margin_median,
									div_yield = t.div_yield_median,
									div_fcf = t.div_fcf_median,
									div_payout_ratio = t.div_payout_ratio_median,
									net_debt_ebit = t.net_debt_ebit_median,
									net_debt_net_income = t.net_debt_net_income_median,
									net_debt_mcap = t.net_debt_mcap_median,
									net_debt_asset = t.net_debt_asset_median,
									debt_asset = t.debt_asset_median,
									net_debt_equity = t.net_debt_equity_median,
									debt_equity = t.debt_equity_median,
									long_term_debt_to_equity = t.long_term_debt_to_equity_median,
									long_term_debt_to_nopat = t.long_term_debt_to_nopat_median,
									free_cash_flow_debt = t.free_cash_flow_debt_median,
									net_debt_oibda = t.net_debt_oibda_median,
									debt_ratio = t.debt_ratio_median,
									debt_to_equity_ratio = t.debt_to_equity_ratio_median,
									current_ratio = t.current_ratio_median,
									cash_ratio = t.cash_ratio_median,
									quick_ratio = t.quick_ratio_median,
									solvency_ratio = t.solvency_ratio_median,
									asset_coverage_ratio = t.asset_coverage_ratio_median,
									capitalization_ratio = t.capitalization_ratio_median,
									inventory_turnover = t.inventory_turnover_median,
									receivable_turnover_ratio = t.receivable_turnover_ratio_median,
									accounts_payable_turnover = t.accounts_payable_turnover_median,
									cap_turnover = t.cap_turnover_median,
									working_cap_turnover = t.working_cap_turnover_median,
									asset_turnover = t.asset_turnover_median,
									fixed_assets_turnover = t.fixed_assets_turnover_median,
									inv_turnover_p = t.inv_turnover_p_median,
									p_of_repayment = t.p_of_repayment_median,
									payables_repayment_p = t.payables_repayment_p_median,
									cap_turnover_p = t.cap_turnover_p_median,
									asset_turnover_p = t.asset_turnover_p_median,
									operating_cycle = t.operating_cycle_median,
									p_ncf = t.p_ncf_median,
									p_cfo = t.p_cfo_median,
									p_nwc = t.p_nwc_median,
									capex_fcf = t.capex_fcf_median,
									capex_revenue = t.capex_revenue_median,
									ev_bv = t.ev_bv_median,
									p_c = t.p_c_median,
									peg = t.peg_median,
				           			ev_ic = t.ev_ic_median,
									pretax_roa = t.pretax_roa_median,
									graham = t.graham_median,
									oibda_margin = t.oibda_margin_median,
									fcf_margin = t.fcf_margin_median,
									croic = t.croic_median,
									croic_greenblatt = t.croic_greenblatt_median,
									ros = t.ros_median,
									reinvestment_rate = t.reinvestment_rate_median,
									debt_ebitda = t.debt_ebitda_median,
									netDebt_FCF = t.netDebt_FCF_median,
									debt_FCF = t.debt_FCF_median,
									long_term_debt_to_asset = t.long_term_debt_to_asset_median,
									interest_coverage_ratio = t.interest_coverage_ratio_median,
									ocfr = t.ocfr_median,
									capex_cfo = t.capex_cfo_median,
						   			capex_amortization = t.capex_amortization_median,
						   			revenue_growth_1y = t.revenue_growth_1y_median,
						   			ebitda_growth_1y = t.ebitda_growth_1y_median,
						   			operatingIncome_growth_1y = t.operatingIncome_growth_1y_median,
						   			netIncome_growth_1y = t.netIncome_growth_1y_median,
						   			eps_growth_1y = t.eps_growth_1y_median;
	-- Обновление логирования процедуры после завершения	
    update _procedure_calls SET finish=NOW() where name = @name and start = @start;
END

SELECT eps_growth_1y 
from historical_annual_metrics ham 

ALTER TABLE company_average_metrics
ADD CONSTRAINT unique_company_id UNIQUE (company_id);


ALTER TABLE company_average_metrics
ADD CONSTRAINT unique_company_id_mkt_cap UNIQUE (company_id, mkt_cap);



CREATE TABLE company_average_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,               -- Автоинкрементный первичный ключ
    company_id INT NOT NULL,                         -- Уникальный ключ для company_id
    mkt_cap DOUBLE,
    p_e DOUBLE,
    p_s DOUBLE,
    p_bv DOUBLE,
    ev_ebitda DOUBLE,
    roe DOUBLE,
    ev DOUBLE,
    ev_ebit DOUBLE,
    p_fcf DOUBLE,
    ev_fcf DOUBLE,
    ev_cfo DOUBLE,
    ev_nopat DOUBLE,
    ev_s DOUBLE,
    net_debt_ebitda DOUBLE,
    eps DOUBLE,
    book_value_per_share DOUBLE,
    dupont DOUBLE,
    altman_model DOUBLE,
    graham_number DOUBLE,
    e_p DOUBLE,
    free_cash_flow_yield DOUBLE,
    roa DOUBLE,
    roaa DOUBLE,
    roae DOUBLE,
    oroa DOUBLE,
    rona DOUBLE,
    rooa DOUBLE,
    roic DOUBLE,
    roce DOUBLE,
    gross_margin DOUBLE,
    ebitda_margin DOUBLE,
    ebit_margin DOUBLE,
    operating_margin DOUBLE,
    pretax_margin DOUBLE,
    net_margin DOUBLE,
    div_yield DOUBLE,
    div_fcf DOUBLE,
    div_payout_ratio DOUBLE,
    net_debt_ebit DOUBLE,
    net_debt_net_income DOUBLE,
    net_debt_mcap DOUBLE,
    net_debt_asset DOUBLE,
    debt_asset DOUBLE,
    net_debt_equity DOUBLE,
    debt_equity DOUBLE,
    long_term_debt_to_equity DOUBLE,
    long_term_debt_to_nopat DOUBLE,
    free_cash_flow_debt DOUBLE,
    net_debt_oibda DOUBLE,
    debt_ratio DOUBLE,
    debt_to_equity_ratio DOUBLE,
    current_ratio DOUBLE,
    cash_ratio DOUBLE,
    quick_ratio DOUBLE,
    solvency_ratio DOUBLE,
    asset_coverage_ratio DOUBLE,
    capitalization_ratio DOUBLE,
    inventory_turnover DOUBLE,
    receivable_turnover_ratio DOUBLE,
    accounts_payable_turnover DOUBLE,
    cap_turnover DOUBLE,
    working_cap_turnover DOUBLE,
    asset_turnover DOUBLE,
    fixed_assets_turnover DOUBLE,
    inv_turnover_p DOUBLE,
    p_of_repayment DOUBLE,
    payables_repayment_p DOUBLE,
    cap_turnover_p DOUBLE,
    asset_turnover_p DOUBLE,
    operating_cycle DOUBLE,
    p_ncf DOUBLE,
    p_cfo DOUBLE,
    p_nwc DOUBLE,
    capex_fcf DOUBLE,
    capex_revenue DOUBLE,
    ev_bv DOUBLE,
    p_c DOUBLE,
    peg DOUBLE,
    ev_ic DOUBLE,
    pretax_roa DOUBLE,
    pretax_roe DOUBLE,
    graham DOUBLE,
    oibda_margin DOUBLE,
    fcf_margin DOUBLE,
    croic DOUBLE,
    croic_greenblatt DOUBLE,
    ros DOUBLE,
    reinvestment_rate DOUBLE,
    debt_ebitda DOUBLE,
    netDebt_FCF DOUBLE,
    debt_FCF DOUBLE,
    long_term_debt_to_asset DOUBLE,
    interest_coverage_ratio DOUBLE,
    ocfr DOUBLE,
    capex_cfo DOUBLE,
    capex_amortization DOUBLE,
    revenue_growth_1y DOUBLE,
    ebitda_growth_1y DOUBLE,
    operatingIncome_growth_1y DOUBLE,
    netIncome_growth_1y DOUBLE,
    eps_growth_1y DOUBLE,
    DGR DOUBLE,
    UNIQUE KEY (company_id)  -- Уникальный ключ для company_id
);


select ham.company_id, period_end_date, DGR,
		ROW_NUMBER() OVER (PARTITION BY ham.company_id ORDER BY DGR) AS rn_DGR,
       COUNT(*) OVER (PARTITION BY ham.company_id) AS total_rows
FROM historical_annual_metrics ham
LEFT JOIN dividend_graphs AS dg ON dg.company_id = ham.company_id AND dg.year = YEAR(ham.period_end_date)
where ham.company_id in(2, 12, 16)AND YEAR(period_end_date) >= YEAR(CURDATE()) - 11


SELECT *
from users u 