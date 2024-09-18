CREATE OR REPLACE PROCEDURE `median_sector_multipliers`()
BEGIN
	-- выполняем логирование процедуры
	set @startDaily := NOW();
    set @nameDaily := 'median_sector_multipliers';
    insert into _procedure_calls (name, start, finish) value (@nameDaily, @startDaily, null);
   
	-- Отключаем строгие проверки (при необходимости)
    SET @original_sql_mode := @@sql_mode;
    SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES';
    
    -- заносим данные в таблицу 
    INSERT INTO median_sector_metrics (
    					sector_id,
    					exchange_id,
    					mkt_cap_median_sector,
						p_e_median_sector,
						p_s_median_sector,
						p_bv_median_sector,
						ev_ebitda_median_sector,
						roe_median_sector,
						ev_median_sector,
						ev_ebit_median_sector,
						p_fcf_median_sector,
						ev_fcf_median_sector,
						ev_cfo_median_sector,
						ev_nopat_median_sector,
						ev_s_median_sector,
						net_debt_ebitda_median_sector,
						eps_median_sector,
						book_value_per_share_median_sector,
						dupont_median_sector,
						altman_model_median_sector,
						graham_median_sector,
						e_p_median_sector,
						free_cash_flow_yield_median_sector,
						roa_median_sector,
						roaa_median_sector,
						roae_median_sector,
						oroa_median_sector,
						rona_median_sector,
						rooa_median_sector,
						roic_median_sector,
						roce_median_sector,
						gross_margin_median_sector,
						ebitda_margin_median_sector,
						ebit_margin_median_sector,
						operating_margin_median_sector,
						pretax_margin_median_sector,
						net_margin_median_sector,
						net_debt_ebit_median_sector,
						net_debt_net_income_median_sector,
						net_debt_mcap_median_sector,
						net_debt_asset_median_sector,
						debt_asset_median_sector,
						net_debt_equity_median_sector,
						debt_equity_median_sector,
						long_term_debt_to_equity_median_sector,
						long_term_debt_to_nopat_median_sector,
						free_cash_flow_debt_median_sector,
						net_debt_oibda_median_sector,
						debt_ratio_median_sector,
						debt_to_equity_ratio_median_sector,
						current_ratio_median_sector,
						cash_ratio_median_sector,
						quick_ratio_median_sector,
						solvency_ratio_median_sector,
						asset_coverage_ratio_median_sector,
						capitalization_ratio_median_sector,
						inventory_turnover_median_sector,
						receivable_turnover_ratio_median_sector,
						accounts_payable_turnover_median_sector,
						cap_turnover_median_sector,
						working_cap_turnover_median_sector,
						asset_turnover_median_sector,
						fixed_assets_turnover_median_sector,
						inv_turnover_p_median_sector,
						p_of_repayment_median_sector,
						payables_repayment_p_median_sector,
						cap_turnover_p_median_sector,
						asset_turnover_p_median_sector,
						operating_cycle_median_sector,
						p_ncf_median_sector,
						p_cfo_median_sector,
						p_nwc_median_sector,
						capex_fcf_median_sector,
						capex_revenue_median_sector,
						ev_bv_median_sector,
						peg_median_sector,
						ros_median_sector,
						ev_ic_median_sector,
						interest_coverage_ratio_median_sector,
						debt_median_sector,
						net_debt_median_sector,
						oibda_median_sector,
						nopat_median_sector,
						excess_cash_median_sector,
						invested_capital_median_sector,
						employed_capital_median_sector,
						excess_cash_flow_median_sector,
						p_c_median_sector,
						fcf_margin_median_sector,
						croic_median_sector,
						croic_greenblatt_median_sector,
						oibda_margin_median_sector,
						graham_number_median_sector,
						netDebt_FCF_median_sector,
						pretax_roa_median_sector,
						pretax_roe_median_sector,
						debt_ebitda_median_sector,
						debt_FCF_median_sector,
						reinvestment_rate_median_sector,
						long_term_debt_to_asset_median_sector,
						ocfr_median_sector,
						capex_cfo_median_sector,
						capex_amortization_median_sector)
	SELECT 	t.*
	FROM (
			WITH ranked_data AS (
				SELECT lm.company_id, c.sector_id, c.exchange_id,
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
			           graham,
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
			           peg,                                     
			           ros,
			           ev_ic,
			           interest_coverage_ratio,
			           debt,
			           net_debt,
			           oibda,
			           nopat,
			           excess_cash,
			           invested_capital,
			           employed_capital,
			           excess_cash_flow,
			           p_c,
			           fcf_margin,
			           croic,
			           croic_greenblatt,
			           oibda_margin,
					   graham_number,
					   netDebt_FCF,
					   pretax_roa,
					   pretax_roe,
					   debt_ebitda,
					   debt_FCF,
					   reinvestment_rate,
					   long_term_debt_to_asset,
					   ocfr,
					   capex_cfo,
					   capex_amortization,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by mkt_cap) as rn_mkt_cap,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by p_e) as rn_p_e,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by p_s) as rn_p_s,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by p_bv) as rn_p_bv,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by ev_ebitda) as rn_ev_ebitda,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by roe) as rn_roe,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by ev) as rn_ev,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by ev_ebit) as rn_ev_ebit,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by p_fcf) as rn_p_fcf,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by ev_fcf) as rn_ev_fcf,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by ev_cfo) as rn_ev_cfo,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by ev_nopat) as rn_ev_nopat,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by ev_s) as rn_ev_s,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by net_debt_ebitda) as rn_net_debt_ebitda,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by eps) as rn_eps,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by book_value_per_share) as rn_book_value_per_share,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by dupont) as rn_dupont,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by altman_model) as rn_altman_model,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by graham) as rn_graham,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by e_p) as rn_e_p,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by free_cash_flow_yield) as rn_free_cash_flow_yield,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by roa) as rn_roa,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by roaa) as rn_roaa,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by roae) as rn_roae,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by oroa) as rn_oroa,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by rona) as rn_rona,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by rooa) as rn_rooa,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by roic) as rn_roic,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by roce) as rn_roce,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by gross_margin) as rn_gross_margin,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by ebitda_margin) as rn_ebitda_margin,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by ebit_margin) as rn_ebit_margin,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by operating_margin) as rn_operating_margin,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by pretax_margin) as rn_pretax_margin,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by net_margin) as rn_net_margin,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by net_debt_ebit) as rn_net_debt_ebit,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by net_debt_net_income) as rn_net_debt_net_income,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by net_debt_mcap) as rn_net_debt_mcap,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by net_debt_asset) as rn_net_debt_asset,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by debt_asset) as rn_debt_asset,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by net_debt_equity) as rn_net_debt_equity,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by debt_equity) as rn_debt_equity,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by long_term_debt_to_equity) as rn_long_term_debt_to_equity,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by long_term_debt_to_nopat) as rn_long_term_debt_to_nopat,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by free_cash_flow_debt) as rn_free_cash_flow_debt,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by net_debt_oibda) as rn_net_debt_oibda,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by debt_ratio) as rn_debt_ratio,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by debt_to_equity_ratio) as rn_debt_to_equity_ratio,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by current_ratio) as rn_current_ratio,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by cash_ratio) as rn_cash_ratio,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by quick_ratio) as rn_quick_ratio,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by solvency_ratio) as rn_solvency_ratio,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by asset_coverage_ratio) as rn_asset_coverage_ratio,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by capitalization_ratio) as rn_capitalization_ratio,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by inventory_turnover) as rn_inventory_turnover,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by receivable_turnover_ratio) as rn_receivable_turnover_ratio,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by accounts_payable_turnover) as rn_accounts_payable_turnover,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by cap_turnover) as rn_cap_turnover,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by working_cap_turnover) as rn_working_cap_turnover,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by asset_turnover) as rn_asset_turnover,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by fixed_assets_turnover) as rn_fixed_assets_turnover,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by inv_turnover_p) as rn_inv_turnover_p,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by p_of_repayment) as rn_p_of_repayment,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by payables_repayment_p) as rn_payables_repayment_p,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by cap_turnover_p) as rn_cap_turnover_p,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by asset_turnover_p) as rn_asset_turnover_p,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by operating_cycle) as rn_operating_cycle,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by p_ncf) as rn_p_ncf,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by p_cfo) as rn_p_cfo,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by p_nwc) as rn_p_nwc,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by capex_fcf) as rn_capex_fcf,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by capex_revenue) as rn_capex_revenue,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by ev_bv) as rn_ev_bv,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by peg) as rn_peg,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by ros) as rn_ros,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by ev_ic) as rn_ev_ic,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by interest_coverage_ratio) as rn_interest_coverage_ratio,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by debt) as rn_debt,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by net_debt) as rn_net_debt,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by oibda) as rn_oibda,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by nopat) as rn_nopat,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by excess_cash) as rn_excess_cash,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by invested_capital) as rn_invested_capital,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by employed_capital) as rn_employed_capital,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by excess_cash_flow) as rn_excess_cash_flow,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by p_c) as rn_p_c,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by fcf_margin) as rn_fcf_margin,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by croic) as rn_croic,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by croic_greenblatt) as rn_croic_greenblatt,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by oibda_margin) as rn_oibda_margin,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by graham_number) as rn_graham_number,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by netDebt_FCF) as rn_netDebt_FCF,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by pretax_roa) as rn_pretax_roa,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by pretax_roe) as rn_pretax_roe,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by debt_ebitda) as rn_debt_ebitda,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by debt_FCF) as rn_debt_FCF,						
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by reinvestment_rate) as rn_reinvestment_rate,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by long_term_debt_to_asset) as rn_long_term_debt_to_asset,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by ocfr) as rn_ocfr,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by capex_cfo) as rn_capex_cfo,
						ROW_NUMBER() over(PARTITION by c.sector_id, c.exchange_id order by capex_amortization) as rn_capex_amortization,
						COUNT(*) OVER (PARTITION BY c.sector_id, c.exchange_id) AS total_rows
				from ltm_multipliers lm 
				join companies c on c.id = lm.company_id
				where lm.end_date = (SELECT MAX(end_date) FROM ltm_multipliers lm2 where lm.company_id = lm2.company_id))
			SELECT sector_id, exchange_id,
						    ROUND(AVG(CASE WHEN rn_mkt_cap = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_mkt_cap IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN mkt_cap END), 2) AS mkt_cap_median_sector,
							ROUND(AVG(CASE WHEN rn_p_e = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_p_e IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN p_e END), 2) AS p_e_median_sector,
							ROUND(AVG(CASE WHEN rn_p_s = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_p_s IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN p_s END), 2) AS p_s_median_sector,
							ROUND(AVG(CASE WHEN rn_p_bv = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_p_bv IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN p_bv END), 2) AS p_bv_median_sector,
							ROUND(AVG(CASE WHEN rn_ev_ebitda = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ev_ebitda IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ev_ebitda END), 2) AS ev_ebitda_median_sector,
							ROUND(AVG(CASE WHEN rn_roe = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_roe IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN roe END), 2) AS roe_median_sector,
							ROUND(AVG(CASE WHEN rn_ev = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ev IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ev END), 2) AS ev_median_sector,
							ROUND(AVG(CASE WHEN rn_ev_ebit = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ev_ebit IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ev_ebit END), 2) AS ev_ebit_median_sector,
							ROUND(AVG(CASE WHEN rn_p_fcf = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_p_fcf IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN p_fcf END), 2) AS p_fcf_median_sector,
							ROUND(AVG(CASE WHEN rn_ev_fcf = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ev_fcf IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ev_fcf END), 2) AS ev_fcf_median_sector,
							ROUND(AVG(CASE WHEN rn_ev_cfo = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ev_cfo IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ev_cfo END), 2) AS ev_cfo_median_sector,
							ROUND(AVG(CASE WHEN rn_ev_nopat = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ev_nopat IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ev_nopat END), 2) AS ev_nopat_median_sector,
							ROUND(AVG(CASE WHEN rn_ev_s = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ev_s IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ev_s END), 2) AS ev_s_median_sector,
							ROUND(AVG(CASE WHEN rn_net_debt_ebitda = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_net_debt_ebitda IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN net_debt_ebitda END), 2) AS net_debt_ebitda_median_sector,
							ROUND(AVG(CASE WHEN rn_eps = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_eps IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN eps END), 2) AS eps_median_sector,
							ROUND(AVG(CASE WHEN rn_book_value_per_share = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_book_value_per_share IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN book_value_per_share END), 2) AS book_value_per_share_median_sector,
							ROUND(AVG(CASE WHEN rn_dupont = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_dupont IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN dupont END), 2) AS dupont_median_sector,
							ROUND(AVG(CASE WHEN rn_altman_model = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_altman_model IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN altman_model END), 2) AS altman_model_median_sector,
							ROUND(AVG(CASE WHEN rn_graham = FLOOR((total_rows + 1) / 2) 
 									                OR (total_rows % 2 = 0 AND rn_graham IN (total_rows / 2, total_rows / 2 + 1)) 
 									                THEN graham END), 2) AS graham_median_sector,
							ROUND(AVG(CASE WHEN rn_e_p = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_e_p IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN e_p END), 2) AS e_p_median_sector,
							ROUND(AVG(CASE WHEN rn_free_cash_flow_yield = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_free_cash_flow_yield IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN free_cash_flow_yield END), 2) AS free_cash_flow_yield_median_sector,
							ROUND(AVG(CASE WHEN rn_roa = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_roa IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN roa END), 2) AS roa_median_sector,
							ROUND(AVG(CASE WHEN rn_roaa = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_roaa IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN roaa END), 2) AS roaa_median_sector,
							ROUND(AVG(CASE WHEN rn_roae = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_roae IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN roae END), 2) AS roae_median_sector,
							ROUND(AVG(CASE WHEN rn_oroa = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_oroa IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN oroa END), 2) AS oroa_median_sector,
							ROUND(AVG(CASE WHEN rn_rona = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_rona IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN rona END), 2) AS rona_median_sector,
							ROUND(AVG(CASE WHEN rn_rooa = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_rooa IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN rooa END), 2) AS rooa_median_sector,
							ROUND(AVG(CASE WHEN rn_roic = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_roic IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN roic END), 2) AS roic_median_sector,
							ROUND(AVG(CASE WHEN rn_roce = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_roce IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN roce END), 2) AS roce_median_sector,
							ROUND(AVG(CASE WHEN rn_gross_margin = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_gross_margin IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN gross_margin END), 2) AS gross_margin_median_sector,
							ROUND(AVG(CASE WHEN rn_ebitda_margin = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ebitda_margin IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ebitda_margin END), 2) AS ebitda_margin_median_sector,
							ROUND(AVG(CASE WHEN rn_ebit_margin = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ebit_margin IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ebit_margin END), 2) AS ebit_margin_median_sector,
							ROUND(AVG(CASE WHEN rn_operating_margin = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_operating_margin IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN operating_margin END), 2) AS operating_margin_median_sector,
							ROUND(AVG(CASE WHEN rn_pretax_margin = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_pretax_margin IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN pretax_margin END), 2) AS pretax_margin_median_sector,
							ROUND(AVG(CASE WHEN rn_net_margin = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_net_margin IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN net_margin END), 2) AS net_margin_median_sector,
							ROUND(AVG(CASE WHEN rn_net_debt_ebit = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_net_debt_ebit IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN net_debt_ebit END), 2) AS net_debt_ebit_median_sector,
							ROUND(AVG(CASE WHEN rn_net_debt_net_income = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_net_debt_net_income IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN net_debt_net_income END), 2) AS net_debt_net_income_median_sector,
							ROUND(AVG(CASE WHEN rn_net_debt_mcap = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_net_debt_mcap IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN net_debt_mcap END), 2) AS net_debt_mcap_median_sector,
							ROUND(AVG(CASE WHEN rn_net_debt_asset = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_net_debt_asset IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN net_debt_asset END), 2) AS net_debt_asset_median_sector,
							ROUND(AVG(CASE WHEN rn_debt_asset = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_debt_asset IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN debt_asset END), 2) AS debt_asset_median_sector,
							ROUND(AVG(CASE WHEN rn_net_debt_equity = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_net_debt_equity IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN net_debt_equity END), 2) AS net_debt_equity_median_sector,
							ROUND(AVG(CASE WHEN rn_debt_equity = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_debt_equity IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN debt_equity END), 2) AS debt_equity_median_sector,
							ROUND(AVG(CASE WHEN rn_long_term_debt_to_equity = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_long_term_debt_to_equity IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN long_term_debt_to_equity END), 2) AS long_term_debt_to_equity_median_sector,
							ROUND(AVG(CASE WHEN rn_long_term_debt_to_nopat = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_long_term_debt_to_nopat IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN long_term_debt_to_nopat END), 2) AS long_term_debt_to_nopat_median_sector,
							ROUND(AVG(CASE WHEN rn_free_cash_flow_debt = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_free_cash_flow_debt IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN free_cash_flow_debt END), 2) AS free_cash_flow_debt_median_sector,
							ROUND(AVG(CASE WHEN rn_net_debt_oibda = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_net_debt_oibda IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN net_debt_oibda END), 2) AS net_debt_oibda_median_sector,
							ROUND(AVG(CASE WHEN rn_debt_ratio = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_debt_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN debt_ratio END), 2) AS debt_ratio_median_sector,
							ROUND(AVG(CASE WHEN rn_debt_to_equity_ratio = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_debt_to_equity_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN debt_to_equity_ratio END), 2) AS debt_to_equity_ratio_median_sector,
							ROUND(AVG(CASE WHEN rn_current_ratio = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_current_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN current_ratio END), 2) AS current_ratio_median_sector,
							ROUND(AVG(CASE WHEN rn_cash_ratio = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_cash_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN cash_ratio END), 2) AS cash_ratio_median_sector,
							ROUND(AVG(CASE WHEN rn_quick_ratio = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_quick_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN quick_ratio END), 2) AS quick_ratio_median_sector,
							ROUND(AVG(CASE WHEN rn_solvency_ratio = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_solvency_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN solvency_ratio END), 2) AS solvency_ratio_median_sector,
							ROUND(AVG(CASE WHEN rn_asset_coverage_ratio = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_asset_coverage_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN asset_coverage_ratio END), 2) AS asset_coverage_ratio_median_sector,
							ROUND(AVG(CASE WHEN rn_capitalization_ratio = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_capitalization_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN capitalization_ratio END), 2) AS capitalization_ratio_median_sector,
							ROUND(AVG(CASE WHEN rn_inventory_turnover = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_inventory_turnover IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN inventory_turnover END), 2) AS inventory_turnover_median_sector,
							ROUND(AVG(CASE WHEN rn_receivable_turnover_ratio = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_receivable_turnover_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN receivable_turnover_ratio END), 2) AS receivable_turnover_ratio_median_sector,
							ROUND(AVG(CASE WHEN rn_accounts_payable_turnover = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_accounts_payable_turnover IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN accounts_payable_turnover END), 2) AS accounts_payable_turnover_median_sector,
							ROUND(AVG(CASE WHEN rn_cap_turnover = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_cap_turnover IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN cap_turnover END), 2) AS cap_turnover_median_sector,
							ROUND(AVG(CASE WHEN rn_working_cap_turnover = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_working_cap_turnover IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN working_cap_turnover END), 2) AS working_cap_turnover_median_sector,
							ROUND(AVG(CASE WHEN rn_asset_turnover = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_asset_turnover IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN asset_turnover END), 2) AS asset_turnover_median_sector,
							ROUND(AVG(CASE WHEN rn_fixed_assets_turnover = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_fixed_assets_turnover IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN fixed_assets_turnover END), 2) AS fixed_assets_turnover_median_sector,
							ROUND(AVG(CASE WHEN rn_inv_turnover_p = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_inv_turnover_p IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN inv_turnover_p END), 2) AS inv_turnover_p_median_sector,
							ROUND(AVG(CASE WHEN rn_p_of_repayment = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_p_of_repayment IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN p_of_repayment END), 2) AS p_of_repayment_median_sector,
							ROUND(AVG(CASE WHEN rn_payables_repayment_p = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_payables_repayment_p IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN payables_repayment_p END), 2) AS payables_repayment_p_median_sector,
							ROUND(AVG(CASE WHEN rn_cap_turnover_p = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_cap_turnover_p IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN cap_turnover_p END), 2) AS cap_turnover_p_median_sector,
							ROUND(AVG(CASE WHEN rn_asset_turnover_p = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_asset_turnover_p IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN asset_turnover_p END), 2) AS asset_turnover_p_median_sector,
							ROUND(AVG(CASE WHEN rn_operating_cycle = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_operating_cycle IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN operating_cycle END), 2) AS operating_cycle_median_sector,
							ROUND(AVG(CASE WHEN rn_p_ncf = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_p_ncf IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN p_ncf END), 2) AS p_ncf_median_sector,
							ROUND(AVG(CASE WHEN rn_p_cfo = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_p_cfo IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN p_cfo END), 2) AS p_cfo_median_sector,
							ROUND(AVG(CASE WHEN rn_p_nwc = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_p_nwc IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN p_nwc END), 2) AS p_nwc_median_sector,
							ROUND(AVG(CASE WHEN rn_capex_fcf = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_capex_fcf IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN capex_fcf END), 2) AS capex_fcf_median_sector,
							ROUND(AVG(CASE WHEN rn_capex_revenue = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_capex_revenue IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN capex_revenue END), 2) AS capex_revenue_median_sector,
							ROUND(AVG(CASE WHEN rn_ev_bv = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ev_bv IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ev_bv END), 2) AS ev_bv_median_sector,
							ROUND(AVG(CASE WHEN rn_peg = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_peg IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN peg END), 2) AS peg_median_sector,
			                ROUND(AVG(CASE WHEN rn_ros = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ros IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ros END), 2) AS ros_median_sector,
							ROUND(AVG(CASE WHEN rn_ev_ic = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ev_ic IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ev_ic END), 2) AS ev_ic_median_sector,
							ROUND(AVG(CASE WHEN rn_interest_coverage_ratio = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_interest_coverage_ratio IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN interest_coverage_ratio END), 2) AS interest_coverage_ratio_median_sector,
							ROUND(AVG(CASE WHEN rn_debt = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_debt IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN debt END), 2) AS debt_median_sector,
							ROUND(AVG(CASE WHEN rn_net_debt = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_net_debt IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN net_debt END), 2) AS net_debt_median_sector,
							ROUND(AVG(CASE WHEN rn_oibda = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_oibda IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN oibda END), 2) AS oibda_median_sector,
							ROUND(AVG(CASE WHEN rn_nopat = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_nopat IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN nopat END), 2) AS nopat_median_sector,
							ROUND(AVG(CASE WHEN rn_excess_cash = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_excess_cash IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN excess_cash END), 2) AS excess_cash_median_sector,
							ROUND(AVG(CASE WHEN rn_invested_capital = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_invested_capital IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN invested_capital END), 2) AS invested_capital_median_sector,
							ROUND(AVG(CASE WHEN rn_employed_capital = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_employed_capital IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN employed_capital END), 2) AS employed_capital_median_sector,
							ROUND(AVG(CASE WHEN rn_excess_cash_flow = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_excess_cash_flow IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN excess_cash_flow END), 2) AS excess_cash_flow_median_sector,
							ROUND(AVG(CASE WHEN rn_p_c = FLOOR((total_rows + 1) / 2) 
 									                OR (total_rows % 2 = 0 AND rn_p_c IN (total_rows / 2, total_rows / 2 + 1)) 
 									                THEN p_c END), 2) AS p_c_median_sector,
							ROUND(AVG(CASE WHEN rn_fcf_margin = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_fcf_margin IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN fcf_margin END), 2) AS fcf_margin_median_sector,
							ROUND(AVG(CASE WHEN rn_croic = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_croic IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN croic END), 2) AS croic_median_sector,
							ROUND(AVG(CASE WHEN rn_croic_greenblatt = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_croic_greenblatt IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN croic_greenblatt END), 2) AS croic_greenblatt_median_sector,
							ROUND(AVG(CASE WHEN rn_oibda_margin = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_oibda_margin IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN oibda_margin END), 2) AS oibda_margin_median_sector,
			                ROUND(AVG(CASE WHEN rn_graham_number = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_graham_number IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN graham_number END), 2) AS graham_number_median_sector,
							ROUND(AVG(CASE WHEN rn_netDebt_FCF = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_netDebt_FCF IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN netDebt_FCF END), 2) AS netDebt_FCF_median_sector,
							ROUND(AVG(CASE WHEN rn_pretax_roa = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_pretax_roa IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN pretax_roa END), 2) AS pretax_roa_median_sector,
							ROUND(AVG(CASE WHEN rn_pretax_roe = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_pretax_roe IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN pretax_roe END), 2) AS pretax_roe_median_sector,
							ROUND(AVG(CASE WHEN rn_debt_ebitda = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_debt_ebitda IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN debt_ebitda END), 2) AS debt_ebitda_median_sector,
							ROUND(AVG(CASE WHEN rn_debt_FCF = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_debt_FCF IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN debt_FCF END), 2) AS debt_FCF_median_sector,									                
							ROUND(AVG(CASE WHEN rn_reinvestment_rate = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_reinvestment_rate IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN reinvestment_rate END), 2) AS reinvestment_rate_median_sector,
							ROUND(AVG(CASE WHEN rn_long_term_debt_to_asset = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_long_term_debt_to_asset IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN long_term_debt_to_asset END), 2) AS long_term_debt_to_asset_median_sector,
							ROUND(AVG(CASE WHEN rn_ocfr = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_ocfr IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN ocfr END), 2) AS ocfr_median_sector,	
							ROUND(AVG(CASE WHEN rn_capex_cfo = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_capex_cfo IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN capex_cfo END), 2) AS capex_cfo_median_sector,
							ROUND(AVG(CASE WHEN rn_capex_amortization = FLOOR((total_rows + 1) / 2) 
									                OR (total_rows % 2 = 0 AND rn_capex_amortization IN (total_rows / 2, total_rows / 2 + 1)) 
									                THEN capex_amortization END), 2) AS capex_amortization_median_sector
			FROM ranked_data
			GROUP BY sector_id, exchange_id
			ORDER BY sector_id, exchange_id
			) t
			ON DUPLICATE KEY UPDATE mkt_cap_median_sector = t.mkt_cap_median_sector,
									p_e_median_sector = t.p_e_median_sector,
									p_s_median_sector = t.p_s_median_sector,
									p_bv_median_sector = t.p_bv_median_sector,
									ev_ebitda_median_sector = t.ev_ebitda_median_sector,
									roe_median_sector = t.roe_median_sector,
									ev_median_sector = t.ev_median_sector,
									ev_ebit_median_sector = t.ev_ebit_median_sector,
									p_fcf_median_sector = t.p_fcf_median_sector,
									ev_fcf_median_sector = t.ev_fcf_median_sector,
									ev_cfo_median_sector = t.ev_cfo_median_sector,
									ev_nopat_median_sector = t.ev_nopat_median_sector,
									ev_s_median_sector = t.ev_s_median_sector,
									net_debt_ebitda_median_sector = t.net_debt_ebitda_median_sector,
									eps_median_sector = t.eps_median_sector,
									book_value_per_share_median_sector = t.book_value_per_share_median_sector,
									dupont_median_sector = t.dupont_median_sector,
									altman_model_median_sector = t.altman_model_median_sector,
									graham_median_sector = t.graham_median_sector,
									e_p_median_sector = t.e_p_median_sector,
									free_cash_flow_yield_median_sector = t.free_cash_flow_yield_median_sector,
									roa_median_sector = t.roa_median_sector,
									roaa_median_sector = t.roaa_median_sector,
									roae_median_sector = t.roae_median_sector,
									oroa_median_sector = t.oroa_median_sector,
									rona_median_sector = t.rona_median_sector,
									rooa_median_sector = t.rooa_median_sector,
									roic_median_sector = t.roic_median_sector,
									roce_median_sector = t.roce_median_sector,
									gross_margin_median_sector = t.gross_margin_median_sector,
									ebitda_margin_median_sector = t.ebitda_margin_median_sector,
									ebit_margin_median_sector = t.ebit_margin_median_sector,
									operating_margin_median_sector = t.operating_margin_median_sector,
									pretax_margin_median_sector = t.pretax_margin_median_sector,
									net_margin_median_sector = t.net_margin_median_sector,
									net_debt_ebit_median_sector = t.net_debt_ebit_median_sector,
									net_debt_net_income_median_sector = t.net_debt_net_income_median_sector,
									net_debt_mcap_median_sector = t.net_debt_mcap_median_sector,
									net_debt_asset_median_sector = t.net_debt_asset_median_sector,
									debt_asset_median_sector = t.debt_asset_median_sector,
									net_debt_equity_median_sector = t.net_debt_equity_median_sector,
									debt_equity_median_sector = t.debt_equity_median_sector,
									long_term_debt_to_equity_median_sector = t.long_term_debt_to_equity_median_sector,
									long_term_debt_to_nopat_median_sector = t.long_term_debt_to_nopat_median_sector,
									free_cash_flow_debt_median_sector = t.free_cash_flow_debt_median_sector,
									net_debt_oibda_median_sector = t.net_debt_oibda_median_sector,
									debt_ratio_median_sector = t.debt_ratio_median_sector,
									debt_to_equity_ratio_median_sector = t.debt_to_equity_ratio_median_sector,
									current_ratio_median_sector = t.current_ratio_median_sector,
									cash_ratio_median_sector = t.cash_ratio_median_sector,
									quick_ratio_median_sector = t.quick_ratio_median_sector,
									solvency_ratio_median_sector = t.solvency_ratio_median_sector,
									asset_coverage_ratio_median_sector = t.asset_coverage_ratio_median_sector,
									capitalization_ratio_median_sector = t.capitalization_ratio_median_sector,
									inventory_turnover_median_sector = t.inventory_turnover_median_sector,
									receivable_turnover_ratio_median_sector = t.receivable_turnover_ratio_median_sector,
									accounts_payable_turnover_median_sector = t.accounts_payable_turnover_median_sector,
									cap_turnover_median_sector = t.cap_turnover_median_sector,
									working_cap_turnover_median_sector = t.working_cap_turnover_median_sector,
									asset_turnover_median_sector = t.asset_turnover_median_sector,
									fixed_assets_turnover_median_sector = t.fixed_assets_turnover_median_sector,
									inv_turnover_p_median_sector = t.inv_turnover_p_median_sector,
									p_of_repayment_median_sector = t.p_of_repayment_median_sector,
									payables_repayment_p_median_sector = t.payables_repayment_p_median_sector,
									cap_turnover_p_median_sector = t.cap_turnover_p_median_sector,
									asset_turnover_p_median_sector = t.asset_turnover_p_median_sector,
									operating_cycle_median_sector = t.operating_cycle_median_sector,
									p_ncf_median_sector = t.p_ncf_median_sector,
									p_cfo_median_sector = t.p_cfo_median_sector,
									p_nwc_median_sector = t.p_nwc_median_sector,
									capex_fcf_median_sector = t.capex_fcf_median_sector,
									capex_revenue_median_sector = t.capex_revenue_median_sector,
									ev_bv_median_sector = t.ev_bv_median_sector,
									peg_median_sector = t.peg_median_sector,
									ros_median_sector = t.ros_median_sector,
									ev_ic_median_sector = t.ev_ic_median_sector,
									interest_coverage_ratio_median_sector = t.interest_coverage_ratio_median_sector,
									debt_median_sector = t.debt_median_sector,
									net_debt_median_sector = t.net_debt_median_sector,
									oibda_median_sector = t.oibda_median_sector,
									nopat_median_sector = t.nopat_median_sector,
									excess_cash_median_sector = t.excess_cash_median_sector,
									invested_capital_median_sector = t.invested_capital_median_sector,
									employed_capital_median_sector = t.employed_capital_median_sector,
									excess_cash_flow_median_sector = t.excess_cash_flow_median_sector,
									p_c_median_sector = t.p_c_median_sector,
									fcf_margin_median_sector = t.fcf_margin_median_sector,
									croic_median_sector = t.croic_median_sector,
									croic_greenblatt_median_sector = t.croic_greenblatt_median_sector,
									oibda_margin_median_sector = t.oibda_margin_median_sector,
									graham_number_median_sector = t.graham_number_median_sector,
									netDebt_FCF_median_sector = t.netDebt_FCF_median_sector,
									pretax_roa_median_sector = t.pretax_roa_median_sector,
									pretax_roe_median_sector = t.pretax_roe_median_sector,
									debt_ebitda_median_sector = t.debt_ebitda_median_sector,
									debt_FCF_median_sector = t.debt_FCF_median_sector,
									reinvestment_rate_median_sector = t.reinvestment_rate_median_sector,
									long_term_debt_to_asset_median_sector = t.long_term_debt_to_asset_median_sector,
									ocfr_median_sector = t.ocfr_median_sector,
									capex_cfo_median_sector = t.capex_cfo_median_sector,
									capex_amortization_median_sector = t.capex_amortization_median_sector;
	
	-- завершаем логирование процедуры					   
	UPDATE _procedure_calls SET finish = NOW() WHERE name = @nameDaily AND start = @startDaily;
	-- добавляем данные по медианам дивидендов
	CALL median_sector_dividends();
	-- добавляем данные по медианам дивидендов
	CALL median_sector_price_change();
END;

