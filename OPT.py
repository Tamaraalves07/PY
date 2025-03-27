# -*- coding: utf-8 -*-
"""
Created on Mon May 15 14:35:32 2023

@author: JBFO - ASSET ALLOCATION
"""


# !!! 2) Global Environment ===================================================

# General packages
import pandas as pd

import time
from datetime import datetime

# Windows related packages
from pathlib import Path

# Portfolio optimization packages
import asset_allocation.optimize.algoOPT as algoOPT
import asset_allocation.optimize.plotOPT as plotOPT

# !!! 3) Organize data from specifications ====================================

class Otimizador:
    
    def __init__(self, file_cma='CMA Global.xlsx', file_mandate='Global - Trilha Core.xlsx', folder="C:/Temp/", n_simulations=50, n_investment_horizon=120
                 , frac_expectations=1.0, combination_method = 'MEAN' #MEAN, CC5, CC10, HERF
                 , language = 'PT' #PT, EN
                 , rodar=True):        
        """
        Exemplo de chamada: 
        from asset_allocation import OPT
        opt = OPT.Otimizador(file_mandate='Local - Trilha Core.xlsx', file_cma='CMA Local.xlsx', folder=r'O:\SAO\CICH_All\Portfolio Management\11 - Apresentações\Hematitas\2025 01 Simulacao Mandato')

        Parameters
        ----------
        file_cma : TYPE, optional
            DESCRIPTION. The default is 'CMA Global.xlsx'.
        file_mandate : TYPE, optional
            DESCRIPTION. The default is 'Global - Trilha Core.xlsx'.
        folder : TYPE, optional
            DESCRIPTION. The default is "C:/Temp/".
        n_simulations : TYPE, optional
            DESCRIPTION. The default is 50.
        n_investment_horizon : TYPE, optional
            DESCRIPTION. The default is 120.
        frac_expectations : TYPE, optional
            DESCRIPTION. The default is 1.0.
        combination_method : TYPE, optional
            DESCRIPTION. The default is 'MEAN' #MEAN.
        CC5 : TYPE
            DESCRIPTION.
        CC10 : TYPE
            DESCRIPTION.
        HERF : TYPE
            DESCRIPTION.
        language : TYPE, optional
            DESCRIPTION. The default is 'PT' #PT.
        EN : TYPE
            DESCRIPTION.
        rodar : TYPE, optional
            DESCRIPTION. The default is True.

        Returns
        -------
        None.

        """
        # Guarda parâmetros
        self.folder = folder
        self.file_cma = self.folder + '/' + file_cma
        self.file_mandate = file_mandate
        self.n_simulations = n_simulations
        self.n_investment_horizon = n_investment_horizon
        self.frac_expectations = frac_expectations
        self.combination_method = combination_method
        self.language = language
        
        # Execuções
        self.path_mandate = f'{folder}/{file_mandate}'
        self.all_mandates, self.selected_classes, self.override_constraints = algoOPT.get_mandates(self.path_mandate)        
        self.expectations = algoOPT.get_expectations(self.file_cma, self.selected_classes)        
        
        #Rodar otimizador
        if rodar:
            self.rodar()
        
    def rodar(self):

        # !!! 4) Build sample returns from scenarios ==================================
        
        #get resamples from historic bootstraping
        resample, adjusted_hist = algoOPT.resample_historical(self.expectations['expec']['Ret'],
                                                              self.expectations['expec']['Vol'],
                                                              self.expectations['hist'],
                                                              n_sim=self.n_simulations, 
                                                              hor=self.n_investment_horizon,
                                                              autocorr=self.expectations['expec']['AC1'],
                                                              frac_exp=self.frac_expectations,
                                                              regimes= None)
        
        #get adjusted history distribution moments expectations 
        def simple_stats(Ret):
            ret = (1+Ret).prod() **(12/Ret.shape[0]) -1
            vol = Ret.std() *12**0.5
            sharpe = (ret - ret['Liquidez'])/vol
            skew = Ret.skew()
            kurt = Ret.kurt()
            
            var = Ret.quantile(0.05)
            cvar = Ret[Ret.subtract(Ret.quantile(0.05), axis= 1) <= 0].mean()
            
            stats_exp_classes = pd.concat([ret,vol,sharpe,skew,kurt,var,cvar],axis=1)
            stats_exp_classes.columns= ['Ret','Vol','Sharpe','Skew','Kurt', 'VaR(95%)', 'cVaR(95%)']
            
            return stats_exp_classes
        
        aux_stats_hist = simple_stats(self.expectations['hist'])
        aux_stats_expec = simple_stats(adjusted_hist)
        aux_stats_blend = simple_stats(0.5*adjusted_hist + 0.5*self.expectations['hist'].reindex(adjusted_hist.index))
        
        # !!! 5) Build solutions for each mandate for selected resample ===============
        
        #compile all solutions
        all_solutions = {}
        all_solvers = {}
        
        for m,mandate in self.all_mandates.items():
            print(f'\n|---> Optimization: Mandate "{m}"')
            time.sleep(1)
        
            solutions_m, debug = algoOPT.find_optimized_portfolio(
                                                list_resample= resample, 
                                                
                                                param_objective= mandate['objetivo'], 
                                                param_target= mandate['alvo'],
                                                
                                                mandate_risk_type = mandate['tipo_risco'],
                                                
                                                mandate_bound_fin= mandate['limites_fin'],
                                                mandate_bound_risk= mandate['limites_cr'],
                                                mandate_constraints= mandate['rest']
                                                )
        
            all_solutions[m] = solutions_m.copy()
            all_solvers[m] = debug
        
        #best solutions and rounding
        best_solutions = {}
        
        for m,alloc in all_solutions.items():
            best_solutions[m] = algoOPT.get_best_solution(alloc)
            
        df_best = pd.concat([v[self.combination_method].rename(k) for k,v in best_solutions.items()], axis= 1)
            
        # !!! 6) Get allocation bands and max risk tolerance for portfolios ===========
        
        #define allocation bands in iteration
        all_bands = {}
        
        for m,aloc in all_solutions.items():
            print(f'\n|---> Allocation Bands: Mandate "{m}"')
            time.sleep(1)
            
            mandate = self.all_mandates[m].copy()
        
            bands = algoOPT.bands_tracking_error(df_best[m], 
                                                 mandate, adjusted_hist,
                                                 rules_to_ignore=self.override_constraints, 
                                                 class_order= None)
        
            all_bands[m] = bands.copy()
        
        all_bands_classes = {k:v.copy().dropna() for k,v in all_bands.items()}
        
        #redefine bands with cVaR limits
        final_bands = {}
        extreme_alloc = {}
        
        for m in df_best.columns:
            try:
                mandate = self.all_mandates[m].copy()
                m_prev = m
            except:
                mandate = self.all_mandates[list(self.all_mandates.keys())[0]].copy()
        
            bands_risk, given_alloc = algoOPT.bounded_tail_risk(all_bands[m], 
                                                                mandate, 
                                                                self.expectations['hist'], 
                                                                conf= 0.95,
                                                                window= 6)
        
            final_bands[m] = bands_risk.copy()
            extreme_alloc[m] = given_alloc.copy()
        
        # !!! 7) Get statistics and risk contribution profile =========================
        
        #portfolio historical (backtest) + expected statistics
        all_stats = {}
        all_stats_2005 = {}
        all_stats_2010 = {}
        all_stats_2018 = {}
        
        for m in df_best.columns:
            all_stats[m] = algoOPT.eval_stats(df_best[m], self.expectations)
        df_stats = pd.DataFrame(all_stats)
        
        for m in df_best.columns:
            all_stats_2005[m] = algoOPT.eval_stats(df_best[m], self.expectations, begin= '2005-01-01')
        df_stats_2005 = pd.DataFrame(all_stats_2005)
        
        for m in df_best.columns:
            all_stats_2010[m] = algoOPT.eval_stats(df_best[m], self.expectations, begin= '2010-01-01')
        df_stats_2010 = pd.DataFrame(all_stats_2010)
        
        for m in df_best.columns:
            all_stats_2018[m] = algoOPT.eval_stats(df_best[m], self.expectations, begin= '2019-06-01')
        df_stats_2018 = pd.DataFrame(all_stats_2018)
        
        #portfolio expected probabilities 
        df_prob_0, df_prob_cash, df_prob_infla = algoOPT.eval_probabilities(df_best, 
                                                                            resample, 
                                                                            windows= [12,36,60,120,240,300],
                                                                            infla_comp= 0.0)
        
        #risk contribution
        all_riskcont = {}
        
        for m in df_best.columns:
            all_riskcont[m] = algoOPT.eval_riskcont(df_best[m], adjusted_hist)
        
        df_rc_total = pd.concat([v['RiskCont'].rename(k) for k,v in all_riskcont.items()], axis= 1)
        df_rc_worst = pd.concat([v['Worst10%'].rename(k) for k,v in all_riskcont.items()], axis= 1)
        df_rc_tail = pd.concat([v['LossCont'].rename(k) for k,v in all_riskcont.items()], axis= 1)
        
        #historical returns 
        all_hist_ret = {}
        
        hist_class = self.expectations['hist'].copy()
        for m in df_best.columns:
            m_series = hist_class.reindex(columns= df_best.index).dot(df_best[m])
            all_hist_ret[m] = m_series
            
        df_sim_ret = pd.DataFrame(all_hist_ret)
            
        # !!! 8) Export all results ===================================================
            
        #create folder
        folder_mandate = self.file_mandate.split('.')[0]
        suffix = datetime.today().strftime('%Y-%m-%d - T%H%M') + f' - N={self.n_simulations}'
        
        path_folder = f'{self.folder}/Otim/Optimized at {suffix}' 
        Path(path_folder).mkdir(parents= True, exist_ok= True)
        
        #save optimization results in excel
        destination = path_folder+'/Allocation.xlsx'
        
        with pd.ExcelWriter(destination, engine= 'xlsxwriter') as writer:  
            for m,band in all_bands.items():
                band.to_excel(writer, sheet_name= m)
        
        #statistics output
        destination = path_folder+'/Statistics.xlsx'
        
        with pd.ExcelWriter(destination, engine= 'xlsxwriter') as writer:
            df_stats.to_excel(writer, sheet_name= 'Statistics')
            df_best.to_excel(writer, sheet_name= 'Neutral Port')
            df_rc_total.to_excel(writer, sheet_name= 'Risk Contribution')
            df_rc_tail.to_excel(writer, sheet_name= 'RC Worst')
            df_sim_ret.to_excel(writer, sheet_name= 'Simulated Returns')
        
        #historical adjusted returns output
        destination = path_folder+'/Returns_Adjusted.xlsx'
        adjusted_hist.to_excel(destination, sheet_name= 'NonParametric')
        
        # !!! 9) Create factsheets for all optimized mandates =========================
        
        dfig = {}
        
        #simple charting
        for i,(m,band) in enumerate(all_bands_classes.items()):
            
            #plot allocation bands
            dfig[f'Bandas de Alocação - {m}'] = plotOPT.plot_bands(band, mandate= m)
            
            #plot capital gain
            portfolio_series = adjusted_hist.reindex(columns= band.index).dot(band['Neutro'])
            dfig[f'Ganho de Capital - {m}'] = plotOPT.plot_capital_gain(portfolio_series.mean(), portfolio_series.std(), horizon= 10, mandate= m, language=self.language)
        
            #plot risk allocation cash x risk contribution
            dfig[f'Contribuição de Risco - {m}'] = plotOPT.plot_risk_contribution(band['Neutro'], df_rc_total[m], mandate= m, language=self.language)
        
            #plot tail risk (cVaR) decomposition
            dfig[f'Decomposição de Perdas Esperadas - {m}'] = plotOPT.plot_tail_contribution(adjusted_hist, band['Neutro'], mandate= m, conf= 0.90, language=self.language)
        
        #plot efficient frontier against optimization
        dfig['Fronteira Eficiente'] = plotOPT.plot_frontier(adjusted_hist, all_bands_classes, language=self.language)
        
        #plot financial x risk allocation 
        dfig['Orçamento Financeiro x de Riscos'] = plotOPT.plot_all_rc(df_best, df_rc_total, language=self.language)
        
        #plot historical backtest (cumulative returns and volatilities)
        hist_class = self.expectations['hist'].copy()
        dfig['Retornos Históricos'] = plotOPT.plot_backtest(hist_class, all_bands_classes, start= 2010, language=self.language)
        dfig['Drawdown Histórico'] = plotOPT.plot_underwater(hist_class, all_bands_classes, start= 2010, language=self.language)
        dfig['Volatilidade Móvel'] = plotOPT.plot_rolling_vol(hist_class, all_bands_classes, halflife=12, language=self.language)
        
        #export all figures in dictionary
        path_figures = path_folder+'/Figures'
        Path(path_figures).mkdir(parents= True, exist_ok= True)
        
        for title,f in dfig.items():
            f.savefig(f'{path_figures}/{title}.png', bbox_inches='tight')
