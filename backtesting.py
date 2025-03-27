# -*- coding: utf-8 -*-

### 1) DATA PREP ===========================================

#packages
import pandas as pd

from datetime import datetime

import os
import sys
import shutil

import glob
from pathlib import Path

from ambiente import pasta_base_pm
from funcoes import get_range, fill_range_df, fill_range_value

sys.path.insert(0, r'I:\Shared\SAO_Investimentos\Asset Allocation\Repo')
import pizza

class Backtest:
    
    def __init__(self, bkt_path:str='Parâmetros Backtesting Onshore.xlsx', out_path:str='./Resultados/'
                 , rodar:bool=True):        
        #global variables
        self.bkt_path = bkt_path
        self.out_path = out_path

        #get ports data/objects
        self.obj_port = pizza.DataPorts(universe= 'On')
        
        if rodar:
            print('\n ... Rodando ...\n')
            self.rodar()

    def rodar(self):
        #get stats/charts objects
        Calc = pizza.PortAnalysis(universe= 'On', tag= 'Main', freq= 'D')
        
        #get backtesting global parameters
        df_params = get_range(self.bkt_path, 'params')
        params = dict(zip(df_params['Parâmetro'],df_params['Valor']))        
        
        #select starting date for backtesting 
        dt_stt = params['Data Início']
        
        ### 2) COMPILE LONG-TERM WEIGHTS ===========================================
        
        #read all standard mandates 
        df_standard = self.obj_port.get_saa(indexed_by= 'Estratégia')
        
        #read simulated portfolios data
        df_ports = get_range(self.bkt_path, 'alloc')
        df_ports = df_ports.loc[:,df_ports.columns.notnull()]
        
        df_ports = df_ports.set_index('Estratégia').drop(columns= ['Mercado'])
        df_ports = df_ports.reindex(df_standard.index).fillna(0)
        
        #filter comparable standard mandates
        selected_standard = get_range(self.bkt_path, 'pm')
        selected_standard = selected_standard.loc[selected_standard['Usar'] == 1, 'Portfólio'].tolist()
        
        #combine selected portfolios 
        alloc_compare = pd.concat([df_ports, df_standard[selected_standard]], axis= 1)
        
        #raise Exception if more than 5 portfolios
        if alloc_compare.shape[1] > 5:
            raise ValueError('Ao todo 5 portfolios no máximo devem ser utilizados para comparação. Tente novamente')
        
        ### 3) GENERAL OUTPUT ===========================================
        
        #tables with portfolio in columns
        tbl_stats = Calc.run_stats_general(alloc_compare, level= 'index')
        tbl_years = Calc.run_stats_yearly(alloc_compare, level= 'index')
        tbl_rolls = Calc.run_stats_rolling(alloc_compare, level= 'index', window= 252)
        
        tbl_expec = Calc.run_port_expectations(alloc_compare)
        tbl_rcont = Calc.run_risk_contributions(alloc_compare)
        
        #dictionary of tables (key=portfolio)
        dic_stress = Calc.run_stats_stress(alloc_compare)
        
        #dictionaries of figures (key=portfolio)
        figs_loss_range = Calc.plot_range_loss(alloc_compare, fs= (6,5), language= 'PT', conf= 0.95)
        figs_loss_probs = Calc.plot_loss_probability(alloc_compare, fs= (6,5), language= 'PT')
        
        figs_drawdown = Calc.plot_drawdown(alloc_compare, fs= (13,6), language= 'PT', start= None)
        
        figs_cvar = Calc.plot_loss_contribution(alloc_compare, fs= (10,5), language= 'PT')
        
        #single figures (compile all allocations)
        fig_cumuret = Calc.combo_cumulative_returns(alloc_compare, fs= (13,6), language= 'PT', start= dt_stt)
        fig_rollret = Calc.combo_rolling_returns(alloc_compare, window_month= params['Janela Móvel'], fs= (13,6), language= 'PT', start= dt_stt)
        figs_rollvol = Calc.combo_rolling_volatility(alloc_compare, hl= 200, fs= (13,6), language= 'PT', start= None)
        
        ### 3) EXPORT BACKTESTED RESULTS ===========================================
        
        #adjust tables with strategies
        tbl_cash = alloc_compare.copy().drop(index= ['Outros'])
        tbl_rcont = tbl_rcont.copy().drop(index= ['Outros'])
        
        tbl_years = tbl_years.iloc[-10:]
        tbl_years.index.name = 'Ano'
        tbl_years = tbl_years.reset_index()
        
        #adjust paths
        folder_bkt = params['Nome'] + ' ' + datetime.today().strftime('%y.%m.%d %H%M')
        path_layout = pasta_base_pm() + '/Foundation/asset_allocation/backtesting/Layout - Tabelas Proposta.xlsx'
        path_bat = self.out_path + '/' + folder_bkt        
        path_plan = path_bat + '/Resultados.xlsx'
        Path(path_bat).mkdir(parents= True, exist_ok= True)
        shutil.copyfile(path_layout, path_plan)        
        
        #save all figures
        for i,port in enumerate(alloc_compare.columns):
            figs_loss_range[port].savefig(os.path.join(path_bat, f"1-{i}-cvar-range {port}.png"))
            figs_loss_probs[port].savefig(os.path.join(path_bat, f"2-{i}-loss-probs {port}.png"))
            figs_drawdown[port].savefig(os.path.join(path_bat, f"3-{i}-drawdown {port}.png"))
            figs_cvar[port].savefig(os.path.join(path_bat, f"4-{i}-cvar-decomp {port}.png"))
        
        fig_cumuret.savefig(os.path.join(path_bat, "5-cumu-ret ALL.png"))
        fig_rollret.savefig(os.path.join(path_bat, "6-roll-ret ALL.png"))
        figs_rollvol.savefig(os.path.join(path_bat, "7-roll-vol ALL.png"))
        
        #save all relevant tables
        with pd.ExcelWriter(os.path.join(path_bat, 'Tables MAIN.xlsx'), engine= 'xlsxwriter') as writer:
        
            tbl_cash.to_excel(writer, sheet_name= '1-alloc-cash')
            tbl_rcont.to_excel(writer, sheet_name= '2-alloc-risk')
        
            tbl_expec.to_excel(writer, sheet_name= '3-expec')
            tbl_stats.to_excel(writer, sheet_name= '4-stats')
            
            
            tbl_years.to_excel(writer, sheet_name= '5-years')
            tbl_rolls.to_excel(writer, sheet_name= '6-rolls')
            
        with pd.ExcelWriter(os.path.join(path_bat, 'Tables Stress.xlsx'), engine= 'xlsxwriter') as writer:
            for i,port in enumerate(alloc_compare.columns):
                dic_stress[port].to_excel(writer, sheet_name= f'{i+1} {port}')
        
        ### 4) NEW ADDITION FOR BATCH FILE ===========================================
                        
            
        #run for all backtesting tables
        fill_range_df(path_plan, 'tbl_1', tbl_cash)
        fill_range_df(path_plan, 'tbl_2', tbl_rcont)
        fill_range_df(path_plan, 'tbl_3', tbl_expec)
        fill_range_df(path_plan, 'tbl_4', tbl_stats)
        fill_range_df(path_plan, 'tbl_5', tbl_years)
        fill_range_df(path_plan, 'tbl_6', tbl_rolls)
        
        for i,(port,dd) in enumerate(dic_stress.items()):
            fill_range_value(path_plan, f'lab_{i+1}', f'Carteira "{port}"')
            fill_range_df(path_plan, f'dd_{i+1}', dd, keep_index= True, keep_header= False)
        
        #save figures in new folder as well
        for i,port in enumerate(alloc_compare.columns):
            figs_loss_range[port].savefig(os.path.join(path_bat, f"1-{i}-cvar-range {port}.png"))
            figs_loss_probs[port].savefig(os.path.join(path_bat, f"2-{i}-loss-probs {port}.png"))
            figs_drawdown[port].savefig(os.path.join(path_bat, f"3-{i}-drawdown {port}.png"))
            figs_cvar[port].savefig(os.path.join(path_bat, f"4-{i}-cvar-decomp {port}.png"))
        
        fig_cumuret.savefig(os.path.join(path_bat, "5-cumu-ret ALL.png"))
        fig_rollret.savefig(os.path.join(path_bat, "6-roll-ret ALL.png"))
        figs_rollvol.savefig(os.path.join(path_bat, "7-roll-vol ALL.png"))
        
        #open Excel file        
        os.startfile(path_plan)
        
        #print results folder and success message
        print('\n ... Sucesso!\n')
        print(f'\n ... Resultados disponiveis em: {path_bat}\n\n')
        
if __name__ == '__main__':  
    pass
    bkt = Backtest(bkt_path=r'O:\SAO\CICH_All\Portfolio Management\11 - Apresentações\Hematitas\2025 01 Simulacao Mandato\Backtesting 2a geracao.xlsx',
               out_path=r'O:\SAO\CICH_All\Portfolio Management\11 - Apresentações\Hematitas\2025 01 Simulacao Mandato\Backtest')