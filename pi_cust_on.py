# -*- coding: utf-8 -*-

### 1) DATA PREP ===========================================

#packages
import pandas as pd

from datetime import datetime

import os
import sys
import shutil

from pathlib import Path

from funcoes import get_range, fill_range_df

sys.path.insert(0, r'I:\Shared\SAO_Investimentos\Asset Allocation\Repo')
import pizza #pizza_mgt as pizza 


class PICustomizada:
    """
    Exemplo para geração:
        from asset_allocation.pi_cust_on import PICustomizada
        pi_sim = PICustomizada(bkt_path='2aGeracao.xlsx', pasta=r'O:\SAO\CICH_All\Portfolio Management\11 - Apresentações\Hematitas\2025 01 Simulacao Mandato')
    """    
    
    def __init__(self, bkt_path='Parâmetros PI Onshore.xlsx', out_path='/PI/', pasta='C:/Temp', rodar:bool=True):
        #global variables
        self.path_system = r"O:\SAO\CICH_All\Portfolio Management\Foundation\asset_allocation"
        self.bkt_path = pasta + '/' + bkt_path
        self.out_path = pasta + '/'+ out_path
        self.obj_opt = None
        
        if rodar:
            self.rodar()

    def rodar(self):
        #get optimization object
        self.obj_opt = pizza.PortOpt(universe= 'On', tag= 'Main', freq= 'D')
        
        #get backtesting global parameters
        df_params = get_range(self.bkt_path, 'params')
        params = dict(zip(df_params['Parâmetro'],df_params['Valor']))
        
        folder_bkt = datetime.today().strftime('%y.%m.%d') + ' ' + params['Nome']
        
        ### 2) GENERATE ACTIVE-RISK BASED RANGES ===========================================
        
        #get active risk level
        ar_dict = {'Baixo': 0.10, 'Padrão': 0.15, 'Alto': 0.25}
        ar_label = params['Risco Ativo']
        
        act_risk = ar_dict[ar_label]
        
        #read simulated portfolios data
        df_ports = get_range(self.bkt_path, 'alloc')
        df_ports['SAA'] = df_ports['SAA'].fillna(0)
        
        #keep only asset classes in policy
        df_over = get_range(self.bkt_path, 'override')
        df_over = df_over.loc[df_over['Possui Banda'] == 1].copy()
        
        ranges_mkt = df_over.set_index('Mercado')[['Min','Max']]
        
        df_ports = df_ports.loc[df_ports['Mercado'].isin(ranges_mkt.index)]
        
        #organize optimization input
        w_strat = df_ports.set_index('Estratégia')['SAA'].copy()
        lst_mkt = df_ports['Mercado'].tolist()
        
        #run optimization
        results = self.obj_opt.build_policy_ranges(w_strat, ranges_mkt, 
                                              active_risk= act_risk, 
                                              window_cvar= 6, 
                                              window_te= 120,
                                              liquidity= 'RF Pós',
                                              conf= 0.95,
                                              max_tail_multiplier= 5)
        
        results_estrat = self.obj_opt.results_strategy
        
        ### 3) EXPORT RESULTS ===========================================
        
        #adjust labels
        new_labels = dict(zip(df_over['Mercado'],df_over['Nome Print']))
        
        res_print = results.copy()
        res_print = res_print.apply(lambda x: x * 100)
        res_print = res_print.rename(index= new_labels)
        
        res_print = res_print.rename(index= {'Tail Risk': 'Perda Estimada em Cenários de Stress (% em 6 meses)'})
        
        res_print.index.name = None
        
        ### 4) NEW ADDITION FOR BATCH FILE ===========================================
        
        #create results folder
        path_bat = self.out_path + folder_bkt
        print('path_bat',path_bat)
        Path(path_bat).mkdir(parents= True, exist_ok= True)
        
        #duplicate layout file 
        copy_tbl = 'Tabela TAPI Custom - ' + params['Nome'] + '.xlsx'
        
        path_layout = self.path_system + '/pi_customizada/Layout - PI Customizada.xlsx'
        path_copy_tbl = path_bat + '/' + copy_tbl
        
        shutil.copyfile(path_layout, path_copy_tbl)
        
        #duplicate docx file
        copy_doc = 'TAPI Customizada Local - ' + params['Nome'] + '.docx'
        
        path_layout = self.path_system + '/pi_customizada/Layout - TAPI Customizada Local.docx'
        path_copy_doc = path_bat + '/' + copy_doc
        
        shutil.copyfile(path_layout, path_copy_doc)        
            
        #export Policy definition table
        fill_range_df(path_copy_tbl, 'tapi', res_print, 
                      keep_index= True, keep_header= True, sheet_name='PI')
        
        #export Strategy definition table
        fill_range_df(path_copy_tbl, 'tapi', results_estrat, 
                      keep_index= True, keep_header= True, sheet_name='Estrategia')
        
        #open Excel file
        full_path_bat = os.path.abspath(path_bat)
        
        full_path_tbl = os.path.abspath(path_copy_tbl)
        os.startfile(full_path_tbl)
        
        full_path_doc = os.path.abspath(path_copy_doc)
        os.startfile(full_path_doc)
        
        #print results folder and success message
        print('\n ... Sucesso!\n')
        print(f'\n ... Resultados disponiveis em: {full_path_bat}\n\n')
