# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 18:55:01 2023

@author: u45285
"""

####Cálculo de Tracking error do JBFO Juros Ativo

from databases import Bawm, BDS
import pandas as pd
from datetime import  date, datetime as dt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from funcoes_datas import FuncoesDatas
from pretty_html_table import build_table


class te:

    def calculo_tracking_juros_ativo(self,peso_atual):

        
        ##Rodar as classes
        
        self.peso_atual = peso_atual
        bawm = Bawm()
        bds = BDS()
        mdFormat = mdates.DateFormatter('%b/%y')
        data = FuncoesDatas()
        
        #Selecionar as datas
        hoje = data.workday(data = data.hoje(),n_dias = -3,feriados_brasil=True, feriados_eua=False)
        data_inicial = d1 = data.workday(data = data.hoje(),n_dias = -1260,feriados_brasil=True, feriados_eua=False)
        
        #Cálculo de proporção ideal de cada índice IMA (IMAB5 + IMAB5+) para simular uma duration under e over permitida pelo mandato (2.5 anos)
        def calculo_pesos(duration_IMAB,duration_IMAB5,duration_IMAB5_longo,limite):
            peso = 1-((duration_IMAB+limite)-duration_IMAB5)/(duration_IMAB5_longo-duration_IMAB5)
            return peso
        
        #Cálculo da checagem de proposção (se atende a duration entre +/- 2.5 anos)
        #Checagem da range do limite
        def calculo_check(peso,duration_IMAB5,duration_IMAB_5_longo,duration_IMAB):
            duration = peso *duration_IMAB5 + (1-peso)*duration_IMAB_5_longo - duration_IMAB
            return duration
        
        #Cotas do BNP
        rent_bnp = bds.serie_historico(idserie =59935302 ,data_ini= data_inicial, data_fim = hoje)
        rent_bnp = rent_bnp[['dataser','a8','a25']].rename(columns = {'dataser':'Data','a8':'Cota_BNP','a25':'Rent_BNP'})
        rent_bnp['Rent_BNP']=rent_bnp['Rent_BNP']/100
        self.rent_bnp =  rent_bnp
        
        #Dados do IMA
        #Duration 
        duration_IMAB = bawm.in_historico(index_cod='IMAIPCADUR', data_ini=data_inicial, data_fim=hoje)
        duration_IMAB = duration_IMAB[['Data','Valor']]
        duration_IMAB['Valor'] = duration_IMAB['Valor']/252
        duration_IMAB5 = bawm.in_historico(index_cod='BZRFB5DU', data_ini=data_inicial, data_fim=hoje)
        duration_IMAB5 = duration_IMAB5[['Data','Valor']]
        duration_IMAB5['Valor'] = duration_IMAB5['Valor']/252
        duration_IMAB5_longo = bawm.in_historico(index_cod='BZRFB5+D', data_ini=data_inicial, data_fim=hoje)
        duration_IMAB5_longo = duration_IMAB5_longo[['Data','Valor']]
        duration_IMAB5_longo['Valor'] = duration_IMAB5_longo['Valor']/252
        duration = pd.merge(duration_IMAB,duration_IMAB5, on ='Data',how='outer').merge(duration_IMAB5_longo,on ='Data', how = 'outer')
        duration.columns = ['Data','duration_IMAB','duration_IMAB5','duration_IMAB_5_longo']
        
        #Rentabilidade e duration do índice IMAB para os calculos de TE.
        rent_IMAB = bawm.in_historico(index_cod='IMAIPCA', data_ini=data_inicial, data_fim=hoje)
        rent_IMAB = rent_IMAB[['Data','Valor']]
        rent_IMAB['Rentabilidade'] = (rent_IMAB['Valor']/rent_IMAB['Valor'].shift(1))-1
        self.rent_IMAB = rent_IMAB
        
        rent_IMAB5 = bawm.in_historico(index_cod='IMAB5', data_ini=data_inicial, data_fim=hoje)
        rent_IMAB5 = rent_IMAB5[['Data','Valor']]
        rent_IMAB5['Rentabilidade'] = (rent_IMAB5['Valor']/rent_IMAB5['Valor'].shift(1))-1
        self.rent_IMAB5 = rent_IMAB5
        
        rent_IMAB5longo = bawm.in_historico(index_cod='IMAB5+', data_ini=data_inicial, data_fim=hoje)
        rent_IMAB5longo = rent_IMAB5longo[['Data','Valor']]
        rent_IMAB5longo['Rentabilidade'] = (rent_IMAB5longo['Valor']/rent_IMAB5longo['Valor'].shift(1))-1
        self.rent_IMAB5longo = rent_IMAB5longo
        rent = pd.merge(self.rent_IMAB,self.rent_IMAB5, on ='Data',how='outer').merge(self.rent_IMAB5longo,on ='Data', how = 'outer').merge(self.rent_bnp,on ='Data', how = 'outer')
        rent.columns = ['Data','IMAB','Rent_IMAB','IMAB5','Rent_IMAB5','IMAB_5_longo','Rent_IMAB_5_longo','Cota_BNP','Rent_BNP_Inflacao']
        self.rent = rent
            
        
        #Tabela para o cálculo TE da classe IMAB
        tabela = pd.merge(left = rent, right = duration, on = 'Data', how = 'outer')
        tabela['peso_under'] = tabela.apply(lambda row : calculo_pesos(row['duration_IMAB'],row['duration_IMAB5'],row['duration_IMAB_5_longo'],-2.5),axis=1)
        tabela['peso_over'] = tabela.apply(lambda row : calculo_pesos(row['duration_IMAB'],row['duration_IMAB5'],row['duration_IMAB_5_longo'],+2.5),axis=1)
        tabela['check_under']=tabela.apply(lambda row: round(calculo_check(peso= row['peso_under'],duration_IMAB5 = row['duration_IMAB5'],duration_IMAB_5_longo= row['duration_IMAB_5_longo'],duration_IMAB = row['duration_IMAB']),3), axis = 1)
        tabela['check_over']=tabela.apply(lambda row: round(calculo_check(peso= row['peso_over'],duration_IMAB5 = row['duration_IMAB5'],duration_IMAB_5_longo= row['duration_IMAB_5_longo'],duration_IMAB = row['duration_IMAB']),3), axis = 1)
        tabela['Rent_IMAB_under']=tabela['peso_under'].shift(1)*tabela['Rent_IMAB5']+(1-tabela['peso_under'].shift(1))*tabela['Rent_IMAB_5_longo']
        tabela['Rent_IMAB_over']=tabela['peso_over'].shift(1)*tabela['Rent_IMAB5']+(1-tabela['peso_over'].shift(1))*tabela['Rent_IMAB_5_longo']
        tabela.dropna(inplace = True)
        self.tabela = tabela
        
        
        #Cotas do Juros Ativos 
        rent_juros_ativo = bds.serie_historico(idserie = 21464079 ,data_ini= data_inicial, data_fim = hoje)
        rent_juros_ativo = rent_juros_ativo[['dataser','a8','a25']].rename(columns = {'dataser':'Data','a8':'Cota_juros_ativo','a25':'Rent_Juros_Ativo'})
        rent_juros_ativo['Rent_Juros_Ativo']=rent_juros_ativo['Rent_Juros_Ativo']/100
        rent_juros_ativo = rent_juros_ativo.dropna(subset = ['Cota_juros_ativo'])
        rent_juros_ativo= pd.merge(right = self.rent_IMAB, left = rent_juros_ativo,on = 'Data',how ='left').rename(columns ={'Valor':'IMAB','Rentabilidade':'Rentabilidade_IMAB'})
       
        #Simulação fundo 100% tomado ou aplicado
    
        #Rentabilidade da exposição a taxa pré-ficada
        rent_IDKA_5A = bawm.in_historico(index_cod='IDKA_5A', data_ini=data_inicial, data_fim=hoje)
        rent_IDKA_5A = rent_IDKA_5A[['Data','Valor']].rename(columns = {'Valor':'IDKA'})
        rent_IDKA_5A['Rentabilidade_IDKA'] = (rent_IDKA_5A['IDKA']/rent_IDKA_5A['IDKA'].shift(1))-1
        self.rent_IDKA_5A=rent_IDKA_5A
        
        CDI = bawm.in_historico(index_cod='IDKA_5A', data_ini=data_inicial, data_fim=hoje)
        CDI = CDI[['Data','Valor']].rename(columns = {'Valor':'Variação_CDI'})
        self.CDI = CDI
        
        dados_pre = pd.merge(right = rent_IDKA_5A, left = CDI, on='Data', how='outer')
        dados_pre['DI FUT'] = dados_pre['Variação_CDI'] - (dados_pre['Rentabilidade_IDKA'] - dados_pre['Variação_CDI'])
        
        ##Simulação de uma carteira IMAB5+ (100% tomada ou aplicada a taxa pré)
        
        dados_pre= pd.merge(right = rent_IMAB, left = dados_pre,on = 'Data',how ='outer').rename(columns ={'Valor':'IMAB','Rentabilidade':'Rentabilidade_IMAB'})
        dados_pre['Rent_DI_Tomado'] = 1-dados_pre['Rentabilidade_IDKA']
        dados_pre['Rent_DI_Aplicado'] = dados_pre['Rentabilidade_IDKA']+1
        dados_pre['Operação_DI_Tomado']  = (dados_pre['Rent_DI_Tomado'] *  (dados_pre['Rentabilidade_IMAB']+1)-1)
        dados_pre['Operação_Aplicado']  = (dados_pre['Rent_DI_Aplicado'] *  (dados_pre['Rentabilidade_IMAB']+1)-1)      
        self.dados_pre = dados_pre 
    
        #Cálculo dos TE
        
        ##TE dos índices IMAB e BNP Vic Inflação
        
        TE_under_mandato = tabela['Rent_IMAB_under']-tabela['Rent_IMAB']
        self.TE_under_mandato = TE_under_mandato.std()*(252**0.5)
        TE_over_mandato = tabela['Rent_IMAB_over']-tabela['Rent_IMAB']
        self.TE_over_mandato = TE_over_mandato.std()*(252**0.5)
        
        #TE do BNP Vic Inflação e do Juros Ativo
        
        TE_BNP = tabela['Rent_BNP_Inflacao']-tabela['Rent_IMAB']
        self.TE_BNP = TE_BNP.std()*(252**0.5)
        TE_juros_ativo= rent_juros_ativo['Rent_Juros_Ativo']-rent_juros_ativo['Rentabilidade_IMAB']
        self.TE_juros_ativo = TE_juros_ativo.std()*(252**0.5)
    
    
    
        ##TE do veículo completamente pré
        TE_Pre_tomado = dados_pre['Operação_DI_Tomado']-dados_pre['Rentabilidade_IMAB']
        self.TE_Pre_tomado = TE_Pre_tomado.std()*(252**0.5)
        TE_Pre_aplicado = dados_pre['Operação_Aplicado']-dados_pre['Rentabilidade_IMAB']
        self.TE_Pre_aplicado = TE_Pre_aplicado.std()*(252**0.5)
        
        
        def truncate_float(float_number, decimal_places):
            multiplier = 10 ** decimal_places
            return int(float_number * multiplier) / multiplier
                    
        
        def exposicao_maxima(peso_atual):
            max_te = truncate_float(self.TE_over_mandato/peso_atual*100,2)
            max_exposicao = truncate_float(max_te/self.TE_Pre_aplicado,2)
            limite_usado = truncate_float((self.TE_juros_ativo*100/max_te)*100,2)
            te_juros_ativo = truncate_float(self.TE_juros_ativo*100,2)
            return max_te,max_exposicao,limite_usado, te_juros_ativo
        
        
        #return exposicao_maxima(peso_atual)[0],exposicao_maxima(peso_atual)[1],exposicao_maxima(peso_atual)[2]
        return  f'''Atual TE : {exposicao_maxima(self.peso_atual)[3]}%    TE Máximo {exposicao_maxima(self.peso_atual)[0]}%.
                     Uso do Limite {exposicao_maxima(self.peso_atual)[2]}%'''

    def calculo_tracking_juros_ativo_longo(self,peso_atual):

        
        ##Rodar as classes
        
        self.peso_atual = peso_atual
        bawm = Bawm()
        bds = BDS()
        mdFormat = mdates.DateFormatter('%b/%y')
        data = FuncoesDatas()
        
        #Selecionar as datas
        hoje = data.workday(data = data.hoje(),n_dias = -3,feriados_brasil=True, feriados_eua=False)
        data_inicial = d1 = data.workday(data = data.hoje(),n_dias = -1260,feriados_brasil=True, feriados_eua=False)
        
        #Cálculo de proporção ideal de cada índice IMA (IMAB5 + IMAB5+) para simular uma duration under e over permitida pelo mandato (2.5 anos)
        def calculo_pesos(duration_IMAB,duration_IMAB5,duration_IMAB5_longo,limite):
            peso = 1-((duration_IMAB5+limite)-duration_IMAB5)/(duration_IMAB5_longo-duration_IMAB5)
            return peso
        
        #Cálculo da checagem de proposção (se atende a duration entre +/- 2.5 anos)
        #Checagem da range do limite
        def calculo_check(peso,duration_IMAB5,duration_IMAB_5_longo,duration_IMAB):
            duration = peso *duration_IMAB5 + (1-peso)*duration_IMAB_5_longo - duration_IMAB
            return duration
        
        #Cotas do BNP
        rent_bnp = bds.serie_historico(idserie =59935302 ,data_ini= data_inicial, data_fim = hoje)
        rent_bnp = rent_bnp[['dataser','a8','a25']].rename(columns = {'dataser':'Data','a8':'Cota_BNP','a25':'Rent_BNP'})
        rent_bnp['Rent_BNP']=rent_bnp['Rent_BNP']/100
        self.rent_bnp =  rent_bnp
        
        #Dados do IMA
        #Duration 
        duration_IMAB = bawm.in_historico(index_cod='IMAIPCADUR', data_ini=data_inicial, data_fim=hoje)
        duration_IMAB = duration_IMAB[['Data','Valor']]
        duration_IMAB['Valor'] = duration_IMAB['Valor']/252
        duration_IMAB5 = bawm.in_historico(index_cod='BZRFB5DU', data_ini=data_inicial, data_fim=hoje)
        duration_IMAB5 = duration_IMAB5[['Data','Valor']]
        duration_IMAB5['Valor'] = duration_IMAB5['Valor']/252
        duration_IMAB5_longo = bawm.in_historico(index_cod='BZRFB5+D', data_ini=data_inicial, data_fim=hoje)
        duration_IMAB5_longo = duration_IMAB5_longo[['Data','Valor']]
        duration_IMAB5_longo['Valor'] = duration_IMAB5_longo['Valor']/252
        duration = pd.merge(duration_IMAB,duration_IMAB5, on ='Data',how='outer').merge(duration_IMAB5_longo,on ='Data', how = 'outer')
        duration.columns = ['Data','duration_IMAB','duration_IMAB5','duration_IMAB_5_longo']
        
        #Rentabilidade e duration do índice IMAB para os calculos de TE.
        rent_IMAB = bawm.in_historico(index_cod='IMAIPCA', data_ini=data_inicial, data_fim=hoje)
        rent_IMAB = rent_IMAB[['Data','Valor']]
        rent_IMAB['Rentabilidade'] = (rent_IMAB['Valor']/rent_IMAB['Valor'].shift(1))-1
        self.rent_IMAB = rent_IMAB
        
        rent_IMAB5 = bawm.in_historico(index_cod='IMAB5', data_ini=data_inicial, data_fim=hoje)
        rent_IMAB5 = rent_IMAB5[['Data','Valor']]
        rent_IMAB5['Rentabilidade'] = (rent_IMAB5['Valor']/rent_IMAB5['Valor'].shift(1))-1
        self.rent_IMAB5 = rent_IMAB5
        
        rent_IMAB5longo = bawm.in_historico(index_cod='IMAB5+', data_ini=data_inicial, data_fim=hoje)
        rent_IMAB5longo = rent_IMAB5longo[['Data','Valor']]
        rent_IMAB5longo['Rentabilidade'] = (rent_IMAB5longo['Valor']/rent_IMAB5longo['Valor'].shift(1))-1
        self.rent_IMAB5longo = rent_IMAB5longo
        rent = pd.merge(self.rent_IMAB,self.rent_IMAB5, on ='Data',how='outer').merge(self.rent_IMAB5longo,on ='Data', how = 'outer').merge(self.rent_bnp,on ='Data', how = 'outer')
        rent.columns = ['Data','IMAB','Rent_IMAB','IMAB5','Rent_IMAB5','IMAB_5_longo','Rent_IMAB_5_longo','Cota_BNP','Rent_BNP_Inflacao']
        self.rent = rent
            
        
        #Tabela para o cálculo TE da classe IMAB
        tabela = pd.merge(left = rent, right = duration, on = 'Data', how = 'outer')
        tabela['peso_under'] = tabela.apply(lambda row : calculo_pesos(row['duration_IMAB'],row['duration_IMAB5'],row['duration_IMAB_5_longo'],-2.5),axis=1)
        tabela['peso_over'] = tabela.apply(lambda row : calculo_pesos(row['duration_IMAB'],row['duration_IMAB5'],row['duration_IMAB_5_longo'],+2.5),axis=1)
        tabela['check_under']=tabela.apply(lambda row: round(calculo_check(peso= row['peso_under'],duration_IMAB5 = row['duration_IMAB5'],duration_IMAB_5_longo= row['duration_IMAB_5_longo'],duration_IMAB = row['duration_IMAB']),3), axis = 1)
        tabela['check_over']=tabela.apply(lambda row: round(calculo_check(peso= row['peso_over'],duration_IMAB5 = row['duration_IMAB5'],duration_IMAB_5_longo= row['duration_IMAB_5_longo'],duration_IMAB = row['duration_IMAB']),3), axis = 1)
        tabela['Rent_IMAB_under']=tabela['peso_under'].shift(1)*tabela['Rent_IMAB5']+(1-tabela['peso_under'].shift(1))*tabela['Rent_IMAB_5_longo']
        tabela['Rent_IMAB_over']=tabela['peso_over'].shift(1)*tabela['Rent_IMAB5']+(1-tabela['peso_over'].shift(1))*tabela['Rent_IMAB_5_longo']
        tabela.dropna(inplace = True)
        self.tabela = tabela
        
        
        #Cotas do Juros Ativos 
        rent_juros_ativo = bds.serie_historico(idserie = 21464079 ,data_ini= data_inicial, data_fim = hoje)
        rent_juros_ativo = rent_juros_ativo[['dataser','a8','a25']].rename(columns = {'dataser':'Data','a8':'Cota_juros_ativo','a25':'Rent_Juros_Ativo'})
        rent_juros_ativo['Rent_Juros_Ativo']=rent_juros_ativo['Rent_Juros_Ativo']/100
        rent_juros_ativo = rent_juros_ativo.dropna(subset = ['Cota_juros_ativo'])
        rent_juros_ativo= pd.merge(right = self.rent_IMAB, left = rent_juros_ativo,on = 'Data',how ='left').rename(columns ={'Valor':'IMAB','Rentabilidade':'Rentabilidade_IMAB'})
       
        #Simulação fundo 100% tomado ou aplicado
    
        #Rentabilidade da exposição a taxa pré-ficada
        rent_IDKA_5A = bawm.in_historico(index_cod='IDKA_5A', data_ini=data_inicial, data_fim=hoje)
        rent_IDKA_5A = rent_IDKA_5A[['Data','Valor']].rename(columns = {'Valor':'IDKA'})
        rent_IDKA_5A['Rentabilidade_IDKA'] = (rent_IDKA_5A['IDKA']/rent_IDKA_5A['IDKA'].shift(1))-1
        self.rent_IDKA_5A=rent_IDKA_5A
        
        CDI = bawm.in_historico(index_cod='IDKA_5A', data_ini=data_inicial, data_fim=hoje)
        CDI = CDI[['Data','Valor']].rename(columns = {'Valor':'Variação_CDI'})
        self.CDI = CDI
        
        dados_pre = pd.merge(right = rent_IDKA_5A, left = CDI, on='Data', how='outer')
        dados_pre['DI FUT'] = dados_pre['Variação_CDI'] - (dados_pre['Rentabilidade_IDKA'] - dados_pre['Variação_CDI'])
        
        ##Simulação de uma carteira IMAB5+ (100% tomada ou aplicada a taxa pré)
        
        dados_pre= pd.merge(right = rent_IMAB, left = dados_pre,on = 'Data',how ='outer').rename(columns ={'Valor':'IMAB','Rentabilidade':'Rentabilidade_IMAB'})
        dados_pre['Rent_DI_Tomado'] = 1-dados_pre['Rentabilidade_IDKA']
        dados_pre['Rent_DI_Aplicado'] = dados_pre['Rentabilidade_IDKA']+1
        dados_pre['Operação_DI_Tomado']  = (dados_pre['Rent_DI_Tomado'] *  (dados_pre['Rentabilidade_IMAB']+1)-1)
        dados_pre['Operação_Aplicado']  = (dados_pre['Rent_DI_Aplicado'] *  (dados_pre['Rentabilidade_IMAB']+1)-1)      
        self.dados_pre = dados_pre 
    
        #Cálculo dos TE
        
        ##TE dos índices IMAB e BNP Vic Inflação
        
        TE_under_mandato = tabela['Rent_IMAB_under']-tabela['Rent_IMAB']
        self.TE_under_mandato = TE_under_mandato.std()*(252**0.5)
        TE_over_mandato = tabela['Rent_IMAB_over']-tabela['Rent_IMAB']
        self.TE_over_mandato = TE_over_mandato.std()*(252**0.5)
        
        #TE do BNP Vic Inflação e do Juros Ativo
        
        TE_BNP = tabela['Rent_BNP_Inflacao']-tabela['Rent_IMAB']
        self.TE_BNP = TE_BNP.std()*(252**0.5)
        TE_juros_ativo= rent_juros_ativo['Rent_Juros_Ativo']-rent_juros_ativo['Rentabilidade_IMAB']
        self.TE_juros_ativo = TE_juros_ativo.std()*(252**0.5)
    
    
    
        ##TE do veículo completamente pré
        TE_Pre_tomado = dados_pre['Operação_DI_Tomado']-dados_pre['Rentabilidade_IMAB']
        self.TE_Pre_tomado = TE_Pre_tomado.std()*(252**0.5)
        TE_Pre_aplicado = dados_pre['Operação_Aplicado']-dados_pre['Rentabilidade_IMAB']
        self.TE_Pre_aplicado = TE_Pre_aplicado.std()*(252**0.5)
        
        
        def truncate_float(float_number, decimal_places):
            multiplier = 10 ** decimal_places
            return int(float_number * multiplier) / multiplier
                    
        
        def exposicao_maxima(peso_atual):
            max_te = truncate_float(self.TE_over_mandato/peso_atual*100,2)
            max_exposicao = truncate_float(max_te/self.TE_Pre_aplicado,2)
            limite_usado = truncate_float((self.TE_juros_ativo*100/max_te)*100,2)
            te_juros_ativo = truncate_float(self.TE_juros_ativo*100,2)
            return max_te,max_exposicao,limite_usado, te_juros_ativo
        
        
        #return exposicao_maxima(peso_atual)[0],exposicao_maxima(peso_atual)[1],exposicao_maxima(peso_atual)[2]
        return  f'''Atual TE : {exposicao_maxima(self.peso_atual)[3]}%    TE Máximo {exposicao_maxima(self.peso_atual)[0]}%.
                     Uso do Limite {exposicao_maxima(self.peso_atual)[2]}%'''

