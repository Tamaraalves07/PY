from databases import Crm, Bawm, BDS
import pandas as pd
import matplotlib as plt
from datetime import  date, datetime as dt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import calendar
import numpy as np
from emailer import Email, EmailLer
from funcoes_datas import FuncoesDatas
from zipfile import ZipFile
from objetos import PosicaoFuncoes
from pretty_html_table import build_table
from emailer import Email 
from scipy import stats 

crm = Crm()
bawm = Bawm()
bds = BDS()
mdFormat = mdates.DateFormatter('%b/%y')
data = FuncoesDatas()

def resumo_rf():
    data = FuncoesDatas()
    list_path_figures = 'C:/Temp/teste.png'
    
    # df = bds.fundos_busca_serie('ETTJ IPCA',220)
    taxas_b = bawm.spread_ntn()
    table = pd.pivot_table(taxas_b, values='TxIndicativa', index=['data'],
                       columns=['Maturity'])
    table.columns = ['2024','2026','2028','2030','2032','2035','2040','2045','2050','2055']
    d1 = data.workday(data = data.hoje(),n_dias = -7,feriados_brasil=True, feriados_eua=False)
    d252 = data.workday(data = data.hoje(),n_dias = -252,feriados_brasil=True, feriados_eua=False)
    table['26x32']=(table['2032']-table['2026'])*100
    table['28x35']=(table['2035']-table['2028'])*100
    table['35x50']=(table['2050']-table['2035'])*100
    table['mediana_28x35']= table['28x35'].rolling(252,min_periods=1).median()
    table['mediana_26x32']= table['26x32'].rolling(252,min_periods=1).median()
    table['mediana_35x50']= table['35x50'].rolling(252,min_periods=1).median()
    table['desvio_28x35']= table['28x35'].rolling(252,min_periods=1).std()
    table['desvio_26x32']= table['26x32'].rolling(252,min_periods=1).std()
    table['desvio_35x50']= table['35x50'].rolling(252,min_periods=1).std()
    table['max_28x35']= table['28x35'].rolling(252,min_periods=1).max()
    table['max_26x32']= table['26x32'].rolling(252,min_periods=1).max()
    table['max_35x50']= table['35x50'].rolling(252,min_periods=1).max()
    table['min_28x35']= table['28x35'].rolling(252,min_periods=1).min()
    table['min_26x32']= table['26x32'].rolling(252,min_periods=1).min()
    table['min_35x50']= table['35x50'].rolling(252,min_periods=1).min()
    # table['desvio_35x50']= table['35x50'].rolling(252)
    table['tail_28x35']= table['28x35'].rolling(252,min_periods=1).quantile(0.05)
    table['tail_26x32']= table['26x32'].rolling(252,min_periods=1).quantile(0.05)
    table['tail_35x50']= table['35x50'].rolling(252,min_periods=1).quantile(0.05)
    table['data'] = pd.to_datetime(table.index)
    data = table['data']
    Curva_spread = table[['26x32','28x35','35x50']].set_index(data)
    fig = plt.figure(figsize=(10,6), dpi= 90)
    lista = ['26x32','28x35','35x50']
    for col in lista:
            plt.plot( Curva_spread.index, Curva_spread[col])
    
    plt.legend(lista)
    plt.ylabel('BPS')
    plt.title('Spread_bs')
    plt.gca().xaxis.set_major_formatter(mdFormat)
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=5))
    plt.tight_layout()
    plt.savefig(list_path_figures, format='png',bbox_inches='tight')
    
    #Dash 28 x 35

    dash = table.loc[(table['data']>=d1)]
    calculo_28x35 = table.loc[(table['data']>=d252)]
    calculo_28x35 = calculo_28x35['28x35']
    dash_28X35 = dash[['28x35','mediana_28x35','desvio_28x35','max_28x35','min_28x35']]
    dash_28X35.reset_index(inplace = True)
    dash_28X35.columns = ['Data','Diferença','Mediana','Desvio','Máximo','Mínimo']
    dash_28X35['Data'] = dash_28X35['Data'].dt.strftime("%d.%m.%Y")
    dash_28X35['Percentil'] = dash_28X35['Diferença'].apply(lambda x:stats.percentileofscore(calculo_28x35,x))
    
    #Dash 26 x 32

    calculo_26x32 = table.loc[(table['data']>=d252)]
    calculo_26x32 = calculo_26x32['26x32']
    dash_26x32 = dash[['26x32','mediana_26x32','desvio_26x32','max_26x32','min_26x32']]
    dash_26x32.reset_index(inplace = True)
    dash_26x32.columns = ['Data','Diferença','Mediana','Desvio','Máximo','Mínimo']
    dash_26x32['Data'] = dash_26x32['Data'].dt.strftime("%d.%m.%Y")
    dash_26x32['Percentil'] = dash_26x32['Diferença'].apply(lambda x:stats.percentileofscore(calculo_26x32,x))
    
    #Dash 35 x 50

    calculo_35x50 = table.loc[(table['data']>=d252)]
    calculo_35x50 = calculo_35x50['35x50'].to_list()
    dash_35x50 = dash[['35x50','mediana_35x50','desvio_35x50','max_35x50','min_35x50']]
    dash_35x50.reset_index(inplace = True)
    dash_35x50.columns = ['Data','Diferença','Mediana','Desvio','Máximo','Mínimo']
    dash_35x50['Data'] = dash_35x50['Data'].dt.strftime("%d.%m.%Y")
    dash_35x50['Percentil'] = dash_35x50['Diferença'].apply(lambda x:stats.percentileofscore(calculo_35x50,x))
    
    tbl_html_28X35 = build_table(dash_28X35, 'blue_light')
    tbl_html_26X32 = build_table(dash_26x32, 'blue_light')
    tbl_html_35X50 = build_table(dash_35x50, 'blue_light')
    
    
    subject='[RelMercado] Spreads das Bs'
    to=['portfolio@juliusbaer.com']
    
    text = '''Vértice 28 x 35 da NTN-B .</h3><br>
    {}<br>
    Vértice 26 x 32 da NTN-B .</h3><br>
    {}<br>
    Vértice 35 x 50 da NTN-B .</h3><br>
    {}<br>
    
    '''.format(tbl_html_28X35,tbl_html_26X32,tbl_html_35X50)
    
    email = Email(to = to , subject = subject, text= text,send = True,list_path_figures = list_path_figures)
    
if __name__ == '__main__':  
    resumo_rf()