import pandas as pd
import math
from databases import BDS, PosicaoDm1, Bawm
from pandas import ExcelWriter
from funcoes_datas import FuncoesDatas
from dateutil.relativedelta import relativedelta
import xlwings as xw
from emailer import Email
from datetime import date, datetime
import numpy as np
import ambiente
from objetos import Ativo
from pretty_html_table import build_table


class TabelaRentabilidade:
    
    def __init__(self, homologacao=False):
        self.bds = BDS(homologacao=homologacao)
        self.dm1 = PosicaoDm1(homologacao=homologacao)
        self.bawm = Bawm(homologacao=homologacao)        
        self.fdt = FuncoesDatas()
        hoje = self.fdt.hoje()
        self.d1 = self.fdt.workday(hoje,n_dias = -2,feriados_brasil=True, feriados_eua=False)

    def gerar_tabela(self, data_base=None):
        # 1. Lista de datas
        if data_base:
            data = data_base
        else:
            data = self.bds.banco.hoje()
            data = data - relativedelta(days=data.day -1)
            data = self.fdt.workday(data=data, n_dias=-1)
        
        datas = [data]
        for mes in range(0, 36):
            data = data - relativedelta(days=data.day -1)
            data = self.fdt.workday(data=data, n_dias=-1)
            datas.append(data)
        
        fundos = self.__tabela_fundos__(datas=datas)
        indices = self.__tabela_indices__(datas=datas)
        dicio_dfs = {'Fundos': fundos, 'Indices': indices}
        destino = f'{ambiente.pasta_base_pm()}/06 - Relatório/TabelaRent/base_tabela.xlsx'
        with ExcelWriter(destino) as writer:
            for item in dicio_dfs.keys():
                dicio_dfs[item].to_excel(writer, item)        
    
    def __tabela_fundos__(self, datas):
        # 1. Lista de fundos
        lista_fundos = self.dm1.fundos_coletivos()
        lista_fundos['IdBDS'] = pd.to_numeric(lista_fundos['IdBDS'])
        colunas = ['Mês', '3M', 'Ano', '12M', '24M', '36M', 'PL', 'PL12M']
        for col in colunas:
            lista_fundos.insert(len(lista_fundos.columns), col, [None]*len(lista_fundos))
        
        # 2. Busca as cotas
        cotas = []
        for idx, row in lista_fundos.iterrows():
            lista_fundos.loc[idx, 'NomeContaCRM'] = str(row['NomeContaCRM'])[1:]
            if str(row['IdBDS'])[:3] == '599' or str(row['IdBDS'])[:3] == '214':
                df = self.bds.serie_historico(idserie=row['IdBDS'], lista_idcampos=[8,20])
                if not datas[0] in df['dataser'].to_list():
                    try:
                        df = self.__bds_completa_historico__(idserie=row['IdBDS'], cnpj=row['CNPJ'], df=df)                                            
                    except:
                        df = pd.DataFrame()
                        print('erro')
                cotas.append(df)
        cotas = pd.concat(cotas)
        
        # 3. Cálculo de rentabilidade
        cotas_m = cotas[cotas['dataser'].isin(datas)].copy()
        cotas_pvt = pd.pivot_table(cotas_m, index='idser', columns='dataser', values='a8')
        pls_pvt = pd.pivot_table(cotas_m, index='idser', columns='dataser', values='a20')
        
        calc_rent = pd.merge(left=lista_fundos, left_on='IdBDS', right=cotas_pvt, right_index=True)
        calc_rent['Mês'] = calc_rent[datas[0]] / calc_rent[datas[1]] - 1
        calc_rent['3M'] = calc_rent[datas[0]] / calc_rent[datas[3]] - 1
        calc_rent['12M'] = calc_rent[datas[0]] / calc_rent[datas[12]] - 1
        calc_rent['24M'] = calc_rent[datas[0]] / calc_rent[datas[24]] - 1
        calc_rent['36M'] = calc_rent[datas[0]] / calc_rent[datas[36]] - 1
        calc_rent['Ano'] = calc_rent[datas[0]] / calc_rent[datas[datas[0].month]] - 1
        calc_rent = calc_rent[['NomeContaCRM', 'CodigoProduto', 'CNPJ', 'Classe', 'SubClasse', 'IdBDS', 'Ordem', 'Mês', '3M', 'Ano', '12M', '24M', '36M']]
        
        # 4. PL e PL Médio
        pls = pls_pvt[[datas[0]]]
        pls.columns = ['Atual']
        for i in range(1,12):
            pls = pd.concat([pls, pls_pvt[[datas[i]]]], axis=1)
        pls['PL12M'] = pls.mean(axis=1)
        pls = pls[['Atual', 'PL12M']]
        calc_rent = pd.merge(left=calc_rent, left_on='IdBDS', right=pls, right_index=True)
        return calc_rent.set_index('CodigoProduto')
    
    def __bds_completa_historico__(self, idserie, cnpj, df):
        id_series_bds = self.bds.fundo_busca_series_por_cnpj(cnpj=cnpj)
        if id_series_bds.empty:
            # não achou séries
            return df
        # Busca séries do fundo por CNPJ
        lista = id_series_bds['idser'].to_list()
        cotas = []
        for item in lista:
            cota = self.bds.serie_historico(idserie=item, lista_idcampos=[8,20])
            cotas.append(cota)
        cotas = pd.concat(cotas)
        # Monta pivot-tables de cota e pls com as series encontradas
        cts = pd.pivot_table(cotas, index='dataser', columns='idser', values='a8').reset_index().set_index('dataser')
        pls = pd.pivot_table(cotas, index='dataser', columns='idser', values='a20').reset_index().set_index('dataser')
        cts.insert(0, 'Cota', [math.nan]*len(cts))
        pls.insert(0, 'PL', [math.nan]*len(cts))        
        # Completa as series
        cts = self.__bds_completa_hist_run__(df=cts, nome_col='Cota')[['Cota']]
        pls = self.__bds_completa_hist_run__(df=pls, nome_col='PL')[['PL']]
        cotas = pd.concat([cts,pls], axis=1)
        df = pd.merge(left=df, left_on='dataser', right=cotas, right_index=True, how='right')
        df['idser'].fillna(method='ffill', inplace=True)
        df['intervalo'].fillna(method='ffill', inplace=True)
        df.index.name = 'nd'
        df.reset_index(inplace=True)
        for idx, row in df.iterrows():
            if math.isnan(row['a8']):
                df.loc[idx, 'a8'] = row['Cota']
            if math.isnan(row['a20']):
                df.loc[idx, 'a20'] = row['PL']
        # Retorna
        df['a8'] = pd.to_numeric(df['a8'])
        df['a20'] = pd.to_numeric(df['a20'])
        return df[['idser', 'dataser', 'intervalo', 'a8', 'a20']]
    
    @staticmethod
    def __bds_completa_hist_run__(df, nome_col):
        for idx, row in df.iterrows():
            cota = None
            for col in df.columns:
                if col != nome_col:
                    if not math.isnan(row[col]):
                        cota = row[col]
                        break
            df.loc[idx, nome_col] = cota
        return df
    
    def __tabela_indices__(self, datas):
        # 1. Lista de fundos
        lista_indices = self.bawm.in_lista_indices(datas)
        
        # 2. Busca as cotas
        cotas = []
        for idx, row in lista_indices.iterrows():        
            cotas.append(self.bawm.in_historico(index_cod=row['IndexCod'], data_ini=datas[-1], data_fim=datas[0]))
        cotas = pd.concat(cotas)
        cotas_m = cotas[cotas['Data'].isin(datas)].copy()
        
        # 3. Cálculo de rentabilidade
        cotas_pvt = pd.pivot_table(cotas_m, index='IndexCod', columns='Data', values='Valor')
        calc_rent = pd.merge(left=lista_indices, left_on='IndexCod', right=cotas_pvt, right_index=True)
        calc_rent['Mês'] = calc_rent[datas[0]] / calc_rent[datas[1]] - 1
        calc_rent['3M'] = calc_rent[datas[0]] / calc_rent[datas[3]] - 1
        calc_rent['12M'] = calc_rent[datas[0]] / calc_rent[datas[12]] - 1
        calc_rent['24M'] = calc_rent[datas[0]] / calc_rent[datas[24]] - 1
        calc_rent['36M'] = calc_rent[datas[0]] / calc_rent[datas[36]] - 1
        calc_rent['Ano'] = calc_rent[datas[0]] / calc_rent[datas[datas[0].month]] - 1
        calc_rent = calc_rent[['IndexCod', 'Nome', 'ShortName', 'Mês', '3M', 'Ano', '12M', '24M', '36M']]
        
        return calc_rent.set_index('IndexCod')

    def controle_soberaano_mes(self,fundo,bench): 
        dicio_benchs = {'IMAB':'IMAIPCA','IMAB5+':'IMAB5+','IMAB5':'IMAB5'}
        #fundo2 = '_NTN-B Ativo FIM'
        #fundo1 = '_JBFO Juros Ativo FIRF'
        #fundo3='_Mandur Previdenciario FIM CrPr'
        #fundo4='_JBFO Juros Ativo Longo FIRF'
        
        def pgto_cupom(vencimento, data):
            
            if vencimento.month==8 and (data.month==2 or data.month==8):
                pgto = self.fdt.verificar_du(datetime(data.year, data.month, 15),forcar_du_seguinte=True)                                                                                  
            elif vencimento.month==5 and (data.month==11 or data.month==5):
                pgto = self.fdt.verificar_du(datetime(data.year, data.month, 15),forcar_du_seguinte=True)
            else:
                pgto = np.nan
            return pgto    
        
        def proc_bds(guid):
            ativo = Ativo(guid)
            vct = ativo.data_vencimento
            return vct
        
        hoje = self.fdt.hoje()
        #hoje = self.fdt.workday(self.fdt.hoje(),n_dias = -2,feriados_brasil=True, feriados_eua=False)
        dia = date(hoje.year, hoje.month, 1) 
        dia=pd.to_datetime(dia)
        self.d1 = self.fdt.workday(hoje,n_dias = -1,feriados_brasil=True, feriados_eua=False)
        dia = self.fdt.workday(dia,n_dias = -1,feriados_brasil=True, feriados_eua=False)
        precos = self.bds.titulos_publicos(dia)
        precos_bawm = self.bawm.rf_historico(data_ini=dia, data_fim=self.d1)
       
        dados_bench = self.bawm.in_historico(index_cod=dicio_benchs[bench], data_ini=dia, data_fim=hoje) 
        dados_bench['Rent']=(dados_bench['Valor']/dados_bench['Valor'].shift(1))-1
                        
        #Ajuste dos precos
        precos = precos[precos['data']>=dia]
        precos = precos[['id_serie','dt_venc','data','pu','duration']]
        precos['dt_venc'] = pd.to_datetime(precos['dt_venc'])
        precos['data'] = pd.to_datetime(precos['data'])
        VNA = self.bawm.in_historico(index_cod='VNANTNB', data_ini=dia, data_fim=hoje) 
        precos = pd.merge(left = precos, right = VNA, left_on ='data',right_on ='Data', how='left' )
        precos = precos[precos['data']>=dia]
        cupom = (1.06**(1/2))-1
        precos['pu_cupom'] = precos.apply(lambda row:row['pu']+(row['Valor']*cupom) if pgto_cupom(row['dt_venc'],row['data'])==row['data'] else row['pu'],axis=1)

        #Trazendo a posição do JBFO Juros Ativo e fazendo os calculos de rentabilidade
        fundos= self.dm1.posicao_fundos_periodo(trazer_colunas = True,data_ini = dia,data_fim = hoje, Nome_fundo = fundo)
        fundos = fundos[['DataArquivo','GuidContaCRM','NomeContaCRM','NomeProduto','QuantidadeFinal','GuidProduto','FinanceiroFinal','SubClasse','TipoProduto']]
        fundos['DataArquivo'] = pd.to_datetime(fundos['DataArquivo'])
        fundos = fundos[fundos['SubClasse']=='RF Infl Soberano']
        fundos = fundos[fundos['GuidProduto']!='ad6316e2-9e15-e811-95f8-005056912b96']
        fundos['dt_venc']=fundos['GuidProduto'].apply(lambda x: proc_bds(x))
        fundos['dt_venc']= pd.to_datetime(fundos['dt_venc'])
        fundos_total = fundos.groupby(['DataArquivo']).sum().reset_index().sort_values(by='DataArquivo')
        fundos = pd.merge(left = fundos, right = fundos_total,on = 'DataArquivo',how ='left')
        fundos['Peso'] = fundos['FinanceiroFinal_x']/fundos['FinanceiroFinal_y']
                
    
        #Rentabilidade do PU (corringindo pelo pgto de Cupom)   
        rentabilidade_sem_cupom = precos.groupby(['data','dt_venc'])[['pu']].mean().unstack('dt_venc')
        rentabilidade_sem_cupom = rentabilidade_sem_cupom/rentabilidade_sem_cupom.shift()-1
        rentabilidade_sem_cupom  = rentabilidade_sem_cupom.stack().reset_index()
        rentabilidade_cupom = precos.groupby(['data','dt_venc'])[['pu_cupom']].mean().unstack('dt_venc')
        rentabilidade_cupom = rentabilidade_cupom/rentabilidade_cupom.shift()-1
        rentabilidade_cupom = rentabilidade_cupom.stack().reset_index()
        rentabilidade = pd.merge(right =rentabilidade_sem_cupom,left = rentabilidade_cupom, on = ['data','dt_venc'])
        rentabilidade['Rentabilidade_dia'] = rentabilidade.apply(lambda row: row['pu_cupom'] if pgto_cupom(row['dt_venc'],row['data'])==row['data'] else row['pu'],axis = 1)
        
       
        #Juntando a tabela de rentabilidade com a posição dia a dia
        fundos = pd.merge(right = fundos,right_on =['DataArquivo','dt_venc'],left=rentabilidade,left_on =['data','dt_venc'],how='right')
        fundos = fundos.dropna(subset=['Rentabilidade_dia'])
        fundos['Rentabilidade_ponderada']=(fundos['Peso']*fundos['Rentabilidade_dia'])+1
        lista = fundos['Rentabilidade_ponderada']
        dia_dia = fundos[['DataArquivo','Rentabilidade_ponderada']]
        fundos = fundos[['DataArquivo','dt_venc','Peso','Rentabilidade_dia','Rentabilidade_ponderada']]
        fundos.columns = ['Data','Vencimento','Peso','Rentabilidade_dia','Rentabilidade_ponderada']

        
        resultado_dia_dia = dia_dia.groupby('DataArquivo',as_index=False).prod()
        resultado_dia_dia = pd.merge(right = resultado_dia_dia,left =dados_bench,right_on='DataArquivo',left_on = 'Data',how='right' )
        resultado_dia_dia['Rent_bench'] = resultado_dia_dia['Rent']+1
        resultado_dia_dia['+/- bench bps'] = ((resultado_dia_dia['Rentabilidade_ponderada'] / resultado_dia_dia['Rent_bench'])-1)*100
        resultado_dia_dia['Rentabilidade_ponderada'] = (resultado_dia_dia['Rentabilidade_ponderada']-1)*100
        resultado_dia_dia['Rent_bench'] = (resultado_dia_dia['Rent_bench']-1)*100
        resultado_dia_dia['+/- bench bps'] = resultado_dia_dia['+/- bench bps'].apply(lambda x: round(x,3))
        resultado_dia_dia['Rent_bench'] = resultado_dia_dia['Rent_bench'].apply(lambda x: round(x,3))
        resultado_dia_dia['Rentabilidade_ponderada'] = resultado_dia_dia['Rentabilidade_ponderada'].apply(lambda x: round(x,3))
        resultado_dia_dia = resultado_dia_dia[['Data','Rent_bench','Rentabilidade_ponderada','+/- bench bps']]
        resultado_dia_dia.columns = ['Data',f'Rent_{bench}','Rent_Carteira',f'+/- {bench} bps']
        resultado = round(((np.prod(lista))-1)*100,2)
        resultado_bench = round((np.prod(dados_bench['Rent']+1)-1)*100,2)
        Diferença = round(resultado - resultado_bench,2)
       
        return resultado, resultado_bench, Diferença, resultado_dia_dia
        
    def email_composicao_b(self):
        
        fundos,benchs = ['_JBFO Juros Ativo FIRF','_JBFO Juros Ativo Longo FIRF','_NTN-B Ativo FIM','_Mandur Previdenciario FIM CrPr'],['IMAB','IMAB5+','IMAB','IMAB5'] 
        hoje = "{:%d/%m/%Y}".format(self.d1)
        text_inicio = f'''Prezados .</h3><br>'''  
        text =''             
        for (fundo,bench) in zip(fundos,benchs):
            funcao = self.controle_soberaano_mes(fundo,bench)
            resultado=funcao[0]  
            resultado_bench=funcao[1] 
            Diferença=funcao[2] 
            resultado_dia_dia=funcao[3]
            tbl1 = build_table(resultado_dia_dia,'blue_dark',text_align = 'center',width="300px")

            text_fundo =   f'''Seguem os resultados da composição de Bs do {fundo}.</h3><br>
                       Resultado Carteira : {resultado}.</h3><br>
                       {bench} : {resultado_bench}.</h3><br>
                       Diferença : {Diferença}.</h3><br>
                       {tbl1}<br>                                           
                       '''
                       
            text =  text+ text_fundo          
        texto=text_inicio+ text         
                    
                    
                    
        subject= f'Resultado da composição de NTN-Bs {hoje}'
        to = ['portfolio@jbfo.com']            
        email = Email(to = to , subject = subject, text= texto,send = False)      

class TabelaRentabilidadeGlobal:
    
    def __init__(self, homologacao=False):
        self.bds = BDS(homologacao=homologacao)
        self.dm1 = PosicaoDm1(homologacao=homologacao)
        self.bawm = Bawm(homologacao=homologacao)        
        self.fdt = FuncoesDatas()
        self.wb = None
        
    def gerar_tabela(self, data_base=None, nome_planilha=None):        
        # Carrega dados da planilha
        if nome_planilha:
            self.wb = xw.Book(nome_planilha)
        else:
            self.wb = xw.Book.caller()
            
        # 1. Lista de datas
        if data_base:
            data = data_base
        else:
            data = self.bds.banco.hoje()
            data = data - relativedelta(days=data.day -1)
            data = self.fdt.workday(data=data, n_dias=-1, feriados_brasil=False)
        
        datas = [data]
        for mes in range(0, 36):
            data = data - relativedelta(days=data.day -1)
            data = self.fdt.workday(data=data, n_dias=-1, feriados_brasil=False)
            datas.append(data)
        
        # 2. Pega series da planilha
        series_bds = self.lista_series_bds()
        series_bawm = self.lista_series_bawm()
        
        # 3. Busca dados do BDS
        cotas = []
        for idx, row in series_bds.iterrows():
            cotas.append(self.bds.serie_historico(idserie=row['CodBDS'], intervalo=row['Intervalo'], lista_idcampos=[9001]))
        cotas = pd.concat(cotas)
        cotas_pvt = pd.pivot_table(cotas, columns='idser', index='dataser', values='a9001')
        
        # 3. Busca dados da BAWM
        cotas = []
        for idx, row in series_bawm.iterrows():
            cotas.append(self.bawm.in_historico_mensal(index_cod=row['CodBAWM']))
        cotas = pd.concat(cotas)
        cotas['Mes'] = cotas['Mes'].apply(lambda x: x - relativedelta(days=x.day -1))
        cotas = pd.pivot_table(cotas, columns='IndexCod', index='Mes', values='Valor')
        
        # 4. Salva os dados
        cotas_pvt = pd.concat([cotas_pvt, cotas], axis=1)
        cotas_pvt.to_excel(f'{ambiente.pasta_base_pm()}/06 - Relatório/TabelaRent/base_tabela_global.xlsx')
        
    def lista_series_bds(self):
        lista = self.wb.sheets('Series').range('SeriesBDS').options(pd.DataFrame, header=1, index=False, expand='table').value
        return lista
        #SeriesBAWM

    def lista_series_bawm(self):
        lista = self.wb.sheets('Series').range('SeriesBAWM').options(pd.DataFrame, header=1, index=False, expand='table').value
        return lista
    
    
    
class TabelaRentabilidade_diaria:
    
    def __init__(self, homologacao=False):
        self.bds = BDS(homologacao=homologacao)
        self.dm1 = PosicaoDm1(homologacao=homologacao)
        self.bawm = Bawm(homologacao=homologacao)        
        self.fdt = FuncoesDatas()

    def gerar_tabela(self, data_base=None):
        # 1. Lista de datas
        if data_base:
            data = data_base
        else:
            data = self.bds.banco.hoje()
            mes_atual = data - relativedelta(days=data.day -1)
            mes_atual = self.fdt.workday(data=mes_atual, n_dias=-1)
            data = self.fdt.workday(data=data, n_dias=-3,feriados_brasil=True, feriados_eua=False)
        
        hoje = self.fdt.hoje()    
        hoje =  self.fdt.workday(hoje,n_dias = -3,feriados_brasil=True, feriados_eua=False)    
        hoje = "{:%d/%m/%Y}".format(hoje)    
        datas = []
        datas.append(mes_atual)
        datas.append(data)
                    
        for dia in range(0, 5):
            data = self.fdt.workday(data=data, n_dias=-1)
            datas.append(data)  
        
        fundos = self.__tabela_fundos__(datas=datas)
        indices = self.__tabela_indices__(datas=datas)
        dicio_dfs = {'Fundos': fundos, 'Indices': indices}
        destino = f'{ambiente.pasta_base_pm()}/06 - Relatório/TabelaRent/base_tabela_v2.xlsx'
        with ExcelWriter(destino) as writer:
            for item in dicio_dfs.keys():
                  dicio_dfs[item].to_excel(writer, item)
                          
        wb = xw.Book(f'{ambiente.pasta_base_pm()}/06 - Relatório/TabelaRent/Rentdia.xlsm',update_links=True)
        app = wb.app
        sheet = wb.sheets['Resumo']
        sheet['K2'].value = data  
        macro_vba = app.macro("'Rentdia.xlsm'!AtualizarBaseDados")
        macro_vba()
        wb.save()
        wb.save('C:/Temp/Foundation/Rentdia.xlsm')
        wb.close()        
        
        #Envia Email
        subject= f'Rentabilidade diária dos Veículos até {hoje}'
        to=['tamara.alves@jbfo.com']
        to=['portfolio@jbfo.com','ricardo.gaspar@jbfo.com','vitor.edo@jbfo.com','felipe.takeshi@jbfo.com','investimentos@juliusbaer.com']
        caminho_anexo = 'C:/Temp/Foundation/Rentdia.xlsm'
        text = '''Segue a rentabilidade dos veículos coletivos
        
        '''
        email = Email(to = to , subject = subject, text= text,send = True,attachments=caminho_anexo) 
    
    def __tabela_fundos__(self, datas):
        # 1. Lista de fundos
        lista_fundos = self.dm1.fundos_coletivos()
        lista_fundos['IdBDS'] = pd.to_numeric(lista_fundos['IdBDS'])
        colunas = ['Mês', 'D-1', 'D-2', 'D-3', 'D-4', 'D-5', 'PL', 'PL12M']
        for col in colunas:
            lista_fundos.insert(len(lista_fundos.columns), col, [None]*len(lista_fundos))
        
        # 2. Busca as cotas
        cotas = []
        for idx, row in lista_fundos.iterrows():
            lista_fundos.loc[idx, 'NomeContaCRM'] = str(row['NomeContaCRM'])[1:]
            if str(row['IdBDS'])[:3] == '599' or str(row['IdBDS'])[:3] == '214':
                df = self.bds.serie_historico(idserie=row['IdBDS'], lista_idcampos=[8,20])
                if not datas[0] in df['dataser'].to_list():
                    try:
                        df = self.__bds_completa_historico__(idserie=row['IdBDS'], cnpj=row['CNPJ'], df=df)                                            
                    except:
                        df = pd.DataFrame()
                        print('erro')
                cotas.append(df)
        cotas = pd.concat(cotas)
        
        
         # 3. Cálculo de rentabilidade
        cotas_m = cotas[cotas['dataser'].isin(datas)].copy()
        cotas_pvt = pd.pivot_table(cotas_m, index='idser', columns='dataser', values='a8')
        pls_pvt = pd.pivot_table(cotas_m, index='idser', columns='dataser', values='a20')
        cotas_pvt.to_excel('cotas.xlsx')         
        calc_rent = pd.merge(left=lista_fundos, left_on='IdBDS', right=cotas_pvt, right_index=True)
        calc_rent['Mês'] = calc_rent[datas[1]] / calc_rent[datas[0]] - 1
        calc_rent['D-1'] = calc_rent[datas[1]] / calc_rent[datas[2]] - 1
        calc_rent['D-2'] = calc_rent[datas[2]] / calc_rent[datas[3]] - 1
        calc_rent['D-3'] = calc_rent[datas[3]] / calc_rent[datas[4]] - 1
        calc_rent['D-4'] = calc_rent[datas[4]] / calc_rent[datas[5]] - 1
        calc_rent['D-5'] = calc_rent[datas[5]] / calc_rent[datas[6]] - 1
        calc_rent = calc_rent[['NomeContaCRM', 'CodigoProduto', 'CNPJ', 'Classe', 'SubClasse', 'IdBDS', 'Ordem', 'Mês', 'D-1', 'D-2', 'D-3', 'D-4', 'D-5']]
        cotas_pvt.to_excel('cotas.xlsx')              
                    # 4. PL e PL Médio
        pls = pls_pvt[[datas[0]]]
        pls.columns = ['Atual']
        for i in range(1,6):
            pls = pd.concat([pls, pls_pvt[[datas[i]]]], axis=1)
        pls['PL12M'] = pls.mean(axis=1)
        pls = pls[['Atual', 'PL12M']]
        calc_rent = pd.merge(left=calc_rent, left_on='IdBDS', right=pls, right_index=True)
        return calc_rent.set_index('CodigoProduto')
        
        # 4. PL e PL Médio
        pls = pls_pvt[[datas[0]]]
        pls.columns = ['Atual']
        for i in range(1,12):
            pls = pd.concat([pls, pls_pvt[[datas[i]]]], axis=1)
        pls['PL12M'] = pls.mean(axis=1)
        pls = pls[['Atual', 'PL12M']]
        calc_rent = pd.merge(left=calc_rent, left_on='IdBDS', right=pls, right_index=True)
        return calc_rent.set_index('CodigoProduto')
    
    def __bds_completa_historico__(self, idserie, cnpj, df):
        id_series_bds = self.bds.fundo_busca_series_por_cnpj(cnpj=cnpj)
        if id_series_bds.empty:
                       # não achou séries
                return df
                # Busca séries do fundo por CNPJ
                lista = id_series_bds['idser'].to_list()
                cotas = []
                for item in lista:
                    cota = self.bds.serie_historico(idserie=item, lista_idcampos=[8,20])
                    cotas.append(cota)
                    cotas = pd.concat(cotas)
                    # Monta pivot-tables de cota e pls com as series encontradas
                    cts = pd.pivot_table(cotas, index='dataser', columns='idser', values='a8').reset_index().set_index('dataser')
                    pls = pd.pivot_table(cotas, index='dataser', columns='idser', values='a20').reset_index().set_index('dataser')
                    cts.insert(0, 'Cota', [math.nan]*len(cts))
                    pls.insert(0, 'PL', [math.nan]*len(cts))        
                    # Completa as series
                    cts = self.__bds_completa_hist_run__(df=cts, nome_col='Cota')[['Cota']]
                    pls = self.__bds_completa_hist_run__(df=pls, nome_col='PL')[['PL']]
                    cotas = pd.concat([cts,pls], axis=1)
                    df = pd.merge(left=df, left_on='dataser', right=cotas, right_index=True, how='right')
                    df['idser'].fillna(method='ffill', inplace=True)
                    df['intervalo'].fillna(method='ffill', inplace=True)
                    df.index.name = 'nd'
                    df.reset_index(inplace=True)
                    for idx, row in df.iterrows():
                        if math.isnan(row['a8']):
                            df.loc[idx, 'a8'] = row['Cota']
                            if math.isnan(row['a20']):
                                df.loc[idx, 'a20'] = row['PL']
                                # Retorna
        df['a8'] = pd.to_numeric(df['a8'])
        df['a20'] = pd.to_numeric(df['a20'])
        return df[['idser', 'dataser', 'intervalo', 'a8', 'a20']]
    
    @staticmethod
    def __bds_completa_hist_run__(df, nome_col):
            for idx, row in df.iterrows():
                cota = None
                for col in df.columns:
                    if col != nome_col:
                        if not math.isnan(row[col]):
                            cota = row[col]
                            break
                        df.loc[idx, nome_col] = cota
                        return df
    
    def __tabela_indices__(self, datas):
        # 1. Lista de fundos
        lista_indices = self.bawm.in_lista_indices(datas)
        data_min = min(datas)
                    
                    # 2. Busca as cotas
        cotas = []
        for idx, row in lista_indices.iterrows():        
            cotas.append(self.bawm.in_historico(index_cod=row['IndexCod'], data_ini=data_min, data_fim=datas[1]))
        cotas = pd.concat(cotas)
        cotas_m = cotas[cotas['Data'].isin(datas)].copy()
                    
                    # 3. Cálculo de rentabilidade
          
        cotas_pvt = pd.pivot_table(cotas_m, index='IndexCod', columns='Data', values='Valor')
        calc_rent = pd.merge(left=lista_indices, left_on='IndexCod', right=cotas_pvt, right_index=True)
        calc_rent['Mês'] = calc_rent[datas[1]] / calc_rent[datas[0]] - 1
        calc_rent['D-1'] = calc_rent[datas[1]] / calc_rent[datas[2]] - 1
        calc_rent['D-2'] = calc_rent[datas[2]] / calc_rent[datas[3]] - 1
        calc_rent['D-3'] = calc_rent[datas[3]] / calc_rent[datas[4]] - 1
        calc_rent['D-4'] = calc_rent[datas[4]] / calc_rent[datas[5]] - 1
        calc_rent['D-5'] = calc_rent[datas[5]] /calc_rent[datas[6]] - 1
            

        calc_rent = calc_rent[['IndexCod', 'Nome', 'ShortName', 'Mês', 'D-1', 'D-2', 'D-3', 'D-4', 'D-5']]
                    
        return calc_rent.set_index('IndexCod')
    
    


if __name__ == '__main__':           
    #TabelaRentabilidade().email_composicao_b()
     TabelaRentabilidade_diaria().gerar_tabela()
    # TabelaRentabilidadeGlobal().gerar_tabela(nome_planilha='TabelaRentMensalGlobal.xlsm')
    #pass