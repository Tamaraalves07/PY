 '''
Classe criada para a gestão das carteiras offshore

@author: u45285
'''

from databases import BDS, PosicaoDm1, Bawm,Crm,BaseExtrato
from funcoes_datas import FuncoesDatas
from dateutil.relativedelta import relativedelta
import pandas as pd
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import datetime
from datetime import date
import numpy as np
import math
from emailer import Email, EmailLer
from pretty_html_table import build_table
from funcoes import CalculosRiscoPort


class posicao_off:
    
    def __init__(self, verbose=False, homologacao=False):
        self.homologacao = homologacao
        self.verbose = verbose
        self.crm = Crm()
        self.bawm = Bawm()
        self.datas = FuncoesDatas()
        self.extrato = BaseExtrato()
        self.bds = BDS()
        self.fdt = FuncoesDatas()
        
        #Lista com os usuários e emails de toda Julius Baer Brazil.
        self.emails_usuarios = self.crm.lista_usuarios_u()
        
        # Ãšltima data do mes anterior para rodar a query do extrato
        self.data = self.bds.banco.hoje()
        self.data = datetime.datetime.date(self.data)
        
        #de_paras de mandatos (PI anterior para a atual - equivalÃªncia)
        self.mandatos = pd.read_excel('O:/SAO/CICH_All/Portfolio Management/20 - Offshore/De Para Ativos.xlsx',sheet_name='de_para_mandato')
        self.mandatos = self.mandatos[['Anterior','Novo']]
        
        #Verificar os self.responsaveis por cada sc de gestão com base no mapa de ativos
        self.responsaveis = pd.read_excel('O:/SAO/CICH_All/Portfolio Management/20 - Offshore\Mapa de Ativos/Mapa JBFO_v2.xlsx',sheet_name='Mapa_DB')
        self.responsaveis = self.responsaveis[(self.responsaveis['Origem']=='Offshore') & (self.responsaveis['Tipo de Contrato']!='Distrato em Andamento')][['Conta','SC/ContaMov','RM','Co-RM','Controller','Controller Backup']]
        
        #Altera os usuários u para letra minÃºscula para padronização (será usado no envio de e-mail)
        self.responsaveis['RM'] =self.responsaveis['RM'].str.lower()
        self.responsaveis['Co-RM'] =self.responsaveis['Co-RM'].str.lower()
        self.responsaveis['Controller'] = self.responsaveis['Controller'].str.lower()
        self.responsaveis['Controller Backup'] = self.responsaveis['Controller Backup'].str.lower()
        
        #Clientes que estão em distrato
        self.distrato = self.crm.clientes_em_distrato_off()
        
        #Formatar valor em financeiro
        def formatar_financeiro(valor):
            int_part, dec_part = format(valor, ',.2f').split('.')
            return f"{int_part.replace(',', '.')},{dec_part}"        
        
        
    def posicao_gps(self):
        
        
        """
        Retorna as posições da GPS e se geracao_movimentos estiver igual a True, também gera as movimentaçÃões para a carteira chegar no portfÃ³lio recomendado.

        Returns
        -------
        DF de posições.

        """
        
        
   ##Funções     
        #Retira espaços inÃºteis
        def arruma_espaco(x):
            while x[-1]==' ':
                x=x[:-1]
            return x 
        
        #identifica clientes inativos
        def inativo(x):
            x = x.replace('(','').replace(')','')
            x = x.replace(' ','')
            x = x.strip()
            if 'inativ' in x.lower():
                valor='inativo'
            else:
                valor='ok'
            return valor   
        
        #Tabela com as informaçoes das SC de gestão offshore (nomes, responsáveis e perfil de cada cliente)'''
        po_cadastro =self. bawm.po_cadastro_all().drop(columns=['CodigoProduto'])
        
        #Puxa da PO_Cadastro os self.responsaveis por cada supercarteira de gestão (DPMS) - puxa do CRM - Todo
        
        
        #Verificar os self.responsaveis por cada sc de gestão com base no mapa de ativos de MIS
        self.responsaveis = pd.read_excel('O:/SAO/CICH_All/Portfolio Management/20 - Offshore\Mapa de Ativos/Mapa JBFO_v2.xlsx',sheet_name='Mapa_DB')
        self.responsaveis = self.responsaveis[(self.responsaveis['Origem']=='Offshore')& (self.responsaveis['Tipo de Contrato']!='Distrato em Andamento')][['Conta','SC/ContaMov','RM','Co-RM','Controller','Controller Backup']]
        
        #Altera os usuários u para letra minÃºscula para padronização (será usado no envio de e-mail)
        self.responsaveis['RM'] =self.responsaveis['RM'].str.lower()
        self.responsaveis['Co-RM'] =self.responsaveis['Co-RM'].str.lower()
        self.responsaveis['Controller'] = self.responsaveis['Controller'].str.lower()
        self.responsaveis['Controller Backup'] = self.responsaveis['Controller Backup'].str.lower()
        

        #Traz os fundos da JBFO (prioridade na aplicação das movimentações)
        fundos_jbfo = pd.read_excel('O:/SAO/CICH_All/Portfolio Management/20 - Offshore/De Para Ativos.xlsx',sheet_name='Fundos_Casa')
        fundos_jbfo = fundos_jbfo[['NomeDoProduto','CodigoProduto','Classificação']]
        fundos_jbfo['Fundo']=fundos_jbfo['NomeDoProduto'].str.lower()

        #de_paras de mandatos (PI anterior para a atual - equivalÃªncia)
        mandatos = pd.read_excel('O:/SAO/CICH_All/Portfolio Management/20 - Offshore/De Para Ativos.xlsx',sheet_name='de_para_mandato')
        mandatos = mandatos[['Anterior','Novo']]
        #mandatos = dict(mandatos.values)
    
        #Traz os deparas de Classificação gps para classes de asset Allocation.
        dicio_classificacoes = pd.read_excel('O:/SAO/CICH_All/Portfolio Management/20 - Offshore/De Para Ativos.xlsx',sheet_name='de_para_classificao')
        dicio_classificacoes=dicio_classificacoes[['Classificação GPS','De Para - Asset Allocation']]
        dicio_classificacoes = dict(dicio_classificacoes.values)
        
        #Novos mandatos - Pontos Táticos
        novos_mandatos = pd.read_excel('O:/SAO/CICH_All/Portfolio Management/20 - Offshore/De Para Ativos.xlsx',sheet_name='Novos_mandatos')
        novos_mandatos= novos_mandatos[['Portfolio','Classe','Tático']]
        
        #Clientes que estão em distrato
        distrato = self.crm.clientes_em_distrato_off()
        
        #Trazer os produtos GPS e segregar o que é imobiliario (caso precise segregar o que é Reits / ILiquidos)
        produtos_gps = self.crm.produtos_off()
        produtos_gps =produtos_gps[['name','productid','new_idmysql','new_tickerid','new_isin','new_idbds','new_idsistemaorigem','new_indgroupaxysidname']].rename(columns={'name':'Ativo','new_idmysql':'IdMysqlProduto','new_tickerid':'Ticker','new_isin':'ISIN','new_idsistemaorigem':'CodigoProduto'}).reset_index()
        produtos_gps['ISIN'] = produtos_gps['ISIN'].str.lower()
        
        #Retorna o nome dos veiculos que fazemos gestao (substituindo a classe JBFO)
        dicio_veiculos = dict(pd.read_excel('O:/SAO/CICH_All/Portfolio Management/20 - Offshore/De Para Ativos.xlsx',sheet_name='Fundos_Casa')[['Classe','Veiculo Abreviado']].drop_duplicates().values)

        #Traz a alocação ideal por perfil de portfolio
        alocacao_por_sc = self.crm.alocacao_ideial_off()
        
        #Traz a pi de cada cliente
        pi = alocacao_por_sc[['new_name','new_politicadeinvestimentoidname']]
        
        #Consolida a posicao atual versus a ideial
        #alocacao_por_sc = pd.pivot_table(data=alocacao_por_sc, values = 'new_percentual',columns=['new_classedeinvestimentoidname'],index=['new_name','new_conta_supercarteiraidname','new_politicadeinvestimentoidname']).reset_index()
        #alocacao_por_sc['new_name']=alocacao_por_sc['new_name'].str.lower()
        #alocacao_por_sc = pd.merge(left = alocacao_por_sc, right = po_cadastro,left_on ='new_conta_supercarteiraidname',right_on = 'NomeSuperCarteiraCRM',how='left')
        #alocacao_por_sc['inativo']=alocacao_por_sc['new_name'].apply(lambda x: inativo(x))
        #alocacao_por_sc=alocacao_por_sc[alocacao_por_sc['inativo']!='inativo']
        #alocacao_por_sc = alocacao_por_sc[['new_name','new_conta_supercarteiraidname','Alt Crédito', 'Alt Multistr','Alt RV L/S', 'Alt Trad/Macro', 'Alt Valor Rel', 'Commodities','Imobiliário', 'Outros', 'P. Equity', 'RF CP', 'RF Glob', 'RF Infl','RF Livre -', 'RF Outros -', 'RV', 'RV Outros -','new_politicadeinvestimentoidname','Titularidade','VetoPowerGestaoTatica','DPM','OfficerUsuarioU','ControllerUsuarioU']].reset_index(drop=True)

        ##Traz a posicao do ultimo extrato gerado

        #Retorna o ùltimo dia do mes
        ultimo_extrato = self.data - relativedelta(days=self.data.day -1)
        ultimo_extrato = ultimo_extrato + timedelta(days = -1)
        
        #Gera a carteira da primeira sc da lista
        teste = alocacao_por_sc.drop_duplicates()
        sc = teste.iloc[0,0]
        sc = '@5AGL1'
        df = self.extrato.posicao_extrato(sc,ultimo_extrato)
        
        #Se o extrato não foi processado no Ãºltimo dia do mÃªs, trazer o Ãºltimo disponÃ­vel
        if df.empty:
            ultimo_extrato = ultimo_extrato - relativedelta(days=ultimo_extrato.day -1)
            ultimo_extrato = ultimo_extrato + timedelta(days = -1)
        else:
            ultimo_extrato = ultimo_extrato
        
            
        #Gerar a carteira de todas as sc de gestão - portfolioss padrao
        for i,(index,row) in enumerate(teste.iterrows()):
             if i >0 :
                    ultimo_extrato = self.data
                    ultimo_extrato = ultimo_extrato - relativedelta(days=ultimo_extrato.day -1)
                    ultimo_extrato = ultimo_extrato + timedelta(days = -1)            
                    sc = row['new_name']
                    df1 = self.extrato.posicao_extrato(sc,ultimo_extrato)
                    if df1.empty:
                        ultimo_extrato = ultimo_extrato - relativedelta(days=ultimo_extrato.day -1)
                        ultimo_extrato = ultimo_extrato + timedelta(days = -1)
                    else:
                        ultimo_extrato = self.data
                        ultimo_extrato = ultimo_extrato - relativedelta(days=ultimo_extrato.day -1)
                        ultimo_extrato = ultimo_extrato + timedelta(days = -1)

        
                    df1 = self.extrato.posicao_extrato(sc,ultimo_extrato)
                    df1['data_ultimo_extrato']=ultimo_extrato          
                    df = pd.concat([df,df1])
        
        #Mapeando as informações da base GPS e linkando com os produtos da base (Classificação, isin e ticker do CRM).
        
        #Carteira GPS
        carteira_gps = df
        carteira_gps=pd.merge(left=carteira_gps,right=produtos_gps,on='IdMysqlProduto',how='left')
        carteira_gps = carteira_gps.drop(columns={'PercentualSobreTotal','PercentualIntraClasse','CarteiraVirtual','OrigemProduto','FlagExplodida','FlagCongelada'}).rename(columns={'NomeDoProduto':'Nome_produto'})
        
        #Ajustes nas - futuratmente tirar colunas inÃºteis
        #carteira_gps['Moeda']='NA'
        #carteira_gps = carteira_gps[['DataPosicao','NomeSupercarteiraCrm','NomeContaCrm','NomeDoProduto','CodigoProduto_x','ISIN','QuantidadeCotas','ValorCota','SaldoNaData','Moeda','Classe', 'Subclasse']].rename(columns={'DataPosicao':'Data_Posicao','NomeContaCrm':'Nome_Cliente','NomeDoProduto':'Nome_produto','QuantidadeCotas':'quantidade','ValorCota':'PU','CodigoProduto_x':'CodigoProduto'})
        carteira_gps['Esteira']='ex-Gps'
        carteira_gps['Nome_produto']=carteira_gps['Nome_produto'].str.lower().apply(lambda x: arruma_espaco(x))
        
        #Filtra os ativos que são imobiliarios e gera uma planilha para consultar na bbg o que é Reit
        ativos_imob = carteira_gps[carteira_gps['Classe']=='Imobiliário'][['Nome_produto','ISIN']]
        ativos_imob.to_excel('I:/Shared/SAO_Investimentos/Portfolio Management/Tamara/verificar_reits.xlsx')
        
        #Traz a planilha com as infos da bbg (Reits Classifados e assim segregando dos iliquidos)
        classificacao_imob = pd.read_excel('O:/SAO/CICH_All/Portfolio Management/20 - Offshore/Base Real Estate.xlsx')        
        #Separando os Reits dos Iliquidos
        carteira_gps = pd.merge(left=carteira_gps,right=classificacao_imob,on='ISIN',how='left').drop(columns=['Nome_produto_y']).rename(columns={'Tipo':'Classificacao_Real_Estate'})
        carteira_gps['Classe'].loc[carteira_gps['Classe']=='Imobiliário'] = carteira_gps['Classificacao_Real_Estate'].loc[carteira_gps['Classe']=='Imobiliário']
        carteira_gps = carteira_gps.drop_duplicates()
        
        #Ajuste das classes (padronização dos nomes das classes) - tirar pontos dos nomes das classes("-")
        carteira_gps['Classe']=carteira_gps['Classe'].astype('str')
        classes = carteira_gps['Classe']
        carteira_gps['Classe']= [x.replace(' -','') for x in classes]
        
        #Calcula o percentual do produto dentro da carteira, o financeiro total e desconsiderando os ativos iliquidos.
        percentual_produto=[]
        financeiro_total_spe =[]
        financeiro_total =[]
        
        for ind,row in carteira_gps.iterrows():   
                #Soma de todos os ativos
                soma = carteira_gps[carteira_gps['NomeSupercarteiraCrm']==row['NomeSupercarteiraCrm']]['SaldoNaData'].sum()
                financeiro_total.append(soma)
                
                #Soma dos ativos exceto iliquidos
                iliquidos = carteira_gps[(carteira_gps['NomeSupercarteiraCrm']==row['NomeSupercarteiraCrm'])&(carteira_gps['Classe']=='Iliq. P. Equity')]['SaldoNaData'].sum()
                financeiro_total_spe.append(soma-iliquidos)
                
                #Percentual de cada produto (do financeiro sem contar com os ativos iliquidos)    
                percentual= row['SaldoNaData']/(soma-iliquidos)
                percentual_produto.append(percentual)
                  
            
        #Incluir no dataframe as informaçÃões geradas acima
        carteira_gps['percentual_produto']=percentual_produto
        carteira_gps['Financeiro_Total']=financeiro_total
        carteira_gps['Financeiro_Total_s/pe']=financeiro_total_spe                                       
        carteira_gps = carteira_gps.sort_values(by=['Classe'])
        carteira_gps = carteira_gps.fillna(0)
        carteira_gps=carteira_gps.drop_duplicates()
                
        #DEixando apenas as colunas necessárias, renomeando as mesmas com nomes mais intuitivos
        #carteira_gps = carteira_gps[['Data_Posicao','NomeSupercarteiraCrm', 'Nome_Cliente', 'Nome_produto_x','CodigoProduto', 'ISIN', 'quantidade','PU','SaldoNaData', 'Moeda', 'Classe', 'SubClasse', 'Esteira']].rename(columns={'Classe':'Classe','Nome_produto_x':'Nome_produto'})
        carteira_gps = pd.merge(left=carteira_gps,right=self.responsaveis,left_on='NomeSupercarteiraCrm',right_on='SC/ContaMov',how='left')

        
        #carteira_gps=carteira_gps[['Data_Posicao','NomeSupercarteiraCrm', 'Nome_Cliente', 'Nome_produto','CodigoProduto', 'ISIN', 'quantidade',
                #'PU','SaldoNaData', 'Moeda', 'Classe', 'Subclasse', 'Esteira',
                #'new_politicadeinvestimentoidname','Officer', 'Controller']].rename(columns={'new_politicadeinvestimentoidname':'Politica'})
        carteira_gps = carteira_gps.drop_duplicates(subset=['Data_Posicao', 'Nome_Cliente', 'Nome_produto', 'ISIN', 'quantidade','PU']).sort_values(by=['Esteira'])
        carteira_gps.to_excel('O:/SAO/CICH_All/Portfolio Management/20 - Offshore/8 - Carteiras Off/carteira_off_gps.xlsx')
        
        posicao_classe= carteira_gps.groupby(['Data_Posicao','NomeContaCrm','NomeSupercarteiraCrm','Classe','Portfolio','Financeiro_Total','Financeiro_Total_s/pe']).sum().reset_index()                                        
        posicao_classe = pd.merge(left = carteira_gps, right = alocacao_por_sc,right_on ='new_conta_supercarteiraidname',left_on = 'Nome_Cliente',how='left')
        return carteira_gps_posicao
        
if __name__ == '__main__':
    off =  posicao_off()
    off.posicao_gps()

                        