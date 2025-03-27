import datetime
import time
import pandas as pd
import warnings
import math
import numpy as np
from dateutil.relativedelta import relativedelta
import ambiente
from cronometro import Cronometro, RunWErrorCheck
from funcoes import obter_carteira_simulada
from objetos import Ativo, Fundo, Titularidade
from databases import PosicaoDm1Pickle, Crm, Boletador, CreditManagement, Mandato
from sys_documentacao import AnaliseDocumentacao

# Não importar nada do sys_boletador

def monta_lista_port(id_tipo_portfolio=None, guid_portfolio=None, obj_portfolio=None, base_dm1=None, classe_mandato=None, data_pos=None):
    lista_port = []
    if str(type(obj_portfolio)) != "<class 'NoneType'>":
        if str(type(obj_portfolio)) == "<class 'list'>":
            lista_port = obj_portfolio
        else:
            lista_port = [obj_portfolio]
    elif id_tipo_portfolio == 1:            
        # Ler dados do fundo
        port = Fundo(guid_conta_crm=guid_portfolio, base_dm1=base_dm1, classe_mandato=classe_mandato, data_pos=data_pos)
        lista_port = [port]
    else:
        port = Titularidade(guid_titularidade=guid_portfolio, base_dm1=base_dm1, classe_mandato=classe_mandato, data_pos=data_pos)
        lista_port = [port]
    
    return lista_port


class RegrasFundo:
    def __init__(self, df_carteira_simulada, pl:float, limite_risk_score:float, multiplicador_credito:float, limites_credito, limites_credito_exc, df_ordens:pd.DataFrame, tipo_investidor=None, obj_portfolio=None, homologacao=False):
        self.df_carteira_simulada = df_carteira_simulada
        self.pl = pl
        self.patr = {'Original': pl, 'FinanceiroFinal': pl, 'FinanceiroFuturo': pl}    
        self.limite_risk_score = limite_risk_score
        self.multiplicador_credito = multiplicador_credito
        self.df_ordens = df_ordens
        self.limites_credito = limites_credito
        self.limites_credito_exc = limites_credito_exc
        self.verificador = {}
        self.memoria_calculo = {}
        self.tipo_investidor = tipo_investidor
        self.obj_portfolio = obj_portfolio
        # Lista com os ETFs Brasileiros
        self.lista_etfs = ['BOVA11', 'BOVV11', 'DEFI11', 'ETHE11', 'GOLD11', 'HASH11', 'IVVB11', 'PIBB11', 'SMAL11', 'SPXI11']
    
    def __verifica_enquadramento__(self, nome_limite:str, alocacao:dict, limites:dict):
        """
        Função que recebe dois dicionários, um com os limites, outro com a alocação atual e projetada e determina
        se é a ordem causa um desenquadramento, alerta ou está aprovava

        Parameters
        ----------
        nome_limite : str
            Texto para ajudar o usuário a entender qual limite foi violado.
        alocacao : dict
            dicionario com colunas 'antes', 'final' e 'futuro'.
        limites : dict
            dicionario de limites com campos 'max', 'max_alerta', 'min', 'min_alerta'.

        Returns
        -------
        None.

        """
        # Se função encontra desenquadramento (3), sai da função, se não continua rodando regras
        
        # Tolerância de 0.1%
        tolerancia = 0.001        
        
        # Testa limites máximos
        if 'max' in limites.keys():
            # Verificação se está acima do máximo do limite
            if alocacao['final'] > limites['max'] and alocacao['antes'] <= limites['max']:
                # Novo desenquadramento
                self.verificador[nome_limite] = 3
                return
            elif alocacao['final'] > limites['max'] and alocacao['antes'] > limites['max']:
                # Já estava desenquadrado
                if abs(alocacao['final'] - alocacao['antes']) <= tolerancia:
                    # Não houve mudança na posição
                    self.verificador[nome_limite] = 1
                elif alocacao['final'] > alocacao['antes']:
                    # Desenquadramento piorou
                    self.verificador[nome_limite] = 3
                    return
                elif alocacao['final'] < alocacao['antes']:
                    # reduziu desenqudramento
                    self.verificador[nome_limite] = 2
                else:
                    self.verificador[nome_limite] = 1
            elif alocacao['final'] >= limites['max_alerta'] and (abs(alocacao['final'] - alocacao['antes']) >= tolerancia):
                self.verificador[nome_limite] = 2
            else:
                self.verificador[nome_limite] = 1
        
        # Testa limites mínimos
        if 'min' in limites.keys():
            # Verificação se está abaixo do mínimo do limite
            if alocacao['final'] < limites['min'] and alocacao['antes'] >= limites['min']:
                # Novo desenquadramento
                self.verificador[nome_limite] = 3
                return
            elif alocacao['final'] < limites['min'] and alocacao['antes'] < limites['min']:
                # Já estava desenquadrado
                if alocacao['final'] < alocacao['antes']:
                    # Desenquadramento piorou
                    self.verificador[nome_limite] = 3
                    return
                elif alocacao['final'] > alocacao['antes']:
                    # reduziu desenqudramento
                    self.verificador[nome_limite] = 2
                else:
                    # manteve desenquadramento
                    self.verificador[nome_limite] = 1
            elif alocacao['final'] <= limites['min_alerta'] and (abs(alocacao['final'] - alocacao['antes']) >= tolerancia):
                self.verificador[nome_limite] = 2
            else:
                self.verificador[nome_limite] = 1
    
    def __count_compras__(self):
        try:
            return len(self.df_ordens[self.df_ordens['TipoMov']=='C'])
        except:
            return 0
    
    def __count_vendas__(self):
        try:
            return len(self.df_ordens[self.df_ordens['TipoMov']=='V'])        
        except:
            return 0
    
    def __compromissada_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
        parametro = 'Comp'
        pos.insert(0,"compromissada", np.where(pos['NomeProduto'].apply(lambda x: parametro in x) == True, "S", "N"))
        
        self.comp_antes = (pos.loc[(pos["Compromissada"] == 'S'), 'Original'].sum()) / self.pl
        
        self.comp_final = (pos.loc[(pos["Compromissada"] == 'S'), 'FinanceiroFinal'].sum()) / self.pl
        
        self.comp_futuro = (pos.loc[(pos["Compromissada"] == 'S'), 'FinanceiroFuturo'].sum()) / self.pl
        
        dicio = {'antes': self.comp_antes, 'final': self.comp_final, 'futuro': self.comp_futuro}
        return dicio
    
    def comp_exposicao_25(self):
        alocacao = self.__compromissada_exposicao__()
        limites = {'max': 0.25, 'max_alerta': 0.17}
        self.__verifica_enquadramento__(nome_limite='Comp até 25%', alocacao=alocacao, limites=limites)  

    def __percentual_ie__(self):        
        pos = self.df_carteira_simulada.copy()                
        posicao_soma = []
        
        # 1. Pega classes que costumam ter ativos negociados no exterior
        pos_temp = pos[pos['Classe'].isin(['Investimento no Exterior', 'RF Internacional', 'RV Internacional', 'RV Global Hedged'])].copy()
        pos_temp = pos_temp[~pos_temp['CodNegociacao'].isin(self.lista_etfs)]
        pos_temp = pos_temp[~pos_temp['TipoProduto'].isin(['DEB', 'COTAS','FUNDO', 'Fundo'])] # Tira cotas e Fundo pois daria dupla contagem com o controle 2
        posicao_soma.append(pos_temp.copy())
        
        # 2. Fundos com IE no nome
        pos_temp = pos[(pos['DicioCad'].str.contains("'InvExt': True")) & pos['TipoProduto'].isin(['COTAS','FUNDO', 'Fundo'])].copy()
        posicao_soma.append(pos_temp.copy())
        
        # Fazer a soma        
        pos = pd.concat(posicao_soma)
        self.memoria_calculo['IE_Anterior'] = pos['Original'].sum() / self.pl
        self.memoria_calculo['IE_Final'] = pos['FinanceiroFinal'].sum() / self.pl        
        self.memoria_calculo['IE_Futuro'] = pos['FinanceiroFuturo'].sum() / self.pl
        
        dicio = {'antes': self.memoria_calculo['IE_Anterior'], 'final': self.memoria_calculo['IE_Final'], 'futuro': self.memoria_calculo['IE_Futuro']}
        return dicio
    
    def __percentual_ie_fundacao__(self):        
        pos = self.df_carteira_simulada.copy()                
        posicao_soma = []
        
        # 1. Pega classes que costumam ter ativos negociados no exterior
        pos_temp = pos[pos['Classe'].isin(['Investimento no Exterior', 'RF Internacional', 'RV Internacional', 'RV Global Hedged'])].copy()
        # pos_temp = pos_temp[~pos_temp['CodNegociacao'].isin(self.lista_etfs)]   # não tirar etfs
        pos_temp = pos_temp[~pos_temp['TipoProduto'].isin(['DEB', 'COTAS','FUNDO', 'Fundo'])] # Tira cotas e Fundo pois daria dupla contagem com o controle 2
        posicao_soma.append(pos_temp.copy())
        
        # 2. Fundos com IE no nome
        pos_temp = pos[(pos['DicioCad'].str.contains("'InvExt': True")) & pos['TipoProduto'].isin(['COTAS','FUNDO', 'Fundo'])].copy()
        posicao_soma.append(pos_temp.copy())
        
        # 3. BDRs e Fundos de BDR
        pos_temp = pos[~pos['DicioCad'].str.contains("'BDR': True")].copy()
        pos_temp = pos_temp[~pos_temp['DicioCad'].str.contains("'InvExt': True")] # evita dupla contagem com item 2
        
        posicao_soma.append(pos_temp.copy())
        
        # Fazer a soma        
        pos = pd.concat(posicao_soma)
        self.memoria_calculo['IE_Anterior'] = pos['Original'].sum() / self.pl
        self.memoria_calculo['IE_Final'] = pos['FinanceiroFinal'].sum() / self.pl        
        self.memoria_calculo['IE_Futuro'] = pos['FinanceiroFuturo'].sum() / self.pl
        
        dicio = {'antes': self.memoria_calculo['IE_Anterior'], 'final': self.memoria_calculo['IE_Final'], 'futuro': self.memoria_calculo['IE_Futuro']}
        return dicio
    
    def ie_ate_40(self):
        alocacao = self.__percentual_ie__()
        limites = {'max': 0.40, 'max_alerta': 0.38}
        self.__verifica_enquadramento__(nome_limite='IE até 40%', alocacao=alocacao, limites=limites)        
            
    def ie_ate_33(self):
        alocacao = self.__percentual_ie__()
        limites = {'max': 0.33, 'max_alerta': 0.32}
        self.__verifica_enquadramento__(nome_limite='IE até 33%', alocacao=alocacao, limites=limites)                
            
    def ie_ate_20(self):
        alocacao = self.__percentual_ie__()
        limites = {'max': 0.20, 'max_alerta': 0.19}
        self.__verifica_enquadramento__(nome_limite='IE até 20%', alocacao=alocacao, limites=limites)                            

    def ie_ate_15(self):
        alocacao = self.__percentual_ie__()
        limites = {'max': 0.15, 'max_alerta': 0.17}
        self.__verifica_enquadramento__(nome_limite='IE até 15%', alocacao=alocacao, limites=limites)                            

    def ie_min_67(self):
        alocacao = self.__percentual_ie__()
        limites = {'min': 0.67, 'min_alerta': 0.69}
        self.__verifica_enquadramento__(nome_limite='IE mín 67%', alocacao=alocacao, limites=limites)                                    

    def ie_vedado(self):
        alocacao = self.__percentual_ie__()
        limites = {'max': 0.00, 'max_alerta': -0.01}
        self.__verifica_enquadramento__(nome_limite='IE vedado', alocacao=alocacao, limites=limites)     

    def __percentual_ie_ex_FIFE__(self):        
        pos = self.df_carteira_simulada.copy()
        # Pega classes que costumam ter ativos negociados no exterior
        pos = pos[pos['Classe'].isin(['Investimento no Exterior', 'RF Internacional', 'RV Internacional'])]
        # Tira ativos que são locais
        pos = pos[~pos['CodNegociacao'].isin(self.lista_etfs)]
        # Tira ativos que são locais
        pos = pos[~pos['TipoProduto'].isin(['DEB'])]
        # Tira ativos que são FIFES
        pos = pos[~pos['DicioCad'].isin(["'FIFE': True"])]
        # Fazer a soma
        self.memoria_calculo['IE_Anterior'] = pos['Original'].sum() / self.pl
        self.memoria_calculo['IE_Final'] = pos['FinanceiroFinal'].sum() / self.pl        
        self.memoria_calculo['IE_Futuro'] = pos['FinanceiroFuturo'].sum() / self.pl
        
        dicio = {'antes': self.memoria_calculo['IE_Anterior'], 'final': self.memoria_calculo['IE_Final'], 'futuro': self.memoria_calculo['IE_Futuro']}
        return dicio                                              
    
    def ie_ex_FIFE_ate_40(self):
        alocacao = self.__percentual_ie_ex_FIFE__()
        limites = {'max': 0.40, 'max_alerta': 0.38}
        self.__verifica_enquadramento__(nome_limite='IE até 40%', alocacao=alocacao, limites=limites)   
    
    def ie_ex_FIFE_ate_20(self):
        alocacao = self.__percentual_ie_ex_FIFE__()
        limites = {'max': 0.20, 'max_alerta': 0.18}
        self.__verifica_enquadramento__(nome_limite='IE até 20%', alocacao=alocacao, limites=limites)  
    
    def ie_ex_FIFE_ate_10(self):
        alocacao = self.__percentual_ie_ex_FIFE__()
        limites = {'max': 0.10, 'max_alerta': 0.07}
        self.__verifica_enquadramento__(nome_limite='IE até 10%', alocacao=alocacao, limites=limites)  
        
    def __percentual_rv__(self):
        # TipoTrib=1 : Tributação de ações
        # TODO: se fundo for FIA, precisamos tirar FIMs do cálculo
        # TODO: se fundo não for FIA BDR Nivel I, precisamos tirar Investimentos no exterior e FIA BDR Nivel I do calculo
        posicao_classe = self.df_carteira_simulada[(self.df_carteira_simulada['TipoTrib']==1) & (self.df_carteira_simulada['ExcluirAtivo']==0)].copy()        
        posicao_classe = posicao_classe[~posicao_classe['TipoProduto'].isin(['FUT', 'SWAP'])]
        self.memoria_calculo['RV_Anterior'] = posicao_classe['Original'].sum() / self.pl
        self.memoria_calculo['RV_Final'] = posicao_classe['FinanceiroFinal'].sum() / self.pl
        self.memoria_calculo['RV_Futuro'] = posicao_classe['FinanceiroFuturo'].sum() / self.pl
        dicio = {'antes': self.memoria_calculo['RV_Anterior'], 'final': self.memoria_calculo['RV_Final'], 'futuro': self.memoria_calculo['RV_Futuro']}        
        return dicio
        
    def rv_min_67(self):        
        alocacao = self.__percentual_rv__()        
        limites = {'min': 0.67, 'min_alerta': 0.69}
        self.__verifica_enquadramento__(nome_limite='Mín. 67% em RV', alocacao=alocacao, limites=limites)                
    
    def rv_vedado(self):        
        alocacao = self.__percentual_rv__()        
        limites = {'max': 0.00, 'max_alerta': 0.01}
        self.__verifica_enquadramento__(nome_limite='RV Vedado', alocacao=alocacao, limites=limites)  

    def rv_min_95(self):        
        alocacao = self.__percentual_rv__()        
        limites = {'min': 0.95, 'min_alerta': 0.97}
        self.__verifica_enquadramento__(nome_limite='Mín. 95% em RV', alocacao=alocacao, limites=limites)

    def rv_min_50(self):        
        alocacao = self.__percentual_rv__()        
        limites = {'min': 0.50, 'min_alerta': 0.60}
        self.__verifica_enquadramento__(nome_limite='Mín. 50% em RV', alocacao=alocacao, limites=limites)               
    
    def __percentual_vedados_fic__(self):
        # 1. Lista de produtos vedados
        lista_negra = ['OPT','FUT', 'SWAP', 'COE', 'CRA', 'CRI', 'DEB', 'DEBI', 'BOND']
        df_ln = self.df_carteira_simulada[self.df_carteira_simulada['TipoProduto'].isin(lista_negra)].copy()
        
        # 2. Testa se há posições em ações (pode apenas ETFs)
        df_acoes = self.df_carteira_simulada[self.df_carteira_simulada['TipoProduto'].isin(['BOLSA', 'ACOES', 'ACAO'])].copy()
        df_acoes = df_acoes[~df_acoes['CodNegociacao'].isin(self.lista_etfs)]
        
        # 3. Junta os dataframes
        df = pd.concat([df_acoes, df_ln])
        if not df.empty:    
            self.memoria_calculo['FIC_V_Anterior'] = abs(df['Original'].sum() / self.pl)
            self.memoria_calculo['FIC_V_Final'] = abs(df['FinanceiroFinal'].sum() / self.pl)
            self.memoria_calculo['FIC_V_Futuro'] = abs(df['FinanceiroFuturo'].sum() / self.pl)
            dicio = {'antes': self.memoria_calculo['FIC_V_Anterior'], 'final': self.memoria_calculo['FIC_V_Final'], 'futuro': self.memoria_calculo['FIC_V_Futuro']}        
        else:
            dicio = {'antes': 0, 'final': 0, 'futuro': 0}        
        return dicio
    
    def __percentual_cotas_fundos__(self):
        # Tipos de produtos ex-ações
        df = self.df_carteira_simulada[self.df_carteira_simulada['TipoProduto'].isin(['COTAS','FUNDO', 'FIDC', 'FII','FIP'])].copy()
        
        df_acoes = self.df_carteira_simulada[self.df_carteira_simulada['TipoProduto'].isin(['BOLSA', 'ACOES', 'ACAO'])].copy()
        df_acoes = df_acoes[df_acoes['CodNegociacao'].isin(self.lista_etfs)]        
        df = pd.concat([df, df_acoes])
        
        self.memoria_calculo['Cotas_Anterior'] = abs(df['Original'].sum() / self.pl)
        self.memoria_calculo['Cotas_Final'] = abs(df['FinanceiroFinal'].sum() / self.pl)
        self.memoria_calculo['Cotas_Futuro'] = abs(df['FinanceiroFuturo'].sum() / self.pl)
        dicio = {'antes': self.memoria_calculo['Cotas_Anterior'], 'final': self.memoria_calculo['Cotas_Final'], 'futuro': self.memoria_calculo['Cotas_Futuro']}        
        return dicio
    
    def fic_produtos_vedados(self):
        alocacao = self.__percentual_vedados_fic__()        
        limites = {'max': 0.00, 'max_alerta': 0.01}
        self.__verifica_enquadramento__(nome_limite='FIC: Vedados', alocacao=alocacao, limites=limites)                        
    
    def cotas_min_95(self):        
        alocacao = self.__percentual_cotas_fundos__()        
        limites = {'min': 0.95, 'min_alerta': 0.96}
        self.__verifica_enquadramento__(nome_limite='Cotas: 95%', alocacao=alocacao, limites=limites)                       

    def __derivativos_exposicao__(self):
        lista_derivativos = ['OPT','FUT', 'SWAP']
        df = self.df_carteira_simulada[self.df_carteira_simulada['TipoProduto'].isin(lista_derivativos)].copy()
        if not df.empty:           
            df['Original'] = df['Original'].apply(lambda x: abs(x) / self.pl)
            df['FinanceiroFinal'] = df['FinanceiroFinal'].apply(lambda x: abs(x) / self.pl)
            df['FinanceiroFuturo'] = df['FinanceiroFuturo'].apply(lambda x: abs(x) / self.pl)
        
            self.memoria_calculo['ExpDeriv_Antes'] = df['Original'].sum()
            self.memoria_calculo['ExpDeriv_Final'] = df['FinanceiroFinal'].sum()
            self.memoria_calculo['ExpDeriv_Futuro'] = df['FinanceiroFuturo'].sum()
        else:
            self.memoria_calculo['ExpDeriv_Antes'] = 0
            self.memoria_calculo['ExpDeriv_Final'] = 0
            self.memoria_calculo['ExpDeriv_Futuro'] = 0
        dicio = {'antes': self.memoria_calculo['ExpDeriv_Antes'], 'final': self.memoria_calculo['ExpDeriv_Final'], 'futuro': self.memoria_calculo['ExpDeriv_Futuro']}        
        return dicio        

    def derivativos_vedado(self):        
        limites = {'max': 0.00, 'max_alerta': 0.01}
        alocacao = self.__derivativos_exposicao__()
        self.__verifica_enquadramento__(nome_limite='Derivativos: vedado', alocacao=alocacao, limites=limites)                                   

    def derivativos_meia_vez(self):
        nome = 'Deriv - 0,5x'        
        limites = {'max': 0.50, 'max_alerta': 0.40}
        alocacao = self.__derivativos_exposicao__()
        self.__verifica_enquadramento__(nome_limite=nome, alocacao=alocacao, limites=limites)                                   

    def derivativos_uma_vez(self):
        nome = 'Deriv - 1,0x'
        limites = {'max': 1.00, 'max_alerta': 0.90}
        alocacao = self.__derivativos_exposicao__()
        self.__verifica_enquadramento__(nome_limite=nome, alocacao=alocacao, limites=limites)           
    
    def derivativos_um_ponto_tres(self):
        nome = 'Deriv - 1,3x'
        limites = {'max': 1.30, 'max_alerta': 0.20}
        alocacao = self.__derivativos_exposicao__()
        self.__verifica_enquadramento__(nome_limite=nome, alocacao=alocacao, limites=limites)                   
    
    def derivativos_um_ponto_cinco(self):
        nome = 'Deriv - 1,5x'
        limites = {'max': 1.50, 'max_alerta': 1.40}
        alocacao = self.__derivativos_exposicao__()
        self.__verifica_enquadramento__(nome_limite=nome, alocacao=alocacao, limites=limites)                      
        
    def derivativos_duas_vezes(self):
        nome = 'Deriv - 2,0x'
        limites = {'max': 2.00, 'max_alerta': 1.90}
        alocacao = self.__derivativos_exposicao__()
        self.__verifica_enquadramento__(nome_limite=nome, alocacao=alocacao, limites=limites)                      

    def derivativos_tres_vezes(self):
        nome = 'Deriv - 3,0x'
        limites = {'max': 3.00, 'max_alerta': 2.90}
        alocacao = self.__derivativos_exposicao__()
        self.__verifica_enquadramento__(nome_limite=nome, alocacao=alocacao, limites=limites)                      
    
    def derivativos_quatro_vezes(self):
        nome = 'Deriv - 4,0x'
        limites = {'max': 4.00, 'max_alerta': 3.90}
        alocacao = self.__derivativos_exposicao__()
        self.__verifica_enquadramento__(nome_limite=nome, alocacao=alocacao, limites=limites)                      
            
    def derivativos_cinco_vezes(self):
        nome = 'Deriv - 5,0x'
        limites = {'max': 5.00, 'max_alerta': 4.90}
        alocacao = self.__derivativos_exposicao__()
        self.__verifica_enquadramento__(nome_limite=nome, alocacao=alocacao, limites=limites)                      
    
    def derivativos_duzentos_vezes(self):
        nome = 'Deriv - 200,0x'
        limites = {'max': 200.00, 'max_alerta': 190.00}
        alocacao = self.__derivativos_exposicao__()
        self.__verifica_enquadramento__(nome_limite=nome, alocacao=alocacao, limites=limites)

    def enquadramento_credito(self):
        pos = self.df_carteira_simulada.copy().reset_index()
        pos = pos[pos['Classe'].isin(['R Fixa Pós', 'R Fixa Pré', 'R Fixa Infl', 'RF', 'RF Internacional'])]
        pos = pos[~pos['TipoProduto'].isin(['CAIXA'])]

        # cruza os limites com a posição
        produto = pd.merge(left=pos, left_on='GuidProduto', right=self.limites_credito, right_index=True, how='left')
        produto['Limite'] = produto['Limite'].fillna(0)
        # Limite dos fundos exclusivos é 100%
        indice = produto[produto['TipoCobranca']=='FundoExclusivo'].index
        if len(indice) > 0:
            produto.loc[indice, 'Limite'] = 1
        
        emissor = pd.merge(left=pos, left_on='GuidEmissor', right=self.limites_credito, right_index=True, how='left')
        emissor['Limite'] = emissor['Limite'].fillna(0)
        # Seleciona produtos relevantes
        produto = produto[['IdLimCred', 'IdLimCredConglomerado', 'NomeConglomerado', 'GuidProduto', 'TipoProduto', 'NomeLimite', 'Limite', 'GuidEmissor', 'Original', 'FinanceiroFinal', 'FinanceiroFuturo', 'Movimentacao']]
        emissor = emissor[['IdLimCred', 'IdLimCredConglomerado', 'NomeConglomerado', 'GuidProduto', 'NomeLimite', 'Limite', 'GuidEmissor', 'Original', 'FinanceiroFinal', 'FinanceiroFuturo', 'Movimentacao']]
        # Tira ativos sem emissor
        emissor = emissor[~emissor['GuidEmissor'].isnull()] 
        # Preenche limites zerados dos produtos com o limite do emissor
        filtro = produto[produto['Limite']==0]
        for idx, row in filtro.iterrows():
            if row['GuidEmissor']:
                if row['GuidEmissor'] in self.limites_credito.index and not row['TipoProduto'] in ['LFS', 'LFSN', 'LFSC']:
                    produto.loc[idx, 'Limite'] = self.limites_credito.loc[row['GuidEmissor'], 'Limite']
                elif row['GuidEmissor'] in self.limites_credito.index and row['TipoProduto'] in ['LFS', 'LFSN', 'LFSC']:
                    produto.loc[idx, 'Limite'] = self.limites_credito.loc[row['GuidEmissor'], 'Limite'] * 0.25
        filtro = produto[produto['IdLimCred'].isnull()].index
        produto.loc[filtro, 'IdLimCred'] = -filtro
        # Preenche limites zerados dos emissores com o limite dos produtos
        filtro = emissor[emissor['Limite']==0]
        for idx, row in filtro.iterrows():
            if row['GuidProduto'] in self.limites_credito.index:
                emissor.loc[idx, 'Limite'] = self.limites_credito.loc[row['GuidProduto'], 'Limite']
        filtro = produto[produto['IdLimCred'].isnull()].index
        
        # Transforma posição em % PL
        colunas = ['Original', 'FinanceiroFinal', 'FinanceiroFuturo']
        for col in colunas:
            produto[col] = produto.apply(lambda x: x[col] / self.pl,axis=1)
            emissor[col] = emissor.apply(lambda x: x[col] / self.pl,axis=1)
        
        cong_1 = produto[~produto['IdLimCredConglomerado'].isna()]
        cong_2 = emissor[~emissor['IdLimCredConglomerado'].isna()]
        cong = pd.DataFrame()
        if not cong_1.empty or not cong_2.empty:        
            cong = pd.concat([cong_1, cong_2])
            cong = cong.groupby(['IdLimCredConglomerado', 'NomeConglomerado', 'Limite']).sum().reset_index()            
        
        
        # Limites por Produto
        self.__enquadramento_credito_cat__(df_cat=produto, nome='Cred:Prod')        
        
        # Limites por Emissor
        self.__enquadramento_credito_cat__(df_cat=emissor, nome='Cred:Emi')
        
        # Limites por Conglomerado
        self.__enquadramento_credito_cat__(df_cat=cong, nome='Cred:Cong')
        
        # return produto, emissor, cong
        
    def __enquadramento_credito_cat__(self, df_cat:pd.DataFrame, nome:str) -> dict:
        dicio = {'antes': 0.00, 'final': 0.00, 'futuro': 0.00}
        colunas = ['Original', 'FinanceiroFinal', 'FinanceiroFuturo']
        if df_cat.empty:
            return dicio
        dicio = {}
        # teste = df_cat[(df_cat['Movimentacao']!=0) & (df_cat['FinanceiroFuturo']>df_cat['Limite'])]  
        # if teste.empty:
        #    return dicio
        df = df_cat.copy()
        for col in colunas:
            df.insert(0, f"n{col}", [0] * len(df))
            df[f"n{col}"] = df.apply(lambda x: 1 if x[col] > x['Limite'] else 0, axis=1) 
        df = df[df['nFinanceiroFinal']==1]
        if 'NomeConglomerado' in df.columns and not 'NomeLimite' in df.columns:
            df.rename(columns={'NomeConglomerado':'NomeLimite'}, inplace=True)
        if 'GuidProduto' in df.columns:
            df['NomeLimite'] = df['NomeLimite'].fillna(df['GuidProduto'])
        for idx, row in df.iterrows():
            limite = round(row['Limite'] * self.multiplicador_credito, 4)
            limite_str = '{:0.2f}'.format(limite*100) + '%'            
            nome_lim = f"[{limite_str}]{row['NomeLimite']}"
            
            self.memoria_calculo[f'{nome_lim}_Anterior'] = row['Original']
            self.memoria_calculo[f'{nome_lim}_Final'] = row['FinanceiroFinal']
            self.memoria_calculo[f'{nome_lim}_Futuro'] = row['FinanceiroFuturo']
            alocacao = {'antes': row['Original'], 'final': row['FinanceiroFinal'], 'futuro': row['FinanceiroFuturo']}  
            limites = {'max': limite, 'max_alerta': limite * 9/10}
            self.__verifica_enquadramento__(nome_limite=nome_lim, alocacao=alocacao, limites=limites)
    
    def enquadramento_limites_individuais(self):
        pos = self.df_carteira_simulada.copy()
        lim = self.limites_port_individual[self.limites_port_individual['IdPortfolio']==self.obj_portfolio.guid]
        if lim.empty:
            return
        for idx, row in lim.iterrows():
            teste = False
            linha = pd.Series()
            id_restrito = row['IdRestrito']
            nome_lim = f"{row['TipoRestricao']}: {row['IdRestrito']} ({row['Observacao']})" 
            limites = {'max': row['PercMax'], 'max_alerta': row['PercMax'] * 9/10}  # Só há limite máximo
            if row['IdTpRestr'] in [1, 3]:
                # Ação / Fundo
                if id_restrito in pos.index:
                    teste = True                    
                    soma = pos.loc[[id_restrito]][['NomeProduto', 'Original', 'FinanceiroFinal', 'FinanceiroFuturo']].groupby('NomeProduto').sum()
                    linha = soma.iloc[0]
                    nome_lim = soma.index[0]                    
            elif row['IdTpRestr'] == 2:
                # Emissor
                if id_restrito in pos['GuidEmissor'].unique():
                    teste = True               
                    soma = pos[pos['GuidEmissor']==id_restrito][['GuidEmissor', 'Original', 'FinanceiroFinal', 'FinanceiroFuturo']].groupby('GuidEmissor').sum()
                    linha = soma.iloc[0]
            elif row['IdTpRestr'] == 4:
                # TipoProduto
                if id_restrito in pos['TipoProduto'].unique():
                    teste = True               
                    soma = pos[pos['TipoProduto']==id_restrito][['GuidEmissor', 'Original', 'FinanceiroFinal', 'FinanceiroFuturo']].groupby('TipoProduto').sum()
                    linha = soma.iloc[0]
            else:
                raise Exception('Tipo de restrição individual não cadastrado na regra enquadramento_limites_individuais')
            if teste:
                alocacao = {'antes': linha['Original'], 'final': linha['FinanceiroFinal'], 'futuro': linha['FinanceiroFuturo']}
                self.memoria_calculo[f'{nome_lim}_Anterior'] = linha['Original']
                self.memoria_calculo[f'{nome_lim}_Final'] = linha['FinanceiroFinal']
                self.memoria_calculo[f'{nome_lim}_Futuro'] = linha['FinanceiroFuturo']
                self.__verifica_enquadramento__(nome_limite=nome_lim, alocacao=alocacao, limites=limites)
    
    ### debentures ###
    def __debenture_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
        
        self.deb_antes = (pos.loc[(
                            (pos['TipoProduto'] == 'DEB') | 
                            (pos['TipoProduto'] == 'DEBI')
                            ), 'Original'].sum()) / self.pl
        
        self.deb_final = (pos.loc[(
                            (pos['TipoProduto'] == 'DEB') | 
                            (pos['TipoProduto'] == 'DEBI')
                            ), 'FinanceiroFinal'].sum()) / self.pl
        
        self.deb_futuro = (pos.loc[(
                            (pos['TipoProduto'] == 'DEB') | 
                            (pos['TipoProduto'] == 'DEBI')
                            ), 'FinanceiroFuturo'].sum()) / self.pl
        
        dicio = {'antes': self.deb_antes, 'final': self.deb_final, 'futuro': self.deb_futuro}
        return dicio

    def debenture_exposicao_30(self):
        alocacao = self.__debenture_exposicao__()
        limites = {'max': 0.3, 'max_alerta': 0.22}
        self.__verifica_enquadramento__(nome_limite='Deb até 30%', alocacao=alocacao, limites=limites)    
        
    def debenture_exposicao_75(self):
        alocacao = self.__debenture_exposicao__()
        limites = {'max': 0.75, 'max_alerta': 0.65}
        self.__verifica_enquadramento__(nome_limite='Deb até 75%', alocacao=alocacao, limites=limites)    
        
    ### cotas ###
    def __cotas_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
    
        self.cotas_antes = (pos.loc[(pos['TipoProduto'] == 'COTAS'), 'Original'].sum()) / self.pl
        
        self.cotas_final = (pos.loc[(pos['TipoProduto'] == 'COTAS'), 'FinanceiroFinal'].sum()) / self.pl
        
        self.cotas_futuro = (pos.loc[(pos['TipoProduto'] == 'COTAS'), 'FinanceiroFuturo'].sum()) / self.pl
        
        dicio = {'antes': self.cotas_antes, 'final': self.cotas_final, 'futuro': self.cotas_futuro}
        return dicio
    
    def cotas_exposicao_40(self):
        alocacao = self.__cotas_exposicao__()
        limites = {'max': 0.40, 'max_alerta': 0.33}
        self.__verifica_enquadramento__(nome_limite='Cotas até 40%', alocacao=alocacao, limites=limites) 
    
    ### cra ###
    def __cra_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
    
        self.cra_antes = (pos.loc[(pos['TipoProduto'] == 'CRA'), 'Original'].sum()) / self.pl
        
        self.cra_final = (pos.loc[(pos['TipoProduto'] == 'CRA'), 'FinanceiroFinal'].sum()) / self.pl
        
        self.cra_futuro = (pos.loc[(pos['TipoProduto'] == 'CRA'), 'FinanceiroFuturo'].sum()) / self.pl
        
        dicio = {'antes': self.cra_antes, 'final': self.cra_final, 'futuro': self.cra_futuro}
        return dicio

    def cra_exposicao_0(self):
        alocacao = self.__cra_exposicao__()
        limites = {'max': 0.001, 'max_alerta': 0.0005}
        self.__verifica_enquadramento__(nome_limite='CRA até 0%', alocacao=alocacao, limites=limites)
        
    ### cri ###
    def __cri_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
    
        self.cri_antes = (pos.loc[(pos['TipoProduto'] == 'CRI'), 'Original'].sum()) / self.pl
        
        self.cri_final = (pos.loc[(pos['TipoProduto'] == 'CRI'), 'FinanceiroFinal'].sum()) / self.pl
        
        self.cri_futuro = (pos.loc[(pos['TipoProduto'] == 'CRI'), 'FinanceiroFuturo'].sum()) / self.pl
        
        dicio = {'antes': self.cri_antes, 'final': self.cri_final, 'futuro': self.cri_futuro}
        return dicio
    
    def cri_exposicao_15(self):
        alocacao = self.__cri_exposicao__()
        limites = {'max': 0.15, 'max_alerta': 0.10}
        self.__verifica_enquadramento__(nome_limite='CRI até 15%', alocacao=alocacao, limites=limites) 
        
    def cri_exposicao_20(self):
        alocacao = self.__cri_exposicao__()
        limites = {'max': 0.20, 'max_alerta': 0.15}
        self.__verifica_enquadramento__(nome_limite='CRI até 20%', alocacao=alocacao, limites=limites)
    
    def cri_exposicao_25(self):
        alocacao = self.__cri_exposicao__()
        limites = {'max': 0.25, 'max_alerta': 0.20}
        self.__verifica_enquadramento__(nome_limite='CRI até 25%', alocacao=alocacao, limites=limites) 
        
    def cri_exposicao_40(self):
        alocacao = self.__cri_exposicao__()
        limites = {'max': 0.40, 'max_alerta': 0.33}
        self.__verifica_enquadramento__(nome_limite='CRI até 40%', alocacao=alocacao, limites=limites) 
    
    def cri_exposicao_50(self):
        alocacao = self.__cri_exposicao__()
        limites = {'max': 0.50, 'max_alerta': 0.43}
        self.__verifica_enquadramento__(nome_limite='CRI até 50%', alocacao=alocacao, limites=limites) 
        
    def cri_exposicao_0(self):
        alocacao = self.__cri_exposicao__()
        limites = {'max': 0.001, 'max_alerta': 0.0005}
        self.__verifica_enquadramento__(nome_limite='CRI até 0%', alocacao=alocacao, limites=limites) 
    
    ### deb infra + fidc + cri min 85% ###
    def __debi_fidc_cri_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
        self.compilado_antes = (pos.loc[(
                                    (pos['TipoProduto'] == 'DEBI') | 
                                    (pos['TipoProduto'] == 'FIDC') |
                                    (pos['TipoProduto'] == 'CRI')
                                    ), 'Original'].sum()) / self.pl
        
        self.compilado_final = (pos.loc[(
                                    (pos['TipoProduto'] == 'DEBI') | 
                                    (pos['TipoProduto'] == 'FIDC') |
                                    (pos['TipoProduto'] == 'CRI')
                                    ), 'FinanceiroFinal'].sum()) / self.pl
        
        self.compilado_futuro = (pos.loc[(
                                    (pos['TipoProduto'] == 'DEBI') | 
                                    (pos['TipoProduto'] == 'FIDC') |
                                    (pos['TipoProduto'] == 'CRI')
                                    ), 'FinanceiroFuturo'].sum()) / self.pl
        dicio = {'antes': self.compilado_antes, 'final': self.compilado_final, 'futuro': self.compilado_futuro}
        return dicio
    
    def debi_fidc_cri_85(self):
        alocacao = self.__debi_fidc_cri_exposicao__()
        limites = {'min': 0.85, 'min_alerta': 0.87}
        self.__verifica_enquadramento__(nome_limite='DEBI_FIDC_CRI min 85%', alocacao=alocacao, limites=limites) 
        
    ### FIDC ###
    def __fidc_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
    
        self.fidc_antes = (pos.loc[(pos['TipoProduto'] == 'FIDC'), 'Original'].sum()) / self.pl
        
        self.fidc_final = (pos.loc[(pos['TipoProduto'] == 'FIDC'), 'FinanceiroFinal'].sum()) / self.pl
        
        self.fidc_futuro = (pos.loc[(pos['TipoProduto'] == 'FIDC'), 'FinanceiroFuturo'].sum()) / self.pl
        
        dicio = {'antes': self.fidc_antes, 'final': self.fidc_final, 'futuro': self.fidc_futuro}
        return dicio
    
    def fidc_exposicao_15(self):
        alocacao = self.__fidc_exposicao__()
        limites = {'max': 0.15, 'max_alerta': 0.10}
        self.__verifica_enquadramento__(nome_limite='FIDC até 15%', alocacao=alocacao, limites=limites)
        
    def fidc_exposicao_20(self):
        alocacao = self.__fidc_exposicao__()
        limites = {'max': 0.20, 'max_alerta': 0.15}
        self.__verifica_enquadramento__(nome_limite='FIDC até 20%', alocacao=alocacao, limites=limites) 
        
    def fidc_exposicao_40(self):
        alocacao = self.__fidc_exposicao__()
        limites = {'max': 0.40, 'max_alerta': 0.33}
        self.__verifica_enquadramento__(nome_limite='FIDC até 40%', alocacao=alocacao, limites=limites) 
        
    def fidc_exposicao_0(self):
        alocacao = self.__fidc_exposicao__()
        limites = {'max': 0.001, 'max_alerta': 0.0005}
        self.__verifica_enquadramento__(nome_limite='FIDC até 0%', alocacao=alocacao, limites=limites)
    
    ### FIDC-NP ###
    def __fidc_np_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
        parametro = 'FIDCNP'
        parametro_string = "'FIDC-NP': True"
        pos.insert(0,parametro, np.where(pos['DicioCad'].apply(lambda x: parametro_string in x) == True, "S", "N"))
        
        self.fidc_np_antes = (pos.loc[(pos[parametro] == 'S'), 'Original'].sum()) / self.pl
        
        self.fidc_np_final = (pos.loc[(pos[parametro] == 'S'), 'FinanceiroFinal'].sum()) / self.pl
        
        self.fidc_np_futuro = (pos.loc[(pos[parametro] == 'S'), 'FinanceiroFuturo'].sum()) / self.pl
        
        dicio = {'antes': self.fidc_np_antes, 'final': self.fidc_np_final, 'futuro': self.fidc_np_futuro}
        return dicio
    
    def fidc_np_0(self):
        alocacao = self.__fidc_np_exposicao__()
        limites = {'min': 0.0, 'min_alerta': 0.0}
        self.__verifica_enquadramento__(nome_limite='FIDC-NP max 0%', alocacao=alocacao, limites=limites) 
    
    ### FII ###
    def __fii_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
    
        self.fii_antes = (pos.loc[(pos['TipoProduto'] == 'FII'), 'Original'].sum()) / self.pl
        
        self.fii_final = (pos.loc[(pos['TipoProduto'] == 'FII'), 'FinanceiroFinal'].sum()) / self.pl
        
        self.fii_futuro = (pos.loc[(pos['TipoProduto'] == 'FII'), 'FinanceiroFuturo'].sum()) / self.pl
        
        dicio = {'antes': self.fii_antes, 'final': self.fii_final, 'futuro': self.fii_futuro}
        return dicio
       
    def fii_exposicao_20(self):
        alocacao = self.__fii_exposicao__()
        limites = {'max': 0.20, 'max_alerta': 0.15}
        self.__verifica_enquadramento__(nome_limite='FII até 20%', alocacao=alocacao, limites=limites) 
        
    
    def fii_exposicao_40(self):
        alocacao = self.__fii_exposicao__()
        limites = {'max': 0.40, 'max_alerta': 0.33}
        self.__verifica_enquadramento__(nome_limite='FII até 40%', alocacao=alocacao, limites=limites)     
    
    def fii_exposicao_50(self):
        alocacao = self.__fii_exposicao__()
        limites = {'max': 0.50, 'max_alerta': 0.43}
        self.__verifica_enquadramento__(nome_limite='FII até 50%', alocacao=alocacao, limites=limites)     
        
    def fii_exposicao_0(self):
        alocacao = self.__fii_exposicao__()
        limites = {'max': 0.001, 'max_alerta': 0.0005}
        self.__verifica_enquadramento__(nome_limite='FII até 0%', alocacao=alocacao, limites=limites) 
        
    ### FIP ###
    # from cl_tipo_tributação
    
    def __fip_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
    
        self.fip_antes = (pos.loc[(pos['TipoTrib'] == 18), 'Original'].sum()) / self.pl
        
        self.fip_final = (pos.loc[(pos['TipoTrib'] == 18), 'FinanceiroFinal'].sum()) / self.pl
        
        self.fip_futuro = (pos.loc[(pos['TipoTrib'] == 18), 'FinanceiroFuturo'].sum()) / self.pl
        
        dicio = {'antes': self.fip_antes, 'final': self.fip_final, 'futuro': self.fip_futuro}
        return dicio
    
    def fip_exposicao_10(self):
        alocacao = self.__fip_exposicao__()
        limites = {'max': 0.10, 'max_alerta': 0.05}
        self.__verifica_enquadramento__(nome_limite='FIP até 10%', alocacao=alocacao, limites=limites)
        
    def fip_exposicao_0(self):
        alocacao = self.__fip_exposicao__()
        limites = {'max': 0.001, 'max_alerta': 0.0005}
        self.__verifica_enquadramento__(nome_limite='FIP até 0%', alocacao=alocacao, limites=limites)
        
    ### produtos bancarios ###
    def __prod_bancos_exposicao__(self):
        produtos_bancarios = ['LF', 'LFS', 'CDB', 'LIG', 'LC']
        pos_total = self.df_carteira_simulada.copy()
        # pos = pos_total.query('TipoProduto in @produtos_bancarios')
        pos = pos_total[pos_total['TipoProduto'].isin(produtos_bancarios)]
        
        self.bancos_antes = pos['Original'].sum() / self.pl
        
        self.bancos_final = pos['FinanceiroFinal'].sum() / self.pl
        
        self.bancos_futuro = pos['FinanceiroFuturo'].sum() / self.pl
        
        
        dicio = {'antes': self.bancos_antes, 'final': self.bancos_final, 'futuro': self.bancos_futuro}
        return dicio
    
    #Emissor Bancos
    def __prod_bancos_emissor__(self):
        produtos_bancarios = ['LF', 'LFS', 'CDB', 'LIG', 'LC']
        pos_total = self.df_carteira_simulada.copy()
        # pos = pos_total.query('TipoProduto in @produtos_bancarios')
        pos = pos_total[pos_total['TipoProduto'].isin(produtos_bancarios)]
        pos = pos[['NomeEmissor','Original','FinanceiroFinal','FinanceiroFuturo']]
        pos = pos.groupby(['NomeEmissor']).sum().reset_index()
        maxi_original = pos['Original'].max()
        maxi_final = pos['FinanceiroFinal'].max()
        maxi_futuro = pos['FinanceiroFuturo'].max()

        self.bancos_antes = maxi_original/ self.pl
        
        self.bancos_final = maxi_final/ self.pl
        
        self.bancos_futuro = maxi_futuro .sum() / self.pl
        
        
        dicio = {'antes': self.bancos_antes, 'final': self.bancos_final, 'futuro': self.bancos_futuro}
        return dicio
    
    
    def prod_bancos_15(self):
        alocacao = self.__prod_bancos_exposicao__()
        limites = {'max': 0.15, 'max_alerta': 0.10}
        self.__verifica_enquadramento__(nome_limite='IF até 15%', alocacao=alocacao, limites=limites)

    def prod_bancos_20(self):
        alocacao = self.__prod_bancos_emissor__()
        limites = {'max': 0.2, 'max_alerta': 0.15}
        self.__verifica_enquadramento__(nome_limite='IF até 20%', alocacao=alocacao, limites=limites)
        
    def prod_bancos_50(self):
        alocacao = self.__prod_bancos_exposicao__()
        limites = {'max': 0.5, 'max_alerta': 0.43}
        self.__verifica_enquadramento__(nome_limite='IF até 50%', alocacao=alocacao, limites=limites)
        
    ### CDI ###
    def __cdi_exposicao__(self):
        #todo: classificar fundos que referenciam DI no nome p/ indexador = CDI
        pos = self.df_carteira_simulada.copy()
    
        self.cdi_antes = (pos.loc[(pos['Indexador'] == 'CDI'), 'Original'].sum()) / self.pl
        
        self.cdi_final = (pos.loc[(pos['Indexador'] == 'CDI'), 'FinanceiroFinal'].sum()) / self.pl
        
        self.cdi_futuro = (pos.loc[(pos['Indexador'] == 'CDI'), 'FinanceiroFuturo'].sum()) / self.pl
        
        dicio = {'antes': self.cdi_antes, 'final': self.cdi_final, 'futuro': self.cdi_futuro}
        return dicio

    def cdi_exposicao_95(self):
        alocacao = self.__cdi_exposicao__()
        limites = {'min': 0.95, 'min_alerta': 0.975}
        self.__verifica_enquadramento__(nome_limite='CDI min 95%', alocacao=alocacao, limites=limites)
    
    ### publico alvo profissional###
    def __publ_alvoP_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
        self.publico_alvoP_antes = (pos.loc[(pos['PubAlvo'] == 'Profissional'), 'Original'].sum()) / self.pl
        
        self.publico_alvoP_final = (pos.loc[(pos['PubAlvo'] == 'Profissional'), 'FinanceiroFinal'].sum()) / self.pl
        
        self.publico_alvoP_futuro = (pos.loc[(pos['PubAlvo'] == 'Profissional'), 'FinanceiroFuturo'].sum()) / self.pl
        
        dicio = {'antes': self.publico_alvoP_antes, 'final': self.publico_alvoP_final, 'futuro': self.publico_alvoP_futuro}
        return dicio

    def publ_alvoP_exposicao_10(self):
        alocacao = self.__publ_alvoP_exposicao__()
        limites = {'max': 0.1, 'max_alerta': 0.07}
        self.__verifica_enquadramento__(nome_limite='Profissional até 10%', alocacao=alocacao, limites=limites) 
        
    def publ_alvoP_exposicao_33(self):
        alocacao = self.__publ_alvoP_exposicao__()
        limites = {'max': 0.33, 'max_alerta': 0.25}
        self.__verifica_enquadramento__(nome_limite='Profissional até 33%', alocacao=alocacao, limites=limites) 
        
    def publ_alvoP_exposicao_5(self):
        alocacao = self.__publ_alvoP_exposicao__()
        limites = {'max': 0.05, 'max_alerta': 0.03}
        self.__verifica_enquadramento__(nome_limite='Profissional até 5%', alocacao=alocacao, limites=limites) 

    def publ_alvoP_exposicao0(self):
        alocacao = self.__publ_alvoP_exposicao__()
        limites = {'max': 0.001, 'max_alerta': 0.0005}
        self.__verifica_enquadramento__(nome_limite='Profissional até 0%', alocacao=alocacao, limites=limites) 
        
    ### publico alvo qualificado###
    def __publ_alvoQ_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
        self.publico_alvoQ_antes = (pos.loc[(pos['PubAlvo'] == 'Qualificado'), 'Original'].sum()) / self.pl
        
        self.publico_alvoQ_final = (pos.loc[(pos['PubAlvo'] == 'Qualificado'), 'FinanceiroFinal'].sum()) / self.pl
        
        self.publico_alvoQ_futuro = (pos.loc[(pos['PubAlvo'] == 'Qualificado'), 'FinanceiroFuturo'].sum()) / self.pl
        
        dicio = {'antes': self.publico_alvoQ_antes, 'final': self.publico_alvoQ_final, 'futuro': self.publico_alvoQ_futuro}
        return dicio
    
    def publ_alvoQ_exposicao_20(self):
        alocacao = self.__publ_alvoQ_exposicao__()
        limites = {'max': 0.2, 'max_alerta': 0.18}
        self.__verifica_enquadramento__(nome_limite='Qualificado até 20%', alocacao=alocacao, limites=limites) 
        
    def publ_alvoQ_exposicao_40(self):
        alocacao = self.__publ_alvoQ_exposicao__()
        limites = {'max': 0.4, 'max_alerta': 0.38}
        self.__verifica_enquadramento__(nome_limite='Qualificado até 40%', alocacao=alocacao, limites=limites) 
        
    def publ_alvoQ_exposicao_0(self):
        alocacao = self.__publ_alvoQ_exposicao__()
        limites = {'max': 0.001, 'max_alerta': 0.0005}
        self.__verifica_enquadramento__(nome_limite='Qualificado até 0%', alocacao=alocacao, limites=limites)        
    
    
    def __Cota95__(self):
        pos = self.df_carteira_simulada.copy()
                
        self.cotas_antes = (pos.loc[(pos['SubClasse'] == 'Caixa') | (pos['SubClasse'] == 'RF Pos Liquidez'), 'Original'].sum()) / self.pl
        
        self.cotas_final = (pos.loc[(pos['SubClasse'] == 'Caixa') | (pos['SubClasse'] == 'RF Pos Liquidez'), 'FinanceiroFinal'].sum()) / self.pl
        
        self.cotas_futuro = (pos.loc[(pos['SubClasse'] == 'Caixa') | (pos['SubClasse'] == 'RF Pos Liquidez'), 'FinanceiroFuturo'].sum()) / self.pl
        
        dicio = {'antes': self.cotas_antes, 'final': self.cotas_final, 'futuro': self.cotas_futuro}
        return dicio

    def Cota_5(self):
        alocacao = self.__Cota95__()
        limites = {'max': 0.05, 'max_alerta': 0.035}
        self.__verifica_enquadramento__(nome_limite='Liquidez ate 5%', alocacao=alocacao, limites=limites)
        
    def risk_score_limite(self):
        nome = 'RiskScore'
        limites = {'max': self.limite_risk_score, 'max_alerta': self.limite_risk_score - 0.1}
        alocacao = self.__risk_score_calculo__()
        self.__verifica_enquadramento__(nome_limite=nome, alocacao=alocacao, limites=limites)          
    ### FI vedado ###
    def __FI_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
    
        self.FI_antes = (pos.loc[(pos['TipoProduto'] == 'COTAS'), 'Original'].sum()) / self.pl
        
        self.FI_final = (pos.loc[(pos['TipoProduto'] == 'COTAS'), 'FinanceiroFinal'].sum()) / self.pl
        
        self.FI_futuro = (pos.loc[(pos['TipoProduto'] == 'COTAS'), 'FinanceiroFuturo'].sum()) / self.pl
        
        dicio = {'antes': self.FI_antes, 'final': self.FI_final, 'futuro': self.FI_futuro}
        return dicio
   
    def FI_exposicao_0(self):
        alocacao = self.__FI_exposicao__()
        limites = {'max': 0.0005, 'max_alerta': 0.00005}
        self.__verifica_enquadramento__(nome_limite='FI até 0%', alocacao=alocacao, limites=limites) 
        
    def __BDR_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
        parametro = 'BDR'
        parametro_string = "'BDR': True"
        pos.insert(0,parametro, np.where(pos['DicioCad'].apply(lambda x: parametro_string in x) == True, "S", "N"))
        
        BDR_antes = (pos.loc[(pos[parametro] == 'S'), 'Original'].sum()) / self.pl
        
        BDR_final = (pos.loc[(pos[parametro] == 'S'), 'FinanceiroFinal'].sum()) / self.pl
        
        BDR_futuro = (pos.loc[(pos[parametro] == 'S'), 'FinanceiroFuturo'].sum()) / self.pl
        
        self.memoria_calculo['BDR_Antes'] = BDR_antes
        self.memoria_calculo['BDR_Final'] = BDR_final
        self.memoria_calculo['BDR_Futuro'] = BDR_futuro
        
        dicio = {'antes': BDR_antes, 'final': BDR_final, 'futuro': BDR_futuro}
        return dicio
    
    def __compromissada_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
        df = pos.loc[(pos['TipoProduto']=='COMP') | (pos['TipoProduto']=='CAIXA')]
        comp_antes = (df['Original'].sum()) / self.pl        
        comp_final = (df['FinanceiroFinal'].sum()) / self.pl        
        comp_futuro = (df['FinanceiroFuturo'].sum()) / self.pl
        
        self.memoria_calculo['COMP_Antes'] = comp_antes
        self.memoria_calculo['COMP_Final'] = comp_final
        self.memoria_calculo['COMP_Futuro'] = comp_futuro
        
        dicio = {'antes': comp_antes, 'final': comp_final, 'futuro': comp_futuro}
        return dicio
    
    def comp_exposicao_25(self):
        alocacao = self.__compromissada_exposicao__()
        limites = {'max': 0.25, 'max_alerta': 0.17}
        self.__verifica_enquadramento__(nome_limite='Comp até 25%', alocacao=alocacao, limites=limites)  

    def BDR_exposicao_15(self):
        alocacao = self.__BDR_exposicao__()
        limites = {'max': 0.15, 'max_alerta': 0.10}
        self.__verifica_enquadramento__(nome_limite='BDR até 15%', alocacao=alocacao, limites=limites)  
    
    def BDR_exposicao_30(self):
        alocacao = self.__BDR_exposicao__()
        limites = {'max': 0.30, 'max_alerta': 0.22}
        self.__verifica_enquadramento__(nome_limite='BDR até 30%', alocacao=alocacao, limites=limites)  
    
    def BDR_exposicao_7meio(self):
        alocacao = self.__BDR_exposicao__()
        limites = {'max': 0.075, 'max_alerta': 0.045}
        self.__verifica_enquadramento__(nome_limite='BDR até 7.5%', alocacao=alocacao, limites=limites)
        
    def __cambial_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
        parametro = 'CAMBIAL'
        parametro_string = "'CAMBIAL': True"
        pos.insert(0,parametro, np.where(pos['DicioCad'].apply(lambda x: parametro_string in x) == True, "S", "N"))
        
        self.cambial_antes = (pos.loc[(pos[parametro] == 'S'), 'Original'].sum()) / self.pl
        
        self.cambial_final = (pos.loc[(pos[parametro] == 'S'), 'FinanceiroFinal'].sum()) / self.pl
        
        self.cambial_futuro = (pos.loc[(pos[parametro] == 'S'), 'FinanceiroFuturo'].sum()) / self.pl
        
        dicio = {'antes': self.cambial_antes, 'final': self.cambial_final, 'futuro': self.cambial_futuro}
        return dicio
    
    def cambial_10(self):
        alocacao = self.__cambial_exposicao__()
        limites = {'max': 0.10, 'max_alerta': 0.065}
        self.__verifica_enquadramento__(nome_limite='Cambial até 10%', alocacao=alocacao, limites=limites)
    
    def cambial_20(self):
        alocacao = self.__cambial_exposicao__()
        limites = {'max': 0.20, 'max_alerta': 0.14}
        self.__verifica_enquadramento__(nome_limite='Cambial até 20%', alocacao=alocacao, limites=limites) 
    
    def cambial_40(self):
        alocacao = self.__cambial_exposicao__()
        limites = {'max': 0.40, 'max_alerta': 0.33}
        self.__verifica_enquadramento__(nome_limite='Cambial até 40%', alocacao=alocacao, limites=limites)
        
    def __emissor_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
        pos['GuidEmissor'] = np.where(pos['TipoProduto'].apply(lambda x: "COTAS" in x) == True, pos.index, pos['GuidEmissor'])
        
        temp_pos_antes = pos.groupby('GuidEmissor')['Original'].sum() / self.pl
        df_temp_pos_antes = pd.DataFrame({'GuidProduto':temp_pos_antes.index, 'Tamanho':temp_pos_antes.values})
        self.emissor_antes = df_temp_pos_antes['Tamanho'].max()
        
        temp_pos_final = pos.groupby('GuidEmissor')['FinanceiroFinal'].sum() / self.pl
        df_temp_pos_final = pd.DataFrame({'GuidProduto':temp_pos_final.index, 'Tamanho':temp_pos_final.values})
        self.emissor_final = df_temp_pos_final['Tamanho'].max()
        
        temp_pos_futuro = pos.groupby('GuidEmissor')['FinanceiroFuturo'].sum() / self.pl
        df_temp_pos_futuro = pd.DataFrame({'GuidProduto':temp_pos_futuro.index, 'Tamanho':temp_pos_futuro.values})
        self.emissor_futuro = df_temp_pos_futuro['Tamanho'].max()
        
        dicio = {'antes': self.emissor_antes, 'final': self.emissor_final, 'futuro': self.emissor_futuro}
        return dicio
    
    def emissor_33(self):
        alocacao = self.__emissor_exposicao__()
        limites = {'max': 0.33, 'max_alerta': 0.22}
        self.__verifica_enquadramento__(nome_limite='Emissor até 33%', alocacao=alocacao, limites=limites)
        
    def emissor_5(self):
        alocacao = self.__emissor_exposicao__()
        limites = {'max': 0.05, 'max_alerta': 0.03}
        self.__verifica_enquadramento__(nome_limite='Emissor até 10%', alocacao=alocacao, limites=limites)
        
    ### Emissor fundo ###
    def __emissor_fundo_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
        pos = pos[pos['TipoProduto'].isin(["COTAS"])]
        pos['GuidEmissor'] = np.where(pos['TipoProduto'].apply(lambda x: "COTAS" in x) == True, pos.index, pos['GuidEmissor'])
        
        temp_pos_antes = pos.groupby('GuidEmissor')['Original'].sum() / self.pl
        df_temp_pos_antes = pd.DataFrame({'GuidProduto':temp_pos_antes.index, 'Tamanho':temp_pos_antes.values})
        self.emissor_antes = df_temp_pos_antes['Tamanho'].max()
        
        temp_pos_final = pos.groupby('GuidEmissor')['FinanceiroFinal'].sum() / self.pl
        df_temp_pos_final = pd.DataFrame({'GuidProduto':temp_pos_final.index, 'Tamanho':temp_pos_final.values})
        self.emissor_final = df_temp_pos_final['Tamanho'].max()
        
        temp_pos_futuro = pos.groupby('GuidEmissor')['FinanceiroFuturo'].sum() / self.pl
        df_temp_pos_futuro = pd.DataFrame({'GuidProduto':temp_pos_futuro.index, 'Tamanho':temp_pos_futuro.values})
        self.emissor_futuro = df_temp_pos_futuro['Tamanho'].max()
        
        dicio = {'antes': self.emissor_antes, 'final': self.emissor_final, 'futuro': self.emissor_futuro}
        return dicio
    
    def emissor_fundos_49(self):
        alocacao = self.__emissor_fundo_exposicao__()
        limites = {'max': 0.49, 'max_alerta': 0.40}
        self.__verifica_enquadramento__(nome_limite='Emissor até 49%', alocacao=alocacao, limites=limites)
        
    def __emissor_ex_FIA_exposicao__(self):
        pos = self.df_carteira_simulada.copy()
        pos = pos[~pos['DicioCad'].isin(["'FIA': True"])]
        #pos.loc[pos['GuidEmissor'].isnull(),'GuidEmissor'] = 'None'
        pos = pos.replace({np.nan:'None'})
        pos['GuidEmissor'] = np.where(pos['TipoProduto'].apply(lambda x: "COTAS" in x) == True, pos.index, pos['GuidEmissor'])
        
        temp_pos_antes = pos.groupby('GuidEmissor')['Original'].sum() / self.pl
        df_temp_pos_antes = pd.DataFrame({'GuidProduto':temp_pos_antes.index, 'Tamanho':temp_pos_antes.values})
        self.emissor_antes = df_temp_pos_antes['Tamanho'].max()
        
        temp_pos_final = pos.groupby('GuidEmissor')['FinanceiroFinal'].sum() / self.pl
        df_temp_pos_final = pd.DataFrame({'GuidProduto':temp_pos_final.index, 'Tamanho':temp_pos_final.values})
        self.emissor_final = df_temp_pos_final['Tamanho'].max()
        
        temp_pos_futuro = pos.groupby('GuidEmissor')['FinanceiroFuturo'].sum() / self.pl
        df_temp_pos_futuro = pd.DataFrame({'GuidProduto':temp_pos_futuro.index, 'Tamanho':temp_pos_futuro.values})
        self.emissor_futuro = df_temp_pos_futuro['Tamanho'].max()
        
        dicio = {'antes': self.emissor_antes, 'final': self.emissor_final, 'futuro': self.emissor_futuro}
        return dicio
    
    def emissor_ex_FIA_10(self):
        alocacao = self.__emissor_ex_FIA_exposicao__()
        limites = {'max': 0.10, 'max_alerta': 0.07}
        self.__verifica_enquadramento__(nome_limite='Emissor ex FIA até 10%', alocacao=alocacao, limites=limites)
        
    def __risk_score_calculo__(self):
        df = self.df_carteira_simulada.copy()        
        if not df.empty:           
            df['RiskScore'] = df['RiskScore'].fillna(5)
            # Transforma pesos em módulo
            colunas = ['Original', 'FinanceiroFinal', 'FinanceiroFuturo']
            for col in colunas:
                df[col] = df.apply(lambda x: abs(x[col]) * x['RiskScore'] / self.patr[col], axis=1)
        
            self.memoria_calculo['RiskScore_Antes'] = df['Original'].sum()
            self.memoria_calculo['RiskScore_Final'] = df['FinanceiroFinal'].sum() 
            self.memoria_calculo['RiskScore_Futuro'] = df['FinanceiroFuturo'].sum() 
        else:
            self.memoria_calculo['RiskScore_Antes'] = 0
            self.memoria_calculo['RiskScore_Final'] = 0
            self.memoria_calculo['RiskScore_Futuro'] = 0
        dicio = {'antes': self.memoria_calculo['RiskScore_Antes'], 'final': self.memoria_calculo['RiskScore_Final'], 'futuro': self.memoria_calculo['RiskScore_Futuro']}        
        return dicio
    
    def __exposicao_adm_emissor__(self):
        dicio = {}
        if not self.obj_portfolio.administrador_guidemissor:
            return dicio
        df = self.df_carteira_simulada.copy()        
        if not df.empty:
            df = df[df['GuidEmissor'] == self.obj_portfolio.administrador_guidemissor]
            self.memoria_calculo['AdmEmissor_Antes'] = df['Original'].sum() / self.pl
            self.memoria_calculo['AdmEmissor_Final'] = df['FinanceiroFinal'].sum() / self.pl
            self.memoria_calculo['AdmEmissor_Futuro'] = df['FinanceiroFuturo'].sum() / self.pl
            dicio = {'antes': self.memoria_calculo['AdmEmissor_Antes'], 'final': self.memoria_calculo['AdmEmissor_Final'], 'futuro': self.memoria_calculo['AdmEmissor_Futuro']}        
        return dicio
    
    def exposicao_adm_emissor_10(self):
        alocacao = self.__exposicao_adm_emissor__()
        limites = {'max': 0.10, 'max_alerta': 0.07}
        self.__verifica_enquadramento__(nome_limite='EmissorAdm até 10%', alocacao=alocacao, limites=limites)
        
    def exposicao_adm_emissor_20(self):
        alocacao = self.__exposicao_adm_emissor__()
        limites = {'max': 0.20, 'max_alerta': 0.14}
        self.__verifica_enquadramento__(nome_limite='EmissorAdm até 20%', alocacao=alocacao, limites=limites)
    
    def exposicao_adm_emissor_40(self):
        alocacao = self.__exposicao_adm_emissor__()
        limites = {'max': 0.40, 'max_alerta': 0.34}
        self.__verifica_enquadramento__(nome_limite='EmissorAdm até 40%', alocacao=alocacao, limites=limites)
    
    def exposicao_adm_emissor_0(self):
        alocacao = self.__exposicao_adm_emissor__()
        limites = {'max': 0, 'max_alerta': 0}
        self.__verifica_enquadramento__(nome_limite='EmissorAdm até 0%', alocacao=alocacao, limites=limites)
        
    def __exposicao_adm_emissor_ex_cotas__(self):
        dicio = {}
        if not self.obj_portfolio.administrador_guidemissor:
            return dicio
        df = self.df_carteira_simulada.copy()        
        if not df.empty:
            df = df[df['GuidEmissor'] == self.obj_portfolio.administrador_guidemissor]
            df = df[~df['TipoProduto'].isin(['COTAS'])]
            self.memoria_calculo['AdmEmissor_Antes'] = df['Original'].sum() / self.pl
            self.memoria_calculo['AdmEmissor_Final'] = df['FinanceiroFinal'].sum() / self.pl
            self.memoria_calculo['AdmEmissor_Futuro'] = df['FinanceiroFuturo'].sum() / self.pl
            dicio = {'antes': self.memoria_calculo['AdmEmissor_Antes'], 'final': self.memoria_calculo['AdmEmissor_Final'], 'futuro': self.memoria_calculo['AdmEmissor_Futuro']}        
        return dicio
    
    def exposicao_adm_emissor_ex_cotas_20(self):
        alocacao = self.__exposicao_adm_emissor_ex_cotas__()
        limites = {'max': 0.20, 'max_alerta': 0.17}
        self.__verifica_enquadramento__(nome_limite='EmissorAdm ex cota até 20%', alocacao=alocacao, limites=limites)
        
    def exposicao_adm_emissor_ex_cotas_50(self):
        alocacao = self.__exposicao_adm_emissor_ex_cotas__()
        limites = {'max': 0.50, 'max_alerta': 0.43}
        self.__verifica_enquadramento__(nome_limite='EmissorAdm ex cota até 50%', alocacao=alocacao, limites=limites)
        
    def exposicao_adm_emissor_ex_cotas_80(self):
        alocacao = self.__exposicao_adm_emissor_ex_cotas__()
        limites = {'max': 0.80, 'max_alerta': 0.70}
        self.__verifica_enquadramento__(nome_limite='EmissorAdm ex cota até 80%', alocacao=alocacao, limites=limites)
    
    def __emissor_exposicao_extpf__(self):
        pos = self.df_carteira_simulada.copy()
        pos = pos[~pos['GuidEmissor'].isin(["3ca7c6d6-c603-e011-82b1-d8d385b9752e"])]
        pos = pos[~pos['NomeContaCRM'].str.contains('FIF')]
        #pos.loc[pos['GuidEmissor'].isnull(),'GuidEmissor'] = 'None'
        pos = pos.replace({np.nan:'None'})
        pos['GuidEmissor'] = np.where(pos['TipoProduto'].apply(lambda x: "COTAS" in x) == True, pos.index, pos['GuidEmissor'])
        
        temp_pos_antes = pos.groupby('GuidEmissor')['Original'].sum() / self.pl
        df_temp_pos_antes = pd.DataFrame({'GuidProduto':temp_pos_antes.index, 'Tamanho':temp_pos_antes.values})
        self.emissor_antes = df_temp_pos_antes['Tamanho'].max()
        
        temp_pos_final = pos.groupby('GuidEmissor')['FinanceiroFinal'].sum() / self.pl
        df_temp_pos_final = pd.DataFrame({'GuidProduto':temp_pos_final.index, 'Tamanho':temp_pos_final.values})
        self.emissor_final = df_temp_pos_final['Tamanho'].max()
        
        temp_pos_futuro = pos.groupby('GuidEmissor')['FinanceiroFuturo'].sum() / self.pl
        df_temp_pos_futuro = pd.DataFrame({'GuidProduto':temp_pos_futuro.index, 'Tamanho':temp_pos_futuro.values})
        self.emissor_futuro = df_temp_pos_futuro['Tamanho'].max()
        
        dicio = {'antes': self.emissor_antes, 'final': self.emissor_final, 'futuro': self.emissor_futuro}
        return dicio
    
    def emissor_ex_TPF_10(self):
        alocacao = self.__emissor_exposicao_extpf__()
        limites = {'max': 0.10, 'max_alerta': 0.07}
        self.__verifica_enquadramento__(nome_limite='Emissor ex TPF até 10%', alocacao=alocacao, limites=limites)
    
    def controle_publico_alvo_pf(self):
        """
        Função desativada, em favor de controle criado no sys_documentacao

        Returns
        -------
        None.

        """
        if not self.tipo_investidor:
            return
        if self.tipo_investidor == 'profissional':
            return
        dicio = {'geral': 0, 'qualificado': 1, 'profissional': 2}
        df = self.df_carteira_simulada.copy()
        df.insert(len(df.columns), 'PubAlvoNum', [0] *  len(df))
        df['PubAlvoNum']  = df.apply(lambda x: dicio[str(x['PubAlvo']).lower()] if str(x['PubAlvo']).lower() in dicio.keys() else 0, axis=1)
        limite = dicio[self.tipo_investidor]
        
        for idx, row in df.iterrows():            
            if row['PubAlvoNum'] > 0:
                if row['NomeProduto']:
                    nome_lim = f"{row['PubAlvo']}: {row['NomeProduto']}"                                
                else:
                    nome_lim = f"{row['PubAlvo']}: {idx}"            
                self.memoria_calculo[f'{nome_lim}_Anterior'] = row['Original']
                self.memoria_calculo[f'{nome_lim}_Final'] = row['FinanceiroFinal']
                self.memoria_calculo[f'{nome_lim}_Futuro'] = row['FinanceiroFuturo']
                
                if row['Original'] == 0:
                    antes = 0
                else:
                    antes = row['PubAlvoNum']
                if row['FinanceiroFinal'] == 0:
                    atual = 0
                else:
                    atual = row['PubAlvoNum']
                if row['FinanceiroFuturo'] == 0:
                    futuro = 0
                else:
                    futuro = row['PubAlvoNum']
                
                alocacao = {'antes': antes, 'final': atual, 'futuro': atual}  
                limites = {'max': limite, 'max_alerta': limite}
                self.__verifica_enquadramento__(nome_limite=nome_lim, alocacao=alocacao, limites=limites)                
                
    def fundo_minimos_movimentacao(self):
        self.__verifica_minimo_movimentacao__()
    
    def titularidade_minimos_movimentacao(self):
        self.__verifica_minimo_movimentacao__()
    
    def __verifica_minimo_movimentacao__(self):
        df = self.df_carteira_simulada.copy()
        df = df[(df['Movimentacao']!=0) & (~df['TipoProduto'].isin(['FUT', 'OPT']))]
        if df.empty:
            return
        indice = df[df['TipoProduto'].isin(['FUNDO','COTAS', 'Fundo'])].index        
        cols = ['MovMinIni', 'MovMinMov', 'MovMinSaldo']
        for col in cols:
            df.loc[indice, col].fillna(1000)
            df[col] = df[col].fillna(0)
        for idx, row in df.iterrows():
            if row['NomeProduto']:
                nome_lim = row['NomeProduto']
            else:
                nome_lim = idx   
            # Aplicação Inicial
            if row['Movimentacao'] > 0 and row['MovMinIni'] > 0 and row['Original'] == 0:
                limites = {'min': row['MovMinIni'], 'min_alerta': row['MovMinIni']}
                alocacao = {'antes': 0, 'final': abs(row['Movimentacao']), 'futuro': abs(row['Movimentacao'])}
                self.__verifica_enquadramento__(nome_limite=f"[AplicIni]:{nome_lim}", alocacao=alocacao, limites=limites)
            # Limite de movimentação
            elif row['MovMinMov'] > 0:
                limites = {'min': row['MovMinMov'], 'min_alerta': row['MovMinMov']+1}
                alocacao = {'antes': row['MovMinMov'], 'final': abs(row['Movimentacao']), 'futuro': abs(row['Movimentacao'])}
                self.__verifica_enquadramento__(nome_limite=f"[MovMin]:{nome_lim}", alocacao=alocacao, limites=limites)
            # Saldo Mínimo
            if row['Movimentacao'] < 0:
                if row['FinanceiroFinal'] != 0:
                    limites = {'min': row['MovMinSaldo'], 'min_alerta': row['MovMinSaldo']}
                    alocacao = {'antes': 0, 'final': row['FinanceiroFinal'], 'futuro': row['FinanceiroFinal']}
                    self.__verifica_enquadramento__(nome_limite=f"[SaldoMin]:{nome_lim}", alocacao=alocacao, limites=limites)
                if row['FinanceiroFuturo'] != 0:
                    limites = {'min': row['MovMinSaldo'], 'min_alerta': row['MovMinSaldo']}
                    alocacao = {'antes': row['MovMinSaldo'], 'final': row['FinanceiroFuturo'], 'futuro': row['FinanceiroFuturo']}
                    self.__verifica_enquadramento__(nome_limite=f"[SaldoMin]:{nome_lim}", alocacao=alocacao, limites=limites)
    
    def fundos_liquidacao_distrato(self):        
        # Se não há compras, não é preciso verificar
        if self.__count_compras__() == 0:
            return
        
        if not self.obj_portfolio.distrato and not self.obj_portfolio.em_liquidacao:
            self.memoria_calculo['Ongoing'] = 1
            return
        
        df = self.df_carteira_simulada.copy()
        df = df[df['Movimentacao']>0]
        
        if self.obj_portfolio.distrato:
            # Objetivo é evitar compras que não sejam de fundos de liquidez        
            self.memoria_calculo['Distrato'] = 1
            nome_limite = 'Distrato'
            df.insert(0, 'Restrito', [1] * len(df))
            lista_comprar = ['LFT', 'NTN-B', 'ACAO', 'BOLSA', 'COMP']
            indice = df[df['TipoProduto'].isin(lista_comprar)].index
            df.loc[indice, 'Restrito'] = 0
            
            lista_subclasse = ['RF Pos Liquidez']
            indice = df[df['SubClasse'].isin(lista_subclasse)].index
            df.loc[indice, 'Restrito'] = 0
            
        elif self.obj_portfolio.em_liquidacao:
            # Objetivo é evitar compras que sejam ilíquidas
            self.memoria_calculo['Liquidar'] = 1
            nome_limite = 'Em liquidação'
            df.insert(0, 'Restrito', [0] * len(df))
            # Tipos de produto ilíquido
            lista_tpprod = ['FIDC', 'FIP', 'COE', 'LF', 'LFS', 'CRI', 'CRA', 'DEB', 'CDB']
            indice = df[df['TipoProduto'].isin(lista_tpprod)].index
            df.loc[indice, 'Restrito'] = 1
            # Fundos de condomnínio fechado
            lista_cond = ['Fechado', 'Indeterminado']
            indice = df[df['FormaCondominio'].isin(lista_cond)].index
            df.loc[indice, 'Restrito'] = 1
            # Resgates estranhos
            colunas = ['ResgPedido', 'ResgCot']
            palavras_chave = ['A', 'Q', 'T', 'M']
            for col in colunas:
                df[col] = df[col].fillna('-')
                for palavra in palavras_chave:
                    cont = len(df[df[col].str.contains(palavra)])
                    if cont > 0:
                        indice = df[df[col].str.contains(palavra)].index
                        df.loc[indice, 'Restrito'] = 1
        
        # Faz o teste
        df = df[df['Restrito']==1]            
        limites = {'max': 0.0, 'max_alerta': 0.00}
        alocacao = {'antes': df['Original'].sum(), 'final': df['FinanceiroFinal'].sum(), 'futuro': df['FinanceiroFuturo'].sum()}    
        self.__verifica_enquadramento__(nome_limite=nome_limite, alocacao=alocacao, limites=limites)                                                
                

class RegrasTitularidade(RegrasFundo):
    
    def __init__(self, df_carteira_simulada, pl, limite_risk_score, limites_credito, limites_credito_exc, df_ordens:pd.DataFrame, distrato:bool=False, espolio:bool=False, tipo_investidor=None, pessoa_fisica:bool=False, obj_portfolio=None, homologacao=False):
        super().__init__(df_carteira_simulada=df_carteira_simulada, pl=pl, limite_risk_score=limite_risk_score, limites_credito=limites_credito, limites_credito_exc=limites_credito_exc,
                         df_ordens=df_ordens, multiplicador_credito=1, homologacao=homologacao, tipo_investidor=tipo_investidor, obj_portfolio=obj_portfolio)
        self.patr = {'Original': df_carteira_simulada['Original'].sum(), 'FinanceiroFinal': df_carteira_simulada['FinanceiroFinal'].sum(),
                     'FinanceiroFuturo': df_carteira_simulada['FinanceiroFuturo'].sum()}        
        self.distrato = distrato
        self.espolio = espolio        
        self.pessoa_fisica = pessoa_fisica

    def ordem_espolio(self):
        if not self.espolio:
            self.memoria_calculo['Espolio'] = 0
            return
        self.memoria_calculo['Espolio'] = 1
        movs = self.__count_compras__() + self.__count_vendas__()
        alocacao = {'antes': 0, 'final': movs, 'futuro': movs}    
        limites = {'max': 0.5, 'max_alerta': 0.01}

        self.__verifica_enquadramento__(nome_limite='Espólio', alocacao=alocacao, limites=limites)
    
    def ordem_distrato(self):
        # Objetivo é evitar compras que não sejam de fundos de liquidez        
        if not self.distrato:
            self.memoria_calculo['Distrato'] = 0
            return
        self.memoria_calculo['Distrato'] = 1
        if self.__count_compras__() == 0:
            return
        df = self.df_carteira_simulada.copy()
        df['TipoVeto'] = df['TipoVeto'].fillna(0)
        df['TipoVeto'] = df['TipoVeto'].apply(lambda x: 60 if x==60 else 0)
        df = df[(df['TipoVeto'] == 0) & (df['Movimentacao'] > 0)]
        if len(df) == 0:
            contagem = 0
        else:
            df_cont = df.groupby('TipoVeto').count()
            contagem = df_cont.loc[0,'FinanceiroFinal']
        limites = {'max': 0.5, 'max_alerta': 0.01}
        alocacao = {'antes': 0, 'final': contagem, 'futuro': contagem}    
        self.__verifica_enquadramento__(nome_limite='Distrato', alocacao=alocacao, limites=limites)
        
    def ativos_apenas_pf(self):
        if self.pessoa_fisica:
            return
        # Verifica se carteira investe em ativos isentos
        tx_selic = 10
        df = self.df_carteira_simulada.copy()
        for idx, row in df.iterrows():
            if row['TipoProduto'] in ['LCA', 'LCI', 'LIG', 'CRI', 'CRA', 'DEBI'] and row['Indexador'] == 'CDI':
                taxa_p = tx_selic * row['Taxa'] / 100 + row['Coupon'] /100
                if taxa_p < tx_selic:
                    if row['NomeProduto']:
                        nome_lim = f"[Isento<CDI]: {row['NomeProduto']}"    
                    else:
                        nome_lim = f"[Isento<CDI]: {idx}"                                    
                    limites = {'max': 0.0, 'max_alerta': 0.00}
                    alocacao = {'antes': row['Original'] / self.pl, 'final': row['FinanceiroFinal'] / self.pl, 'futuro': row['FinanceiroFuturo']/ self.pl}    
                    self.__verifica_enquadramento__(nome_limite=nome_lim, alocacao=alocacao, limites=limites)


class PreTrade:
    
    def __init__(self, data_pos=None, pos_trade:bool=False, base_dm1=None, homologacao:bool=False):
        self.data_pos = data_pos
        self.homologacao = homologacao
        self.mensagem = None 
        self.pos_trade = pos_trade
        self.memoria_calculo = []
        if base_dm1:
            self.dm1 = base_dm1
        else:
            self.dm1 = PosicaoDm1Pickle(homologacao=homologacao)  
        self.bol = Boletador(homologacao=homologacao)                              
        self.crm = Crm(homologacao=homologacao)
        self.produtos = self.dm1.lista_produtos_bol()
        self.cm = CreditManagement(homologacao=homologacao)
        self.limites_credito = self.cm.limites_credito().set_index('GuidLimite')
        self.limites_credito_exc = self.cm.limites_excecoes_all()
        self.limites_port_individual = self.bol.regras_portfolio_individual()
        self.dicionario_retorno = {1: 'Aprovado', 2: 'Alerta', 3: 'Recusado'}
        self.verificacao = pd.DataFrame()
    
    # Motor de cálculo de carteira pós ordens 
    def obter_carteira_simulada(self, df_posicao_dm1, ordens=pd.DataFrame(), id_tipo_portfolio=None, guid_produto_liquidacao:str=None):
        
        # Dex: 26/ago/24: Função foi movida para funcoes.py para poder ser utilizada em outros módutos
        return obter_carteira_simulada(df_posicao_dm1=df_posicao_dm1, cad_produtos=self.produtos, ordens=ordens, id_tipo_portfolio=id_tipo_portfolio,
                                       guid_produto_liquidacao=guid_produto_liquidacao, pos_trade=self.pos_trade, base_dm1=self.dm1, base_crm=self.crm)
        
    
    # Verificações de enquadramento para cada tipo de produto
    def verificar_enquadramento(self, id_tipo_portfolio, guid_portfolio, df_ordens:pd.DataFrame=pd.DataFrame(), obj_portfolio=None) -> int:
        """
        Função que recebe o portfolio e as ordens a aprovar

        Parameters
        ----------
        id_tipo_portfolio : 1 = Fundo, 2 = Supercarteira, 3 = Titularidade, 4 = Offshore
        guid_portfolio : Guid do Portfolio
        df_ordens : pd.DataFrame(), optional
            DESCRIPTION. Dataframe com as ordens a verificar no formato da tabela BL_Ordem

        Outras saidas
        -------
        Definir mensagem de texto em self.mensagem com resultado descritivo do enquadramento
        
        Returns
        -------
        int
            1: Enquadramento aprovado
            2: Enquadramento aprovado com alerta
            3: Enquadramento reprovado

        """
        lista_port = monta_lista_port(id_tipo_portfolio, guid_portfolio, obj_portfolio, base_dm1=self.dm1, data_pos=self.data_pos)  
        
        # Dados para resolver
        for port in lista_port:
            self.mensagem = 'Sem regras para verificar'
            regras_port = port.regras_enquadramento() # Puxa regras relacionadas ao portfólio
            # Se não houver regras, retorna enquadrado
            if regras_port.empty:            
                self.mensagem = 'Sem regras para o fundo'
                return 1
            if len(regras_port) - regras_port['FuncaoPython'].isna().sum() == 0:
                self.mensagem = 'Regras sem função python'
                return 1
            
            # Ajustar posicao da carteira para fluxo de ordens
            if df_ordens.empty:
                df_ordens_sim = df_ordens
            else:
                df_ordens_sim = df_ordens[df_ordens['GuidPortfolio']==port.guid].copy()
            posicao_simulada = self.obter_carteira_simulada(port.pos, ordens=df_ordens_sim, id_tipo_portfolio=port.tipo_portfolio
                                                            , guid_produto_liquidacao=port.produto_liquidacao_guid)               
            
            # (MAIN LOGIC) Passar iterativamente nas regras
            if port.tipo_portfolio == 1:    
                regras = RegrasFundo(posicao_simulada, port.pl_est, limite_risk_score=port.risk_score_limite, multiplicador_credito=port.limite_credito_alavancagem,
                                     limites_credito=self.limites_credito, limites_credito_exc=self.limites_credito_exc, df_ordens=df_ordens, homologacao=self.homologacao,
                                     obj_portfolio=port) # Cria classe de verificação para o portfólio
            elif port.tipo_portfolio == 3:    
                regras = RegrasTitularidade(posicao_simulada, port.pl_est, port.risk_score_limite(), limites_credito=self.limites_credito, 
                                            limites_credito_exc=self.limites_credito_exc, df_ordens=df_ordens, distrato=port.distrato, 
                                            espolio=port.espolio, tipo_investidor=port.tipo_investidor, pessoa_fisica=port.pf, obj_portfolio=port,
                                            homologacao=self.homologacao) # Cria classe de verificação para o portfólio
            regras.limites_port_individual = self.limites_port_individual
            for idx, row in regras_port.iterrows(): # Executa todas que tiverem função em python
                if row['FuncaoPython']:                
                    funcao = getattr(regras, row['FuncaoPython'])
                    exec_func = RunWErrorCheck(funcao=funcao, mensagem=row['FuncaoPython'])                
                    exec_func.executar()
            # salva as regras
            port.memoria_calculo = regras.memoria_calculo
            port.verificador = regras.verificador
        
        # for item in regras.verificador.keys():
        #    print(item, regras.verificador[item])        
        # Vários portfolios
        num_regras = 0
        self.port_erro_nome = None
        for port in lista_port:            
            self.memoria_calculo = port.memoria_calculo
            df_verificar = pd.Series(port.verificador)
            self.verificacao = df_verificar
            if any(df_verificar == 3):
                aprovacao = 3
                erros = df_verificar[df_verificar == 3] 
                self.mensagem = ','.join(erros.index) + f" ({port.nome})"
                self.port_erro_nome = port.nome
                break
            elif any(df_verificar == 2):
                aprovacao = 2
                erros = df_verificar[df_verificar == 2]
                self.mensagem = ','.join(erros.index)
                num_regras += len(df_verificar)
            else:
                aprovacao = 1
                num_regras += len(df_verificar)
        if aprovacao ==1 or (len(lista_port)>1 and aprovacao == 2):
            self.mensagem = f'Sim ({num_regras} regras)'               
        
        return aprovacao
    
    def simulacao(self, boletas:pd.DataFrame) -> dict:
        """
        Pega um grupo de boletas e executa as verificações de documentação e pre-trade

        Parameters
        ----------
        boletas : pd.DataFrame
            dataframe com as boletas.

        Returns
        -------
        dict
            Dicionario com os campos: Titulo, PreTrade_Msg, PreTrade_MemoriaCalc e Verificacoes.

        """        
        if 'Titularidade' in boletas.columns:
            portfolios = boletas[['GuidPortfolio', 'IdTipoPortfolio', 'Titularidade']].copy().drop_duplicates().set_index('GuidPortfolio')
            campo = 'Titularidade' 
        else:
            portfolios = boletas[['GuidPortfolio', 'IdTipoPortfolio', 'ContaCRM']].copy().drop_duplicates().set_index('GuidPortfolio')
            campo = 'ContaCRM' 
        lista = []
        resultado = 1
        resultado_doc = True
        txt_doc = 'OK'
        adoc = AnaliseDocumentacao()
        for idx, row in portfolios.iterrows():
            df_ordens = boletas[boletas['GuidPortfolio']==idx].copy()
            df_ordens.insert(len(df_ordens.columns), 'ValorAdj', [0] * len(df_ordens))
            df_ordens.insert(len(df_ordens.columns), 'IdOrdem', df_ordens.index)
            
            for idx2, row2 in df_ordens.iterrows():
                sinal = 1
                if row2['TipoMov'] == 'V':
                    sinal = -1
                if row2['Financeiro']:
                    df_ordens.loc[idx2, 'ValorAdj'] = sinal * row2['Financeiro']
                else:
                    df_ordens.loc[idx2, 'ValorAdj'] = sinal * row2['Quantidade']
            # Pré-trade
            teste = self.verificar_enquadramento(id_tipo_portfolio=row['IdTipoPortfolio'], 
                                 guid_portfolio=idx, 
                                 df_ordens=df_ordens)
            if teste > resultado:
                resultado = teste
            lista.append(self.memoria_calculo)
            
            # Análise documentacao            
            df_ordens['IdStatusOrdem'] = 72
            ad_dicio, ad_mensagem = adoc.verificar_documentacao(id_tipo_portfolio=row['IdTipoPortfolio'], guid_portfolio=idx, 
                                                                df_ordens=df_ordens, quando=datetime.datetime.now(), pre_teste=True)
            for linha in ad_dicio:
                if linha['IdStatusOrdem'] != 75:
                    txt_doc = ad_mensagem                
            df_doc = pd.DataFrame(ad_dicio)
            df_doc = pd.merge(left=df_ordens, right=df_doc, left_index=True, right_on='IdOrdem')[['AtivoNome', 'Comentario']].drop_duplicates()
            df_doc = pd.concat([pd.DataFrame([{'AtivoNome': '*** Docs', 'Comentario': ' ***'}]), df_doc]).set_index('AtivoNome')
            df_doc.columns = ['Resultado']
        
        # Prepara retorno das informações
        dicionario = {}
        dicionario['Titulo'] = 'Pré-Trade: ' + self.dicionario_retorno[resultado] + ', Docs: ' + txt_doc
        dicionario['PreTrade_Msg'] = self.mensagem
        if len(portfolios) == 1:
            dicionario['PreTrade_MemoriaCalc'] = pd.DataFrame(lista, index=['Valor']).T
        else:
            listinha = []
            for i in range(0, len(lista)):
                item = lista[i]
                df_temporario = pd.DataFrame(item, index=['Valor']).T
                df_temporario.insert(len(df_temporario.columns), 'Port', [portfolios.iloc[i][campo]]*len(df_temporario))
                listinha.append(df_temporario)
            dicionario['PreTrade_MemoriaCalc'] = pd.concat(listinha)
        df_temp = pd.DataFrame(self.verificacao, columns=['Resultado'])
        df_temp['Resultado'] = df_temp['Resultado'].map(self.dicionario_retorno)
        dicionario['Verificacoes'] = pd.concat([df_temp, df_doc])
        return dicionario    
    

class AnaliseMandato:
    
    def __init__(self, data_pos=None, pos_trade:bool=False, base_dm1=None, classe_mandato=None, homologacao:bool=False):
        self.data_pos=data_pos
        self.homologacao = homologacao
        self.mensagem = None 
        self.pos_trade = pos_trade
        self.memoria_calculo = []
        if base_dm1:
            self.dm1 = base_dm1
        else:
            self.dm1 = PosicaoDm1Pickle(homologacao=homologacao) 
        self.mand = Mandato(buffer=True)
        self.bol = Boletador(homologacao=homologacao)                              
        self.crm = Crm(homologacao=homologacao)
        self.produtos = self.dm1.lista_produtos_bol()    
        
        self.dicionario_retorno = {1: 'Aprovado', 2: 'Alerta', 3: 'Recusado'}
        self.verificacao = pd.DataFrame()
        
        self.verificador = {}
        self.memoria_calculo = {}
    
    def __verifica_enquadramento__(self, nome_limite:str, alocacao:dict, limites:dict, nome_port:str=None):
        """
        Função que recebe dois dicionários, um com os limites, outro com a alocação atual e projetada e determina
        se é a ordem causa um desenquadramento, alerta ou está aprovava
        Copiada da classe RegrasFundo

        Parameters
        ----------
        nome_limite : str
            Texto para ajudar o usuário a entender qual limite foi violado.
        alocacao : dict
            dicionario com colunas 'antes', 'final' e 'futuro'.
        limites : dict
            dicionario de limites com campos 'max', 'max_alerta', 'min', 'min_alerta'.

        Returns
        -------
        None.

        """
        # Se função encontra desenquadramento (3), sai da função, se não continua rodando regras
        
        # Tolerância de 0.1%
        tolerancia = 0.001        
        nome_lim = nome_limite
        if nome_port:
            nome_lim = nome_port + ',' + nome_lim
        # Testa limites máximos
        if 'max' in limites.keys():
            # Verificação se está acima do máximo do limite
            if alocacao['final'] > limites['max'] and limites['max'] == 0 and alocacao['antes'] == 0:
                self.verificador[nome_lim] = 4
                return
            elif alocacao['final'] > limites['max'] and alocacao['antes'] <= limites['max']:
                # Novo desenquadramento
                self.verificador[nome_lim] = 3
                return
            elif alocacao['final'] > limites['max'] and alocacao['antes'] > limites['max']:
                # Já estava desenquadrado
                if abs(alocacao['final'] - alocacao['antes']) <= tolerancia:
                    # Não houve mudança na posição
                    self.verificador[nome_lim] = 1
                elif alocacao['final'] > alocacao['antes']:
                    # Desenquadramento piorou
                    self.verificador[nome_lim] = 3
                    return
                elif alocacao['final'] < alocacao['antes']:
                    # reduziu desenqudramento
                    self.verificador[nome_lim] = 2
                else:
                    self.verificador[nome_lim] = 1
            elif alocacao['final'] >= limites['max_alerta'] and (abs(alocacao['final'] - alocacao['antes']) >= tolerancia):
                self.verificador[nome_lim] = 2
            else:
                self.verificador[nome_lim] = 1
        
        # Testa limites mínimos
        if 'min' in limites.keys():
            # Verificação se está abaixo do mínimo do limite
            if alocacao['final'] < limites['min'] and alocacao['antes'] >= limites['min']:
                # Novo desenquadramento
                self.verificador[nome_lim] = 3
                return
            elif alocacao['final'] < limites['min'] and alocacao['antes'] < limites['min']:
                # Já estava desenquadrado
                if alocacao['final'] < alocacao['antes']:
                    # Desenquadramento piorou
                    self.verificador[nome_lim] = 3
                    return
                elif alocacao['final'] > alocacao['antes']:
                    # reduziu desenqudramento
                    self.verificador[nome_lim] = 2
                else:
                    # manteve desenquadramento
                    self.verificador[nome_lim] = 1
            elif alocacao['final'] <= limites['min_alerta'] and (abs(alocacao['final'] - alocacao['antes']) >= tolerancia):
                self.verificador[nome_lim] = 2
            else:
                self.verificador[nome_lim] = 1
    
    def verificar_enquadramento(self, id_tipo_portfolio=None, guid_portfolio=None, df_ordens:pd.DataFrame=pd.DataFrame(), obj_portfolio=None):
        # 1. Converte portfolios em lista
        lista_port = monta_lista_port(id_tipo_portfolio, guid_portfolio, obj_portfolio, base_dm1=self.dm1, classe_mandato=self.mand)
        
        # 2. Roda o enquadramento de cada portfólio
        resposta = 0
        for port in lista_port:
            port.mandato_mensagem = 'Sem regras para verificar'
            port.mandato_teste = 0
            port.verificador = {}
            regras_port = port.mandato_detalhe()
            # Se não houver regras, retorna enquadrado
            if regras_port.empty:            
                port.mandato_mensagem = 'Sem regras para o portfolio'
                port.mandato_teste = 1
            else:
                # Ajustar posicao da carteira para fluxo de ordens
                if df_ordens.empty:
                    df_ordens_sim = df_ordens
                else:
                    df_ordens_sim = df_ordens[df_ordens['GuidPortfolio']==port.guid].copy()
            
                posicao_simulada = obter_carteira_simulada(df_posicao_dm1=port.pos, ordens=df_ordens_sim, cad_produtos=self.produtos, id_tipo_portfolio=id_tipo_portfolio,
                                           guid_produto_liquidacao=port.produto_liquidacao_guid, pos_trade=self.pos_trade, base_dm1=self.dm1, base_crm=self.crm)
                posicao_simulada.reset_index(inplace=True)
                
                colunas_base = ['Original', 'FinanceiroFinal', 'FinanceiroFuturo', 'Movimentacao']
                for idx, row in regras_port.iterrows():
                    nome_lim = row['NomeCampo']
                    minimo = row['ValorMin']
                    if not minimo:
                        minimo = 0.0
                    maximo = row['ValorMax']
                    if not maximo:
                        maximo = 1.0
                    limites = {'min': minimo, 'min_alerta': minimo + 0.01, 'max': maximo, 'max_alerta': maximo - 0.01}
                    df = posicao_simulada[[nome_lim] + colunas_base].groupby(nome_lim).sum()                
                    if not df.empty:
                        linha = df.iloc[0]
                        alocacao = {'antes': linha['Original'] / port.pl_est, 'final': linha['FinanceiroFinal'] / port.pl_est, 'futuro': linha['FinanceiroFuturo']/ port.plfut_est}
                    else:
                        alocacao = {'antes': 0, 'final': 0, 'futuro': 0}    
                    self.__verifica_enquadramento__(nome_limite=row['NomeCampo'], alocacao=alocacao, limites=limites, nome_port=port.nome)
                    self.memoria_calculo[f"{nome_lim}_original"] = linha['Original'] / port.pl_est
                    self.memoria_calculo[f"{nome_lim}_final"] = linha['FinanceiroFinal'] / port.pl_est
                    self.memoria_calculo[f"{nome_lim}_futuro"] = linha['FinanceiroFuturo'] / port.plfut_est
                    port.verificador[nome_lim] = self.verificador[port.nome + ',' + nome_lim]
        
        # 3. Retorna
        num_regras = 0
        for port in lista_port:            
            self.memoria_calculo = port.memoria_calculo
            df_verificar = pd.Series(port.verificador)
            self.verificacao = df_verificar
            if any(df_verificar == 4):
                resposta = 4
                erros = df_verificar[df_verificar == 3] 
                self.mensagem = ','.join(erros.index) + f" ({port.nome})"
                self.port_erro_nome = port.nome
                break
            elif any(df_verificar == 3):
                resposta = 3
                erros = df_verificar[df_verificar == 3] 
                self.mensagem = ','.join(erros.index) + f" ({port.nome})"
                self.port_erro_nome = port.nome
                break
            elif any(df_verificar == 2):
                resposta = 2
                erros = df_verificar[df_verificar == 2]
                self.mensagem = ','.join(erros.index)
                num_regras += len(df_verificar)
            else:
                resposta = 1
                num_regras += len(df_verificar)
        if resposta == 1 or (len(lista_port)>1 and resposta==2):
            self.mensagem = f'Sim ({num_regras} regras)'               
        
        return resposta        
            

if __name__ == '__main__':    
    pre_trade = False  
    if pre_trade:
        print('*** PRE TRADE ***')    
        pret = PreTrade()  
        bol = Boletador()        
        
        df_ordens = bol.ordens_grupo(131190)    # id_ordem=aba.cells(lin,1).value
        # df_ordens = pd.read_excel('C:\\Users\\u53725\\Downloads\\teste.xlsx')
        guid_port = df_ordens.iloc[-1]['GuidPortfolio']
        
        # lista = [Fundo('_Acacias FIM CrPr IE'), Titularidade('Acacia')]
        teste = pret.verificar_enquadramento(id_tipo_portfolio= df_ordens.iloc[-1]['IdTipoPortfolio'], 
                                     guid_portfolio= guid_port, 
                                     df_ordens= df_ordens
                                     #, obj_portfolio=lista
                                     )
        #aba.cells(lin,3).value = teste
        #aba.cells(lin,4).value = pret.mensagem
        print(pret.memoria_calculo)
        print(teste)
        print(pret.mensagem)
        print(pret.verificacao)           
    else:
        #analise = AnaliseMandato()
        pret = PreTrade()  
        teste = pret.verificar_enquadramento(id_tipo_portfolio= 1, 
                                     guid_portfolio = 'b0a117dc-33a9-e211-a45e-000c29c91374')
        print(pret.memoria_calculo)
        print(pret.verificacao) 
                                     # df_ordens= df_ordens
                                     #, obj_portfolio=lista
                                     
        #print(analise.memoria_calculo)
        #print(teste)
        #print(analise.mensagem)
        #print(analise.verificacao)  
    pass

    
