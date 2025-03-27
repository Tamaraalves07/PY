import multiprocessing
import pandas as pd
import numpy as np
import math
from pandas import ExcelWriter
import datetime
import xlwings as xw
from dateutil.relativedelta import relativedelta
import os
import ambiente
from cronometro import Cronometro
from databases import PosicaoDm1, PosicaoDm1Pickle, Crm, Bawm, Liquidez
from objetos import Fundo, Supercarteira, Titularidade
from filemanager import FileMgmt
from funcoes_datas import FuncoesDatas
from funcoes import CalculosRiscoPort
from TE_Juros_Ativos import te
from delta import opção_delta,duration_modificada,risco_ativo



class RelatoriosExecucao:
    
    def __init__(self, verbose=False, homologacao=False):
        self.homologacao = homologacao
        self.verbose = verbose
    
    def relatorio_enquadramento(self):
        relogio = Cronometro(nome_processo='Relatórios de posição', verbose=self.verbose)
        
        relogio.marca_tempo('Carregando posições...')
        dm1 = PosicaoDm1(load_all_positions=True, homologacao=self.homologacao)
        
        relogio.marca_tempo('Gerando relatório por segmentos...')
        rel = RelatorioEnquadramento(rodar=True, filtro='Segmento', verbose=False, base_dm1=dm1)
        
        relogio.marca_tempo('Gerando relatório por officer...')
        rel = RelatorioEnquadramento(rodar=True, filtro='Officer', verbose=False, base_dm1=dm1)
        
        relogio.marca_tempo('Gerando relatório por DPM...')
        rel = RelatorioEnquadramento(rodar=True, filtro='DPM', verbose=False, base_dm1=dm1)
        
        relogio.concluido()

class RelatorioEnquadramento:
    
    def __init__(self, rodar=True, filtro='Segmento', verbose=False, base_dm1=None, homologacao=False):
        self.homologacao = homologacao
        self.dm1 = PosicaoDm1Pickle(homologacao=homologacao)
        self.liq = Liquidez(buffer=True, homologacao=homologacao)
        self.crm = Crm(homologacao=homologacao)
        self.num_cpu = multiprocessing.cpu_count()
        self.num_cpu = int(self.num_cpu * 1/2)
        self.relogio = Cronometro(nome_processo='Relatório posição', verbose=verbose)
        self.campos = ['GuidSupercarteira', 'NomeSuperCarteiraCRM', 'Segmento', 'Perfil', 'VetoPowerGestaoTatica', 'VetoPowerGestoresExternos', 'VetoPowerTitulosCredito', 'VetoPowerAcoes', 'Officer', 'OfficerUsuarioU', 'DPM', 'DPMUsuarioU']
        self.filtro = filtro
        self.risk = CalculosRiscoPort()

        if not filtro in self.campos:
            self.campos = self.campos + [filtro]
        self.classes = self.dm1.lista_classes().set_index('ClasseAt').sort_values('Ordem')
        if rodar:
            self.rodar()
    
    def rodar(self):
        self.relogio.marca_tempo('Buscando supercarteiras...')
        self.lista_sc = self.dm1.lista_super_carteiras(campos=self.campos)        
        self.lista_sc[self.filtro] = self.lista_sc[self.filtro].fillna('NA')
        if 'Officer' in self.lista_sc.columns: # Evita bugs malucos
            self.lista_sc = self.lista_sc[~self.lista_sc['Officer'].isin(['Administrador CRM'])]
        self.segmentos = list(self.lista_sc[self.filtro].unique())
        if 'NA' in self.segmentos:
            self.segmentos.remove('NA')
        if 'Fundos' in self.segmentos:        
            self.segmentos.remove('Fundos')
        self.segmentos.sort()
        # self.segmentos = self.segmentos[24:26]
        dicio = {}
        self.relogio.marca_tempo('Rodando segmentos...')        
        for segmento in self.segmentos:
            df = self.lista_sc[self.lista_sc[self.filtro]==segmento]
            # print(len(df))
            if len(df) != 0:
                self.relogio.marca_tempo(f'{segmento}: início')
                dicio[segmento] = self.__rodar_segmento__(segmento)
                self.relogio.marca_tempo(f'{segmento}: fim')
        self.relogio.marca_tempo('Segmentos concluídos.')
        
        self.__salvar_planilha__(dicio, self.relogio.tempos())
        
        self.relogio.marca_tempo('Relatório gerado.')
        self.relogio.concluido(True)
            
    def __salvar_planilha__(self, dicionario, df_relogio):        
        pasta = f'{ambiente.pasta_base_pm()}/06 - Relatório/Posição/'
        if not FileMgmt().file_exists(f"{pasta}nao_apagar.txt"):
            pasta = 'I:/3 - Asset Allocation/Posições/RelatPython/'
        data = self.dm1.banco.hoje()
        arquivo = f"posdia_{self.filtro}_{data.strftime('%Y%m%d')}"
        caminho = f"{pasta}{arquivo}.xlsx"
        with ExcelWriter(caminho, engine='xlsxwriter') as writer:
            wb = writer.book
            f1 = wb.add_format()
            f1.set_num_format('#,##0.00;(#,##0.00);-_)')
            for segmento in dicionario.keys():
                df = dicionario[segmento]
                if self.filtro.lower() in ['officer', 'dpm']:
                    nome = self.crm.usuario_email(nome=segmento)
                    if nome:
                        nome = nome.replace('@jbfo.com', '').replace('@gpsbr.com', '').replace('@juliusbaer.com', '')
                    else:
                        nome = segmento
                else:
                    nome = segmento
                try:
                    df.to_excel(writer, nome, index=False)
                    aba = writer.sheets[nome]
                    aba.set_column('A:A', 30, f1)
                    aba.set_column('D:Z', 10, f1)
                except:
                    print(f"Aba não escrita: {nome}")                          
            
            if self.filtro.lower() in ['officer', 'dpm']:
                if self.filtro.lower() == 'officer':   
                    df = self.dm1.lista_officers_controllers()                    
                else:
                    df = self.dm1.lista_dpms_deputies()
                    df.insert(len(df.columns), 'Nome', df['DPM'])
                # Salva aba na planilha com os respectivos controllers
                df.insert(len(df.columns), 'Email1', [None] * len(df))
                df.insert(len(df.columns), 'Email2', [None] * len(df))
                campo = 'Controller'
                if 'DeputyDPM' in df.columns:
                    campo = 'DeputyDPM'
                for idx, row in df.iterrows():
                    if row[self.filtro]:
                        nome = self.crm.usuario_email(nome=row[self.filtro])
                        if nome:
                            df.loc[idx, 'Email1'] = nome
                            nome = nome.replace('@jbfo.com', '').replace('@gpsbr.com', '').replace('@juliusbaer.com', '')
                            df.loc[idx, self.filtro] = nome
                    if row[campo]:
                        nome = self.crm.usuario_email(nome=row[campo])
                        df.loc[idx, 'Email2'] = nome
                df.to_excel(writer, 'Email', index=False)
            
            df_relogio.to_excel(writer, 'Relógio', index=False)
    
    @staticmethod
    def __numero_int__(valor):
        try:
            return int(valor)
        except:
            return 0
    
    def __rodar_segmento__(self, nome_segmento):
        print(f'____Rodando {self.filtro}: {nome_segmento}')
        lista_sc = self.lista_sc[self.lista_sc[self.filtro]==nome_segmento].copy()
        lista = lista_sc['NomeSuperCarteiraCRM'].to_list()
        pool = multiprocessing.Pool(processes=self.num_cpu)
        records = pool.map(self.carrega_sc, lista)
        resultado = []
        campo = 'TE' 
        for sc in records:
            if not sc.posexp.empty:
                df = sc.asset_class_allocation()
                risco_vol = True
                minimo = 0
                maximo = 100
                risco_atu = None; risco_fut = None
                try:
                    df_risco = pd.DataFrame(self.risk.calcula_vols(df)).set_index('Campo')
                except:
                    df_risco = pd.DataFrame()
                if str(sc.perfil).lower()[:6] == 'flutua':
                    risco_vol = False
                    texto = sc.perfil
                    texto = texto[texto.find('('):].replace('(', '').replace(')', '').split(' ')
                    if texto[0] == 'acima':
                        minimo = float(texto[2])
                        maximo = minimo * 2
                    else:
                        minimo = float(texto[0])
                        maximo = float(texto[2])                    
                if not df_risco.empty:
                    campo = 'Pontos'
                    multi_risco = 1
                    if risco_vol:
                        campo = 'TE'                    
                        multi_risco = 100
                    try:
                        risco_atu = round(df_risco.loc['PesoFinal', campo] * multi_risco,2)
                        risco_fut = round(df_risco.loc['PesoFuturo', campo] * multi_risco,2)
                    except:
                        risco_atu = 0
                        risco_fut = 0
                    if campo == 'Pontos':
                        risco_atu = round(risco_atu,0)
                        risco_fut = round(risco_fut,0)
                    if not risco_atu:
                        risco_atu = 0
                    if not risco_fut:
                        risco_fut = 0
                df.drop('FinanceiroFinal', axis=1, inplace=True)
                df.drop('FinanceiroFuturo', axis=1, inplace=True)
                for col in df.columns:
                    if col != 'Classe':
                        try:
                            df[col] = df[col].fillna(0)
                            df[col] = df[col].apply(lambda x: int(x * 10000) / 100)
                        except:
                            print(f'Sys_RelExposicao\__rodar_segmento__: erro no apply, coluna {col}, supercarteira: {sc.nome}')
                df.insert(0, 'Supercarteira', [sc.nome] * len(df))
                df.insert(1, 'Perfil', [sc.perfil] * len(df))
                df.insert(2, 'VetoP', [sc.veto_power] * len(df))
                ordem = 3
                if str(sc.veto_power)[0] != 'S':
                    ordem = 2
                df.insert(2, 'OrdemD', [ordem] * len(df))
                df.insert(3, 'PL', [int(self.__numero_int__(sc.pl) / 10 ** 4) / 100] * len(df))
                df.insert(4, 'PLFut', [int(self.__numero_int__(sc.pl_fut) / 10 ** 4) / 100] * len(df))
                df.insert(4, 'LiqOp', [int(self.__numero_int__(sc.liquidez_operacional()) / 10 ** 4) / 100] * len(df))
                df.insert(3, 'Risco', [risco_atu] * len(df))
                df.insert(4, 'RiscoFut', [risco_fut] * len(df))
                if campo == 'Vol':
                    df.insert(5, 'Enq', [sc.asset_class_allocation_enquadrada_texto()] * len(df))
                else:
                    texto = ''
                    try:
                        if sc.distrato:
                            texto = 'Distrato'
                        elif minimo <= risco_atu <= maximo and minimo <= risco_fut <= maximo:
                            texto = 'OK'
                        elif risco_atu > maximo or risco_atu < minimo:
                            if minimo <= risco_fut <= maximo:
                                texto = 'Previsto'
                            else:
                                texto = 'Desenq.'
                    except:
                        texto = 'erro'
                    df.insert(5, 'Enq', [sc.asset_class_allocation_enquadrada_texto()] * len(df))
                veic = sc.fundos_exclusivos(nome_curto=True)
                if len(veic) == 0:
                    veic = ''
                else:
                    veic = ','.join(veic)
                df.insert(5, 'Veic', [veic] * len(df))
                resultado.append(df)
        if len(resultado) == 0:
            return None
        resultado = pd.concat(resultado)
        # Supercarteiras
        lista_classes = resultado['Classe'].unique()
        df_fin = pd.pivot_table(resultado, index=['Perfil', 'Supercarteira', 'OrdemD', 'VetoP', 'Veic', 'Risco', 'PL', 'LiqOp'], columns=['Classe'], values='PesoFinal', dropna=True).reset_index()
        df_fin.insert(2, 'Tipo', ['Atual']*len(df_fin))
        df_fut = pd.pivot_table(resultado, index=['Perfil', 'Supercarteira', 'OrdemD', 'Enq', 'Veic', 'RiscoFut', 'PLFut', 'LiqOp'], columns=['Classe'], values='PesoFuturo', dropna=True).reset_index()
        df_fut.insert(2, 'Tipo', ['Var.Proj.']*len(df_fut))
        df_fut.rename(columns={'PLFut': 'PL', 'Enq': 'VetoP', 'RiscoFut': 'Risco'}, inplace=True)
        for col in lista_classes:
            if col in df_fut.columns:
                df_fut[col] = df_fut[col] - df_fin[col]
        
        # Modelos
        tem_atual = False        
        tem_banda = False
        if not 'Atual' in resultado.columns:
            resultado.insert(len(resultado.columns), 'Atual', [0] * len(resultado))
        resultado.insert(len(resultado.columns),'Banda',[None] * len(resultado))
        if 'Minimo' in resultado.columns and 'Maximo' in resultado.columns:
            tem_banda = True
            resultado['Banda'] = resultado.apply(lambda x: f"{str(x['Minimo'])} - {str(x['Maximo'])}", axis=1)
        
        if 'Atual' in resultado.columns:
            df_atual = pd.pivot_table(resultado, index=['Perfil'], columns=['Classe'], values='Atual', dropna=False).reset_index()
            df_atual.insert(1, 'Supercarteira', ['-'] * len(df_atual))
            df_atual.insert(1, 'PL', [0] * len(df_atual))
            df_atual.insert(1, 'Risco', [0] * len(df_atual))
            df_atual.insert(1, 'OrdemD', [0] * len(df_atual))
            df_atual.insert(1, 'Tipo', ['Target'] * len(df_atual))
            tem_atual = True
            if tem_banda:
                df_banda = pd.DataFrame(index=df_atual.index, columns=df_atual.columns)
                for idx, row in df_atual.iterrows():
                    df_banda.loc[idx, 'Perfil'] = row['Perfil']
                    df_banda.loc[idx, 'Tipo'] = 'Banda'
                    df_banda.loc[idx, 'OrdemD'] = 1
                    df_banda.loc[idx, 'Risco'] = 0
                    df_banda.loc[idx, 'PL'] = 0
                    df_banda.loc[idx, 'Supercarteira'] = '-'
                    for coluna in range(6, len(df_banda.columns)):
                        busca = resultado[(resultado['Perfil']==row['Perfil']) & (resultado['Classe']==df_banda.columns[coluna])]
                        if not busca.empty:
                            busca = busca.iloc[0]
                            df_banda.loc[idx, df_banda.columns[coluna]] = busca['Banda']
                    
                # print('teste')
            
        # Ordenação das colunas
        ordem = list(resultado['Classe'].unique())
        lista_cols = []
        for col in df_fin.columns:
            if col not in ordem:
                if col != 'Veic':
                    lista_cols.append(col)
        lista_o = []
        for idx, row in self.classes.iterrows():
            if idx in ordem:
                lista_o.append(idx)
        lista_cols = lista_cols + lista_o + ['Veic']
        
        # TODO: confirmar que deu certo
        #df_fin = df_fin[lista_cols]
        #df_fut = df_fut[lista_cols]
        df_fin = df_fin.reindex(labels=lista_cols, axis=1)
        df_fut = df_fut.reindex(labels=lista_cols, axis=1)
        
        # df_atual = df_atual[lista_cols]
        if tem_atual and tem_banda:
            relat = pd.concat([df_fin, df_fut, df_atual, df_banda])
        elif tem_atual:
            relat = pd.concat([df_fin, df_fut, df_atual])
        else:
            relat = pd.concat([df_fin, df_fut])
        relat = relat.reset_index().drop('index', axis=1)
        # Ordenação das políticas
        relat.insert(0, 'OrdemP', [5] * len(relat))
        for idx, row in relat.iterrows():
            ordem = 5
            if str(row['Perfil'])[0:4] == 'Vol ':
                ordem = 0
            elif row['Perfil'] == 'N/A':
                ordem = 10
            relat.loc[idx, 'OrdemP'] = ordem
            
        # Faz o sort
        relat.sort_values(['OrdemP', 'Perfil', 'OrdemD', 'Supercarteira', 'Tipo'], inplace=True)        
        relat.drop('OrdemP', axis=1, inplace=True)
        relat.drop('OrdemD', axis=1, inplace=True)
        # Final
        relat = relat[relat['Perfil'] != 'N/A']
        relat = relat[relat['PL'] >= 0]
        df = relat[relat['Tipo']=='Target'].copy()
        for idx, row in df.iterrows():
            relat.loc[idx, 'Supercarteira'] = row['Perfil']
        relat.drop('Perfil', axis=1, inplace=True)    
        return relat
        
    def carrega_sc(self, nome_sc):
        sc = Supercarteira(nome_supercarteira=nome_sc, base_dm1=self.dm1, classe_liquidez=self.liq, homologacao=self.homologacao)
        return sc


class RelatorioLiquidez:
    
    def __init__(self, rodar=True, verbose=False, homologacao=False):
        from databases import Liquidez
        self.relogio = Cronometro(nome_processo='Relatório Liquidez', verbose=verbose)
        self.dm1 = PosicaoDm1(homologacao=homologacao)
        self.liq = Liquidez(homologacao=homologacao)
        
        if rodar:
            self.rodar()            
    
    def rodar(self):
        abas = []
        # 1. Fundos        
        lista_fundos = self.dm1.lista_fundos_com_idcarteira(gestor_jbfo=True)
        lista_fundos = list(lista_fundos['NomeContaCRM'].unique())
        df = self.liq.busca_liquidez(tipo_veiculo=1, liquidez=True)
        df = df[~df['IdCampo'].isin([20])]
        pls = self.dm1.fundos_pl_estimado()
        df_trib = self.dm1.fundos_tributacao()
        df_trib = df_trib[['NomeContaCRM','RiskScoreMax', 'LimCredAlav']].set_index('NomeContaCRM')
        pls = pd.merge(left=pls, left_on='NomeContaCRM', right=df_trib, right_index=True)
        pls = pls[pls['NomeContaCRM'].isin(lista_fundos)]
        
        colunas = df['NomeCampo'].unique()
        relat = pd.pivot_table(df, index='PortfolioNome', columns='NomeCampo', values='ValorFlt')
        relat = relat[colunas].reset_index()
        relat = pd.merge(left=relat, left_on='PortfolioNome', right=pls, right_on='NomeContaCRM')
        relat.drop('NomeContaCRM', axis=1, inplace=True)
        col = relat.columns.get_loc("Liquidez Operacional")
        relat.insert(col+1, 'LiqOp%PL', [None] * len(relat))
        relat['LiqOp%PL'] = relat['Liquidez Operacional'] / relat['PL']
        abas.append({'Nome': 'Fundos', 'Info': relat})

        # 2. Supercarteiras
        df = self.liq.busca_liquidez(tipo_veiculo=2, liquidez=True)
        df = df[~df['IdCampo'].isin([20])]
        pls = self.dm1.sc_gestao_pl_estimado()
        cad = self.dm1.lista_super_carteiras(campos=['NomeSuperCarteiraCRM', 'Segmento', 'Perfil', 'VetoPowerGestaoTatica', 'VetoPowerGestoresExternos', 'VetoPowerTitulosCredito', 'VetoPowerAcoes'])
        cad.insert(2, 'VetoP', [''] * len(cad))
        for idx, row in cad.iterrows():
            if row['VetoPowerGestaoTatica']:
                texto = str(row['VetoPowerGestaoTatica'])[0]
            else:
                texto = 'I'
            if row['VetoPowerGestoresExternos']:
                texto = texto + str(row['VetoPowerGestoresExternos'])[0]
            else:
                texto = texto + 'I'
            if row['VetoPowerTitulosCredito']:
                texto = texto + str(row['VetoPowerTitulosCredito'])[0]
            else:
                texto = texto + 'I'
            if row['VetoPowerAcoes']:
                texto = texto + str(row['VetoPowerAcoes'])[0]
            else:
                texto = texto + 'I'
            cad.loc[idx, 'VetoP'] = texto
        cad = cad[['NomeSuperCarteiraCRM', 'Segmento', 'Perfil', 'VetoP']]
        colunas = df['NomeCampo'].unique()
        relat = pd.pivot_table(df, index='PortfolioNome', columns='NomeCampo', values='ValorFlt')
        relat = relat[colunas].reset_index()
        relat = pd.merge(left=relat, left_on='PortfolioNome', right=pls, right_on='NomeSuperCarteiraCRM')
        relat.drop('NomeSuperCarteiraCRM', axis=1, inplace=True)
        relat = pd.merge(right=relat, right_on='PortfolioNome', left=cad, left_on='NomeSuperCarteiraCRM')
        relat.drop('NomeSuperCarteiraCRM', axis=1, inplace=True)
        col = relat.columns.get_loc("Liquidez Operacional")
        relat.insert(col+1, 'LiqOp%PL', [None] * len(relat))
        relat['LiqOp%PL'] = relat['Liquidez Operacional'] / relat['PL']
        abas.append({'Nome': 'Supercarteiras', 'Info': relat})
        
        # 3. Monta planilha
        pasta = f'{ambiente.pasta_base_pm()}/06 - Relatório/Posição/'
        if not FileMgmt().file_exists(f"{pasta}nao_apagar.txt"):
            raise Exception('Pasta não encontrada.')
        data = self.dm1.banco.hoje()
        arquivo = f"posdia_Liquidez_{data.strftime('%Y%m%d')}"
        caminho = f"{pasta}{arquivo}.xlsx"
        with ExcelWriter(caminho, engine='xlsxwriter') as writer:
            wb = writer.book
            f1 = wb.add_format()
            f1.set_num_format('#,##0;(#,##0);-_)')
            for aba in abas:
                df = aba['Info']
                nome = aba['Nome']                
                try:
                    df.to_excel(writer, nome, index=False)
                    aba = writer.sheets[nome]
                    aba.set_column('A:A', 30, f1)
                    aba.set_column('D:Z', 12, f1)
                except:
                    print(f"Aba não escrita: {nome}")                          
                    

class RelatorioTableau:
    
    def __init__(self, rodar=True, verbose=False, homologacao=False):
        from databases import Liquidez
        self.relogio = Cronometro(nome_processo='Relatório Consolidado', verbose=verbose)
        self.dm1 = PosicaoDm1(load_all_positions=True, homologacao=homologacao)
        self.bawm = Bawm(homologacao=homologacao)
        self.liq = Liquidez(homologacao=homologacao)
        self.num_cpu = multiprocessing.cpu_count()
        self.num_cpu = int(self.num_cpu * 1/2)
        self.campos = ['GuidSupercarteira', 'NomeSuperCarteiraCRM', 'Segmento', 'Perfil', 'VetoPowerGestaoTatica', 'VetoPowerGestoresExternos', 'VetoPowerTitulosCredito', 'VetoPowerAcoes', 'Officer', 'DPM']
        self.dicio_class_ind = {'R Fixa Pós': 'CDI','R Fixa Pré': 'IRFM','R Fixa Infl': 'IMAIPCA',
                                'RF Internacional': 'BMLUSHY','Alternativos': 'IHFA','R Variável': 'Ibvsp',
                                'Real Estate': 'IFIX','RV Internacional': 'NDUEACWF;PTAXV','Crédito Alternativo': 'CDI',
                                'P. Equity': 'CDI','Outros': 'Ibvsp','Cambial': 'PTAXV',
                                'Infraestrutura Ilíquidos': 'CDI','RF Distressed': 'CDI','F Exclusivo': 'IHFA',
                                'Investimento no Exterior': 'NDUEACWF;PTAXV','RF Infl': 'IHFA','Imobiliário': 'IFIX'}
        # TODO: como comparar PIs com e sem Ilíquidos (no nível do fundo exclusivo)
        # 'P. Equity': 'SMLLBVA', 'Infraestrutura Ilíquidos': 'SMLLBVA'
        self.tracking = []
        if rodar:
            self.rodar()
    
    def carrega_sc(self, nome_sc):
        sc = Supercarteira(nome_supercarteira=nome_sc, base_dm1=self.dm1)
        return sc
    
    def __df_indices__(self):
        data_fim = self.bawm.banco.hoje() - relativedelta(days=5)
        data_ini = data_fim - relativedelta(days=370)
        
        cotas = []
        for item in self.dicio_class_ind.items():
            classe = item[0]
            indice = item[1]
            # Separação entre índices compostos
            if ';' in indice:
                indice = indice.split(';')
            else:
                indice = [indice]
            if len(indice) == 1:
                df = self.bawm.in_historico(index_cod=indice[0], data_ini=data_ini, data_fim=data_fim).drop('DataAtu', axis=1)
            else:
                df = []
                for ind in indice:
                    df.append(self.bawm.in_historico(index_cod=ind, data_ini=data_ini, data_fim=data_fim).drop('DataAtu', axis=1))
                df = pd.concat(df)
                df = pd.pivot_table(df, values='Valor', index='Data', columns='IndexCod').fillna(method='ffill')
                df['Valor'] = df[indice[0]] * df[indice[1]]
                df.reset_index(inplace=True)
            df.insert(0, 'Classe', [classe] * len(df))
            cotas.append(df)
        cotas = pd.concat(cotas)
        cotas = pd.pivot_table(cotas, values='Valor', index='Data', columns='Classe').fillna(method='ffill')
        cotas.replace(0, method='ffill', inplace=True)
        retornos = cotas / cotas.shift(1) - 1
        retornos.dropna(inplace=True)
        vols = retornos.std() * (252 ** 0.5)
        correl = retornos.corr()
        return vols, correl
    
    def __asset_allocation_te__(self, df, futuro=False):
        if not 'Atual' in df.columns:
            return None
        teste = df.copy()
        correl = self.correl.copy()
        correl = correl[df['Classe']]
        teste.set_index('Classe', inplace=True)
        campo = 'PesoFinal'
        if futuro:
            campo = 'PesoFuturo'
        teste['Delta'] = teste[campo] - teste['Atual']
        teste['Vol'] = self.vols
        teste['CTR'] = teste['Delta'] * teste['Vol']
        teste = teste[['CTR']]
        teste = pd.concat([teste, correl], axis=1).dropna()
        
        # Calculo
        ctr = teste['CTR'].to_numpy()
        correl = teste.drop('CTR', axis=1).to_numpy()
        te = np.dot(np.dot(ctr,correl),ctr)
        return te ** 0.5
    
    @staticmethod
    def arredondar(valor):
        try:
            return int(valor * 10000) / 100
        except:
            return 0
    
    def rodar(self):   
        # 1. Verificação se tem acesso à pasta para gerar arquivo
        pasta = f'{ambiente.pasta_base_pm()}/06 - Relatório/Posição/'
        if not FileMgmt().file_exists(f"{pasta}nao_apagar.txt"):
            pasta = 'I:/3 - Asset Allocation/Posições/RelatPython/'      
        
        # 2. Vols e matriz de correlação
        self.vols, self.correl = self.__df_indices__()
        
        # 3. Execução do relatório
        self.relogio.marca_tempo('Buscando supercarteiras...')
        lista_sc = self.dm1.lista_super_carteiras(campos=self.campos)
        liq_op = self.liq.busca_liquidez(tipo_veiculo=2, liquidez=True)
        liq_op = liq_op[liq_op['IdCampo']==9].set_index('PortfolioNome')
        pool = multiprocessing.Pool(processes=self.num_cpu)
        lista = lista_sc['NomeSuperCarteiraCRM'].to_list()
        self.relogio.marca_tempo('Execução do pool...')
        records = pool.map(self.carrega_sc, lista)
        self.relogio.marca_tempo('Gerando relatório...')
        resultado = []        
        for sc in records:
            if not sc.posexp.empty:
                df = sc.asset_class_allocation()                
                df.drop('FinanceiroFinal', axis=1, inplace=True)
                df.drop('FinanceiroFuturo', axis=1, inplace=True)
                for col in df.columns:
                    if col != 'Classe':
                        df[col] = df[col].apply(lambda x: self.arredondar(x))
                
                try:
                    te = self.__asset_allocation_te__(df)
                except:
                    te = None
                if te:
                    self.tracking.append({'IdCampoVeiculo': 2, 'DataArquivo': sc.data_arquivo, 'PortfolioNome': sc.nome, 
                                     'PortfolioGuid': sc.guid, 'IdCampo': 16, 'ValorFlt': te})
                try:
                    te_fut = self.__asset_allocation_te__(df, futuro=True)
                except:
                    te_fut = None
                if te_fut:
                    self.tracking.append({'IdCampoVeiculo': 2, 'DataArquivo': sc.data_arquivo, 'PortfolioNome': sc.nome, 
                                     'PortfolioGuid': sc.guid, 'IdCampo': 17, 'ValorFlt': te_fut})
                
                df = sc.asset_class_allocation_subclass()                
                df.drop('FinanceiroFinal', axis=1, inplace=True)
                df.drop('FinanceiroFuturo', axis=1, inplace=True)
                
                df.insert(0, 'Supercarteira', [sc.nome] * len(df))
                df.insert(1, 'Perfil', [sc.perfil] * len(df))
                df.insert(2, 'VetoP', [sc.veto_power] * len(df)) 
                ordem = 0
                if str(sc.veto_power)[0] != 'S':
                    ordem = 1
                df.insert(2, 'OrdemD', [ordem] * len(df))
                df.insert(3, 'PL', [int(sc.pl * 100) / 100] * len(df))
                df.insert(4, 'PLFut', [int(sc.pl_fut * 100) / 100] * len(df))
                df.insert(5, 'Enq', [sc.asset_class_allocation_enquadrada_texto()] * len(df))
                df.insert(6, 'Distrato', [sc.distrato] * len(df))
                df.insert(7, 'Officer', [sc.po_cad_atual['Officer']] * len(df))
                df.insert(8, 'Controller', [sc.po_cad_atual['Controller']] * len(df))
                df.insert(9, 'Segmento', [sc.po_cad_atual['Segmento']] * len(df))
                df.insert(8, 'DPM', [sc.po_cad_atual['DPM']] * len(df))
                df.insert(9, 'DeputyDPM', [sc.po_cad_atual['DeputyDPM']] * len(df))
                veic = sc.fundos_exclusivos(nome_curto=True)
                if len(veic) == 0:
                    veic = ''
                else:
                    veic = ','.join(veic)
                df.insert(5, 'Veic', [veic] * len(df))
                df.insert(10, 'TrackingError', [te] * len(df))
                df.insert(11, 'TrackingErrorFut', [te_fut] * len(df))
                # Liquidez
                if sc.nome in liq_op.index:
                    liquidez = liq_op.loc[sc.nome, 'ValorFlt']
                    liq_perc = liquidez / sc.pl 
                    df.insert(len(df.columns), 'LiquidezOp', [liquidez] * len(df))
                    df.insert(len(df.columns), 'LiquidezOpPercPL', [liq_perc] * len(df))
                # Final
                resultado.append(df)
        resultado = pd.concat(resultado)                
        
        # 5. Monta planilha
        self.relogio.marca_tempo('Exportando planilha...')
        pasta_old = 'I:/3 - Asset Allocation/Posições/RelatPython/'
        pasta = f'{ambiente.pasta_base_pm()}/06 - Relatório/Posição/'
        if not FileMgmt().file_exists(f"{pasta}nao_apagar.txt"):
            pasta = pasta_old
        data = self.dm1.banco.hoje()
        arquivo = f"posdia_Tableau" # "_{data.strftime('%Y%m%d')}"
        caminho = f"{pasta}{arquivo}.xlsx"
        resultado.to_excel(caminho, index=False)
        
        # 6. Final do processo
        self.relogio.marca_tempo('Relatório gerado.')
        
        # 7. Upload dos dados de tracking error
        self.tracking = pd.DataFrame(self.tracking)
        resultado = self.liq.upload_dataframe(df=self.tracking)
        
        self.relogio.concluido(True)


class RelatorioExplosao:
    
    def __init__(self, rodar=True, por_titularidade:bool=False, verbose=False, homologacao=False):
        from databases import Liquidez
        self.relogio = Cronometro(nome_processo='Relatório Explosão', verbose=verbose)
        self.dm1 = PosicaoDm1(load_all_positions=True, pos_apenas_fundos_exclusivos=True, por_titularidade=por_titularidade, requisito_multi_thread=True, homologacao=homologacao)
        self.bawm = Bawm(homologacao=homologacao)
        self.liq = Liquidez(homologacao=homologacao)
        self.num_cpu = multiprocessing.cpu_count()
        self.num_cpu = int(self.num_cpu * 1/2)
        self.campos = ['GuidSupercarteira', 'NomeSuperCarteiraCRM', 'Segmento', 'Perfil', 'DiscricionarioInterClasse', 'DiscricionarioIntraClasse', 'DiscricionarioCredito', 'Officer']
        if por_titularidade:
            self.campos = ['Segmento', 'Perfil', 'DiscricionarioInterClasse', 'DiscricionarioIntraClasse', 'DiscricionarioCredito', 'Officer']
        self.por_titularidade = por_titularidade
        if rodar:
            self.rodar()
    
    def carrega_sc(self, nome_sc):
        if self.por_titularidade:
            sc = Titularidade(titularidade=nome_sc, base_dm1=self.dm1, load_posicao_explodida=True)
            sc.pl = sc.pl_est
            sc.pl_fut = sc.plfut_est
        else:
            sc = Supercarteira(nome_supercarteira=nome_sc, base_dm1=self.dm1)
        return sc
    
    @staticmethod
    def arredondar(valor):
        try:
            return round(valor, 2)
        except:
            return 0
    
    def rodar(self, multi_threading=True):   
        # 1. Verificação se tem acesso à pasta para gerar arquivo
        pasta = f'{ambiente.pasta_base_pm()}/06 - Relatório/Posição/'
        if not FileMgmt().file_exists(f"{pasta}nao_apagar.txt"):
            pasta = 'I:/3 - Asset Allocation/Posições/RelatPython/'             
        
        # 3. Execução do relatório
        self.relogio.marca_tempo('Buscando supercarteiras...')
        if not self.por_titularidade:
            lista_sc = self.dm1.lista_super_carteiras(campos=self.campos)
            campo_lista = 'NomeSuperCarteiraCRM'
            arquivo = f"posdia_Explosao" # "_{data.strftime('%Y%m%d')}"
        else:
            campo_lista = 'Titularidade'
            arquivo = f"posdia_ExplTit" # "_{data.strftime('%Y%m%d')}"
            lista_sc = self.dm1.titularidade_lista()
            cad = self.dm1.titularidades_cadastro(campos_extras=self.campos).drop('Titularidade', axis=1).set_index('TitularidadeGuid')
            lista_sc = pd.merge(left=lista_sc, left_on='TitularidadeGuid', right=cad, right_index=True)
            
        pool = multiprocessing.Pool(processes=self.num_cpu)        
        lista = lista_sc[campo_lista].to_list()
        self.relogio.marca_tempo('Execução do pool...')
        if multi_threading:
            records = pool.map(self.carrega_sc, lista)
        else:
            records = []
            for item in lista:
                records.append(self.carrega_sc(item))
        self.relogio.marca_tempo('Gerando relatório...')
        resultado = []
        for sc in records:
            if sc:
                if not sc.posexp.empty:
                    df = sc.posexp
                    df.insert(1, 'Perfil', [sc.perfil] * len(df))
                    df.insert(2, 'VetoP', [sc.veto_power] * len(df)) 
                    ordem = 0
                    if str(sc.veto_power)[0] != 'S':
                        ordem = 1
                    df.insert(2, 'OrdemD', [ordem] * len(df))
                    df.insert(3, 'PL', [round(sc.pl ,2)] * len(df))
                    df.insert(4, 'PLFut', [round(sc.pl_fut, 2)] * len(df))
                    if self.por_titularidade:
                        df.insert(5, 'Enq', [sc.enquadramento_texto()] * len(df))
                    else:
                        df.insert(5, 'Enq', [sc.asset_class_allocation_enquadrada_texto()] * len(df))
                    df.insert(6, 'Distrato', [sc.distrato] * len(df))
                    df.insert(7, 'Officer', [sc.officer] * len(df))
                    df.insert(8, 'Controller', [sc.controller] * len(df))
                    df.insert(9, 'Segmento', [sc.segmento] * len(df))                
                    
                    # Final
                    resultado.append(df)
        resultado = pd.concat(resultado)
        
        # 5. Monta planilha
        self.relogio.marca_tempo('Exportando planilha...')
        pasta_old = 'I:/3 - Asset Allocation/Posições/RelatPython/'
        pasta = f'{ambiente.pasta_base_pm()}/06 - Relatório/Posição/'
        if not FileMgmt().file_exists(f"{pasta}nao_apagar.txt"):
            pasta = pasta_old
        data = self.dm1.banco.hoje()
        
        caminho = f"{pasta}{arquivo}.xlsx"
        resultado.to_excel(caminho, index=False)
        
        # 6. Final do processo
        self.relogio.marca_tempo('Relatório gerado.')
        self.relogio.concluido(True)    


class RelatorioExposicaoTributaria:
    """
    Gera posição dos clientes e fundos classificando investimentos de maneira tributária
    """
    def __init__(self):
        self.dm1 = PosicaoDm1Pickle()
        self.classificacao = pd.read_excel(f'{ambiente.pasta_base_pm()}/11 - Apresentações/202312 TributacaoLocal/Tributação Domestica 2023_Anexo_ClassificacaoAtivos.xlsx')
    
    def ativo_busca(self, tipo_produto:str, classe:str, sub_classe:str, tipo_trib:str, guid_ativo:str=None, nome_ativo:str=None):
        """
        Busca o regime tributário de cada ativo, em 3 etapas:
            1. pelo Guid
            2. pelo nome
            3. Conforme a matriz de classificação

        Parameters
        ----------
        tipo_produto : str
            DESCRIPTION.
        classe : str
            DESCRIPTION.
        sub_classe : str
            DESCRIPTION.
        tipo_trib : str
            DESCRIPTION.
        guid_ativo : str, optional
            DESCRIPTION. The default is None.
        nome_ativo : str, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        # 1. busca por guid
        if guid_ativo:
            if guid_ativo in ['8a9ca84c-ff9f-e411-a411-005056912b96', '8a9ca84c-ff9f-e411-a411-005056912b96']:
                return 'RF Pós Corporativo'
            if guid_ativo in ['7eb3d5ec-7d48-e611-9f3c-005056912b96', '9829664c-4ae5-e511-9f3c-005056912b96', '7818e889-ceac-ea11-b3a7-005056912b96']:
                return 'RF Infl Corp Isento'
            if guid_ativo in ['7668529b-80c5-eb11-9294-005056912b96', '90f78cdc-ca2b-df11-b222-0003ffe3d283', 'ed91728e-1685-df11-97c8-0003ffe3d283','8391df07-1856-e911-91ed-005056912b96', 'a3828538-1856-e911-91ed-005056912b96', '56f47bd2-a820-ec11-a569-005056912b96', 'fc0003b7-43e0-e711-b4fa-005056912b96']:
                return 'Multimercados Prev'
        
        # 2. Fragmentos do nome
        if nome_ativo:
            nome_lista = nome_ativo.lower().split(' ')
            if 'fii' in nome_lista:
                if classe == 'R Fixa Infl':
                    return 'RF Infl FII'
                elif classe == 'R Fixa Pós':
                    'RF Pós FII'
                else:
                    return 'Real Estate'
            elif 'infra' in nome_lista:
                return 'RF Infl Corp Isento'
            
        # 3. Outros casos
        lista = []
        lista.append({'TipoProduto': tipo_produto})
        lista.append({'Classe': classe})
        lista.append({'SubClasse': sub_classe})
        lista.append({'TipoTrib': tipo_trib})
        filtro = self.classificacao.copy()
        for item in lista:
            sub_lista = list(item.keys())
            filtro = filtro[filtro[sub_lista[0]]==item[sub_lista[0]]]
            if len(filtro) == 1:
                return filtro.iloc[0]['Classificação']
            itens = filtro['Classificação'].unique()
            if len(itens) == 1:
                return filtro.iloc[0]['Classificação']
        return None

    def classifica_fundo(self, nome:str, resumo:pd.DataFrame) -> str:
        """
        Classifica um fundo pelo nome e/ou pela composição

        Parameters
        ----------
        nome : str
            Nome do fundo.
        resumo : pd.DataFrame
            Dataframe com a composição do fundo por modalidade tributária.

        Returns
        -------
        str
            Classificação do fundo.

        """
        # 1. pelo nome
        palavras = nome.lower().split(' ')
        if ('infra' in palavras) or ('infraestrutura' in palavras) or ('incentivado'  in palavras):
            return 'FI Infraestrutura'
        if 'fia'  in palavras:
            return 'FI Ações'
        if 'fifa'  in palavras:
            return 'FI Ações'
        if ('prev' in palavras) or ('pgbl' in palavras) or ('vgbl'  in palavras) or ('previdenciario'  in palavras):
            return 'FI Previdência'
        
        # 2. pela carteira
        def soma_classes(lista_classes, df_cart):
            soma = 0
            for item in lista_classes:
                if item in df_cart.index:
                    soma += df_cart.loc[item, 'PercFundo'].sum()
            return soma
        cart = resumo[resumo.index==nome].copy().reset_index()
        # print(nome)
        cart.set_index('ClassificTribAtivo',inplace=True)
        if soma_classes(['R Variável', 'RV Internacional'], cart) > 0.67:
            return 'FI Ações'
        if soma_classes(['RF Infl Isento'], cart) > 0.85:
            return 'FI Infraestrutura'
        if soma_classes(['Ativos Privados', 'R Variável', 'RV Internacional'], cart) > 0.95:
            return 'FIM Consolidador'
        
        # 3. Final
        return 'FI Multimercado'
    
    def rodar(self):
        # 1. Busca posições
        pos_sc = self.dm1.posicao_sc_all()
        pos_fundos = self.dm1.posicao_fundos_all(apenas_exclusivos=True)
        fundos_pls = self.dm1.fundos_pl_estimado(lista_campos=['NomeContaCRM', 'PL']).set_index('NomeContaCRM')
        ativos = self.dm1.ativos_tributacao()
        ativos = ativos[['TipoTrib']]
        lista_sc = list(pos_sc['SC'].unique())
        assoc_fundos = self.dm1.lista_fundos_com_idcarteira(apenas_exclusivos=True)
        assoc_fundos = assoc_fundos[['NomeContaCRM', 'CodigoProduto']].set_index('CodigoProduto')
        assoc_fundos.columns = ['NomeFundo']
        pos_fundos = pd.merge(left=pos_fundos, left_on='GuidProduto', right=ativos, right_index=True, how='inner')
        pos_fundos.insert(len(pos_fundos.columns), 'ClassificTrib', [None]*len(pos_fundos))
        
        # 2. Classifica ativos dos fundos e supercarteiras
        pos_fundos['ClassificTrib'] = pos_fundos.apply(lambda x: self.ativo_busca(tipo_produto=x['TipoProduto'], classe=x['Classe'], sub_classe=x['SubClasse'],
                                                                     tipo_trib=x['TipoTrib'], guid_ativo=x['GuidProduto'], nome_ativo=x['NomeProduto']), axis=1)
        df_erro = pos_fundos[pos_fundos['ClassificTrib'].isnull()]
        df_erro = df_erro[['TipoProduto', 'Classe', 'SubClasse', 'TipoTrib']]
        

class ResumoGestao:
    from databases import Crm, Bawm, BDS,PosicaoDm1,BaseExtrato
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
    from objetos import Ativo
    import numpy as np
    from datetime import date,datetime
    from delta import opção_delta,duration_modificada,risco_ativo
    """
       Traz informações que auxiliam na tomada de decisão da gestão.
       São as principais posições dos veiculos"""


                        
    def calculo_delta_rv_tatico(self):
        """
        Calcula o delta das opções do JBFO RV Tático.
         
         Parameters
         ----------
         Não possui parametros obrigatorios mas precisa dos dados do BDS de D-1 das opções disponiveis e futuros.
    
     
         Returns
         -------
         Retorna a exposição a delta do JBFO RV Tático na pasta O:/SAO/CICH_All/Portfolio Management/01 - Rotinas Diarias/CW
     
         """ 
         
        crm = Crm()
        bawm = Bawm()
        bds = self.BDS()
        mdFormat = self.mdates.DateFormatter('%b/%y')
        funcoes_datas = FuncoesDatas()
        posicao = PosicaoDm1()
        extrato = self.BaseExtrato()
        nome_fundo = '_JBFO RV TÁTICO FIA - BDR Nível I'
        bench = 'Ibvsp'
        serie_bds = 59936961
        
        hoje = funcoes_datas.workday(funcoes_datas.hoje(),n_dias = -4,feriados_brasil=True, feriados_eua=False)
        dia = self.date(hoje.year, hoje.month, 1) 
        data_inicial= funcoes_datas.workday( dia,n_dias = -30,feriados_brasil=True, feriados_eua=False)
        
        produtos = crm.Produtos_isin_ticker()
        Ibovespa = bawm.in_historico(index_cod='Ibvsp', data_ini=data_inicial, data_fim=hoje) 
        Ibovespa['Ibovespa']=(Ibovespa['Valor']/Ibovespa['Valor'].shift(1))-1
        
        #gps.posicao_extrato('_JBFO RV Tatico FIM',hoje)
        
        df = extrato.posicao_extrato(nome_fundo,hoje)
        df = df[['DataPosicao','NomeDoProduto','TipoProduto','Subclasse','Classe','IdSistemaOrigem','ValorCota','PercentualSobreTotal','SaldoNaData']]
        n = 0
        for i in range(len(Ibovespa)):
            n = n+1
            dia = funcoes_datas.workday(data = hoje ,n_dias = -n,feriados_brasil=True, feriados_eua=False)
            df_new = extrato.posicao_extrato(nome_fundo,dia)
            df_new = df_new[['DataPosicao','NomeDoProduto','TipoProduto','Subclasse','Classe','IdSistemaOrigem','ValorCota','PercentualSobreTotal','SaldoNaData']]
            df = pd.concat([df_new,df])
        df['tipo'] = df['NomeDoProduto'].apply(lambda x: x.split('BRAD 100%')[0])
        df = df.sort_values(by=['IdSistemaOrigem','DataPosicao'])
        df['Nome_Produto'] = df['tipo'].apply(lambda x:'Compromissada' if x == 'COMP ' else x)
        df = df[['DataPosicao','Nome_Produto','TipoProduto','Subclasse','Classe','IdSistemaOrigem','ValorCota','PercentualSobreTotal','SaldoNaData']]
        classes = df[['Nome_Produto','Subclasse']].drop_duplicates()
        df['DataPosicao']=pd.to_datetime(df['DataPosicao'])
        ativos_retirar = ['999996','999999','999997']
        df = df[~df['IdSistemaOrigem'].isin(ativos_retirar)].sort_values(by=['Nome_Produto','DataPosicao'])
        
        #TRaz o PL do BDS
        cota = bds.serie_historico(idserie = serie_bds,intervalo=1,data_ini=data_inicial, data_fim=hoje)
        pl=cota[['dataser','a20',]].rename(columns={'a20':'PL'})
        pl['dataser']=pd.to_datetime(pl['dataser'])
        dias = pl['dataser']
        
        df = pd.merge(right =df,left=pl, right_on ='DataPosicao',left_on ='dataser',how='right').drop(columns=['dataser'])
        df['Nome_Produto'] = df['Nome_Produto'].str.lower()
        df = pd.merge(left = df, right = produtos, right_on = 'new_idsistemaorigem',left_on ='IdSistemaOrigem', how = 'left')
        df['qtd']=df['SaldoNaData']/df['ValorCota']
        
        rv  = df[(df['Classe']!='R Fixa Pós')&(df['Classe']!='Ajuste')]
        rv['TipoProduto']=rv['TipoProduto'].str.lower()
        rv['TipoProduto']=rv['Nome_Produto'].apply(lambda x:'opt' if x[0:5]=='opção' else x)
        
        
        delta_adj = []
        for idx, row in rv.iterrows():    
            qtd = row['qtd']
            data =row['DataPosicao']                                       
            if row['TipoProduto']=='opt':
                id_sistema=row['new_idsistemaorigem']+'_20'+row['Nome_Produto'][-2:] 
                try:
                        posicao= opção_delta(id_sistema,qtd,data)
                except:
                        posicao=row['SaldoNaData']
            else:
                 posicao=row['SaldoNaData']
            delta_adj.append(posicao)        
        
        rv['adj_delta']= delta_adj
        rv['Peso'] = rv['adj_delta'] / rv['PL']
        rv=rv.dropna(subset=['PL','PercentualSobreTotal'])
        rv = rv[rv['Peso']!=0]    
        
        rv = rv.groupby('DataPosicao').sum().reset_index()[['DataPosicao','PercentualSobreTotal','Peso']]
                
        rv.to_excel(f'{ambiente.pasta_base_pm()}/01 - Rotinas Diarias/CW/delta_por_dia_RV Tático.xlsx')     
        
    def foto_portfolio(self):  
            
        """
        São as principais posições dos veiculos geridos por PM.
         
         Parameters
         ----------
         Não possui parametros obrigatorios mas precisa que a D-1 seja rodada e os dados do BDS de D-1 (data de hoje -1) dos titulos publicos, opções e futuros.
         Utiliza os PUs dos ativos da BAWM
     
         Returns
         -------
         Retorna a posicao atual dos fundos.
     
         """ 
            
        app = xw.App(add_book=False)
        app.display_alerts = False
        #wb = app.books.api.Open('O:/SAO/CICH_All/Portfolio Management/01 - Rotinas Diarias/CW/Fundos_coletivos.xlsm', UpdateLinks=False)
        fundos_coletivos = pd.read_excel(f'{ambiente.pasta_base_pm()}/01 - Rotinas Diarias/CW/Foto_portfolios.xlsm',sheet_name='Lista_fundos_coletivos')
        lista_fundos =  fundos_coletivos['Nome_fundo']
        lista_bench= fundos_coletivos['Benchmark']
        bawm = Bawm()
        Te = te()
        datas = FuncoesDatas()
        crm = Crm()
        
        #Pegando as datas a serem puxadas
        data_inicio = datas.workday(data = datas.hoje(),n_dias = -1,feriados_brasil=True, feriados_eua=False)
        data_fim = datas.workday(data = datas.hoje(),n_dias = -1,feriados_brasil=True, feriados_eua=False)
        
        
        #Puxando a classificação dos fundos e se o fundo é um FIC.
        cadastro = bawm.po_cadastro()
        classificacao_fundos = crm.classificacao_fundos_jbfo() 
        
        #Dados do IMAB para serem usados frente a duration dos fundos
        IMAB_B = bawm.in_historico(index_cod='IMAIPCADU',data_ini=data_inicio, data_fim=data_fim)
        IMAB5_5 = bawm.in_historico(index_cod='BZRFB5DU',data_ini=data_inicio, data_fim=data_fim)
        IMAB5_5_mais = bawm.in_historico(index_cod='BZRFB5+D',data_ini=data_inicio, data_fim=data_fim)
        IMAB_Consolidado = pd.concat([IMAB_B,IMAB5_5,IMAB5_5_mais]).drop(columns=['DataAtu'])
        IMAB_Consolidado = IMAB_Consolidado.replace(['IMAIPCADU','BZRFB5DU','BZRFB5+D'],['IMAB','IMAB5','IMAB-5+'])
        IMAB_Consolidado['Valor'] = IMAB_Consolidado['Valor']/252
        
        #Puxa o preço dos futuros, para o calculo de qtd de contratos para deixar as exposições cambio/ativos off em 100%
        def preco_fut(cod):
            dia = data_inicio
            futuros = bawm.dados_futuro(cod,dia)
            pu = futuros['Preco'].values[0]
            return pu 
        
        #Traz do py objetos os dados que usaremos de cada fundo.
        def info_fundos(guid):
            fundo = Fundo(guid_conta_crm = guid)
            fic = fundo.fundo_cotas
            cod_crm = fundo.cod_produto
            posicao = fundo.pos
            pl = fundo.pl_est
            return fic,cod_crm,posicao,pl
            
        
        def pl_fundo(fundo):
            fundo = Fundo(fundo)
            pl = fundo.pl_est
            return pl
        
        #Preenche as durations dos fidcs e do JBFO CORP
        def preenchendo_duration(ativo,duration_atual):
            ativo = ativo.replace('(','').replace(')','')
            ativo = ativo.replace(' ','')
            ativo = ativo.strip()
            if ativo == 'JBFOInflacaoCorpIcatuPrevidenciarioFIEIIFIMCrPrBRL':
                duration = duration_total_corp        
            elif  'fidc' in ativo.lower():
                duration = 365
            elif 'ODF' in ativo:
                ativo = ativo[-5:].strip()
                duration = duration_modificada(ativo)
            else:
                duration = duration_atual
            return duration    
                
        
        #Calcula a duration do JBFO CORP (será usado em outros fundos de previdencia).
        carteira_jbfo_corp = info_fundos('0d961262-961f-eb11-b838-005056912b96')[2]
        carteira_jbfo_corp['Duration'] = carteira_jbfo_corp.apply(lambda row: row['Duration'] if math.isnan(row['Duration']) == False else preenchendo_duration(row['NomeProduto'],row['Duration']), axis = 1)
        posicao_inflacao_cred_corp = carteira_jbfo_corp[(carteira_jbfo_corp['SubClasse']=='RF Infl Créd Estr')|(carteira_jbfo_corp['SubClasse']=='RF Infl Créd')|(carteira_jbfo_corp['SubClasse']=='RF Infl Bancos')]
        duration_credito_corp = (posicao_inflacao_cred_corp['PesoFinal']*(posicao_inflacao_cred_corp['Duration'])).sum()
        posicao_inflacao_soberana_corp = carteira_jbfo_corp[carteira_jbfo_corp['SubClasse']=='RF Infl Soberano']
        duration_inflacao_soberana_corp = (posicao_inflacao_soberana_corp['PesoFinal']*(posicao_inflacao_soberana_corp['Duration'])).sum()
        duration_total_corp = round(duration_credito_corp + duration_inflacao_soberana_corp,2)
        
        #Traz as informações dos fundos de forma consolidada (se é fic e a classificação) - Tabela principal que reune todas as caracteristicas do fundo
        fundos_coletivos = pd.read_excel('O:/SAO\CICH_All/Portfolio Management/01 - Rotinas Diarias/CW/Foto_portfolios.xlsm',sheet_name='Lista_fundos_coletivos')
        fundos_coletivos['Guid_conta'] = fundos_coletivos['Guid_conta'].str.lower()
        fundos_coletivos['cod_produto'] = fundos_coletivos['Guid_conta'].apply(lambda x: info_fundos(x)[1])
        fundos_coletivos['fic'] = fundos_coletivos['Guid_conta'].apply(lambda x: info_fundos(x)[0])
        fundos_coletivos = pd.merge(left = fundos_coletivos, right = classificacao_fundos, left_on = 'cod_produto', right_on = 'new_idsistemaorigem', how = 'left')
        fundos_coletivos = fundos_coletivos.drop(columns = {'new_idsistemaorigem','new_name','Benchmark'})
        fundos_coletivos.columns = ['NomeContaCRM','Guid_conta','Cod_produto','EFIC','classificacao']
        fundos_coletivos['PL'] = fundos_coletivos['NomeContaCRM'].apply(lambda x: Fundo(x).pl_est)
        pls = fundos_coletivos[['NomeContaCRM','PL']].drop_duplicates()
        
        #Gera um dataframe com todas as posicoes dos veiculos
        guid = fundos_coletivos.iloc[0,1]
        df = info_fundos(guid)[2]
        
        for i,(index,row) in enumerate(fundos_coletivos.iterrows()):
            if i > 0:
                guid = row['Guid_conta'] 
                df1 = info_fundos(guid)[2]
                df = pd.concat([df,df1])
        df = pd.merge(left = df, right = pls, on = 'NomeContaCRM', how='left')
        df = df[['DataArquivo','NomeContaCRM','Classe','SubClasse','TipoProduto','IdProdutoProfitSGI','NomeProduto','FinanceiroFinal','QuantidadeFinal','PesoFinal','Stress','PL']]        
        df['opt_delta'] = df.apply(lambda row :np.round(opção_delta(row['IdProdutoProfitSGI'],row['QuantidadeFinal'],data_inicio),2)-row['FinanceiroFinal'] if row['TipoProduto']=='OPT' else row['FinanceiroFinal'] ,axis=1)
        df['peso_adj_delta'] = df.apply(lambda row: row['opt_delta'] / row['PL']*100, axis = 1)
        df['premio_opção'] = df.apply(lambda row: row['FinanceiroFinal']/row['PL']*100 if row['TipoProduto']=='OPT' else np.nan, axis = 1)
        df['stress_adj'] = df['Stress']*df['peso_adj_delta']*100
        df['PesoFinal'] = df['PesoFinal']*100
        teste = df
        teste['NomeContaCRM'] = teste['NomeContaCRM'].apply(lambda x: x.lower())
        teste['stress_adj'] = teste['stress_adj'].astype('float')
        
        
        #Pega o preço dos futuros para calcular a quantidade de contratos a negociar
        futuros = teste[teste['TipoProduto']=='FUT'][['Classe','IdProdutoProfitSGI']]
        futuros['IdProdutoProfitSGI'] = futuros['IdProdutoProfitSGI'].astype('str')
            
        # futuros['PU'] = futuros['IdProdutoProfitSGI'].apply(lambda x:preco_fut(x.strip()))
        futuros['Tipo'] = futuros['IdProdutoProfitSGI'].apply(lambda x:'R Variável' if x[0:2]=='BZ' else 'RV Internacional' if x[0:2]=='BS' else 'Cambial' if x[0:2]=='UC' else np.nan)
        futuros = futuros.drop_duplicates()
        fut_cambial = futuros.loc[futuros['Tipo']=='Cambial'].iat[0,2]*50
        fut_rv_local = futuros.loc[futuros['Tipo']=='R Variável'].iat[0,2]
        fut_rv_global = round(futuros.loc[futuros['Tipo']=='RV Internacional'].iat[0,2]/1000*fut_cambial,2)
        futuros['PU']= futuros.apply(lambda row: fut_cambial if row['Tipo']=='Cambial' else fut_rv_global if row['Tipo']=='RV Internacional' else row['PU'], axis = 1)
        
        #Segrega o tipo de duration
        ## duration_total_classe_x' : A duration total de cada classe, não ponderada pelo tamanho no fundo (os ativos da classe especifica somam 100)
        ## exp_inflacao_x : A duration da classe ponderada ao tamanho dela dentro do fundo (aqui a base 100 são todos os ativos do fundo)
        ##duration_add_portf : duration adicionada ao portfolio via alocações em inflação e pre
        
        duration = []
        for idx, row in fundos_coletivos.iterrows():
            fundo = row['NomeContaCRM']
            fundo = fundo.lower()
            guid = row['Guid_conta']
            posicao = info_fundos(guid)[2]        
            posicao['Duration']=posicao.apply(lambda row: preenchendo_duration(row['NomeProduto'],row['Duration']),axis=1)
            if 'RF Infl Soberano' in row['classificacao']:
                posicao_pre = posicao[(posicao['SubClasse']=='Renda Fixa')]
                posicao_pre_valor =  posicao_pre['PesoFinal'].sum()
                duration_total_classe_soberana_pre= ((posicao_pre['PesoFinal']/posicao_pre_valor)*(posicao_pre['Duration']/252)).sum()
                exp_pre = (posicao_pre['PesoFinal']*(posicao_pre['Duration']/252)).sum()
                
                posicao_inflacao = posicao[(posicao['Classe']=='R Fixa Infl')]
                posicao_inflacao_valor =  posicao_inflacao['PesoFinal'].sum()
                
                posicao_inflacao_cred = posicao[(posicao['SubClasse']=='RF Infl Créd Estr')|(posicao['SubClasse']=='RF Infl Créd')|(posicao['SubClasse']=='RF Infl Bancos')]
                duration_total_classe_cred_inflacao = ((posicao_inflacao_cred['PesoFinal']/posicao_inflacao_valor)*(posicao_inflacao_cred['Duration']/252)).sum()
                exp_inflacao_cred = (posicao_inflacao_cred['PesoFinal']*(posicao_inflacao_cred['Duration']/252)).sum()
                           
                posicao_inflacao_soberana = posicao[posicao['SubClasse']=='RF Infl Soberano']
                duration_total_classe_soberana_inflacao = ((posicao_inflacao_soberana['PesoFinal']/posicao_inflacao_valor)*(posicao_inflacao_soberana['Duration']/252)).sum()
                exp_inflacao_soberana = (posicao_inflacao_soberana['PesoFinal']*(posicao_inflacao_soberana['Duration']/252)).sum()
                
                duration_add_portf = (posicao['PesoFinal']*(posicao['Duration']/252)).sum()
                           
                                      
            else:       
                
                posicao_pre = posicao[(posicao['SubClasse']=='Renda Fixa')]
                posicao_pre_valor =  posicao_pre['PesoFinal'].sum()
                duration_total_classe_soberana_pre= ((posicao_pre['PesoFinal']/posicao_pre_valor)*(posicao_pre['Duration']/252)).sum()
                exp_pre = (posicao_pre['PesoFinal']*(posicao_pre['Duration']/252)).sum()
                
                posicao_inflacao = posicao[(posicao['Classe']=='R Fixa Infl')]
                posicao_inflacao_valor =  posicao_inflacao['PesoFinal'].sum()
                posicao_inflacao_cred = posicao[(posicao['SubClasse']=='RF Infl Créd Estr')|(posicao['SubClasse']=='RF Infl Créd')|(posicao['SubClasse']=='RF Infl Bancos')]
                duration_total_classe_cred_inflacao = ((posicao_inflacao_cred['PesoFinal']/posicao_inflacao_valor)*(posicao_inflacao_cred['Duration']/252)).sum()
                exp_inflacao_cred = (posicao_inflacao_cred['PesoFinal']*(posicao_inflacao_cred['Duration']/252)).sum()
                           
                posicao_inflacao_soberana = posicao[posicao['SubClasse']=='RF Infl Soberano']
                duration_total_classe_soberana_inflacao = ((posicao_inflacao_soberana['PesoFinal']/posicao_inflacao_valor)*(posicao_inflacao_soberana['Duration']/252)).sum()
                exp_inflacao_soberana = (posicao_inflacao_soberana['PesoFinal']*(posicao_inflacao_soberana['Duration']/252)).sum()
                           
                duration_add_portf = exp_inflacao_soberana + exp_inflacao_cred +  exp_pre
                
            duration.append({'NomeContaCRM': fundo,'duration_add_portf':duration_add_portf, 'duration_total_classe_soberana_pre': duration_total_classe_soberana_pre,'duration_total_classe_soberana_inflacao':duration_total_classe_soberana_inflacao,'duration_total_classe_cred_inflacao':duration_total_classe_cred_inflacao})
        
        #Transformando o dicionario de duration em dataframe e incluindo os dados na tabela de informações do fundo (inclui a duration atual)    
        pd_duration = pd.DataFrame.from_dict(duration)
        fundos_coletivos['NomeContaCRM'] = fundos_coletivos['NomeContaCRM'].apply(lambda x: x.lower())
        pd_duration['duration_cred_txt'] = round(pd_duration['duration_total_classe_cred_inflacao'],2).astype('str').fillna(0)
        pd_duration['duration_infl_txt']  = round(pd_duration['duration_total_classe_soberana_inflacao'],2).astype('str').fillna(0)
        pd_duration['Quebra_duration_inflacao'] = 'Duration Crédito é '+ pd_duration['duration_cred_txt']+' e a Soberana é '+pd_duration['duration_infl_txt']
        pd_duration['duration_total_classe_soberana_pre'] =  round(pd_duration['duration_total_classe_soberana_pre'],2)
        pd_duration['duration_total_classe_soberana_inflacao'] = round(pd_duration['duration_total_classe_soberana_inflacao'],2)
        pd_duration['duration_add_portf'] = round(pd_duration['duration_add_portf'],2)
        pd_duration = pd_duration[['NomeContaCRM','duration_add_portf','Quebra_duration_inflacao','duration_total_classe_soberana_pre','duration_total_classe_soberana_inflacao']]
        
        #Inclui o a duration dos fundos na tabela consolidadora
        fundos_coletivos = pd.merge(right = pd_duration, left = fundos_coletivos, on = 'NomeContaCRM', how='left')  
        
        #Verifica quais fundos são Fic e o somatorio de cotas deles(se está com mais de 95%)
        fundos_cotas = []
        for idx, row in fundos_coletivos.iterrows():
            fundo = row['NomeContaCRM']
            if row['EFIC']==True:
                posicao_fic = teste[teste['NomeContaCRM']==fundo]
                posicao_fic = posicao_fic[posicao_fic['TipoProduto']=='COTAS']
                posicao_fic = posicao_fic['FinanceiroFinal'].sum()
                perc_cotas = posicao_fic/row['PL']
            fundos_cotas.append({'NomeContaCRM':fundo,'perc_cotas':perc_cotas})    
        fundos_cotas =  pd.DataFrame.from_dict(fundos_cotas)
        fundos_cotas['NomeContaCRM'] = fundos_cotas['NomeContaCRM'].apply(lambda x: x.lower())
        #Inclui o percentual de cotas dos fundos na tabela consolidadora
        fundos_coletivos = pd.merge(right = fundos_cotas, left = fundos_coletivos, on = 'NomeContaCRM', how='left')
        
        #Pega a exposição em ativos e à moedas dos veiculos que investem em ativos offshore.
        pls['NomeContaCRM']=pls['NomeContaCRM'].str.lower()
        exposicao_off = []
        for idx, row in fundos_coletivos.iterrows():
            fundo = row['NomeContaCRM']
            if row['classificacao']=='RV Internacional':
                #Exposicao aos ativos off
                df = teste[(teste['NomeContaCRM']==fundo)]
                df = df[df['Classe']!='R Fixa Pós']
                df = df[df['Classe']!='Cambial']
                df = df[df['Classe']!='Ajuste']
                exp_off =  df['peso_adj_delta'].sum()
                
                #Exposicao cambial
                df = teste[(teste['NomeContaCRM']==fundo)]
                df = df[((df['SubClasse']!='Ações') | (df['TipoProduto']!='FUT'))&(df['Classe']!='R Fixa Pós')]
                df = df[df['Classe']!='Ajuste']
                ex_moeda =  df['peso_adj_delta'].sum()
                exposicao_off.append({'NomeContaCRM': fundo, 'somatorio_ativos_off': exp_off,'Exposicão_Moedas': ex_moeda})
        
        #Gera um dataframe da exposição dos ativos off/exposicao cambial e popula a tabela das informações dos fundos com esses dados consolidados (fundos_coletivos) 
        ##Calcula o quanto devemos vender/comprar de contratos futuros nos fundos que compram ativos off
        pd_exp_off = pd.DataFrame.from_dict(exposicao_off)
        pd_exp_off['somatorio_ativos_off_tex'] = round(pd_exp_off['somatorio_ativos_off'],2).astype('str').fillna(0)
        pd_exp_off['Exposicão_Moedas_tex'] = round(pd_exp_off['Exposicão_Moedas'],2).astype('str').fillna(0)
        pd_exp_off['Quebra_ativos_off'] = 'A Exposição Cambial é '+ pd_exp_off['Exposicão_Moedas_tex']+' e a ativos offshore é '+ pd_exp_off['somatorio_ativos_off_tex']
        pd_exp_off = pd_exp_off[['NomeContaCRM','Quebra_ativos_off','somatorio_ativos_off','Exposicão_Moedas']]
        pd_exp_off['NomeContaCRM'] = pd_exp_off['NomeContaCRM'].apply(lambda x: x.lower())
        pd_exp_off = pd.merge(left = pd_exp_off, right = pls, on = 'NomeContaCRM', how='left') 
        pd_exp_off['operacao_cts_futuros_padrão_moedas_off'] = pd_exp_off.apply(lambda row: round((100-row['Exposicão_Moedas'])/100*row['PL']/fut_cambial/5,0)*5,axis=1)
        pd_exp_off['operacao_cts_futuros_mini_moedas_off'] = pd_exp_off.apply(lambda row:(100-row['Exposicão_Moedas'])/100*row['PL']/fut_cambial-(round((100-row['Exposicão_Moedas'])/100*row['PL']/fut_cambial/5,0)*5),axis = 1)*5
        pd_exp_off['operacao_cts_futuros_mini_moedas_off'] = round(pd_exp_off['operacao_cts_futuros_mini_moedas_off'],0)
        pd_exp_off['operacao_cts_futuros_padrão_ativos_off'] = pd_exp_off.apply(lambda row: round((100-row['Exposicão_Moedas'])/100*row['PL']/fut_rv_global,0),axis=1)
        pd_exp_off['operacao_cts_futuros_padrão_moedas_off'] = pd_exp_off['operacao_cts_futuros_padrão_moedas_off'] .apply(lambda x: f'Comprar {abs(x)} cts' if x >0 else  f'Vender {abs(x)} cts')
        pd_exp_off['operacao_cts_futuros_mini_moedas_off'] = pd_exp_off['operacao_cts_futuros_mini_moedas_off'] .apply(lambda x: f'Comprar {abs(x)} cts' if x >0 else  f'Vender {abs(x)} cts')
        pd_exp_off['operacao_cts_futuros_padrão_ativos_off'] = pd_exp_off['operacao_cts_futuros_padrão_ativos_off'] .apply(lambda x: f'Comprar {x} cts' if x >0 else  f'Vender {x} cts')
        fundos_coletivos = pd.merge(right = pd_exp_off, left = fundos_coletivos, on = 'NomeContaCRM', how='left')
                                    
        
        #Explode a exposição a gestores de RV, Multimercados e Gestores OFF.
                        
        #Exposão RV  (fundos locais)  
        fundos = teste['NomeContaCRM'].unique()
        posicao_gestores_rv = []
        for fundo in fundos:
            teste_v1 = teste[teste['NomeContaCRM']==fundo]    
            teste_v2 = teste_v1[(teste_v1['Classe']=='R Variável')&(teste_v1['TipoProduto']=='COTAS')]    
            posicao_rv = teste_v2['PesoFinal'].sum()
            dicio = {}
            texto = ''
            for idx, row in teste_v2.iterrows():
                if texto != '':
                    texto += ', '
                fundo = row['NomeContaCRM']
                posicao = row['PesoFinal']/posicao_rv*100
                posicao = str(round(posicao,2))
                produto = row['NomeProduto'].split(" ")[0]
                classe = row['Classe']           
                texto = f"{texto}{produto} {posicao}"
                dicio['NomeContaCRM'] = fundo.lower()
                dicio['RV'] = texto
            posicao_gestores_rv.append(dicio)
        
        #Exposão Alternativos (fundos locais)    
            
        posicao_gestores_alternativos = []
        for fundo in fundos:
            teste_v1 = teste[teste['NomeContaCRM']==fundo]    
            teste_v2 = teste_v1[(teste_v1['Classe']=='Alternativos')&(teste_v1['TipoProduto']=='COTAS')]    
            posicao_alternativos = teste_v2['PesoFinal'].sum()
            dicio = {}
            texto = ''
            for idx, row in teste_v2.iterrows():
                if texto != '':
                    texto += ', '
                fundo = row['NomeContaCRM']
                posicao = row['PesoFinal']/posicao_alternativos*100
                posicao = str(round(posicao,2))
                produto = row['NomeProduto'].split(" ")[0]
                classe = row['Classe']           
                texto = f"{texto}{produto} {posicao}"
                dicio['NomeContaCRM'] = fundo.lower()
                dicio['Alternativos'] = texto
            posicao_gestores_alternativos.append(dicio)
        
        #Exposão ativo off (explode todos os gestores de todas as classes, dentro do fundo que é classificado como RV Internacional)    
            
        posicao_produtos_rvinternacional= []
        for fundo in fundos:
            teste_v1 = teste[teste['NomeContaCRM']==fundo]    
            teste_v2 = teste_v1[(teste_v1['Classe']=='RV Internacional')]    
            posicao_rvinternacional = teste_v2['PesoFinal'].sum()
            dicio = {}
            texto = ''
            for idx, row in teste_v2.iterrows():
                if texto != '':
                    texto += ', '
                fundo = row['NomeContaCRM']
                posicao = row['PesoFinal']/posicao_rvinternacional*100
                posicao = str(round(posicao,2))
                produto = row['NomeProduto'].split(" ")[0]
                classe = row['Classe']           
                texto = f"{texto}{produto} {posicao} "
                dicio['NomeContaCRM'] = fundo.lower()
                dicio['RV_Internacional'] = texto                
            posicao_produtos_rvinternacional.append(dicio)                         
        
        posicao_produtos_rvglobal= []
        for fundo in fundos:
            teste_v1 = teste[teste['NomeContaCRM']==fundo]    
            teste_v2 = teste_v1[(teste_v1['SubClasse']=='RV Global')]    
            posicao_rvglobal = teste_v2['PesoFinal'].sum()
            dicio = {}
            texto = ''
            for idx, row in teste_v2.iterrows():
                if texto != '':
                    texto += ', '
                fundo = row['NomeContaCRM']
                posicao = row['PesoFinal']/posicao_rvglobal*100
                posicao = str(round(posicao,2))
                produto = row['NomeProduto'].split(" ")[0]
                classe = row['Classe']           
                texto = f"{texto}{produto} {posicao} "
                dicio['NomeContaCRM'] = fundo.lower()
                dicio['RV_Global'] = texto                
            posicao_produtos_rvglobal.append(dicio)
        
        #Exposão RV Local (offshore - Vic Global)  
                
        posicao_produtos_rvlocal= []
        for fundo in fundos:
            teste_v1 = teste[teste['NomeContaCRM']==fundo]    
            teste_v2 = teste_v1[(teste_v1['Classe']=='R Variável')&(teste_v1['TipoProduto']!='OPT')]    
            posicao_rvglobal = teste_v2['PesoFinal'].sum()
            dicio = {}
            texto = ''
            for idx, row in teste_v2.iterrows():
                if texto != '':
                    texto += ', '
                fundo = row['NomeContaCRM']
                posicao = row['PesoFinal']/posicao_rvglobal*100
                posicao = str(round(posicao,2))
                produto = row['NomeProduto'].split(" ")[0]
                classe = row['Classe']           
                texto = f"{texto}{produto} {posicao} "
                dicio['NomeContaCRM'] = fundo.lower()
                dicio['Posicao_rvlocal'] = texto                
            posicao_produtos_rvlocal.append(dicio)     
        
        #Inclui as quebras da alocação de gestores (de diferentes classes) na tabela consolidadora (fundos coletivos)
            
        explosao_gestores_alternativos = pd.DataFrame.from_dict(posicao_gestores_alternativos)
        explosao_gestores_rv = pd.DataFrame.from_dict(posicao_gestores_rv)
        explosao_gestores_rvinternacional= pd.DataFrame.from_dict(posicao_produtos_rvinternacional)
        explosao_gestores_rvglobal= pd.DataFrame.from_dict(posicao_produtos_rvglobal)
        explosao_produtos_rvlocal= pd.DataFrame.from_dict(posicao_produtos_rvlocal)
        explosao_gestores = pd.merge(right = explosao_gestores_alternativos, left = explosao_gestores_rv, on='NomeContaCRM',how='outer')
        explosao_gestores = pd.merge(right = explosao_gestores_rvinternacional, left = explosao_gestores, on='NomeContaCRM',how='outer')
        explosao_gestores = pd.merge(right = explosao_gestores_rvglobal, left = explosao_gestores, on='NomeContaCRM',how='outer')
        explosao_gestores = pd.merge(right = explosao_produtos_rvlocal, left = explosao_gestores, on = 'NomeContaCRM', how='outer')
        fundos_coletivos = pd.merge(right = explosao_gestores, left = fundos_coletivos, on = 'NomeContaCRM', how='left')    
        
        #Traz o somatório segregado por classe/subclasse de cada fundo coletivo.
        teste = teste[['NomeContaCRM','SubClasse','Classe','stress_adj','PesoFinal','peso_adj_delta','premio_opção']]
        posicao_classe = teste.groupby(by=['NomeContaCRM','Classe']).sum().reset_index()
        posicao_subclasse = teste.groupby(by=['NomeContaCRM','SubClasse']).sum().reset_index()
        posicao_classe['Posicao_opcoes'] = abs(posicao_classe['peso_adj_delta']-posicao_classe['PesoFinal'])
        posicao_subclasse['seg'] = posicao_subclasse['SubClasse'].apply(lambda x: x.replace(' Estr','').replace(' Bancos','Créd').replace('Caixa','RF Pos Liquidez'))
        
        #Ajuste de quantos contratos futuroes devem ser comprados ou vendidos para RV Local
        posicao_rv_local = posicao_classe[posicao_classe['Classe']=='R Variável']
        posicao_rv_local = pd.merge(left = posicao_rv_local, right = pls, on = 'NomeContaCRM', how='left')
        posicao_rv_local['operacao_cts_futuros_rv_local_padrao'] = posicao_rv_local.apply(lambda row: round((100-row['peso_adj_delta'])/100*row['PL']/fut_rv_local/5,0)*5,axis =1)
        posicao_rv_local['operacao_cts_futuros_rv_local_mini'] = posicao_rv_local.apply(lambda row: round((((100-row['peso_adj_delta'])/100*row['PL']/fut_rv_local)-row['operacao_cts_futuros_rv_local_padrao'])*5,0),axis =1)
        posicao_rv_local['operacao_cts_futuros_rv_local_padrao'] = posicao_rv_local['operacao_cts_futuros_rv_local_padrao'].apply(lambda x: f'Comprar {abs(x)} cts' if x >0 else  f'Vender {abs(x)} cts')
        posicao_rv_local['operacao_cts_futuros_rv_local_mini']  = posicao_rv_local['operacao_cts_futuros_rv_local_mini'].apply(lambda x: f'Comprar {abs(x)} cts' if x >0 else  f'Vender {abs(x)} cts')
        posicao_rv_local=posicao_rv_local[['NomeContaCRM','operacao_cts_futuros_rv_local_padrao','operacao_cts_futuros_rv_local_mini']]
        fundos_coletivos = pd.merge(right = posicao_rv_local, left = fundos_coletivos, on = 'NomeContaCRM', how='left') 
        
        #Traz a quebra (percentual) de alocação por classe de ativo
        explosão_rf=[]
        for fundo in fundos:
            dicio = {}
            credito_pos = posicao_subclasse[(posicao_subclasse['seg']=='RF Pos Créd')&(posicao_subclasse['NomeContaCRM']==fundo)] 
            credito_pos = credito_pos['PesoFinal'].sum()
            credito_pos = round(credito_pos,2)
            liquidez_pos = posicao_subclasse[(posicao_subclasse['seg']=='RF Pos Liquidez')&(posicao_subclasse['NomeContaCRM']==fundo)] 
            liquidez_pos = liquidez_pos['PesoFinal'].sum()
            liquidez_pos = round(liquidez_pos,2)                        
            credito_inflacao = posicao_subclasse[(posicao_subclasse['seg']=='RF Infl Créd')&(posicao_subclasse['NomeContaCRM']==fundo)] 
            credito_inflacao = credito_inflacao['PesoFinal'].sum()
            credito_inflacao = round(credito_inflacao,2)
            soberana_inflacao = posicao_subclasse[(posicao_subclasse['seg']=='RF Infl Soberano') &(posicao_subclasse['NomeContaCRM']==fundo)] 
            soberana_inflacao = soberana_inflacao['PesoFinal'].sum()
            soberana_inflacao = round(soberana_inflacao,2)            
            dicio['NomeContaCRM'] = fundo.lower()
            dicio['% RF Infl'] = f'Crédito Inflação é {credito_inflacao} e Inflação Soberana é {soberana_inflacao}'
            dicio['% RF Pos'] = f'Crédito Pós é { credito_pos} e Pós Liquidez é {liquidez_pos}'
            explosão_rf.append(dicio)
        explosão_rf = pd.DataFrame.from_dict(explosão_rf)
        fundos_coletivos = pd.merge(right = explosão_rf, left = fundos_coletivos, on = 'NomeContaCRM', how='left')
        risco_ativo_resultado =  [risco_ativo(fundo,bench)[0] for fundo,bench in zip(lista_fundos, lista_bench)]
        tracking =  [risco_ativo(fundo,bench)[1] for fundo,bench in zip(lista_fundos, lista_bench)]
        fundos_coletivos['Risco_Ativo'] =  risco_ativo_resultado
        fundos_coletivos['TE'] = tracking 
        
        #wb = app.books.open(r'O:/SAO/CICH_All/Portfolio Management/01 - Rotinas Diarias/CW/Foto_portfolios.xlsb')
        wb = xw.Book(f'{ambiente.pasta_base_pm()}/01 - Rotinas Diarias/CW/Foto_portfolios.xlsm')
        
        #Incluindo na planilha os IMBA-Bs
        sheet = wb.sheets['IMAB']
        sheet["A1:D50000"].clear()
        sheet['A1'].value = IMAB_Consolidado
        
        
        #Incluindo na planilha as informações
        sheet = wb.sheets['Dados_consolidados_fundo']
        sheet["A1:O50000"].clear()
        sheet['A1'].value = fundos_coletivos
        
        #Incluindo na planilha as informações
        sheet = wb.sheets['posicao_classe']
        sheet["A1:O50000"].clear()
        sheet['A1'].value = posicao_classe
        
        #Incluindo na planilha as informações
        sheet = wb.sheets['posicao_subclasse']
        sheet["A1:O50000"].clear()
        sheet['A1'].value = posicao_subclasse                            
    
        #Incluindo na planilha as exposições do Juros Ativo
        sheet = wb.sheets['Resumo']
        sheet['I37'].value = te.calculo_tracking_juros_ativo(self,peso_atual = 0.3)
                
        
        #Salvando a imagem         
        sheet = wb.sheets['Resumo']
        sheet.range('Portfolio').api.CopyPicture(Appearance=2)
        sheet.api.Paste()
        pic=sheet.pictures[0]
        pic.api.Copy()
                
        path = 'C:/Temp/Foundation/Foto.png'
                
        try:
            os.remove(path)
        except:
            pass
                
        from PIL import ImageGrab
        img = ImageGrab.grabclipboard()
        img.save('C:/Temp/Foundation/Foto.png')
        pic.delete()            
        
        #Salvando e fechando a rotina        
        wb.save()
        wb.close()
                
        #Envia Email
        subject='Foto dos Portfolios'
        to=['tamara.alves@jbfo.com']
        to=['portfolio@jbfo.com','julio.ferreira@jbfo.com','andre.szasz@jbfo.com','augusto.almeida@jbfo.com','tiago.marchiore@jbfo.com','ricardo.gaspar@jbfo.com','vitor.edo@jbfo.com','felipe.takeshi@jbfo.com','investimentos@jbfo.com']
        list_path_figures = 'C:/Temp/Foundation/Foto.png'
        text = '''Segue composição dos veículos coletivos:
                
        '''
        from emailer import Email
        email = Email(to = to , subject = subject, text= text,send = True,list_path_figures = list_path_figures)
        
        
class RelatorioCIP:
    
    def __init__(self, homologacao=False):
        self.dm1 = PosicaoDm1(homologacao=homologacao)
        self.pasta = f'{ambiente.pasta_base_pm()}/06 - Relatório/EnquadramentoCIP/'
    
    def rodar(self):        
        data = self.dm1.banco.hoje()
        df = self.dm1.cip_enquadramento_conta_crm()
        arquivo = f"enquadramentocip_{data.strftime('%Y%m%d')}"
        df.to_excel(f"{self.pasta}{arquivo}.xlsx", index=False)
        return True


class AnalisePortfolios:

        def __init__(self, homologacao=False):
            self.dm1 = PosicaoDm1Pickle(homologacao=homologacao)
        
        def arquivo_dados_realocacao(self, lista_sc:list, caminho:str):
            supercarteiras = {}
            fundos = {}
            lista_fundos = []
            # Busca Supercarteiras
            for nome in lista_sc:
                supercarteiras[nome] = Supercarteira(nome, base_dm1=self.dm1)
                origens = supercarteiras[nome].posexp['Origem'].unique()
                for x in origens:
                    if x != 'CartAdm' and not x in lista_fundos:
                        lista_fundos.append(x)
            # Busca Fundos            
            for nome in lista_fundos:
                fundos[nome] = Fundo(nome, base_dm1=self.dm1)
            
            # Monta Exportação
            alocacao_exp = []
            sc_pos = []
            sc_pos_exp = []
            for nome in lista_sc:
                # Alocação
                df = supercarteiras[nome].asset_class_allocation()
                df.insert(0, 'SC', [nome]*len(df))
                alocacao_exp.append(df)
                # Posição não-explodida    
                sc_pos.append(supercarteiras[nome].pos)
                # Posição explodida
                sc_pos_exp.append(supercarteiras[nome].posexp)
                
            fundos_pos = []
            for nome in lista_fundos:
                fundos_pos.append(fundos[nome].pos)
                
            alocacao_exp = pd.concat(alocacao_exp)
            sc_pos = pd.concat(sc_pos)
            sc_pos_exp = pd.concat(sc_pos_exp)
            fundos_pos = pd.concat(fundos_pos)
            
            # Escreve Arquivos
            with pd.ExcelWriter(caminho, engine='xlsxwriter') as writer:
                wb = writer.book
                f1 = wb.add_format()
                f1.set_num_format('#,##0;(#,##0);-_)')                
            
                alocacao_exp.to_excel(writer, 'alocacao_exp', index=False)
                sc_pos.to_excel(writer, 'sc_pos', index=False)
                sc_pos_exp.to_excel(writer, 'sc_pos_exp', index=False)
                fundos_pos.to_excel(writer, 'fundos_pos', index=False)
        

if __name__ == '__main__':
    print(datetime.datetime.now())
    RelatorioEnquadramento(rodar=True, filtro='Officer', verbose=True)
    # z = ResumoGestao()
    # z.foto_portfolio()
    #z.calculo_delta_rv_tatico()
    # RelatorioLiquidez(rodar=True)
    # RelatorioTableau(rodar=True)
    # r = RelatorioExplosao(rodar=False, por_titularidade=False)
    # r.rodar(True)
    
    # AnalisePortfolios().arquivo_dados_realocacao(lista_sc=['AgilistaGPS', 'DataGPS', 'AgilGPS'], caminho='O:/SAO/CICH_All/Portfolio Management/15 - WIP/Dex/datas.xlsx')
    # AnalisePortfolios().arquivo_dados_realocacao(lista_sc=['LapisCTGPS','LapisETGPS','LapisF','LapisG','LapisGPS','LapisHTotal','LapisJTGPS','LapisMAGPS','LapisMCGPS','LapisMGPS','LapisMIGPS','LapisR'], caminho='O:/SAO/CICH_All/Dedicated Portfolio Manager/Realocacao/Dados_LapisCTGPS.xlsx')
    print(datetime.datetime.now())
    print('teste')
    
