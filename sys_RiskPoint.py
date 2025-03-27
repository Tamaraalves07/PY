# -*- coding: utf-8 -*-

'''
RISK POINT PARA SUITABILITY: IMPLEMENTAÇÃO METODOLOGIA ANBIMA

A fazer:
     -FIP via nome
     -FIM CP via nome pode ser problema
     -Cotas Classe=Inv Exterior 
     
     -Incluir regras Art 62
     -Fluxo salvar Crédito e Base completa
     -Monitoramento de pontuação por email (gráficos, stats, novos cadastros, )

    -Colocar pontos no nome da regra RP


Descrição
.
.
.

Referências:
.
.
.

'''

# =============================================================================
# !!! --- | 1) Preparação de ambiente | ---------------------------------------
# =============================================================================

# Pacotes básicos
import pandas as pd
import numpy as np

from datetime import datetime

# Pacotes JB
import ambiente
from databases import PosicaoDm1, CVM, BDS, Crm
from filemanager import FileMgmt #from objetos import Ativo

# =============================================================================
# !!! --- | 2) Inicializar Classe de Pontuação | ------------------------------
# =============================================================================

# Inicialização da classe
class RiskPoint:
    def __init__(self, homologacao=False, path_cred=None):
        self.homologacao = homologacao
        self.produtos = None
        
        # Instanciar objetos necessários para extração de dados
        self.dm1 = PosicaoDm1(homologacao= self.homologacao)
        self.crm = Crm(homologacao= self.homologacao)
        
        self.bds = BDS(homologacao= self.homologacao)
        self.cvm = CVM(homologacao= self.homologacao)
        
        # Verificação de ambiente novo ou antigo
        pasta = f'{ambiente.pasta_base_pm()}/08 - Aplicativos/RiskPoint'
        fm = FileMgmt()
        if not fm.file_exists(pasta + '/Cadastro/cadastro_classe_anbima.xlsx'):
            pasta = r'\\filesvr\Investimentos\Aplicativos\RiskPoint'

        if path_cred is None:
            path_cred = pasta + r'\CONTROLE_RATING_CRED.xlsx'
        
        self.path_credito = path_cred
        
        # Leitura de dados de cadastro locais 
        self.cad_anbima = pd.read_excel(pasta + r'\Cadastro\cadastro_classe_anbima.xlsx')
        self.cad_anbima = self.cad_anbima.set_index('Classe')['Pontos']
    
        self.feriados = pd.read_excel(pasta + r'\Cadastro\feriados.xlsx')['Data'].dropna().tolist()
        
        # Leitura de dados de rating de crédito local
        # TODO: trocar para EUC de Cadastro de Crédito
        self.cad_cred_fin = pd.read_excel(path_cred, sheet_name= 'FIN')
        self.cad_cred_fin = self.cad_cred_fin.set_index('GuidEmissor')
    
        self.cad_cred_deb = pd.read_excel(path_cred, sheet_name= 'DEB')
        self.cad_cred_deb = self.cad_cred_deb.set_index('GuidProduto')
    
        self.cad_cred_fidc = pd.read_excel(path_cred, sheet_name= 'FIDC')
        self.cad_cred_fidc = self.cad_cred_fidc.set_index('GuidProduto')
        
# =============================================================================
# !!! --- | 3) Consolidação dos bandos de dados | -----------------------------
# =============================================================================
        
    # Gerar tabela consolidada com todos os produtos e informações necessárias
    def consolidar_produtos(self):
        
        # Ler tabelas da PO_Carteira para fundos e titularidades
        df_fundos = self.dm1.posicao_fundos(apenas_jbfo= False, trazer_colunas= True)
        df_titu = self.dm1.posicao_titularidades(trazer_colunas= True)
        
        self.pos_fundos = df_fundos.copy()
        self.df_titu = df_titu.copy()
        
        # Ajustar linhas únicas dos produtos
        adj_fundos = df_fundos.drop(columns= ['NomeContaCRM','QuantidadeFinal','GuidGestor','CodFundosJBFO','PesoFinal']).copy()
        adj_titu = df_titu.drop(columns= ['Titularidade','TitularidadeGuid','QuantidadeFinal']).copy()
        
        df_prod = pd.concat([adj_fundos,adj_titu])
        df_prod = df_prod.sort_values(by= ['GuidProduto','DataVencimento'])
        df_prod = df_prod.drop_duplicates(subset= ['GuidProduto'], keep= 'first')
        
        # Obter classificação Anbima para fundos (exc. FII, FIDC, FIP)
        depara_bds = self.bds.fundos_equivalencia_cvm_anbima().astype(str)
        
        depara_cvm = depara_bds.copy().sort_values(by= 'ANBIMA')
        depara_cvm = depara_cvm.loc[~depara_cvm['CVM'].duplicated(keep='first')]
        depara_cvm = depara_cvm.set_index('CVM')[['CNPJ']]
        
        depara_anb = depara_bds.copy().sort_values(by= 'CVM')
        depara_anb = depara_anb.loc[~depara_anb['ANBIMA'].duplicated(keep='first')]
        depara_anb = depara_anb.set_index('ANBIMA')[['CNPJ']]        
        
        cad_fundos_cvm = self.cvm.cadastro_fundos_total()
        cad_fundos_cvm['CNPJ'] = ('00000000000000'+cad_fundos_cvm['CNPJ']).str[-14:]
        cad_fundos_cvm = cad_fundos_cvm.sort_values(['CNPJ','IDFdCVM'])
        cad_fundos_cvm = cad_fundos_cvm.drop_duplicates(subset= ['CNPJ'], keep= 'last')
        
        find_cvm = df_prod.join(depara_cvm, on= 'IdBDS')['CNPJ']
        find_anb = df_prod.join(depara_anb, on= 'IdBDS')['CNPJ']
        
        best_cnpj = pd.concat([find_cvm,find_anb], axis= 1).bfill(axis= 1).iloc[:,0]
        best_cnpj  = best_cnpj.str.replace('\D+', '', regex= True)
        df_prod['CNPJ'] = best_cnpj
        
        retrieve = ['FORMA_DE_CONDOMINIO', 'FUNDO_DE_COTAS', 'TRATAMENTO_TRIBUTARIO_DE_LONGO_PRAZO', 'FUNDO_EXCLUSIVO', 'TipoANBIMA']
        df_prod = df_prod.join(cad_fundos_cvm.set_index('CNPJ')[retrieve], on= 'CNPJ')
        
        # Obter tipo de tributação
        df_trib = self.dm1.ativos_tributacao()
        df_prod['TipoTrib'] = df_trib['TipoTrib'].reindex(df_prod['GuidProduto']).values
        
        # Obter informações do CRM para complementar view
        infos_crm = self.crm.product_for_suitability()
        df_prod = df_prod.join(infos_crm.set_index('productid'), on= 'GuidProduto')
        
        infos_fii = self.bds.consulta_ativos_FII()
        infos_fii = infos_fii.set_index('productid')['new_indgroupaxysidname'].rename('IsImob').notnull() *1
        df_prod = df_prod.join(infos_fii, on= 'GuidProduto')        
        
        # Ajustar indexador e subclasses
        df_prod['IndexadorMelhor'] = df_prod[['Indexador','crm_bench']].bfill(axis= 1).iloc[:,0].replace({'?': None})  
        df_prod['SubClasse'] = df_prod[['SubClasse','Subclasse']].bfill(axis= 1).iloc[:,0]
        
        df_prod = df_prod.reset_index(drop= True)
        
        # Ajustar tipo de produto para encontrar de forma simples FII, FIDC e FIP
        for ind,row in df_prod.iterrows():
            
            #imobiliários
            if (row['Classe'].upper() == 'REAL ESTATE') and (row['TipoProduto'].upper() in ['FUNDO', 'COTAS']):
                df_prod.loc[ind,'TipoProduto'] = 'FII'
            elif row['IsImob'] == 1:
                df_prod.loc[ind,'TipoProduto'] = 'FII'
                
            #privados
            if (row['Classe'].upper() in ['P.EQUITY', 'P. EQUITY']) or ('ILÍQUIDO' in row['Classe'].upper()):
                df_prod.loc[ind,'TipoProduto'] = 'FIP'
        
            #FIDC
            if (row['crm_fidc'] == True) or (('FIDC' in row['NomeProduto'].upper().split()) and row['TipoProduto'].upper() in ['FUNDO', 'COTAS']):
                df_prod.loc[ind,'TipoProduto'] = 'FIDC'
        
        # Incorporar informações de rating salvos em cadastro local 
        df_prod = df_prod.join(self.cad_cred_fin.add_suffix('FIN'), on= 'GuidEmissor')
        df_prod = df_prod.join(self.cad_cred_deb.add_suffix('DEB'), on= 'GuidProduto')
        df_prod = df_prod.join(self.cad_cred_fidc.add_suffix('FIDC'), on= 'GuidProduto')  
        
        df_prod['DataRatingALL'] = df_prod[['DataRatingFIN','DataRatingDEB','DataRatingFIDC']].bfill(axis= 1).iloc[:,0]
        df_prod['OrigemRatingALL'] = df_prod[['OrigemRatingFIN','OrigemRatingDEB','OrigemRatingFIDC']].bfill(axis= 1).iloc[:,0]
        df_prod['RatingALL'] = df_prod[['RatingFIN','RatingDEB','RatingFIDC']].bfill(axis= 1).iloc[:,0]

        rating_radical = df_prod['RatingALL'].apply(lambda x: ''.join([ch.upper() for ch in str(x) if ch.isalpha()]) if pd.notnull(x) else None)
        df_prod['GrupoRating'] = rating_radical.apply(lambda x: 'SEM RATING' if pd.isnull(x) else 'INVESTMENT GRADE' if x in ['AAA','AA','A','BBB'] else 'NON INVESTMENT GRADE')
        
        # Calcular prazo dos produtos
        holidays = [x.strftime('%Y-%m-%d') for x in self.feriados]
        wday_dur = lambda d: np.nan if pd.isna(d) else np.busday_count(datetime.today().date(), pd.to_datetime(d).date(), holidays= holidays)/252
        df_prod['DurationMaturidade'] = np.maximum(df_prod['DataVencimento'].apply(wday_dur),0)
        df_prod['DurationAno'] = df_prod['Duration']/252
        
        df_prod['VencimentoALL'] = df_prod[['DataVencimentoFIDC','DataVencimento']].bfill(axis= 1).iloc[:,0]
        
        # Obter duration para FIDCs (cadastro de duration errada para esses ativos) --> será somente via maturidade
        # E combinar as Durations em coluna única
        df_prod['DurationCombinar'] = df_prod[['DurationAno','DurationMaturidade']].bfill(axis= 1).iloc[:,0]
        df_prod['DurationFIDC'] = np.maximum(df_prod['DataVencimentoFIDC'].apply(wday_dur),0)
        
        df_prod['DurationALL'] = df_prod.apply(lambda row: row['DurationFIDC'] if row['TipoProduto'] == 'FIDC' else row['DurationCombinar'], axis= 1)
        df_prod['DurationALL'] = np.maximum(df_prod['DurationALL'],0) 

        # Tratar tipo de cotas de FIDCs
        def tratar_str_cota(s):
            sa = str(s).upper().replace('.',' ').split()
            if len(set(['SR','SEN','SENIOR']) & set(sa)) > 0:
                return 'SEN'
            elif len(set(['MZ','MEZ','MEZANINO']) & set(sa)) > 0:
                return 'MEZ'
            else:
                return np.nan
        
        def tratar_mnem_cota(s):
            if any([(x in str(s)) for x in ['SEN','SR','SN']]):
                return 'SEN'
            elif any([(x in str(s)) for x in ['MEZ','MZ']]):
                return 'MEZ'
            else:
                return np.nan
                
        trat_nome = df_prod['NomeProduto'].apply(tratar_str_cota)
        trat_mnem = df_prod['crm_abrev'].apply(tratar_mnem_cota)
        
        df_prod['TipoCotaFIDC'] = pd.concat([trat_nome,trat_mnem],axis=1).bfill(axis= 1).iloc[:,0]
        
        # Organização final somente com informações necessárias no processo 
        ord_prod = df_prod.copy()
        ord_prod = ord_prod.sort_values(by= ['TipoProduto','GuidProduto'])
        
        selected_cols = ['GuidProduto','IdProdutoProfitSGI','IdBDS','crm_isin','NomeProduto',
                         'TipoProduto','Classe','SubClasse','TipoANBIMA','IndexadorMelhor','TipoTrib',
                         'GuidEmissor','NomeEmissor', 'VencimentoALL','DurationALL','DataRatingALL','OrigemRatingALL','RatingALL','GrupoRating','crm_recomend','TipoCotaFIDC']
        
        rename_cols = ['GuidProduto','IdProdutoSGI','IdBDS','ISIN','NomeProduto',
                       'TipoProduto','Classe','SubClasse','TipoANBIMA','Indexador','TipoTribut',
                       'GuidEmissor','NomeEmissor','DataVencimento','Duration','DataRating','OrigemRating','Rating','GrupoRating','TipoRecomend','TipoCotaFIDC']
        
        ord_prod = ord_prod[selected_cols].copy()
        ord_prod.columns = rename_cols
        
        # Salvar 
        ord_prod = ord_prod.reset_index(drop= True)
        self.todos_produtos = ord_prod.copy()
        
# =============================================================================
# !!! --- | 4) Fluxo de Export. Dados de Crédito | ----------------------------
# =============================================================================
        
    def exportar_infos_credito(self):

        return None
    
    


        
# =============================================================================
# !!! --- | 5) Definição das regras | -----------------------------------------
# =============================================================================
        
        
        
        
        
        
        
        
        
        
        
        
# =============================================================================
# !!! --- | 6) Iteração nos produtos | ----------------------------------------
# =============================================================================
        
    # Calcular pontos de risco para base inteira (RPA)
    def calcular_pontos_anbima(self):
        
        # Inicializar tabela 
        df = self.todos_produtos.copy()
        
        df['RegraRPA'] = np.nan #descritivo da regra de origem
        df['ConfRPA'] = np.nan #nível de confiança nessa pontuação, 1-worst 5-best
        df['RiskPointAnbima'] = np.nan #pontuação (regras Anbima)
        
        # Iterar nas linhas e aplicar cada regra 
        for ind,row in df.iterrows():
            tem_valor = row.notnull()
            dur = row['Duration']
            
            # !!! Regra 1 - Emissões financeiras (exc.: LF, LFS)
            if str(row['TipoProduto']).upper() in ['CDB', 'COMP', 'DPGE', 'LC', 'LCA', 'LCI', 'LIG']:
                
                if row['GrupoRating'] == 'INVESTMENT GRADE': #investment grade cadastrado
                    df.loc[ind,'RegraRPA'] = 'RatingCadastrado'
                    df.loc[ind,'ConfRPA'] = 5    
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 0.75 if dur<2 else 1 if dur<4 else 1.25 if dur<6 else 1.75 if dur<8 else 2
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 1 if dur<2 else 1.5 if dur<4 else 2 if dur<6 else 2.5 if dur<8 else 3
                
                elif row['GrupoRating'] == 'NON INVESTMENT GRADE': #high yield cadastrado
                    df.loc[ind,'RegraRPA'] = 'RatingCadastrado'
                    df.loc[ind,'ConfRPA'] = 5    
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 1.75 if dur<2 else 2 if dur<4 else 2.25 if dur<6 else 2.75 if dur<8 else 3
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 2 if dur<2 else 2.25 if dur<4 else 2.75 if dur<6 else 3.5 if dur<8 else 4
                
                elif str(row['TipoRecomend']).upper() == 'RECOMENDADO': #assumir investment grade nos cadastros
                    df.loc[ind,'RegraRPA'] = 'RecomendadoJBFO'
                    df.loc[ind,'ConfRPA'] = 4   
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 0.75 if dur<2 else 1 if dur<4 else 1.25 if dur<6 else 1.75 if dur<8 else 2
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 1 if dur<2 else 1.5 if dur<4 else 2 if dur<6 else 2.5 if dur<8 else 3
                
                else: #assumir HY resto sem rating
                    df.loc[ind,'RegraRPA'] = 'SemRating'
                    df.loc[ind,'ConfRPA'] = 3
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 1.75 if dur<2 else 2 if dur<4 else 2.25 if dur<6 else 2.75 if dur<8 else 3
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 2 if dur<2 else 2.25 if dur<4 else 2.75 if dur<6 else 3.5 if dur<8 else 4
                
            # !!! Regra 2 - Letras Financeiras Classe Sênior (LF)
            if str(row['TipoProduto']).upper() == 'LF':
                
                if row['GrupoRating'] == 'INVESTMENT GRADE': #investment grade cadastrado
                    df.loc[ind,'RegraRPA'] = 'RatingCadastrado'
                    df.loc[ind,'ConfRPA'] = 5    
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 0.75 if dur<2 else 1 if dur<4 else 1.5 if dur<6 else 2 if dur<8 else 2.25
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 1 if dur<2 else 1.5 if dur<4 else 2 if dur<6 else 2.75 if dur<8 else 3.25
                
                elif row['GrupoRating'] == 'NON INVESTMENT GRADE': #high yield cadastrado
                    df.loc[ind,'RegraRPA'] = 'RatingCadastrado'
                    df.loc[ind,'ConfRPA'] = 5    
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 2 if dur<2 else 2.25 if dur<4 else 2.75 if dur<6 else 3 if dur<8 else 3.5
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 2.25 if dur<2 else 2.75 if dur<4 else 3.25 if dur<6 else 3.75 if dur<8 else 4.25
                
                elif str(row['TipoRecomend']).upper() == 'RECOMENDADO': #assumir investment grade nos cadastros
                    df.loc[ind,'RegraRPA'] = 'RecomendadoJBFO'
                    df.loc[ind,'ConfRPA'] = 4   
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 0.75 if dur<2 else 1 if dur<4 else 1.5 if dur<6 else 2 if dur<8 else 2.25
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 1 if dur<2 else 1.5 if dur<4 else 2 if dur<6 else 2.75 if dur<8 else 3.25
                        
                else: #assumir HY resto sem rating
                    df.loc[ind,'RegraRPA'] = 'SemRating'
                    df.loc[ind,'ConfRPA'] = 3
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 2 if dur<2 else 2.25 if dur<4 else 2.75 if dur<6 else 3 if dur<8 else 3.5
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 2.25 if dur<2 else 2.75 if dur<4 else 3.25 if dur<6 else 3.75 if dur<8 else 4.25
                
            # !!! Regra 3 - Letras Financeiras Classe Subordinada (LFS)
            if str(row['TipoProduto']).upper() == 'LFS':
                
                if row['GrupoRating'] == 'INVESTMENT GRADE': #investment grade cadastrado
                    df.loc[ind,'RegraRPA'] = 'RatingCadastrado'
                    df.loc[ind,'ConfRPA'] = 5    
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 1.75 if dur<2 else 2 if dur<4 else 2.25 if dur<6 else 2.75 if dur<8 else 3
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 2 if dur<2 else 2.5 if dur<4 else 3 if dur<6 else 3.5 if dur<8 else 4
                
                elif row['GrupoRating'] == 'NON INVESTMENT GRADE': #high yield cadastrado
                    df.loc[ind,'RegraRPA'] = 'RatingCadastrado'
                    df.loc[ind,'ConfRPA'] = 5    
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 2.75 if dur<2 else 3 if dur<4 else 3.5 if dur<6 else 3.75 if dur<8 else 4
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 3.25 if dur<2 else 3.5 if dur<4 else 4 if dur<6 else 4.5 if dur<8 else 4.75
                
                elif str(row['TipoRecomend']).upper() == 'RECOMENDADO': #assumir investment grade nos cadastros
                    df.loc[ind,'RegraRPA'] = 'RecomendadoJBFO'
                    df.loc[ind,'ConfRPA'] = 4   
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 1.75 if dur<2 else 2 if dur<4 else 2.25 if dur<6 else 2.75 if dur<8 else 3
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 2 if dur<2 else 2.5 if dur<4 else 3 if dur<6 else 3.5 if dur<8 else 4
                
                else: #assumir HY resto sem rating
                    df.loc[ind,'RegraRPA'] = 'SemRating'
                    df.loc[ind,'ConfRPA'] = 3
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 2.75 if dur<2 else 3 if dur<4 else 3.5 if dur<6 else 3.75 if dur<8 else 4
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 3.25 if dur<2 else 3.5 if dur<4 else 4 if dur<6 else 4.5 if dur<8 else 4.75
                    
            # !!! Regra 4 - Títulos de Crédito não-financeiros
            if str(row['TipoProduto']).upper() in ['DEB', 'DEBI', 'CRI', 'CRA']:
                
                if row['GrupoRating'] == 'INVESTMENT GRADE': #investment grade cadastrado
                    df.loc[ind,'RegraRPA'] = 'RatingCadastrado'
                    df.loc[ind,'ConfRPA'] = 5    
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 1 if dur<2 else 1.25 if dur<4 else 1.75 if dur<6 else 2.25 if dur<8 else 2.75
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 1.25 if dur<2 else 1.75 if dur<4 else 2.25 if dur<6 else 3 if dur<8 else 3.5
                
                elif row['GrupoRating'] == 'NON INVESTMENT GRADE': #high yield cadastrado
                    df.loc[ind,'RegraRPA'] = 'RatingCadastrado'
                    df.loc[ind,'ConfRPA'] = 5    
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 3.5
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 4.25
                
                elif str(row['TipoRecomend']).upper() == 'RECOMENDADO': #assumir investment grade nos cadastros
                    df.loc[ind,'RegraRPA'] = 'RecomendadoJBFO'
                    df.loc[ind,'ConfRPA'] = 4   
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 1 if dur<2 else 1.25 if dur<4 else 1.75 if dur<6 else 2.25 if dur<8 else 2.75
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 1.25 if dur<2 else 1.75 if dur<4 else 2.25 if dur<6 else 3 if dur<8 else 3.5
                
                else: #assumir HY resto sem rating
                    df.loc[ind,'RegraRPA'] = 'SemRating'
                    df.loc[ind,'ConfRPA'] = 3
                    if tem_valor['Indexador']: #pos-fixados
                        df.loc[ind,'RiskPointAnbima'] = 3.5
                    else: #pre-fixados
                        df.loc[ind,'RiskPointAnbima'] = 4.25
                    
            # !!! Regra 5 - Títulos Públicos LFT
            if str(row['TipoProduto']).upper() == 'LFT':
                df.loc[ind,'RegraRPA'] = 'TituloPub'
                df.loc[ind,'ConfRPA'] = 5
                df.loc[ind,'RiskPointAnbima'] = 0.5
            
            # !!! Regra 6 - Títulos Públicos Demais
            if str(row['TipoProduto']).upper() in ['LTN', 'NTN-B', 'NTN-C', 'NTN-F']:
                df.loc[ind,'RegraRPA'] = 'TituloPub'
                df.loc[ind,'ConfRPA'] = 5
                df.loc[ind,'RiskPointAnbima'] = 1 if dur<3 else 1.75 if dur<10 else 2.75
                    
            # !!! Regra 7 - FIIs
            if str(row['TipoProduto']).upper() == 'FII':
                if 'RENDA' in str(row['SubClasse']).upper():
                    df.loc[ind,'RegraRPA'] = 'TextoFII'
                    df.loc[ind,'ConfRPA'] = 4
                    df.loc[ind,'RiskPointAnbima'] = 3
                    
                elif ('DESENV' in str(row['SubClasse']).upper()) or ('IMOB' in str(row['SubClasse']).upper()):
                    df.loc[ind,'RegraRPA'] = 'TextoFII'
                    df.loc[ind,'ConfRPA'] = 4
                    df.loc[ind,'RiskPointAnbima'] = 4.5
                    
                else:
                    df.loc[ind,'RegraRPA'] = 'DemaisFII'
                    df.loc[ind,'ConfRPA'] = 3
                    df.loc[ind,'RiskPointAnbima'] = 3
                    
            # !!! Regra 8 - Fundos com classe Anbima cadastrada
            if tem_valor['TipoANBIMA']:
                if row['TipoANBIMA'] in self.cad_anbima.index:
                    df.loc[ind,'RegraRPA'] = 'FundoClasseAnbima'
                    df.loc[ind,'ConfRPA'] = 5
                    df.loc[ind,'RiskPointAnbima'] = self.cad_anbima[row['TipoANBIMA']]
                else:
                    print('|---> Tipo Anbima sem correspondência:', row['TipoANBIMA'])
            
            # !!! Regra 9 - FIPs
            if str(row['TipoProduto']).upper() == 'FIP':
                df.loc[ind,'RegraRPA'] = 'FIPviaClasse'
                df.loc[ind,'ConfRPA'] = 4
                df.loc[ind,'RiskPointAnbima'] = 4.5    
            
            # !!! Regra 10 - Ações e Derivativos
            if str(row['TipoProduto']).upper() in ['ACOES','AÇOES','BOLSA','FUT','OPT']:
                df.loc[ind,'RegraRPA'] = 'AcoesEDeriv'
                df.loc[ind,'ConfRPA'] = 4
                df.loc[ind,'RiskPointAnbima'] = 4
            
            # !!! Regra 11 - FIDCs
            if str(row['TipoProduto']).upper() == 'FIDC':
                rating =  ''.join([ch for ch in str(row['Rating']).upper() if ch.isalpha()])
                cota = row['TipoCotaFIDC']
                
                if cota == 'SEN': #grade da cota senior
        
                    if rating in ['AAA', 'AA']: #top grade cadastrado
                        df.loc[ind,'RegraRPA'] = 'RatingCadastrado'
                        df.loc[ind,'ConfRPA'] = 4.5

                        if (row['Classe'].upper() in ['R FIXA PÓS', 'R FIXA INFL']) or tem_valor['Indexador']: #pos-fixados
                            df.loc[ind,'RiskPointAnbima'] = 1.25 if dur<=2 else 1.5 if dur<=4 else 2 if dur<=6 else 2.5 if dur<=8 else 3
                        else: #pre-fixados
                            df.loc[ind,'RiskPointAnbima'] = 1.5 if dur<=2 else 2 if dur<=4 else 2.5 if dur<=6 else 3.25 if dur<=8 else 3.75

                    elif rating in ['A', 'BBB']: #mid grade cadastrado
                       df.loc[ind,'RegraRPA'] = 'RatingCadastrado'
                       df.loc[ind,'ConfRPA'] = 4.5
                    
                       if (row['Classe'].upper() in ['R FIXA PÓS', 'R FIXA INFL']) or tem_valor['Indexador']: #pos-fixados
                           df.loc[ind,'RiskPointAnbima'] = 1.5 if dur<=2 else 1.75 if dur<=4 else 2.25 if dur<=6 else 2.75 if dur<=8 else 3.25
                       else: #pre-fixados
                           df.loc[ind,'RiskPointAnbima'] = 1.75 if dur<=2 else 2.25 if dur<=4 else 2.75 if dur<=6 else 3.5 if dur<=8 else 4

                    else: #demais ratings/sem rating
                        if not pd.isna(rating):
                            df.loc[ind,'RegraRPA'] = 'RatingCadastrado'
                            df.loc[ind,'ConfRPA'] = 4.5
                        else:
                            df.loc[ind,'RegraRPA'] = 'SemRating'
                            df.loc[ind,'ConfRPA'] = 3

                        if (row['Classe'].upper() in ['R FIXA PÓS', 'R FIXA INFL']) or tem_valor['Indexador']: #pos-fixados
                            df.loc[ind,'RiskPointAnbima'] = 4
                        else: #pre-fixados
                            df.loc[ind,'RiskPointAnbima'] = 4.5
                        
                else: #grade da cota subordinada/mezanino ou não descoberto
                    
                    if rating in ['AAA', 'AA', 'A', 'BBB']: #investment grade cadastrado
                        df.loc[ind,'RegraRPA'] = 'RatingCadastrado'
                        df.loc[ind,'ConfRPA'] = 4.5

                        if (row['Classe'].upper() in ['R FIXA PÓS', 'R FIXA INFL']) or tem_valor['Indexador']: #pos-fixados
                            df.loc[ind,'RiskPointAnbima'] = 2.5 if dur<=2 else 2.75 if dur<=4 else 3.25 if dur<=6 else 3.75 if dur<=8 else 4.25
                        else: #pre-fixados
                            df.loc[ind,'RiskPointAnbima'] = 2.75 if dur<=2 else 3.25 if dur<=4 else 3.75 if dur<=6 else 4.5 if dur<=8 else 4.5
                
                    else: #demais ratings ou sem rating
                        if not pd.isna(rating):
                            df.loc[ind,'RegraRPA'] = 'RatingCadastrado'
                            df.loc[ind,'ConfRPA'] = 4.5
                        else:
                            df.loc[ind,'RegraRPA'] = 'SemRating'
                            df.loc[ind,'ConfRPA'] = 3

                        df.loc[ind,'RiskPointAnbima'] = 4.5
                        
                    if cota != 'MEZ': #reduzir confiança caso não tenha encontrado que é mezanino
                        df.loc[ind,'RegraRPA'] += 'SemCota'
                        df.loc[ind,'ConfRPA'] -= 1
                        
            # !!! Regra 12 - Demais fundos sem cadastro Anbima (escapes)
            if (not tem_valor['TipoANBIMA']) and (row['TipoProduto'].upper() in ['FUNDO', 'COTAS']):
                nome_quebra = row['NomeProduto'].upper().replace('.',' ').split()
                
                if (len(set(['FIA']) & set(nome_quebra)) > 0) or (len(set(['FIFA']) & set(nome_quebra)) > 0): #FIA
                    df.loc[ind,'RegraRPA'] = 'FundoviaNome'
                    df.loc[ind,'ConfRPA'] = 2
                    df.loc[ind,'RiskPointAnbima'] = 3.5
                    
                elif len(set(['FIM', 'MULTIMERCADO', 'MM']) & set(nome_quebra)) > 0: #FIM
                    df.loc[ind,'RegraRPA'] = 'FundoviaNome'
                    df.loc[ind,'ConfRPA'] = 2
                    df.loc[ind,'RiskPointAnbima'] = 2.5
            
                elif len(set(['RF', 'SELIC', 'CDI', 'FIXA', 'IPCA']) & set(nome_quebra)) > 0: #RF
                    df.loc[ind,'RegraRPA'] = 'FundoviaNome'
                    df.loc[ind,'ConfRPA'] = 2
                    df.loc[ind,'RiskPointAnbima'] = 2
            
            # !!! Regra de escape - Caixa e Risk Point ainda não preenchido
            if str(row['TipoProduto']).upper() == 'CAIXA':
                df.loc[ind,'RegraRPA'] = 'Caixa'
                df.loc[ind,'ConfRPA'] = 5
                df.loc[ind,'RiskPointAnbima'] = 0.5

            if str(row['TipoProduto']).upper() == 'AJUSTES':
                df.loc[ind,'RegraRPA'] = 'AjustesSemPontuacao'
                df.loc[ind,'ConfRPA'] = 1
                df.loc[ind,'RiskPointAnbima'] = np.nan
            
            if pd.isna(df.loc[ind,'RiskPointAnbima']):                
                df.loc[ind,'RegraRPA'] = 'Escape'
                df.loc[ind,'ConfRPA'] = 1
                df.loc[ind,'RiskPointAnbima'] = 5
            
        # !!! Regra de sobreposição - Fazer soma ponderada para os fundos administrados JBFO
        fundos = self.pos_fundos.copy()
        fundos = fundos.loc[fundos['GuidGestor'].isin(['85475fa5-e062-ea11-8e83-005056912b96','5a865d62-45fe-df11-bae8-d8d385b9752e', '2da233c3-44fe-df11-bae8-d8d385b9752e'])]
        
        df['RiskPointExplosao'] = df['RiskPointAnbima'].copy()
        
        for k in range(5):
            sub_rp = df[['GuidProduto','RiskPointExplosao']].set_index('GuidProduto').copy()
            
            sub_fundos = fundos.join(sub_rp, on= 'GuidProduto')
            sub_fundos['PesoFinal'] = sub_fundos['PesoFinal'].abs() #peso em módulo
            
            sub_fundos['Cont_RP'] = sub_fundos['PesoFinal'] * sub_fundos['RiskPointExplosao']
            fundos_rp = sub_fundos.groupby('CodFundosJBFO')['Cont_RP'].sum()
            
            fundos_rp = fundos_rp[fundos_rp >= 0.5]
            fundos_rp = fundos_rp[fundos_rp <= 5]
    
            for ind,row in df.iterrows():
                if row['IdProdutoSGI'] in fundos_rp.index:
                    df.loc[ind,'RegraRPA'] = 'ExplosaoRP'
                    df.loc[ind,'ConfRPA'] = 4
                    df.loc[ind,'RiskPointExplosao'] = fundos_rp[row['IdProdutoSGI']]
        
        #Compor pontuação com explosão
        df['RiskPointFinal'] = np.nan
        for ind,row in df.iterrows():
            if row['RegraRPA'] == 'ExplosaoRP':
                df.loc[ind,'RiskPointFinal'] = row['RiskPointExplosao']
            else:
                df.loc[ind,'RiskPointFinal'] = row['RiskPointAnbima']

        # Organizar tabela de saída
        df_out = df.copy()
        df_out = df_out.reset_index(drop= True)
        
        return df_out
    
    # Obter pontuação de risco via carteiras dos fundos explodida 
    def explodir_carteiras(self):
        return None
    
    # Combinar melhor pontuação dos produtos (mais rigorosa entre explosão x cadastro)
    def combinar_pontos(self):
        return None
    
    # Exportar pontos de risco atualizados (na PO_Ativos)
    def exportar_pontos(self, df):
        if 'GuidAtivo' not in df.columns:
            raise ValueError("'GuidAtivo' precisa estar na coluna da variável df")
            
        df = df[~df['GuidAtivo'].isin(['00000000-0000-0000-0000-000000000000', 
                                       'f578ce29-69de-dd11-b24f-000f1f6a9c1c'])]
        df = df.reset_index(drop= True)
        
        self.dm1.atualiza_cadastro_ativos(df_dados = df, 
                                          colunas_comparar= ['RiskRegra', 'RiskScore'])
    
# Classe agregadora para rodar o processo completo
class RodarRP:
    def __init__(self):
        self.metodoRP = RiskPoint()
        
    def rodar(self):
        # Agrupar informações dos produtos
        self.metodoRP.consolidar_produtos()
        
        # Calcular pontos
        df_rpa_hoje = self.metodoRP.calcular_pontos_anbima()
        
        # Exportar pontuação atualizada
        df = df_rpa_hoje[['GuidProduto','RegraRPA','RiskPointFinal']].copy()
        df.columns = ['GuidAtivo', 'RiskRegra', 'RiskScore']
        
        self.metodoRP.exportar_pontos(df)

if __name__ == '__main__':
    metodo = RodarRP()
    metodo.rodar()
    #metodo.consolidar_produtos()
    #df_rpa_hoje = metodo.calcular_pontos_anbima()
    
#     # Analisar resultados
#     import matplotlib.pyplot as plt
#     import seaborn as sns 

#     #1 - por classe
#     ranks = df_rpa_hoje.groupby('Classe')['RiskPointAnbima'].median().sort_values().index.tolist()    
    
#     fig = plt.figure(figsize=(14,9), dpi=300)
#     sns.countplot(data= df_rpa_hoje, y= 'Classe', order= ranks)
#     plt.show()
    
#     fig = plt.figure(figsize=(14,9), dpi=300)
#     sns.boxplot(data= df_rpa_hoje, x= 'RiskPointAnbima', y= 'Classe', order= ranks)
#     plt.show()
    
#     #2 - confianca
#     summ_conf = df_rpa_hoje.groupby('RegraRPA')['ConfRPA'].agg(['count','mean']).sort_values(by= ['count'], ascending= False)
    
#     fig, ax1 = plt.subplots(figsize=(15,8), dpi=300)

#     ax2 = ax1.twinx()
#     ax1.bar(x=summ_conf.index, height= summ_conf['count'].values, color='g')
#     ax2.plot(summ_conf.index, summ_conf['mean'], 'bo-')
    
#     ax1.set_xlabel('Regra de Pontuação')
#     ax1.set_ylabel('Qtde', color= 'g')
#     ax2.set_ylabel('Média Confiança', color= 'b')
    
#     plt.grid(b=None)
#     ax1.tick_params(axis='x', labelrotation=45)
#     plt.show()


    