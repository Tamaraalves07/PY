import datetime
from dateutil.relativedelta import relativedelta
from datetime import timedelta
import pandas as pd
import numpy as np
import pathlib
from glob import glob
import math

import ambiente
from ambiente import deprecated, teste_valor
from base_dados import BaseSQL

# Terceirização da classe Databases
from DB.crm import Crm as DBCrm
from DB.bawm import Bawm as DBBawm
from DB.bds import BDS as DBBds, BDSMem as DBBDSMem
from DB.base_extrato import BaseExtrato as DBBaseExtrato
from DB.boletador import Boletador as DBBoletador
from DB.posicaodm1 import PosicaoDm1 as DBPosicaoDm1, PosicaoDm1Pickle as DBPosicaoDm1Pickle

# Print Inicial
print('[databases.py directory]: {path}'.format(path=pathlib.Path(__file__).parent.absolute()))


class Crm(DBCrm):
    """
    Classe criada para intemediar interações com o CRM (GPS_MSCRM), no servidor Core.
    Esse pedaço do código foi movido para a pasta Foundation/DB/crm.py
    """
    def __init__(self, single_asset:bool=True, load_cadfundos:bool=False, buffer:bool=False, homologacao:bool=False):
        super().__init__(single_asset=single_asset, load_cadfundos=load_cadfundos, buffer=buffer, homologacao=homologacao)
        

class Bawm(DBBawm):
    """
    Classe criada para intemediar interações com a base BAWM (BAWM), no servidor Aux.
    Notar que há classes lidar com diversas subpartes da base (CW, PosicaoDm1, PosicaoAdmnistrador, etc)
    
    Esta classe foi criada para tratar com os sistemas mais antigos, de posicao de clientes (CL_*)
    e de dados históricos de ativos:
        CL_*: posições de clientes (e tratamento de posições de derivativos)
        FD_*: fundos domésticos
        IN_*: índices
        MD_*: moedas
        OF_*: fundos internacionais
        PA_*: renda fixa, renda variável e futuros
    """
    def __init__(self, buffer:bool=False, homologacao:bool=False):
        super().__init__(buffer=buffer, homologacao=homologacao)
    
    def cl_all_assets_atualizar_guid_derivativos(self):
        df_crm = Crm().derivativos_cadastro(apenas_nao_vencidos=True).reset_index().set_index('new_isin')
        
        # Futuros
        df = self.cl_all_assets_cadastro_futuros(apenas_sem_guid=True)
        cont = 0
        for idx, row in df.iterrows():
            if row['fISIN'] in df_crm.index:
                dicio = {'GPSGuid': str(df_crm.loc[row['fISIN']]['GuidProduto']).lower()}
                teste = self.banco.com_edit(tabela='CL_AllAssets', filtro=f"IdAsset={row['IdAsset']}", campos_valores=dicio)
                if teste == 'Sucesso':
                    cont += 1
        print(f'Atualizados com sucesso: {cont} registros.')   

        # Opções
        data_str = PosicaoDm1().data_posicao_str()
        codsql = f"""
                SELECT DISTINCT Cart.IdProdutoProfitSGI, Cart.NomeProduto, Cart.IdBDS, Cart.DataVencimento, O.SiglaBTG as Codigo, O.Strike, O.BloombergTk, A.GPSGuid, A.IdAsset
                FROM PO_Carteira Cart WITH (NOLOCK) INNER JOIN PA_Opcoes O WITH (NOLOCK) ON Cart.IdProdutoProfitSGI=O.RVcod 
                	 INNER JOIN CL_AllAssets A WITH (NOLOCK) ON A.Codigo=IdProdutoProfitSGI
                WHERE Cart.TipoProduto='OPT' AND GPSGuid IS NULL AND Cart.DataArquivo={data_str}
                """
        df = self.banco.dataframe(codsql)
        if not df.empty:
             df_crm = Crm().acoes_cadastro().reset_index().set_index('new_idsistemaorigem')
             for idx, row in df.iterrows():
                 if row['Codigo'] in df_crm.index:
                     dicio = {'GPSGuid': str(df_crm.loc[row['Codigo']]['productid']).lower()}
                     teste = self.banco.com_edit(tabela='CL_AllAssets', filtro=f"IdAsset={row['IdAsset']}", campos_valores=dicio)  

    def carteira_ima_b(self, data_base=None):
        data_str = self.banco.sql_data(data_base)
        # Busca NTN-Bs
        codsql = f"""SELECT PA_PreçosRF.RFCod, PA_TitulosRF.Maturity, PA_PreçosRF.TxIndicativa, PA_TitulosRF.CodBDS, PA_PreçosRF.Data
                    FROM BAWM.dbo.PA_PreçosRF PA_PreçosRF, BAWM.dbo.PA_TitulosRF PA_TitulosRF
                    WHERE PA_TitulosRF.RFCod = PA_PreçosRF.RFCod AND ((PA_PreçosRF.Data=dbo.Workday({data_str},-1,1)) AND (PA_TitulosRF.IdTpRF=13) AND (PA_TitulosRF.Maturity>{data_str}))"""
        if self.df_carteira_ima_b.empty or data_str != self.df_carteira_ima_b_data:
            ntnbs = self.banco.dataframe(codsql)
            
            # Dados BDS
            data = ntnbs.iloc[0]['Data']
            lista = ntnbs['CodBDS'].unique()
            dados = BDS().series_historico(lista, lista_idcampos=[63, 71, 55, 380], data_ini=data, data_fim=data, campos_por_nome=True).set_index('idser')
            
            df = pd.merge(left=ntnbs, left_on='CodBDS', right=dados, right_index=True)
            df['FinTeorico'] = df.apply(lambda x: x['Qtde. Teórica na Carteira'] * x['PU'], axis=1)
            pl_ind = df['FinTeorico'].sum()
            df['Peso'] = df.apply(lambda x: x['FinTeorico'] / pl_ind, axis=1)
            if self.buffer:
                self.df_carteira_ima_b = df.copy()
                self.df_carteira_ima_b_data = data_str
        else:
            df = self.df_carteira_ima_b.copy()
        
        return df    


class BDS(DBBds):
     
    def __init__(self, buffer:bool=False, homologacao:bool=False):
        super().__init__(buffer=buffer, homologacao=homologacao)                       


class BDSMem(DBBDSMem):
    """
    Classe bds, mas com alguns dados pré carregados na memória
    """
    def __init__(self, homologacao=False):
        super().__init__(homologacao=homologacao)


class BaseExtrato(DBBaseExtrato):

    def __init__(self, buffer:bool=False, homologacao:bool=False):
        super().__init__(buffer=buffer, homologacao=homologacao)
    
    def ativos_privados_controle(self, data_base):
        if not self.ativos_privados_controle_set:
            data_str = self.banco.sql_data(data_base)
            codsql = f"EXEC GPS..Planilha_ConsultaControleAtivosIliquidos {data_str}"
            df = self.banco.dataframe(codsql=codsql)
            
            cotistas = Crm(homologacao=self.homologacao).cotista_carteiravirtual()
            if cotistas.empty:
                raise Exception('BaseExtrato\ativos_privados_controle: não foi possível obter a lista de cotistas do CRM')
            
            lista = ['Capital Comprometido', 'Aplicação']
            df = df[df['TipoFluxo'].isin(lista)]
            df_cart = df[df['Carteira'].notnull()].drop('Cotista', axis=1)
            df_virtual = pd.merge(how='inner', left=df.drop('Carteira', axis=1), left_on=['Cotista', 'ProdutoIliquido'],
                                  right=cotistas, right_on=['new_cotistaidname', 'nome_fundo'])
            df_virtual = df_virtual[df_cart.columns]
            
            fluxo_privados = pd.concat([df_virtual, df_cart], axis=0)
            
            # Controles de qualidade
            if len(fluxo_privados)/len(df)*100 < 75:
                raise Exception('BaseExtrato\ativos_privados_controle: perda de informação muito grande')
            
            if self.buffer:
                self.ativos_privados_controle_df = fluxo_privados.copy()
                self.ativos_privados_controle_set = True
        else:
            fluxo_privados = self.ativos_privados_controle_df.copy()
        return fluxo_privados.reset_index()


class Boletador(DBBoletador):

    def __init__(self, homologacao:bool=False):
        super().__init__(homologacao=homologacao)
    
    def ordem_cancelar(self, id_ordem:int, mensagem:str=None, teste=False):
        """
        Cancela um grupo de ordens

        Parameters
        ----------
        id_ordem : int
            DESCRIPTION.

        Returns
        -------
        None.

        """
        log = []
        # 1. Busca o grupo de ordens
        [usuario, horario] = self.banco.get_user_and_date()
        ordens = self.ordens_grupo(id_ordem=id_ordem)
        if ordens.empty:
            raise Exception(f'Ordens não encontradas com o IdOrdem {id_ordem}')
        
        # 2. Cancela as ordens
        id_status_atual = ordens.iloc[0]['IdStatusOrdem']
        df = ordens.copy().sort_values('IdOrdem')
        df['IdStatusAnterior'] = id_status_atual
        df['IdStatusNovo'] = 15
        if mensagem:
            df['Comentario'] = mensagem
        else:
            df['Comentario'] = 'Ordem excluída'
        
        # 3. Busca as boletas atreladas ao grupo de ordens
        bols = self.boletas_por_id_ordem(list(ordens['IdOrdem'].unique()))
        teste_bols = True
        if bols.empty:
            log.append({'Etapa': 'Boletas', 'Mensagem': "Não havia dados na BL_PreBoletas", 'Status': 'OK'})
            teste_bols = False
        else:
            bols = bols[bols['Deletado']==False]
            if bols.empty:
                teste_bols = False
            
        if not teste_bols:
            log.append({'Etapa': 'Boletas', 'Mensagem': "Não havia dados na BL_PreBoletas", 'Status': 'OK'})
        else:
            # 3.a. Verifica ordens executadas por trading desk
            prebols = bols[bols['PreBoleta']==True]
            if not prebols.empty:
                for idx, row in prebols.iterrows():
                    execucoes = bols[(bols['Execucao']==True) & (bols['IdPreBoleta']==row['IdRealocFundos'])]
                    designado = False
                    if row['IdExterno_Ordem']:
                        designado = True
                    if execucoes.empty and not designado:
                       if not teste: 
                           self.boletas_apagar(int(row['IdRealocFundos']))
                       log.append({'Etapa': 'Pré-boletas', 'Mensagem': f"Pré-boleta apagada: {row['IdRealocFundos']}", 'Status': 'OK', 'IdOrdem': row['IdOrdem']})
                    else:
                        log.append({'Etapa': 'Pré-boletas', 'Mensagem': f"Já em execução por Trading Desk", 'Status': 'Erro', 'IdOrdem': row['IdOrdem']})
            else:
                log.append({'Etapa': 'Boletas', 'Mensagem': "Não havia pré-boletas para Trading Desk", 'Status': 'OK'})
            # 3.b. Verifica boletas simples (se foram exportadas)
            prebols = bols[(bols['PreBoleta']==False) & (bols['Execucao']==False)]    
            if not prebols.empty:
                for idx, row in prebols.iterrows():
                    if not row['Exportado']:
                        if not teste: 
                            self.boletas_apagar(int(row['IdRealocFundos']))
                        log.append({'Etapa': 'Boletas', 'Mensagem': f"Boleta apagada: {row['IdRealocFundos']}", 'Status': 'OK', 'IdOrdem': row['IdOrdem']})
                    else:
                        status_bol = row['StatusCRM']
                        if not status_bol:
                            status_bol = 'status desconhecido'
                        if status_bol in ['Cancelada', 'Devolvida']:
                            log.append({'Etapa': 'Boletas', 'Mensagem': f"Já com status {status_bol} no Cockpit", 'Status': 'OK', 'IdOrdem': row['IdOrdem']})
                        else:
                            log.append({'Etapa': 'Boletas', 'Mensagem': f"Exportada para o Cockpit. Status CRM: {status_bol}", 'Status': 'Erro', 'IdOrdem': row['IdOrdem']})
            else:
                log.append({'Etapa': 'Boletas', 'Mensagem': "Não havia pré-boletas a serem exportadas para o cockpit", 'Status': 'OK'})
        
        # 4. Busca o que está no Book Interno
        sec = Secundario(homologacao=self.homologacao)
        prebols = sec.pr_sec_buyer_busca_id_ordem(list(ordens['IdOrdem'].unique()))
        if prebols.empty:
            log.append({'Etapa': 'BookInterno', 'Mensagem': "Não havia dados na PR_Sec_Buyer", 'Status': 'OK'})
        else:
            for idx, row in prebols.iterrows():
                qtde_ext = row['QtdeExecutadaExt']
                if not qtde_ext:
                    qtde_ext = 0
                qtde_rateio = row['QtdeRateio']
                if not qtde_rateio:
                    qtde_rateio = 0
                if row['Rateado']==False and qtde_ext == 0:           
                    if not teste: 
                        sec.pr_sec_buyer_cancela_id_ordem(row['IdOrdem'], usuario)
                    log.append({'Etapa': 'BookInterno', 'Mensagem': f"Boleta apagada: {row['IdComprador']}", 'Status': 'OK', 'IdOrdem': row['IdOrdem']})
                elif row['Rateado']==True and qtde_rateio == 0:                
                    if not teste: 
                        sec.pr_sec_buyer_cancela_id_ordem(row['IdOrdem'], usuario)
                    log.append({'Etapa': 'BookInterno', 'Mensagem': f"Boleta rateada sem qtde: {row['IdComprador']}", 'Status': 'OK', 'IdOrdem': row['IdOrdem']})
                elif row['Rateado']==True and qtde_rateio != 0:                
                    log.append({'Etapa': 'BookInterno', 'Mensagem': f"Boleta já rateada: IdTrade={row['IdTrade']}, {row['IdComprador']}. Verifique se é possível cancelar", 'Status': 'Erro', 'IdOrdem': row['IdOrdem']})
                elif qtde_ext != 0:
                    if not teste: 
                        sec.pr_sec_buyer_cancela_id_ordem(row['IdOrdem'], usuario)
                    log.append({'Etapa': 'BookInterno', 'Mensagem': f"Apagado, mas havia execução externa: {row['IdComprador']}", 'Status': 'Erro', 'IdOrdem': row['IdOrdem']})
        
        # 5. Vê resultado do processo
        log = pd.DataFrame(log)
        houve_erro = False
        if 'IdOrdem' in log.columns:
            log_ordem = log[log['IdOrdem'].notnull()]
        else:
            log_ordem = pd.DataFrame()
            
        if not log_ordem.empty:
            df['ComInd'] = True
            for idx, row in log_ordem.iterrows():
                indice = df[df['IdOrdem']==row['IdOrdem']].index
                df.loc[indice, 'Comentario'] = row['Mensagem']
                if row['Status'] == 'Erro':
                    df.loc[indice, 'IdStatusNovo'] = 78    
        else:
            # Se não houve problemas, troca o grupo de ordens inteiro para cancelado
            df['ComInd'] = False
            df = df.loc[[df.index[0]]]
        
        # FINAL
        if not teste: 
            resultado = self.ordens_editar(df_ordens=df)  # Garantir na PROC que todas as ordens podem ser canceladas    
        
        retorno = not houve_erro
        return retorno
    
    def __ordens_busca_preco__(self, ativo_guid, tipo_produto, codigo_negociacao):
        bawm = Bawm(homologacao=self.homologacao)
        tipo_prod = tipo_produto
        cod_neg = codigo_negociacao
        
        if ativo_guid[:4] == 'sub:':
            texto = ativo_guid[4:]
            texto = texto.split('\\')
            tipo_prod = texto[0].upper()
            cod_neg = texto[1]
        else:
            preco = self.banco.busca_valor(tabela="PO_Ativos", filtro=f"GuidAtivo='{ativo_guid}'", campo='PrecoAtual')
            if preco and not math.isnan(preco):
                dicio = {'Preco': preco, 'CodNeg': cod_neg, 'TipoProd': tipo_prod}
                return dicio
        
        # Busca o preço
        dicio = {'Preco': None, 'CodNeg': cod_neg, 'TipoProd': tipo_prod}
        if tipo_prod == 'FUT':
            cod_fut = bawm.cod_fut(f"SiglaBTG='{cod_neg}'")
            if not cod_fut.empty:
                cod_fut = cod_fut.iloc[0]['CodigoBMF']
                dicio['Preco'] = bawm.preco_ultimo_codigo(codigo=cod_fut, tipo='FUT')  
        elif tipo_prod in ['AÇOES', 'BOLSA']:
            dicio['Preco'] = bawm.preco_ultimo_codigo(cod_neg, 'RV')
            if not dicio['Preco']:
                dicio['Preco'] = bawm.preco_ultimo_codigo(cod_neg, 'OPT')
                if not dicio['Preco']:
                    codigo = bawm.opt_codigo_mais_recente(cod_neg)
                    dicio['Preco'] = bawm.preco_ultimo_codigo(codigo, 'OPT')
        elif tipo_prod == 'OPT':
            dicio['Preco'] = bawm.preco_ultimo_codigo(cod_neg, 'OPT')
            if not dicio['Preco']:
                codigo = bawm.opt_codigo_mais_recente(cod_neg)
                dicio['Preco'] = bawm.preco_ultimo_codigo(codigo, 'OPT')
        elif tipo_prod == 'SEC':  # Nesse caso, o produto é um IdTrade no secundário interno
            secund = Secundario(homologacao=self.homologacao)
            df = secund.ofertas_buscar_idtrade(int(cod_neg))
            if df.empty:
                raise Exception(f'IdTrade inválido: {cod_neg}')
            linha = df.iloc[0]
            tipo_prod = linha['TipoProduto']
            texto = tipo_prod.split(' ')
            tipo_prod = texto[0]
            dicio['Classe'] = linha['Classe']
            dicio['SubClasse'] = linha['SubClasse']
            dicio['Preco'] = linha['PUFonte']
            
        
        dicio['CodNeg'] = cod_neg
        dicio['TipoProd'] = tipo_prod
        return dicio #  preco, tipo_prod, cod_neg
    
    def ordens_grupo(self, id_ordem:int) -> pd.DataFrame:
        """
        Busca ordens pertencentes a um grupo
        
        Função deprecated em 13/nov/2023 quando coluna com as ordens foi incluída na função que traz
        as ordens ongoing . Em m 08/jul/2024: não mais deprecated
        
        Parameters
        ----------
        id_ordem : int
            id da ordem.

        Returns
        -------
        DataFrame
            Dataframe com os dados.

        """
        
        def teste_valor(valor):
            try:
                if valor:
                    if not math.isnan(valor):
                        return True
            except:
                pass
            return False
        
        codsql = f"""
                 SELECT P.Nome as Status, B.*, A.Classe, A.SubClasse, A.TipoProduto, A.CodNegociacao, A.PrecoAtual, Realocacao.ValorStr as IdSolicitacao, Realocacao.IdTipoSolicitacao, IdOrdemOms, ISNULL(ERolagem,0) as Rolagem, CampoGOMS.IdGrupoOms
                 FROM (SELECT TOP 1 ISNULL(IdOrdemGrupo, IdOrdem) as IdBusca FROM BL_Ordem WITH (NOLOCK) WHERE IdOrdemGrupo={id_ordem} OR IdOrdem={id_ordem}) B1
                     INNER JOIN BL_Ordem B ON (B1.IdBusca=B.IdOrdem OR B1.IdBusca=B.IdOrdemGrupo)
                     INNER JOIN BL_Propriedades P WITH (NOLOCK) ON B.IdStatusOrdem=P.IdCampo
                 	 LEFT JOIN PO_Ativos A WITH (NOLOCK) ON B.AtivoGuid=A.GuidAtivo
                     LEFT JOIN (SELECT IdOrdem, ValorFlt as IdOrdemOms FROM BL_CamposAdicionais A WITH (NOLOCK) WHERE IdCampo=103) CampoOMS ON B.IdOrdem=CampoOMS.IdOrdem
                     LEFT JOIN (SELECT IdOrdem, ValorFlt as IdGrupoOms FROM BL_CamposAdicionais A WITH (NOLOCK) WHERE IdCampo=112) CampoGOMS ON B.IdOrdem=CampoGOMS.IdOrdem
 					 LEFT JOIN (SELECT IdOrdem, ValorBool as ERolagem FROM BL_CamposAdicionais A WITH (NOLOCK) WHERE IdCampo=105) CampoRolagem ON B.IdOrdem=CampoRolagem.IdOrdem
                     LEFT JOIN (SELECT IdOrdem, ValorStr, S.IdTipoSolicitacao FROM BL_CamposAdicionais A WITH (NOLOCK) INNER JOIN SO_Solicitacoes S WITH (NOLOCK) ON CAST(A.ValorStr as INT)=S.IdSolicitacao  WHERE IdCampo=62) Realocacao ON B.IdOrdem=Realocacao.IdOrdem
                 WHERE B.OrdemExcluida=0 
                 """
        df_ordem = self.banco.dataframe(codsql=codsql)
        
        df_ordem.insert(len(df_ordem.columns), 'ValorAdj', [0] * len(df_ordem))
        for idx, row in df_ordem.iterrows():
            if teste_valor(row['Financeiro']):
                df_ordem.loc[idx, 'ValorAdj'] = self.banco.movimentacao_sinal(row['TipoMov']) * row['Financeiro']
            elif teste_valor(row['Quantidade']) and teste_valor(row['PrecoAtual']):
                df_ordem.loc[idx, 'ValorAdj'] = self.banco.movimentacao_sinal(row['TipoMov']) * row['Quantidade'] * row['PrecoAtual']
            
            if row['TipoProduto'] == 'FUT' and row['ValorAdj']:
                df_ordem.loc[idx, 'Financeiro'] = abs(row['ValorAdj'])
                
        # Se houver ativos sem ValorAdj, tenta estimar
        temp = df_ordem[df_ordem['ValorAdj']==0]
        if len(temp) > 0:            
            # TODO: Colocar código bawm na PO_Ativos
            # TODO: Faltam Titulos publicos
            for idx, row in temp.iterrows():
                dicio = self.__ordens_busca_preco__(row['AtivoGuid'], row['TipoProduto'], row['CodNegociacao'])
                preco = dicio['Preco']
                tipo_prod = dicio['TipoProd']
                cod_neg = dicio['CodNeg']
                if not row['TipoProduto']:
                    df_ordem.loc[idx, 'TipoProduto'] = tipo_prod
                if not row['CodNegociacao']:
                    df_ordem.loc[idx, 'CodNegociacao'] = cod_neg
                if not row['Classe'] and 'Classe' in dicio.keys():
                    df_ordem.loc[idx, 'Classe'] = dicio['Classe']
                if not row['SubClasse'] and 'SubClasse' in dicio.keys():
                    df_ordem.loc[idx, 'SubClasse'] = dicio['SubClasse']
                if preco:
                    df_ordem.loc[idx, 'ValorAdj'] = self.banco.movimentacao_sinal(row['TipoMov']) * row['Quantidade'] * preco
                else:
                    df_ordem.loc[idx, 'ValorAdj'] = self.banco.movimentacao_sinal(row['TipoMov']) * row['Quantidade'] * 1  # Melhor ver algo
        return df_ordem
    
    def ordens_ongoing(self, id_tipo_portfolio=None) -> pd.DataFrame:
        """
        Busca as ordens em aberto, com status = GO, ou expiradas, status=WAIT e data menor do que hoje, da tabela BL_Ordem.        
        A função busca apenas as movimentações principais de cada grupo de Ordens (IdOrdemGrupo=NULL).
        Em seguida, são buscadas todas as movimentações destas ordens, e criada uma coluna no DataFrame para abrigar o grupo inteiro
        de ordens.
        
        Ou seja, utilizar um pd.DataFrame(df.loc[idx, 'DfOrdem']) resultará num dataframe com todas as ordens do grupo
        
        Parameters
        ----------
        id_tipo_portfolio : int
            1: Fundos.
            3: Titularidades

        Returns
        -------
        df : pd.DataFrame
            DataFrame com os cados.

        """
        # 1. Busca ordens mãe: principal ordem do grupo
        codsql = f"""
                 SELECT P.Nome as Status, B.*, ISNULL(NumBolsGrupo, 0) as NumBolsGrupo, Realocacao.ValorStr as IdSolicitacao, Realocacao.IdTipoSolicitacao, IdOrdemOms, IdGrupoOms, ISNULL(ERolagem,0) as Rolagem, NULL as DfOrdem
                 FROM BL_Ordem B INNER JOIN BL_Propriedades P WITH (NOLOCK) ON B.IdStatusOrdem=P.IdCampo
                 	 LEFT JOIN (SELECT IdOrdemGrupo, COUNT(IdOrdem) as NumBolsGrupo FROM BL_Ordem GROUP BY IdOrdemGrupo) C ON B.IdOrdem=C.IdOrdemGrupo
                     LEFT JOIN (SELECT IdOrdem, ValorFlt as IdOrdemOms FROM BL_CamposAdicionais A WITH (NOLOCK) WHERE IdCampo=103) CampoOMS ON B.IdOrdem=CampoOMS.IdOrdem
                     LEFT JOIN (SELECT IdOrdem, ValorFlt as IdGrupoOms FROM BL_CamposAdicionais A WITH (NOLOCK) WHERE IdCampo=112) CampoOMSG ON B.IdOrdem=CampoOMSG.IdOrdem
					 LEFT JOIN (SELECT IdOrdem, ValorBool as ERolagem FROM BL_CamposAdicionais A WITH (NOLOCK) WHERE IdCampo=105) CampoRolagem ON B.IdOrdem=CampoRolagem.IdOrdem
                     LEFT JOIN (SELECT IdOrdem, ValorStr, S.IdTipoSolicitacao FROM BL_CamposAdicionais A WITH (NOLOCK) INNER JOIN SO_Solicitacoes S WITH (NOLOCK) ON CAST(A.ValorStr as INT)=S.IdSolicitacao  WHERE IdCampo=62) Realocacao ON B.IdOrdem=Realocacao.IdOrdem
                 WHERE B.OrdemExcluida=0 AND B.IdOrdemGrupo IS NULL 
                       AND (P.TipoValor='GO' OR (DataValidade<(GETDATE()-1) AND P.TipoValor='WAIT'))
                 """
        df = self.banco.dataframe(codsql=codsql)
        if df.empty:
            return df                
        
        # 2. Busca os dados de todas as ordens
        df['DfOrdem'] = df['DfOrdem'].astype('object')
        codsql = f"""
                 SELECT P.Nome as Status, B.*, A.PrecoAtual, A.Classe, A.SubClasse, A.TipoProduto, A.CodNegociacao, IdOrdemOms, ISNULL(ERolagem,0) as Rolagem
                 FROM BL_Ordem B INNER JOIN BL_Propriedades P WITH (NOLOCK) ON B.IdStatusOrdem=P.IdCampo
                 	 LEFT JOIN (SELECT IdOrdem, ValorFlt as IdOrdemOms FROM BL_CamposAdicionais A WITH (NOLOCK) WHERE IdCampo=103) CampoOMS ON B.IdOrdem=CampoOMS.IdOrdem
 					 LEFT JOIN (SELECT IdOrdem, ValorBool as ERolagem FROM BL_CamposAdicionais A WITH (NOLOCK) WHERE IdCampo=105) CampoRolagem ON B.IdOrdem=CampoRolagem.IdOrdem
                     LEFT JOIN PO_Ativos A WITH (NOLOCK)  ON B.AtivoGuid=A.GuidAtivo
                 WHERE B.OrdemExcluida=0
                       AND (P.TipoValor='GO' OR (DataValidade<(GETDATE()-1) AND P.TipoValor='WAIT'))
                 """
        df_sub = self.banco.dataframe(codsql=codsql)
        
        # 3. Inclui uma coluna com o valor ajustado
        def teste_valor(valor):
            try:
                if valor:
                    if not math.isnan(valor):
                        return True
            except:
                pass
            return False
        df_sub.insert(len(df_sub.columns), 'ValorAdj', [0] * len(df_sub))
        for idx, row in df_sub.iterrows():
            if teste_valor(row['Financeiro']):
                df_sub.loc[idx, 'ValorAdj'] = self.banco.movimentacao_sinal(row['TipoMov']) * row['Financeiro']
            elif teste_valor(row['Quantidade']) and teste_valor(row['PrecoAtual']):
                df_sub.loc[idx, 'ValorAdj'] = self.banco.movimentacao_sinal(row['TipoMov']) * row['Quantidade'] * row['PrecoAtual']
            
            if row['TipoProduto'] == 'FUT' and row['ValorAdj']:
                df_sub.loc[idx, 'Financeiro'] = abs(row['ValorAdj'])
        
        # 4.a. Se houver ativos sem ValorAdj, tenta estima        
        temp = df_sub[df_sub['ValorAdj']==0]
        if len(temp) > 0:
            bawm = Bawm(homologacao=self.homologacao)
            # TODO: Colocar código bawm na PO_Ativos
            # TODO: Faltam Titulos publicos
            for idx, row in temp.iterrows():            
                if row['Financeiro'] == 0 and not row['Quantidade']:
                    print(f"Ordem em branco: {row['IdOrdem']}")
                else:
                    dicio = self.__ordens_busca_preco__(row['AtivoGuid'], row['TipoProduto'], row['CodNegociacao']) 
                    preco = dicio['Preco']
                    tipo_prod = dicio['TipoProd']
                    cod_neg = dicio['CodNeg']
                    if not row['TipoProduto']:
                        df_sub.loc[idx, 'TipoProduto'] = tipo_prod
                    if not row['CodNegociacao']:
                        df_sub.loc[idx, 'CodNegociacao'] = cod_neg
                    if not row['Classe'] and 'Classe' in dicio.keys():
                        df_sub.loc[idx, 'Classe'] = dicio['Classe']
                    if not row['SubClasse'] and 'SubClasse' in dicio.keys():
                        df_sub.loc[idx, 'SubClasse'] = dicio['SubClasse']
                    if teste_valor(preco):
                        df_sub.loc[idx, 'ValorAdj'] = self.banco.movimentacao_sinal(row['TipoMov']) * row['Quantidade'] * preco
                    else:
                        df_sub.loc[idx, 'ValorAdj'] = self.banco.movimentacao_sinal(row['TipoMov']) * row['Quantidade'] * 1  # Melhor ver algo
        
        # 4. Preenche uma coluna no dataframe com TODAS as ordens do grupo
        for idx, row in df.iterrows():
            temp = df_sub[(df_sub['IdOrdem']==row['IdOrdem']) | (df_sub['IdOrdemGrupo']==row['IdOrdem'])]
            df.at[idx, 'DfOrdem'] = temp.to_dict('records')
        
        return df    

class CaixaPF:
    """
    Classe criada para intemediar interações com a base da carga (GPS), no servidor Core.
    """
    def __init__(self, data_posicao=None, rodar_refresh:bool=True, titularidade_teste:str=None, homologacao:bool=False):
        self.homologacao = homologacao        
        if homologacao:
            self.banco = BaseSQL(nome_servidor='SQLSVRHOM1.GPS.BR', nome_banco='GPS_HOM1')
            self.crm_db_nome = 'GPS_MSCRM_HOM2'
        else:
            self.banco = BaseSQL(nome_servidor=ambiente.servidor_core(), nome_banco='GPS')
            self.crm_db_nome = 'GPS_MSCRM'
        self.gps = BaseExtrato(homologacao=homologacao)
        self.bawm = BaseSQL(nome_servidor=ambiente.servidor_aux(), nome_banco='BAWM')
        self.cw = CW(homologacao=homologacao)
        self.sec = Secundario(homologacao=homologacao)
        self.sol = SolRealocacao(homologacao=homologacao)
        
        if data_posicao:
            self.data_posicao = data_posicao
        else:
            self.data_posicao = self.banco.hoje()
        bawm = Bawm(homologacao=self.homologacao)
        self.data_dmais1 = bawm.dia_trabalho(self.data_posicao, 1)
        self.titularidade_teste = titularidade_teste
        # Boletas já programadas para liquidação hoje (base não é alterada durante o dia)
        self.df_agenda = self.__boletas_agendadas__()
        self.df_agenda_sec = self.__secundarios_liquidacao__()
        self.boletas = pd.DataFrame()
        self.solicitacoes = pd.DataFrame()
        self.mov_cotistas = pd.DataFrame()
        
        # Dados online
        self.__base_saldo_set__ = False
        self.base_saldo_hora = None
        self.__base_saldo__ = pd.DataFrame()
        if rodar_refresh:
            self.refresh()
        
    def __boletas_agendadas__(self):
        data_str = self.banco.sql_data(self.data_posicao)
        codsql = f"""SELECT * FROM PO_Movimentacao M WITH (NOLOCK)
                     WHERE M.DataPagamento>={data_str} AND M.DataBoleta<{data_str} AND Invalida=0
                  """
        df = self.bawm.dataframe(codsql)
        datas = list(df[['DataPagamento']].sort_values('DataPagamento')['DataPagamento'].unique())
        datas = datas[:2]
        df = df[df['DataPagamento'].isin(datas)]
        
        return df
    
    def __secundarios_liquidacao__(self):    
        return self.sec.secundarios_liquidacao(self.data_dmais1, corrigir_pu=True)
    
    def refresh(self):
        # Não faz sentido o refresh se a D-1 não tiver sido importada e a composição das titularidades processada
        if not self.cw.rotina_diaria(id_rotina=199):
            return
        # De preferência com a exportação do pickle
        if not self.cw.rotina_diaria(id_rotina=225):
            if datetime.datetime.now().hour < 9:
                return
        # Se virar o dia, atualiza as movimentações agendadas
        if self.data_posicao != self.banco.hoje():            
            self.data_posicao = self.banco.hoje()
            bawm = Bawm(homologacao=self.homologacao)
            self.data_dmais1 = bawm.dia_trabalho(self.data_posicao, 1)
            self.df_agenda = self.__boletas_agendadas__()
            self.df_agenda_sec = self.__secundarios_liquidacao__()
            
        # Refresh das boletas do dia
        min_hora = datetime.datetime.now() - relativedelta(minutes=15)
        if not self.__base_saldo_set__ or self.base_saldo_hora < min_hora:
            self.__base_saldo_set__ = True
            self.base_saldo_hora = datetime.datetime.now()
            self.__base_saldo__ = self.__saldos_dia__()
            self.refresh_comandos_adicionais()
    
    def refresh_comandos_adicionais(self):
        # Função deixada aqui para poder ser sobrescrita por classes que herdem essa classe
        # NÃO APAGAR
        pass
    
    def lista_bancos(self) -> list:
        self.refresh()
        return list(self.__base_saldo__['BancoContaMovimento'].unique())

    def lista_titularidades(self) -> list:
        self.refresh()
        return list(self.__base_saldo__['Titularidade'].unique())

    def saldos(self) -> pd.DataFrame:
        self.refresh()
        return self.__base_saldo__
    
    def __solicitacoes__(self):
        # Busca solicitações
        df_sol = self.sol.solicitacoes_aportes_resgates(ultimos_n_dias=4)
        # 1. Ignora onde o tipo de movimentação é realocação
        lista_realoc = list(df_sol[(df_sol['IdCampo']==28) & (df_sol['ValorStr']=='Realocação')]['IdSolicitacao'].unique())        
        df_sol = df_sol[~df_sol['IdSolicitacao'].isin(lista_realoc)]
        # 2. Pega apenas aportes (os resgates terão a TED boletada no Cockpit)
        lista_realoc = list(df_sol[(df_sol['IdCampo']==1) & (df_sol['ValorFlt']>0.0)]['IdSolicitacao'].unique())        
        df_sol = df_sol[df_sol['IdSolicitacao'].isin(lista_realoc)]
        # 3. Organiza os campos para montar um dataframe
        df_base = df_sol[['IdSolicitacao', 'DataPedido', 'NomeSupercarteira', 'GuidSolicitacao']].drop_duplicates().set_index('IdSolicitacao')
        # 3. a. campos de valores
        datas = df_sol[df_sol['IdCampo']==2][['IdSolicitacao', 'ValorDt']].set_index('IdSolicitacao')['ValorDt'].to_dict()
        valores = df_sol[df_sol['IdCampo']==1][['IdSolicitacao', 'ValorFlt']].set_index('IdSolicitacao')['ValorFlt'].to_dict()
        devol = df_sol[df_sol['IdCampo']==17][['IdSolicitacao', 'ValorBool']].set_index('IdSolicitacao')['ValorBool'].to_dict()
        status_sol = df_sol[df_sol['IdCampo']==15][['IdSolicitacao', 'ValorStr']].set_index('IdSolicitacao')['ValorStr'].to_dict()
        conta_mov = df_sol[df_sol['IdCampo']==27][['IdSolicitacao', 'ValorStr']].set_index('IdSolicitacao')['ValorStr'].to_dict()
        conta_mov_guid = df_sol[df_sol['IdCampo']==26][['IdSolicitacao', 'ValorStr']].set_index('IdSolicitacao')['ValorStr'].to_dict()
        motivo = df_sol[df_sol['IdCampo']==28][['IdSolicitacao', 'ValorStr']].set_index('IdSolicitacao')['ValorStr'].to_dict()
        # 3. b. adição ao dataframe
        df_base['DataLiquidacao'] = df_base.index.map(datas)
        df_base['ValorAporteResg'] = df_base.index.map(valores)
        df_base['Devolvida'] = df_base.index.map(devol)
        df_base['Status'] = df_base.index.map(status_sol)
        df_base['ContaMovimento'] = df_base.index.map(conta_mov)
        df_base['ContaMovimentoGuid'] = df_base.index.map(conta_mov_guid)
        df_base['MotivoMov'] = df_base.index.map(motivo)
        return df_base
    
    def cockpit_saldos(self, data_posicao:datetime.datetime) -> pd.DataFrame:
        data_str = self.banco.sql_data(data_posicao)
        codsql = f"""SELECT SaldoVencimentoId as IdSaldo, ArquivoId, BancoContaMovimento, LOWER(GuidBancoContaMovimento) as BancoId, LOWER(GuidTitularidade) as GuidTitularidade, CASE WHEN CHARINDEX(' ', ApelidoTitularidade) = 0 THEN ApelidoTitularidade ELSE LEFT(ApelidoTitularidade, CHARINDEX(' ', ApelidoTitularidade)-1) END as Titularidade
                          ,ContaMovimento, LOWER(GuidContaMovimento) as GuidContaMovimento, AgenciaBanco, Saldo as Abertura, 0 as Saidas, 0 as Entradas, 0 as Projetado, 0 as NetLiqD1, 0 as ProjetadoD1
                    	  ,Vigente,[Boletado],[Boletar],LOWER(GuidProdutoLiquidacao) as GuidProdutoLiquidacao,ProdutoLiquidacao	  
                    	  ,Segmentacao as Segmento, Officer, Controller, FilialNome
                      FROM SaldoVencimento WITH (NOLOCK)
                      WHERE CAST(DataReferencia as DATE)={data_str}
                      ORDER BY BancoContaMovimento, ApelidoTitularidade
                  """
        df = self.banco.dataframe(codsql)
        if self.titularidade_teste:
            df = df[(df['Titularidade']==self.titularidade_teste) | (df['GuidTitularidade']==self.titularidade_teste)]
        return df
    
    def cockpit_saldos_nao_inv(self, data_posicao:datetime.datetime=None, saldo_relevante:int=10000) -> pd.DataFrame:
        # 1. Trata a data
        data = data_posicao
        if not data:
            data = self.banco.hoje()
        # 2. Busca os dados
        data_str = self.banco.sql_data(data - relativedelta(days=31))
        codsql = f"""SELECT SaldoVencimentoId as IdSaldo, ArquivoId, CAST(CAST(DataReferencia as DATE) as Datetime) as DataPos, BancoContaMovimento, LOWER(GuidBancoContaMovimento) as BancoId, LOWER(GuidTitularidade) as GuidTitularidade, CASE WHEN CHARINDEX(' ', ApelidoTitularidade) = 0 THEN ApelidoTitularidade ELSE LEFT(ApelidoTitularidade, CHARINDEX(' ', ApelidoTitularidade)-1) END as Titularidade
                           ,ContaMovimento, LOWER(GuidContaMovimento) as GuidContaMovimento, AgenciaBanco, Saldo,Vigente,[Boletado],[Boletar],LOWER(GuidProdutoLiquidacao) as GuidProdutoLiquidacao,ProdutoLiquidacao	                      	  
                    	   ,Segmentacao as Segmento, Officer, Controller, FilialNome
                      FROM SaldoVencimento WITH (NOLOCK)
                      WHERE CAST(DataReferencia as DATE)>={data_str} AND Saldo>{saldo_relevante}
                      ORDER BY BancoContaMovimento, ApelidoTitularidade
                  """
        df = self.banco.dataframe(codsql)
        if self.titularidade_teste:
            df = df[(df['Titularidade']==self.titularidade_teste) | (df['GuidTitularidade']==self.titularidade_teste)]
        
        # 3. Análise
        analise = pd.pivot_table(df, index='GuidContaMovimento', columns='DataPos', values='Saldo')
        # 3. a. Fill the blanks
        colunas = list(analise.columns)
        for idx, row in analise.iterrows():
            for col in range(1, len(analise.columns)-1):
                if math.isnan(row[colunas[col]]):
                    if row[colunas[col-1]] == row[colunas[col+1]]:
                        analise.loc[idx, colunas[col]] = row[colunas[col-1]]
                    
        # 3. b. Clientes com saldo positivo nos últimos 2 dias
        lista = [-1, -2]
        for i in lista:
            col = analise.columns[i]
            analise = analise[analise[col].notnull()]
        
        # 3. c. Contagem de dias com dinheiro em conta
        colunas = list(reversed(colunas))
        analise.insert(0, 'Dias10k', [0] * len(analise))
        for idx, row in analise.iterrows():
            i = 0
            for col in colunas:
                if math.isnan(row[col]):
                    break
                i += 1
            analise.loc[idx,'Dias10k'] = i
        analise = analise[['Dias10k']]            
        return analise
    
    def __saldos_dia__(self):
        # Busca saldos do cockpit
        df = self.cockpit_saldos(self.data_posicao)
        non_inv = self.cockpit_saldos_nao_inv()
        df = pd.merge(left=df, left_on='GuidContaMovimento', right=non_inv, right_index=True, how='left')
        df['Dias10k'] = df['Dias10k'].fillna(0)
        
        # Busca Boletas já no cockpit
        df_bols = self.gps.get_movimentacoes_titularidades(data_ini=self.data_posicao, data_fim=self.data_dmais1)
        self.boletas = df_bols.copy()        
        
        # a. Boletas de hoje        
        df_bols = df_bols[df_bols['BoletaStatus'].isin([1, 3, 4, 5])] # Pré-Boleta, Pronto para Envio, Movimentação Solicitada, Liquidada        
        df_bols = df_bols[df_bols['BoletaDataPagamento']==self.data_posicao]
        df_bols = df_bols[df_bols['BoletaData']==self.data_posicao]
        
        # b. Boletas de D+1
        df_bols_dm1 = self.boletas[(self.boletas['BoletaDataPagamento']==self.data_dmais1) & (self.boletas['BoletaData']>=self.data_posicao)]
        df_bols_dm1 = df_bols_dm1[df_bols_dm1['BoletaStatus'].isin([1, 3, 4, 5])] # Pré-Boleta, Pronto para Envio, Movimentação Solicitada, Liquidada
        
        # Solicitações de entrada e saída
        df_sol = self.__solicitacoes__()
        self.solicitacoes = df_sol.copy()
        df_sol_hoje = df_sol[(df_sol['DataLiquidacao']==self.data_posicao) & (df_sol['Devolvida']==False)]
        df_sol_dm1 = df_sol[(df_sol['DataLiquidacao']==self.data_dmais1) & (df_sol['Devolvida']==False)]
        
        self.mov_cotistas = df_sol.copy()
        
        # Monta o dataframe
        df_agenda = self.df_agenda[self.df_agenda['DataPagamento']==self.data_posicao]
        df_agenda_d1 = self.df_agenda[self.df_agenda['DataPagamento']==self.data_dmais1]
        for idx, row in df.iterrows():                        
            # Book Interno liquidação em D+1
            bookint_df = self.df_agenda_sec[(self.df_agenda_sec['B_Titularidade']==row['GuidTitularidade']) & (self.df_agenda_sec['B_ContaMovGuid']==row['GuidContaMovimento'])]
            book_int_d1_c = bookint_df[bookint_df['Compra']==True]['Financeiro'].sum()
            book_int_d1_v = bookint_df[bookint_df['Compra']==False]['Financeiro'].sum()
            
            # Boletas de hoje
            # a. Resgates direcionados para a conta
            dicio = self.__dados_movimentacao_boletas__(df_bols, df_agenda, df_sol_hoje, row)
            hj_resg_in = dicio['resg_in']
            # b. Compras sendo pagas pela contas 
            hj_aplic_out = dicio['aplic_out']
            hj_compra_out = dicio['compra_out']
            # c. Pagamento de Fees
            hj_pagfee = dicio['pagfee']
            # d. TEDs Entrando
            hj_ted_in = dicio['ted_in']
            hj_ted_out = dicio['ted_out']
            ag_resg_in = dicio['ag_resg_in']
            ag_compra_out = dicio['ag_compra_out']
            # e. Solicitações
            hj_sol_aplic = dicio['sol_aplic']
            
            # Boletas de d+1
            # a. Resgates direcionados para a conta
            dicio = self.__dados_movimentacao_boletas__(df_bols_dm1, df_agenda_d1, df_sol_dm1, row)
            d1_resg_in = dicio['resg_in']
            # b. Compras sendo pagas pela contas 
            d1_aplic_out = dicio['aplic_out']
            d1_compra_out = dicio['compra_out']
            # c. Pagamento de Fees
            d1_pagfee = dicio['pagfee']
            # d. TEDs Entrando
            d1_ted_in = dicio['ted_in']
            d1_ted_out = dicio['ted_out']
            agd1_resg_in = dicio['ag_resg_in']
            agd1_compra_out = dicio['ag_compra_out']         
            # e. Solicitações
            d1_sol_aplic = dicio['sol_aplic']
            
            # Totais
            entradas = ag_resg_in + hj_resg_in + hj_ted_in + hj_sol_aplic
            saidas = ag_compra_out + hj_aplic_out + hj_compra_out + hj_pagfee + hj_ted_out
            
            entradasd1 = agd1_resg_in + d1_resg_in + d1_ted_in + d1_sol_aplic
            saidasd1 = agd1_compra_out + d1_aplic_out + d1_compra_out + d1_pagfee + d1_ted_out
            
            net_liq_d1 = book_int_d1_v - book_int_d1_c + entradasd1 - saidasd1
            
            # Escreve output
            df.loc[idx, 'Saidas'] = saidas
            df.loc[idx, 'Entradas'] = entradas
            df.loc[idx, 'Projetado'] = row['Abertura'] + entradas - saidas
            df.loc[idx, 'NetLiqD1'] = net_liq_d1
            df.loc[idx, 'ProjetadoD1'] = row['Abertura'] + entradas - saidas + net_liq_d1
        
        return df
    
    def __dados_movimentacao_boletas__(self, df_boletas:pd.DataFrame, df_agendados:pd.DataFrame, df_sol:pd.DataFrame, linha_saldo:pd.Series) -> dict:
        lista_venda = ['Venda', 'Resgate Total', 'Resgate Parcial', 'Venda Total', 'RESGATE TOTAL'] 
        lista_compra = ['Aplicação']
        row = linha_saldo
        retorno = {}
        # Filtra movimentações do cliente
        df_bols = df_boletas[df_boletas['new_titularidadeid']==row['GuidTitularidade']]
        df_agenda = df_agendados[df_agendados['TitularidadeGuid']==row['GuidTitularidade']]
        
        # Boletas        
        # a. Resgates direcionados para a conta
        retorno['resg_in'] = df_bols[(df_bols['BoletaTipoMovimentoNome'].isin(lista_venda)) &
                               (df_bols['new_contamovimentodestinoid']==row['GuidContaMovimento'])]['BoletaValor'].sum()
        # b. Compras sendo pagas pela contas 
        retorno['aplic_out'] = df_bols[(df_bols['BoletaTipoMovimentoNome'].isin(lista_compra)) &
                               (df_bols['new_entradafinanceirabradesco']==row['GuidContaMovimento'])]['BoletaValor'].sum()
        retorno['compra_out'] = df_bols[(df_bols['BoletaTipoMovimentoNome'].isin(['Compra'])) &
                               (df_bols['new_contamovimentoorigemid']==row['GuidContaMovimento'])]['BoletaValor'].sum()
        # c. Pagamento de Fees
        retorno['pagfee'] = df_bols[(df_bols['new_titularidadeid']==row['GuidTitularidade']) & (df_bols['BoletaTipoMovimentoNome'].isin(['Pagamento GPS/Citi'])) &
                             (df_bols['new_contamovimentoorigemid']==row['GuidContaMovimento'])]['BoletaValor'].sum()
        # d. TEDs Entrando
        retorno['ted_in'] = df_bols[(df_bols['BoletaTipoMovimentoNome'].isin(['Transferência (TED)'])) &
                             (df_bols['new_contamovimentodestinoid']==row['GuidContaMovimento'])]['BoletaValor'].sum()
        retorno['ted_out'] = df_bols[(df_bols['BoletaTipoMovimentoNome'].isin(['Transferência (TED)'])) &
                             (df_bols['new_contamovimentoorigemid']==row['GuidContaMovimento'])]['BoletaValor'].sum()
        
        # Agendamentos
        retorno['ag_resg_in'] = df_agenda[(df_agenda['ContMovDestGuid']==row['GuidContaMovimento']) &
                           df_agenda['TipoMovimentacao'].isin(lista_venda)]['ValorLiquidoTrades'].sum()            
        # b. Compras sendo pagas pela contas (RV, FII..)
        retorno['ag_compra_out'] = df_agenda[(df_agenda['ContMovOrigGuid']==row['GuidContaMovimento']) &
                           df_agenda['TipoMovimentacao'].isin(['Compra'])]['ValorLiquidoTrades'].sum()
        
        # Solicitações (apenas aplicação, resgates têm a TED boletada)
        retorno['sol_aplic'] = df_sol[df_sol['ContaMovimentoGuid']==row['GuidContaMovimento']]['ValorAporteResg'].sum()
        
        return retorno            
                          
        
class CVM:
    """
    Classe criada para interagir com as tabelas de Controle do cadastro da CVM
    base BAWM (Servidor: SQLAUX)
    Os nomes das tabelas tem o padrão CVM_* 
    """
    def __init__(self, homologacao=False):
        if homologacao:
            self.banco = BaseSQL(nome_servidor=r'SQLSVRHOM1.GPS.BR\SQL12', nome_banco='BAWM_HOM1')
        else:
            self.banco = BaseSQL(nome_servidor=ambiente.servidor_aux(), nome_banco='BAWM')
            
    def cadastro_fundos_total(self):
        codsql = """SELECT CVM.* 
                    FROM CVM_Fundos CVM WITH (NOLOCK) 
                    	 INNER JOIN
                    	 (SELECT DISTINCT CNPJ, COUNT(CNPJ) Repeticoes, MAX(IdFdCVM) as IdCVM FROM CVM_Fundos WITH (NOLOCK) WHERE Encerrado=0 AND SITUACAO<>'CANCELADA' GROUP BY CNPJ) TBL
                    	 ON TBL.IdCVM=CVM.IDFdCVM
                 """
        df = self.banco.dataframe(codsql=codsql)
        return df
    
    def publico_alvo_pretrade(self):
        codsql = "'select CNPJ, PublicoAlvo from CVM_Fundos WITH (NOLOCK) where CNPJ_DO_GESTOR IN ('12695840000103', '12.695.840/0001-03') and SITUACAO = 'EM FUNCIONAMENTO NORMAL' AND Encerrado = 0'"
        df = self.banco.dataframe(codsql=codsql).set_index('CNPJ')
        return df

    
    def cadastro_admgest_total(self):
        codsql = "SELECT * FROM CVM_ADMGEST"
        df = self.banco.dataframe(codsql=codsql)        
        return df
    
    def eventos_cad(self, em_fundo=True):
        if em_fundo:
            texto = 1
        else:
            texto = 0
        codsql = f"SELECT * FROM CVM_Eventos WITH (NOLOCK) WHERE Coluna<>'' AND EvFundo={texto}"
        return self.banco.dataframe(codsql)
    
    def gestores_eventos_registra(self, df_dados):
        if df_dados.empty:
            return df_dados
        return self.banco.com_add_df(tabela='CVM_GestoresEv', df=df_dados)
    
    def gestores_atualizar_cadastro(self, df_dados):
        return self.banco.com_edit_or_add_df(tabela='CVM_ADMGEST', dados=df_dados, campos_filtros=['CNPJ'],
                                            colunas_comparar=['NomeGestor', 'Gestor', 'Administrador', 'Encerrado'])
    
    def fundos_eventos_registra(self, df_dados):
        if df_dados.empty:
            return df_dados
        return self.banco.com_add_df(tabela='CVM_FundosEv', df=df_dados)
    
    def fundos_atualizar_cadastro(self, df_dados, novos=False, comparar=True):
        if novos:
            return self.banco.com_add_df(tabela='CVM_Fundos', df=df_dados)
        else:
            if comparar:
                df = self.eventos_cad()
                colunas = df['Coluna'].to_list()
            else:
                colunas = []
            return self.banco.com_edit_or_add_df(tabela='CVM_Fundos', dados=df_dados, campos_filtros=['CODIGO_CVM'],
                                                colunas_comparar=colunas)
    
    def fundos_atualizar_pl(self, df_dados):        
        if df_dados.empty:
            return df_dados
        coluna = 'PATRIMONIO_LIQUIDO'
        df = df_dados[['CODIGO_CVM', coluna]].copy()
        return self.banco.com_edit_df(tabela='CVM_Fundos', dados=df, campos_filtros=['CODIGO_CVM'],
                                                colunas_comparar=[coluna], metodo_novo=True)
    def fundos_atualizar_name(self, df_dados):        
        if df_dados.empty:
            return df_dados
        coluna = 'DENOMINACAO_SOCIAL'
        df = df_dados[['CODIGO_CVM', coluna]].copy()
        return self.banco.com_edit_df(tabela='CVM_FUNDOS', dados=df, campos_filtros=['CODIGO_CVM'],
                                                colunas_comparar=[coluna], metodo_novo=True)
    
    def fundo_cadastro(self, cnpj):
        cgc = cnpj.replace('.', '').replace('-', '').replace('/', '')        
        cgc_str = self.banco.cnpj_int_para_str(cgc)
        txt = cgc[0]
        while txt == '0':
            cgc = cgc[1:]
            txt = cgc[0]
        codsql = f"SELECT * FROM CVM_Fundos WITH (NOLOCK) WHERE CNPJ IN('{cgc}','{cgc_str}')"
        df = self.banco.dataframe(codsql=codsql)
        if not df.empty:
            return df.iloc[0]
        else:
            return pd.Series(dtype='object')
      
    
class CW:
    """
    Classe criada para intemediar interações com a base BAWM (BAWM), no servidor Aux,
    tratando especificamente de coisas ligadas ao aplicativo CW (control workstation)
    
    Ela lida com as tabelas cujo nome começa com CW_* e majoritariamente com execução
    de rotinas diárias.
    
    Tabelas são alteradas regularmente ao longo do dia: não usar NO LOCK
    """
    def __init__(self, homologacao=False):
        self.homologacao = homologacao
        if homologacao:
            self.banco = BaseSQL(nome_servidor=r'SQLSVRHOM1.GPS.BR\SQL12', nome_banco='BAWM_HOM1')
        else:
            self.banco = BaseSQL(nome_servidor=ambiente.servidor_aux(), nome_banco='BAWM')
            
    def rotinas_hoje_lista(self):
        codsql = """
        SELECT T.*, C.PythonExec, C.PythonEndereco, C.IdTarefaRequisito
        FROM S_CW_Tasks_RotinasDia T INNER JOIN CW_CadTarefas C ON T.IdTask=C.IdTarefa 
        ORDER BY C.HorarioLim
        """
        return self.banco.dataframe(codsql=codsql)
    
    def rotina_diaria(self, id_rotina, marcar_resolvido=False, data=None, horario_feito=None, tempo_gasto:float=None):
        tabela = "CW_CheckRotinas"
        data_str = self.banco.sql_data(data)
        filtro = f"IdTask={id_rotina} AND Data={data_str}"
        if marcar_resolvido:
            campos_valores = {'Feito': 1}
            if tempo_gasto:
                campos_valores['MinutosExec'] = tempo_gasto
            return self.banco.com_edit(tabela=tabela, filtro=filtro, campos_valores=campos_valores)
        else:
            df = self.banco.dataframe(codsql=f"SELECT * FROM {tabela} WHERE {filtro}")
            if df.empty:
                return True  # Se tarefa não está lá pra ser executada, está feita
            else:
                feito = bool(df.iloc[0]['Feito'])
                horario_feito = df.iloc[0]['When']
                return feito
    
    def rotina_diaria_limpar_inicio(self, id_rotina, data=None):
        tabela = "CW_CheckRotinas"
        data_str = self.banco.sql_data(data)
        filtro = f"IdTask={id_rotina} AND Data={data_str}"
        texto = f"UPDATE {tabela} SET Feito=0, [Who]=NULL, [When]=NULL WHERE {filtro}"
        return self.banco.executar_comando(codsql=texto)


class PosicaoDm1(DBPosicaoDm1):
    """
    Classe criada para intemediar interações com a base BAWM (BAWM), no servidor Aux,
    tratando especificamente de coisas ligadas à posição D-1.
    
    Ela lida com as tabelas cujo nome começa com PO_* 
    """
    def __init__(self, load_all_positions=False, por_titularidade=False, com_explosao=True, pos_apenas_fundos_exclusivos=True, load_cad_ativos=False, data_posicao=False, requisito_multi_thread:bool=False, homologacao=False):
        super().__init__(load_all_positions=load_all_positions, por_titularidade=por_titularidade, com_explosao=com_explosao,
                         pos_apenas_fundos_exclusivos=pos_apenas_fundos_exclusivos, load_cad_ativos=load_cad_ativos, 
                         data_posicao=data_posicao, requisito_multi_thread=requisito_multi_thread, homologacao=homologacao)

    def po_cadastro_fundo_novo(self, guid_conta_crm:str=None, nome_conta_crm:str=None):
        crm = Crm(homologacao=self.homologacao)
        
        # Insere fundo na PO_Cadastro
        df = crm.po_cadastro_fundo_novo(guid_conta_crm=guid_conta_crm, nome_conta_crm=nome_conta_crm)        
        resultado = self.banco.com_add_df(tabela='PO_Cadastro', df=df)
        
        # Insere linha na PO_Patrimnios
        nome = df.iloc[0]['NomeContaCRM']
        data_str = self.data_posicao_str()
        data = self.data_pos_dm1
        dicio = {'DataArquivo': data, 'NomeContaCRM': nome, 'PL': 1000000, 'PLInicial': 1000000, 'PLFuturo': 1000000}
        self.banco.com_add(tabela='PO_Patrimonios', campos_valores=dicio)
        
        # Marca rotina de exportação pickle como não feita
        codsql = f"UPDATE CW_CheckRotinas SET Feito=0, [Who]=NULL, [When]=NULL WHERE IdTask=225 AND Data={data_str}"
        self.banco.executar_comando(codsql)
        CW().rotina_diaria(id_rotina=225)
        
        return resultado

    def passivo_fundo_sc_gestao(self, id_produto=None, fundo_conta_crm=None, data_pos=None):
        """
        Essa função retorna as supercarteiras de gestão que compõe o passivo de um fundo. Pode ser chamada com o NomeContaCRM
        do fundo ou com seu ID de produto no PAS (prover penas 1 dos 2)

        Parameters
        ----------
        id_produto : TYPE, optional
            O Id do fundo no PAS. The default is None.
        fundo_conta_crm : TYPE, optional
            Conta do fundo no CRM. The default is None.
        data_pos : TYPE, optional
            Data do Arquivo da D-1. Se deixado como nulo será utilizado hoje. The default is None.

        Raises
        ------
        Warning
            Aviso de erro se não for provido nem o id de produto nem a Conta CRM do fundo.

        Returns
        -------
        DataFrame
            Dados com todas as supercarteiras de gestão que investem no fundo, financeiro aplicado no fundo e
            percentual do PL que isso representa

        """        
        # Passo 1: dados básicos
        data_str = self.data_posicao_str(data_pos)
        if not id_produto and not fundo_conta_crm:
            raise Warning('Não é possível rodar sem um dos identificadores do fundo!')
        if id_produto:
            codigo = id_produto
            df = self.cadastro_fundos(filtro=f"CodigoProduto='{id_produto}'")
            if not df.empty:
                nome_fundo = df.iloc[0]['NomeContaCRM']
                gestao_jbfo = True
            else:
                nome_fundo = Crm().product(filtro=f"new_IdSistemaOrigem='{id_produto}'").iloc[0]['new_nomedorelatorio']
                gestao_jbfo = False
        if fundo_conta_crm:
            nome_fundo = fundo_conta_crm
            gestao_jbfo = True
            df = self.cadastro_fundos(filtro=f"NomeContaCRM={self.banco.sql_texto(fundo_conta_crm)}")
            if df.empty:
                # Fundo sem passivo na D-1
                return pd.DataFrame()
            else:
                codigo = df.iloc[0]['CodigoProduto']      
        
        # Passo 2: Identifica carteiras que investem no fundo        
        df = self.__passivo_fundo_sc_gestao_listacotistas__(id_produto=codigo, data_str=data_str)
        df_fundos = df[df['CodigoProduto']!='NULL']        
        
        # Passo 3: Há fundos no passivo?
        cont = 0
        while len(df_fundos) != 0:            
            df_fundos = df[df['CodigoProduto']!='NULL']
            df_sc = df[df['NomeSupercarteira'].notnull()].copy()
            lista_fundos = []
            if len(df_fundos) > 0:
                for idx, row in df_fundos.iterrows():
                    df = self.__passivo_fundo_sc_gestao_listacotistas__(id_produto=row['CodigoProduto'], data_str=data_str)
                    # df['CodigoProduto'].loc[df['CodigoProduto']=='NULL'] = row['CodigoProduto']
                    df['FinanceiroFinal'] = row['FinanceiroFinal'] * df['FinanceiroFinal'] / df['FinanceiroFinal'].sum()
                    df['FinanceiroFuturo'] = row['FinanceiroFuturo'] * df['FinanceiroFuturo'] / df['FinanceiroFuturo'].sum()
                    lista_fundos.append(df)
            if len(lista_fundos) > 0:
                
                lista_fundos.append(df_sc)

                df = pd.concat(lista_fundos)
            else:
                df = df_sc
            df = df.loc[~df['CodigoProduto'].isin(df_fundos['CodigoProduto'].tolist())]
            df['CodigoProduto'] = df['CodigoProduto'].fillna('NULL')

            df_fundos = df[df['CodigoProduto']!='NULL']
            
            cont += 1
            
        # Passo 4: Consolida dados
        df.drop(['CodigoProduto', 'IdFonte'], axis=1)
        df = df.groupby('NomeSupercarteira').sum()
        df.reset_index(inplace=True)
        df = df[['NomeSupercarteira', 'FinanceiroFinal', 'FinanceiroFuturo']]
        
        # Passo 5: Busca dados da SC
        cad = self.cadastro_super_carteiras()
        df.insert(1, 'GuidSupercarteira', [None] * len(df))
        df.insert(2, 'SCGestao', [None] * len(df))
        df.insert(3, 'Perfil', [None] * len(df))
        df.insert(len(df.columns), 'PL', [0] * len(df))
        df.insert(len(df.columns), 'PLFuturo', [0] * len(df))
        df.insert(len(df.columns), 'Share', [0] * len(df))
        df.insert(len(df.columns), 'ShareFuturo', [0] * len(df))

        for idx, row in df.iterrows():
            try:
                lin_cad = cad[cad['NomeSuperCarteiraCRM']==row['NomeSupercarteira']].iloc[0]
            except:
                continue
            df.loc[idx, 'GuidSupercarteira'] = lin_cad['GuidSuperCarteira']
            df.loc[idx, 'SCGestao'] = lin_cad['SCGestao']
            df.loc[idx, 'Perfil'] = lin_cad['Perfil']
            # PL do fundo
            if gestao_jbfo:
                base_pl = self.fundos_pl_estimado(filtrar_fundo=nome_fundo).iloc[0]
                # Restante            
                df.loc[idx, 'PLFuturo'] = base_pl['PLFuturo']
                df.loc[idx, 'Share'] = row['FinanceiroFinal'] / base_pl['PL']
                df.loc[idx, 'ShareFuturo'] = row['FinanceiroFuturo'] / base_pl['PLFuturo']
        df = df.loc[df['GuidSupercarteira'].notnull()]
        return df
    
    def passivo_fundo_titularidade(self, id_produto=None, fundo_conta_crm=None, data_pos=None, filtrar_representatividade=False, representacao_share_pl=0.85, representacao_share_pl_cotista=0.1, buffer:bool=False):
        """
        Essa função retorna as titularidades de gestão que compõe o passivo de um fundo. Pode ser chamada com o NomeContaCRM
        do fundo ou com seu ID de produto no PAS (prover penas 1 dos 2)

        Parameters
        ----------
        id_produto : TYPE, optional
            O Id do fundo no PAS. The default is None.
        fundo_conta_crm : TYPE, optional
            Conta do fundo no CRM. The default is None.
        data_pos : TYPE, optional
            Data do Arquivo da D-1. Se deixado como nulo será utilizado hoje. The default is None.
        buffer : bool, optional
            Guarda os dados para facilitar rodar vários fundos em sequencia

        Raises
        ------
        Warning
            Aviso de erro se não for provido nem o id de produto nem a Conta CRM do fundo.

        Returns
        -------
        DataFrame
            Dados com todas as titularidades que investem no fundo, financeiro aplicado no fundo e
            percentual do PL que isso representa

        """        
        # Passo 1: dados básicos
        data_str = self.data_posicao_str(data_pos)
        
        if buffer and self.buffer_passivo_fundo_titularidade_set == False:
            codsql = f"""
                SELECT   DISTINCT Cart.IdFonte, Cart.FinanceiroFinal, ISNULL(D.CodigoProduto, 'NULL') as CodigoProduto, Cart.FinanceiroFuturo, Cart.TitularidadeGuid, Cart.Titularidade, Cart.IdProdutoProfitSGI
                FROM	PO_Carteira Cart WITH (NOLOCK) 
                        INNER JOIN (SELECT DISTINCT Titularidade, TitularidadeGuid, CodigoProduto FROM PO_Cadastro WITH (NOLOCK)) D ON D.TitularidadeGuid=Cart.TitularidadeGuid
                WHERE Cart.DataArquivo = {data_str}
                """
            self.buffer_passivo_fundo_titularidade = self.banco.dataframe(codsql).set_index('IdProdutoProfitSGI')
            self.buffer_passivo_fundo_titularidade_set = True
        elif not self.buffer_passivo_fundo_titularidade_set:
            self.buffer_passivo_fundo_titularidade = pd.DataFrame()
            
        if not id_produto and not fundo_conta_crm:
            raise Warning('Não é possível rodar sem um dos identificadores do fundo!')
        if id_produto:
            codigo = id_produto
            df = self.cadastro_fundos(filtro=f"CodigoProduto='{id_produto}'")
            if not df.empty:
                nome_fundo = df.iloc[0]['NomeContaCRM']
                gestao_jbfo = True
            else:
                nome_fundo = Crm().product(filtro=f"new_IdSistemaOrigem='{id_produto}'").iloc[0]['new_nomedorelatorio']
                gestao_jbfo = False
        if fundo_conta_crm:
            nome_fundo = fundo_conta_crm
            gestao_jbfo = True
            df = self.cadastro_fundos(filtro=f"NomeContaCRM={self.banco.sql_texto(fundo_conta_crm)}")
            if df.empty:
                # Fundo sem passivo na D-1
                return pd.DataFrame()
            else:
                codigo = df.iloc[0]['CodigoProduto']      
        
        # Passo 2: Identifica carteiras que investem no fundo        
        df = self.__passivo_fundo_titularidade_listacotistas__(id_produto=codigo, data_str=data_str)
        df_fundos = df[df['CodigoProduto']!='NULL']
        
        # Passo 3: Há fundos no passivo?
        cont = 0
        while len(df_fundos) != 0:            
            df_fundos = df[df['CodigoProduto']!='NULL']
            df_sc = df[df['CodigoProduto']=='NULL'].copy()
            lista_fundos = []
            if len(df_fundos) > 0:
                for idx, row in df_fundos.iterrows():
                    df = self.__passivo_fundo_titularidade_listacotistas__(id_produto=row['CodigoProduto'], data_str=data_str)
                    df['FinanceiroFinal'] = row['FinanceiroFinal'] * df['FinanceiroFinal'] / df['FinanceiroFinal'].sum()
                    df['FinanceiroFuturo'] = row['FinanceiroFuturo'] * df['FinanceiroFuturo'] / df['FinanceiroFuturo'].sum()
                    lista_fundos.append(df)
            if len(lista_fundos) > 0:
                lista_fundos.append(df_sc)
                df = pd.concat(lista_fundos)
            else:
                df = df_sc
            df['CodigoProduto'] = df['CodigoProduto'].fillna('NULL')
            df_fundos = df[df['CodigoProduto']!='NULL']
            cont += 1
            
        # Passo 4: Consolida dados
        df.drop(['CodigoProduto', 'IdFonte'], axis=1)
        df = df.groupby(['TitularidadeGuid', 'Titularidade']).sum()
        df.reset_index(inplace=True)
        df = df[['TitularidadeGuid', 'Titularidade', 'FinanceiroFinal', 'FinanceiroFuturo']]
        
        # Passo 5: Busca dados do fundo
        df.insert(len(df.columns), 'fPL', [0] * len(df))
        df.insert(len(df.columns), 'fPLFuturo', [0] * len(df))
        df.insert(len(df.columns), 'Share', [0] * len(df))
        df.insert(len(df.columns), 'ShareFuturo', [0] * len(df))

        for idx, row in df.iterrows():            
            # PL do fundo
            if gestao_jbfo:
                base_pl = self.fundos_pl_estimado(filtrar_fundo=nome_fundo).iloc[0]
                # Restante            
                df.loc[idx, 'fPL'] = base_pl['PL']
                df.loc[idx, 'fPLFuturo'] = base_pl['PLFuturo']
                df.loc[idx, 'Share'] = row['FinanceiroFinal'] / base_pl['PL']
                df.loc[idx, 'ShareFuturo'] = row['FinanceiroFuturo'] / base_pl['PLFuturo']
        df = df[df['FinanceiroFinal']>0]  # Bugs aleatórios da D-1
        
        # Passo 5: Busca dados da titularidade
        lista = list(df['TitularidadeGuid'].unique())
        plt = self.titularidade_pls(lista_guids=lista).set_index('TitularidadeGuid')[['PL', 'PLFut']]
        df = pd.merge(left=df, left_on='TitularidadeGuid', right=plt, right_index=True, how='inner')
                
        if filtrar_representatividade:
            # Seleciona passivo válido com base em duas premissas:
            # a. Pelo menos 85% do PL do fundo representado : representacao_share_pl
            # b. todos os cotistas com mais de 10% do fundo representados: representacao_share_pl_cotista
            df = df.sort_values('Share', ascending=False)
            df['ShareAcum'] = df['Share'].cumsum()
            
            df.insert(len(df.columns), 'Sel', [0] * len(df))
            lim_share_acum = df[df['ShareAcum'] >= representacao_share_pl]['ShareAcum'].min()
            df['Sel'] = df['ShareAcum'].apply(lambda x: 1 if x <= lim_share_acum else 0) + df['Share'].apply(lambda x: 1 if x >= representacao_share_pl_cotista else 0)
            df = df[df['Sel']>0]
            df.drop('Sel', axis=1, inplace=True)
        return df
    
    def sc_contas_movimento(self, nome_supercarteira=None, guid_supercarteira=None, data_pos=None):        
        """
        Retorna contas movimento vinculadas a uma supercarteira que tenham
        posição na D-1. Pode-se fazer a busca pelo nome da supercarteira ou 
        seu guid no CRM.

        Parameters
        ----------
        nome_supercarteira : TYPE, optional
            Nome da Supercarteira. The default is None.
        guid_supercarteira : TYPE, optional
            Guid da supercarteira. The default is None.
        data_pos : TYPE, optional
            uma data para buscar na D-1. Se não informado será utilizado hoje.

        Raises
        ------
        Exception
            Erro se não for informado guid ou nome da supercarteira.

        Returns
        -------
        TYPE
            dataframe com 3 colunas: GuidContaMovimento, NomeContaMovimento, Total
            o último contém o saldo da conta na posição d-1 da data_pos

        """
        if nome_supercarteira:
            filtro = f"SCC.NomeSupercarteira='{nome_supercarteira}'"
        elif guid_supercarteira:
            filtro = f"SCC.GuidSupercarteira='{guid_supercarteira}'"
        else:
            raise Exception('PosicaoDm1/sc_contas_movimento: é preciso informar nome_supercarteira ou guid_supercarteira!')
        data_str = self.banco.sql_data(data_pos)
        codsql = f"""
            SELECT C.GuidContaMovimento, C.NomeContaMovimento, SUM(FinanceiroFinal) as Total
            FROM PO_SCComp SCC WITH (NOLOCK) INNER JOIN PO_Carteira C WITH (NOLOCK) ON SCC.NomeCarteira=C.IdFonte
            WHERE SCC.EntraComp<>0 AND DataArquivo={data_str} AND {filtro}
            GROUP BY C.GuidContaMovimento, C.NomeContaMovimento
            """
        df = self.banco.dataframe(codsql=codsql)
        df = df[~df['GuidContaMovimento'].isnull()]
        crm = Crm()
        for idx, row in df.iterrows():
            if not row['NomeContaMovimento']:
                df.loc[idx, 'NomeContaMovimento'] = crm.conta_movimento_nome(row['GuidContaMovimento'])
            
        return df


class PosicaoDm1Pickle(DBPosicaoDm1Pickle):
    """
    Classe PosicaoDm1, mas com suporte a arquivos pickle, exportados anteriormente
    na pasta 'O:/SAO/CICH_All/Portfolio Management/Arquivos/Pickle/PosicaoDm1/'
    """
    def __init__(self, load_all_positions=False, por_titularidade=False, com_explosao=True, pos_apenas_fundos_exclusivos=True, data_posicao=False, homologacao=False):    
        super().__init__(load_all_positions=False, por_titularidade=por_titularidade, com_explosao=com_explosao, 
                     pos_apenas_fundos_exclusivos=pos_apenas_fundos_exclusivos, data_posicao=data_posicao, homologacao=homologacao)

    def crm_cadastro(self):
        
        def __ativo_regra_nome__(nome, nome_rel, mnemonico):        
            retorno = nome
            if nome_rel:
                retorno = nome_rel
            if mnemonico:
                retorno = f"{retorno} ({mnemonico})"
            return retorno
        
        crm = Crm(homologacao=self.homologacao)
        df = crm.product(lista_campos=['productid', 'producttypecodename', 'name', 'new_nomedorelatorio', 'new_mnemonico', 'New_Onshore', 'new_ratinggps'])    
        df.insert(2,'Nome', [None] * len(df))
        df['Nome'] = df.apply(lambda x: __ativo_regra_nome__(x['name'], x['new_nomedorelatorio'], x['new_mnemonico']), axis=1)
        df = df[['producttypecodename', 'Nome', 'New_Onshore', 'new_ratinggps']].sort_values('Nome')
        df.columns = ['Tipo', 'Nome', 'OnShore', 'Rating']
        
        return df
    

class Mandato:
    """
    Classe criada para intemediar interações com a base BAWM (BAWM), no servidor Aux,
    tratando especificamente de coisas ligadas ao controle de mandatos.
    
    Ela lida com as tabelas cujo nome começa com PI_* 
    
    """
    def __init__(self, buffer:bool=False, homologacao=False):
        if homologacao:
            self.banco = BaseSQL(nome_servidor=r'SQLSVRHOM1.GPS.BR\SQL12', nome_banco='BAWM_HOM1')
        else:
            self.banco = BaseSQL(nome_servidor=ambiente.servidor_aux(), nome_banco='BAWM')
        self.campos = self.banco.dataframe("SELECT IdCampo, TipoValor, NomeCampo, Ordem, Enquadramento FROM PI_Campos WITH (NOLOCK) ORDER BY Ordem")
        
        self.buffer = buffer
        
        # Variáveis de buffer
        self.data_base = self.banco.hoje()
        self.df_mandatos_ativos = pd.DataFrame()
        self.df_mandatos_detalhe = pd.DataFrame()
        
    def mandatos_ativos(self, data_base=None):
        """
        Busca os mandatos ativos para uma data. 
        Um mandato ativo significa que a data início é menor ou igual a hoje e a data final está em branco ou é posterior a hoje.
        A estrutura foi feita assim para permitir agendar mudanças de mandato

        Parameters
        ----------
        data_base : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        
        data_pos = data_base
        if not data_pos:
            data_pos = self.banco.hoje()
        
        data_str = self.banco.sql_data(data_pos)
        
        codsql = f""" SELECT * FROM PI_Mandato WITH (NOLOCK)
                      WHERE (DataFinal IS NULL OR DataFinal>{data_str}) AND DataInicio<={data_str}
                      ORDER BY IdMandato
                  """
        if self.df_mandatos_ativos.empty or data_pos != self.data_base:
            df = self.banco.dataframe(codsql)
            if self.buffer:
                self.data_base = data_pos
                self.df_mandatos_ativos = df.copy()
        else:
            df = self.df_mandatos_ativos.copy()
        
        return df

    def portfolio_mandato_ativo(self, guid_portfolio:str, data_base=None):
        """
        Busca o cadastro do mandato ativo de um portfólio, conforme a data base

        Parameters
        ----------
        guid_portfolio : str
            DESCRIPTION.
        data_base : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        df : TYPE
            DESCRIPTION.

        """
        if self.buffer:
            df = self.mandatos_ativos(data_base=data_base)
            df = df[df['GuidPortfolio']==guid_portfolio]
        else:
            data_str = self.banco.sql_data(data_base)
            codsql = f""" SELECT * FROM PI_Mandato WITH (NOLOCK)
                          WHERE (DataFinal IS NULL OR DataFinal>{data_str}) AND DataInicio<={data_str} AND GuidPortfolio='{guid_portfolio}'
                          ORDER BY IdMandato
                      """
            df = self.banco.dataframe(codsql)
        return df
    
    def mandatos_detalhe(self, data_base=None):
        """
        Busca os detalhes de todos os mandatos ativos em uma data base

        Parameters
        ----------
        data_base : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        df : TYPE
            DESCRIPTION.

        """
        data_pos = data_base
        if not data_pos:
            data_pos = self.banco.hoje()        
        data_str = self.banco.sql_data(data_pos)                            
        
        codsql = f"""SELECT D.IdMandato, GuidPortfolio, NomePortfolio, D.IdDetalhe, D.IdCampo, C.NomeCampo, Chave, D.[Level] as Nivel, ValorTgt, ValorMin, ValorMax, ValorBool, ValorInt, ValorIdCampo, IndexCod, Enquadramento, Quem, Quando
                    FROM PI_Detalhe D WITH (NOLOCK) INNER JOIN PI_Campos C WITH (NOLOCK) ON D.IdCampo=C.IdCampo
                    INNER JOIN (SELECT IdMandato, GuidPortfolio, NomePortfolio FROM PI_Mandato WITH (NOLOCK)
				                WHERE (DataFinal IS NULL OR DataFinal>{data_str}) AND DataInicio<={data_str}
                                ) TBL ON D.IdMandato=Tbl.IdMandato
                    ORDER BY D.IdMandato, C.Ordem, C.NomeCampo, D.Chave"""
        
        if self.df_mandatos_detalhe.empty or data_pos != self.data_base:
            df = self.banco.dataframe(codsql)
            if self.buffer:
                self.data_base = data_pos
                self.df_mandatos_detalhe = df.copy()
        else:
            df = self.df_mandatos_detalhe.copy()
                
        return df
    
    def mandato_detalhe(self, id_mandato:int=None, guid_portfolio:str=None, data_base=None):
        """
        Busca os detalhes de um mandato

        Parameters
        ----------
        id_mandato : int, optional
            DESCRIPTION. The default is None.
        guid_portfolio : str, optional
            DESCRIPTION. The default is None.

        Raises
        ------
        Exception
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        if not id_mandato and not guid_portfolio:
            raise Exception('É necessário informar o id do mandato ou o guid_portfolio')
        if self.buffer:
            df = self.mandatos_detalhe(data_base=data_base)
            if id_mandato:
                df = df[df['IdMandato']==id_mandato]
            if guid_portfolio:
                df = df[df['GuidPortfolio']==guid_portfolio]
            return df
        else:
            if id_mandato:
                filtro = f"D.IdMandato={id_mandato}"
            else:
                data_pos = data_base
                if not data_pos:
                    data_pos = self.banco.hoje()        
                data_str = self.banco.sql_data(data_pos)
                
                filtro = f"M.GuidPortfolio='{guid_portfolio}' AND ((DataFinal IS NULL OR DataFinal>{data_str}) AND DataInicio<={data_str})"                              
            
            codsql = f"""SELECT D.IdMandato, GuidPortfolio, NomePortfolio, D.IdDetalhe, D.IdCampo, C.NomeCampo, Chave, D.[Level] as Nivel, ValorTgt, ValorMin, ValorMax, ValorBool, ValorInt, ValorIdCampo, IndexCod, Enquadramento, D.Quem, D.Quando, C.Ordem
                        FROM PI_Detalhe D WITH (NOLOCK) INNER JOIN PI_Campos C WITH (NOLOCK) ON D.IdCampo=C.IdCampo
                             INNER JOIN PI_Mandato M ON D.IdMandato=M.IdMandato
                        WHERE C.Enquadramento<>0 AND {filtro} 
                        
                        UNION ALL
                        
                        SELECT D.IdMandato, GuidPortfolio, NomePortfolio, DS.IdDetalhe, DS.IdCampo, DS.NomeCampo, DS.Chave, Nivel, D.ValorTgt * DS.ValorTgt as ValorTgt, D.ValorTgt * DS.ValorMin as ValorMin, D.ValorTgt * DS.ValorMax as ValorMax, DS.ValorBool, DS.ValorInt, D.ValorIdCampo, D.IndexCod, DS.Enquadramento, DS.Quem, DS.Quando, DS.Ordem
                        FROM PI_Detalhe D WITH (NOLOCK) INNER JOIN PI_Campos C WITH (NOLOCK) ON D.IdCampo=C.IdCampo
                             INNER JOIN PI_Mandato M ON D.IdMandato=M.IdMandato
                        	 INNER JOIN (SELECT D.IdMandato, D.IdDetalhe, D.IdCampo, C.NomeCampo, D.Chave, D.[Level] as Nivel, D.ValorTgt, D.ValorMin, D.ValorMax, D.ValorBool, D.ValorInt, D.ValorIdCampo, D.IndexCod, C.Enquadramento, D.Quem, D.Quando, C.Ordem
                        				 FROM PI_Detalhe D WITH (NOLOCK) INNER JOIN PI_Campos C WITH (NOLOCK) 
                        				 ON D.IdCampo=C.IdCampo) DS ON D.Chave=DS.IdMandato
                        WHERE C.IdCampo=5 AND {filtro} 

                        ORDER BY D.IdMandato, C.Ordem, C.NomeCampo, D.Chave"""
            return self.banco.dataframe(codsql)
    
    def portfolio_historico_mandatos(self, guid_portfolio:str):
        """
        Busca todos os mandatos já cadastrados para um determinado portfolio

        Parameters
        ----------
        guid_portfolio : str
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        codsql = f""" SELECT * FROM PI_Mandato WITH (NOLOCK)
                      WHERE GuidPortfolio='{guid_portfolio}'
                      ORDER BY IdMandato
                  """
        return self.banco.dataframe(codsql)
    
    def portfolio_insere_mandato(self, id_tipo_port:int, guid_portfolio:str, nome_portfolio:str, dados:pd.DataFrame, politica_inv:str, dpm:str=None, 
                                 index_cod_bench:str=None, index_cod_referencia:str=None, data_inicio=None):
        """
        Insere um novo mandato na tabela PI_Mandato
        Se houver um mandato em vigor para o mesmo portfolio, coloca data final nele

        Parameters
        ----------
        guid_portfolio : str
            DESCRIPTION.
        nome_portfolio : str
            DESCRIPTION.
        dpm : str
            DESCRIPTION.
        politica_inv : str
            DESCRIPTION.
        index_cod_bench : str
            DESCRIPTION.
        index_cod_ref : str
            DESCRIPTION.
        data_inicio : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        None.

        """
        # 1. Data de início do mandato
        data_ini = data_inicio
        if not data_ini:
            data_ini = self.banco.hoje()
            
        # 2. Procura se há outro mandato em aberto e o encerra
        hist = self.portfolio_historico_mandatos(guid_portfolio=guid_portfolio)
        if not hist.empty:            
            hist = hist[hist['DataFinal'].isnull()]
            for idx, row in hist.iterrows():
                df = pd.DataFrame([{'IdMandato': row['IdMandato'], 'DataFinal': data_ini - relativedelta(days=1)}])
                res = self.banco.com_edit_df(tabela='PI_Mandato', dados=df, campos_filtros=['IdMandato'])
        
        # 3. Monta Df de campos para adicionar
        dicio = {'DataInicio': data_ini, 'IdTipoPort': id_tipo_port, 'GuidPortfolio': guid_portfolio, 'NomePortfolio': nome_portfolio,
                 'PoliticaInv': politica_inv}
        if dpm:
            dicio['DPM'] = dpm
        if index_cod_bench:
            dicio['IndexCodBench'] = index_cod_bench
        if index_cod_referencia:
            dicio['IndexCodRef'] = index_cod_referencia
        id_mandato = self.banco.com_add(tabela='PI_Mandato', campos_valores=dicio, output_campo='IdMandato')
        return self.__mandato_inserir_detalhes__(id_mandato=id_mandato, dados=dados)
            
    def __mandato_inserir_detalhes__(self, id_mandato, dados:pd.DataFrame):
        if 'Classe' in dados.columns:
            pass
        if 'SubClasse' in dados.columns:
            pass


class CashCow:
    """
    Classe criada para intemediar interações com a base BAWM (BAWM), no servidor Aux,
    tratando especificamente de coisas ligadas à CashCow - Projeção diária de caixa.
    
    Ela lida com as tabelas cujo nome começa com CX_* 
    
    Tabelas são alteradas regularmente ao longo do dia: não usar NO LOCK
    """
    def __init__(self, homologacao=False):
        if homologacao:
            self.banco = BaseSQL(nome_servidor=r'SQLSVRHOM1.GPS.BR\SQL12', nome_banco='BAWM_HOM1')
        else:
            self.banco = BaseSQL(nome_servidor=ambiente.servidor_aux(), nome_banco='BAWM')
        
    def data_posicao_id(self, data_pos=None):
        data_str = self.banco.sql_data(data_pos)
        codsql = f"SELECT IdData FROM CX_DataAtu WITH (NOLOCK) WHERE Data={data_str}"
        return self.banco.busca_valor_codsql(codsql=codsql, campo='IdData')
    
    def data_posicao_id_ultimo(self, data_pos_max=None):        
        data_str = self.banco.sql_data(data_pos_max)
        codsql = f"""
                DECLARE @Data Datetime
                SELECT @Data = Max(Data) FROM CX_DataAtu WHERE Data<={data_str}
                SELECT IdData FROM CX_DataAtu WHERE Data=@Data
                """        
        return self.banco.busca_valor_codsql(codsql=codsql, campo='IdData')
    
    def fluxo_data_posicao(self, id_data=None, data_pos=None, filtro_fundo_nome_conta_crm=None):
        if id_data:
            id_da_data = id_data
        else:
            id_da_data = self.data_posicao_id_ultimo(data_pos_max=data_pos)
        
        filtro = ''
        if filtro_fundo_nome_conta_crm:
            filtro = f"AND NomeContaCRM={self.banco.sql_texto(filtro_fundo_nome_conta_crm)}"
        
        codsql = f"""
                SELECT IdData, LOWER(CAST(Ag.FundoIdCRM as VARCHAR(255))) as GuidContaCRM, Cad.NomeContaCRM, Data,[Ativo],[Passivo], [Net]
                FROM CX_Agendamento Ag INNER JOIN PO_Cadastro Cad ON Cad.GuidContaCRM=CAST(Ag.FundoIdCRM as VARCHAR(255))
                WHERE IdData={id_da_data} {filtro}
                ORDER BY NomeContaCRM, Data
                 """
        df = self.banco.dataframe(codsql=codsql)
        return df
    

class Secundario:
    """
    Tabelas são alteradas regularmente ao longo do dia: não usar NO LOCK
    """    
    def __init__(self, buffer:bool=False, homologacao:bool=False):
        self.homologacao = homologacao
        self.buffer = buffer
        if homologacao:
            self.banco = BaseSQL(nome_servidor=r'SQLSVRHOM1.GPS.BR\SQL12', nome_banco='BAWM_HOM1')
        else:
            self.banco = BaseSQL(nome_servidor=ambiente.servidor_aux(), nome_banco='BAWM')
        
        # Buffers
        self.ativos_capacity = []
        self.compras_em_aberto_set = False
        self.compras_em_aberto_df = pd.DataFrame()
        self.vendas_rateadas_set = False
        self.vendas_rateadas_df = pd.DataFrame()
        
    
    def cadastros_a_pedir(self, data_limite=None, data_posicao=None):
        """
        Função retorna operações rateadas em cotas de fundo no secundário interno que ainda
        vão liquidar, para que possamos pedir / verificar os cadastros
        
        Parameters
        ----------
        data_limite : data, optional
            Data de negociação prevista no sistema de secundário. None traz hoje
        data_posicao : TYPE, optional
            data da posição (data arquivo) na base da D-1. None traz última D-1 disponível

        Returns
        -------
        dataframe com os dados

        """
        data_str = self.banco.sql_data(data_limite)
        dm1 = PosicaoDm1()
        data_dm1 = dm1.data_posicao_str(data_posicao)
        campo_conta = "REPLACE(REPLACE(ISNULL(B.B_FundoGuid, B.B_CRMGuidConta),'}',''),'{','')"
        codsql = f"""
                    SELECT S.IdTrade, S.DataNeg, S.NomeProduto, S.GuidProduto, ISNULL(Cart.NomeContaCRM,ISNULL(B.B_FundoExcl, B.B_ContaCRM)) as Comprador, {campo_conta} as GuidComprador, B.QtdeRateio, B.QtdeRateio* PUFonte as ValorAplicacao, FinanceiroFinal as PosAtual, B.QuandoR as QuandoRateio
                    FROM	PR_Secundario S INNER JOIN PR_Sec_Buyer B ON S.IdTrade=B.IdTrade
                    		LEFT JOIN (SELECT GuidContaCRM, C.NomeContaCRM, D.GuidProduto, FinanceiroFinal FROM PO_Cadastro C INNER JOIN PO_Carteira D ON C.NomeContaCRM=D.NomeContaCRM WHERE D.DataArquivo={data_dm1}) Cart 
                    			ON S.GuidProduto=Cart.GuidProduto AND {campo_conta}=Cart.GuidContaCRM
                    WHERE	S.Rateado<>0 AND S.Cancelado=0 AND S.Negociado=0 AND S.Priced=0 AND S.TipoProduto='COTAS' 
                    		AND (S.DataNeg IS NULL OR S.DataNeg>={data_str})
                    		AND B.Compra<>0 AND B.Cancelado=0
                    
                    ORDER BY S.DataNeg, S.IdTrade
                 """
        return self.banco.dataframe(codsql=codsql)
    
    def ofertas_buscar_ativo(self, guid_ativo):
        codsql = f"""
                  SELECT * 
                  FROM PR_Secundario
                  WHERE Cancelado=0 AND Rateado=0 AND GuidProduto='{guid_ativo}' 
                 """
        return self.banco.dataframe(codsql=codsql)
    
    def ofertas_verificar_primario_ativo(self, guid_ativo):
        if len(self.ativos_capacity) == 0:
            codsql = "SELECT DISTINCT GuidProduto, NomeProduto from PR_Secundario WITH (NOLOCK) WHERE Cancelado=0 AND Rateado=0 AND TipoProduto IN ('COTAS', 'FUNDO') AND LEFT(NomeProduto,4)='[PM]'"
            df =  self.banco.dataframe(codsql)
            self.ativos_capacity = list(df['GuidProduto'].unique())
        
        if guid_ativo in self.ativos_capacity:
            return True
        else:
            return False

    def ofertas_criar(self, guid_produto, nome_produto, emissor, emissor_guid, classe, sub_classe, tipo_produto, data_emissao=None,
                      indexador=None, data_vencimento=None, coupon=None, percentual=None, cetip=None, disp_pf:bool=True,
                      data_fonte=None, pu_fonte=None, data_limite=None, primario:bool=False, por_financeiro:bool=False):
        if disp_pf:
            disp_pf_t = 1
        else:
            disp_pf_t = 0
        if tipo_produto:
            tp_prod = tipo_produto
        else:
            tp_prod = 'COTAS'
            temp = nome_produto.lower().split(' ')
            if 'fidc' in temp:
                tp_prod = 'FIDC'
        if primario:
            prim = 1
        else:
            prim = 0
        if por_financeiro:
            por_fin = 1
        else:
            por_fin = 0
        dicio = {'DispPF': disp_pf_t, 'GuidProduto': guid_produto, 'NomeProduto': nome_produto, 'NomeEmissor': emissor, 
                 'GuidEmissor': emissor_guid, 'DataEmissao': data_emissao, 'DataVencimento': data_vencimento,
                 'Indexador': indexador, 'Classe': classe, 'SubClasse': sub_classe, 'Taxa': percentual,
                 'Coupon': coupon, 'TipoProduto': tp_prod, 'CodigoCetip': cetip, 'AtivoCadastrado': 1, 'DataFonte': data_fonte,
                 'PUFonte': pu_fonte, 'Primario': prim, 'ExportFinanceiro': por_fin}
        if primario:
            dicio['QtdeDispPri'] = 100000000  # R$ 100 mi
        if data_limite:
            dicio['DtLimiteDem'] = data_limite
        df = pd.DataFrame([dicio])
        return self.banco.com_add_df(tabela='PR_Secundario', df=df)
    
    def ofertas_buscar_idtrade(self, id_trade, campos_adicionais:bool=False):
        """
        Busca os dados de uma ordem pelo Idtrade na tabela PR_Secundario

        Parameters
        ----------
        id_trade : inteiro
            Id na tabela PR_Secundario.

        Returns
        -------
        pd.DataFrame
            DataFrame com os dados.

        """
        campos = ""
        if campos_adicionais:
            campos = ", S.Taxa, S.Coupon, S.Indexador, S.DataEmissao, S.DataVencimento"
        codsql = f"""
                  SELECT S.IdTrade, S.GuidProduto, S.NomeProduto, S.NomeEmissor, S.GuidEmissor, S.Classe, S.SubClasse, S.TipoProduto, S.PUFonte, S.PrecoCompra, S.PrecoVenda, S.Primario, S.Compra, S.Rateado, S.Priced, S.Negociado, S.Cancelado, S.DispPF, S.Contraparte, S.DtLimiteDem,
                         CASE WHEN S.Primario<>0 THEN S.QtdeDispPri ELSE (CASE WHEN S.Compra<>0 THEN QtdeCompra ELSE QtdeVenda END) END AS QtdeDisponivel, S.QtdeDispPri, TBL.QtdeCompra, TBL.QtdeVenda {campos}
                  FROM PR_Secundario S LEFT JOIN (
                          SELECT IdTrade, SUM(CASE WHEN Compra=0 THEN ISNULL(QtdeDem,0) ELSE 0 END) as QtdeVenda, SUM(CASE WHEN Compra<>0 THEN ISNULL(QtdeDem,0) ELSE 0 END) as QtdeCompra FROM PR_Sec_Buyer WHERE IdTrade={id_trade} GROUP BY IdTrade
                      ) TBL ON S.IdTrade=TBL.IdTrade
                  WHERE S.IdTrade={id_trade}
                 """
        return self.banco.dataframe(codsql=codsql)
    
    def ofertas_buscar_abertas(self):
        """
        Ofertas Abertas são aquelas que não foram rateadas nem canceladas

        Returns
        -------
        DataFrame
            Dados completos da ordem

        """
        codsql = "SELECT * FROM PR_Secundario WHERE Cancelado=0 AND Rateado=0 AND Negociado=0 AND (DtLimiteDem > GETDATE() OR DtLimiteDem IS NULL) ORDER BY NomeProduto"
        return self.banco.dataframe(codsql=codsql)
    
    def ofertas_buscar_compraveis(self, apenas_com_oferta=True):
        """
        Ofertas Abertas são aquelas que não foram rateadas nem canceladas, e onde há vendedores

        Returns
        -------
        DataFrame
            Dados completos da ordem

        """
        busca_guid = "REPLACE(REPLACE(LOWER(S.GuidProduto), '}', ''), '{', '')"
        codsql = f"""
                    SELECT S.IdTrade, S.Primario, S.Classe, S.SubClasse, S.TipoProduto, {busca_guid} as GuidProduto, S.NomeProduto, A.CodNegociacao, S.GuidEmissor, S.NomeEmissor, S.Indexador, S.PUFonte, SUM(CASE WHEN B.Compra<>0 THEN QtdeDem ELSE 0 END) as Demanda, CASE WHEN S.Primario<>0 THEN S.QtdeDispPri ELSE SUM(CASE WHEN B.Compra=0 THEN QtdeDem ELSE 0 END) END as Oferta, S.ExportFinanceiro
                    FROM PR_Secundario S LEFT JOIN (SELECT * FROM PR_Sec_Buyer WHERE Cancelado=0) B ON S.IdTrade=B.IdTrade
                         LEFT JOIN PO_Ativos A ON {busca_guid}=A.GuidAtivo
                    WHERE S.Cancelado=0 AND S.Rateado=0 AND (DtLimiteDem > GETDATE() OR DtLimiteDem IS NULL) AND S.Negociado=0
                    GROUP BY S.IdTrade, S.Primario, S.Classe, S.SubClasse, S.TipoProduto, {busca_guid}, S.NomeProduto, A.CodNegociacao, S.GuidEmissor, S.NomeEmissor, S.Indexador, S.PUFonte, S.QtdeDispPri, S.ExportFinanceiro
                    ORDER BY S.Primario DESC,S.Classe, S.SubClasse, S.TipoProduto
                  """
        df = self.banco.dataframe(codsql=codsql)
        if apenas_com_oferta:
            df = df[df['Oferta']>0]
            
        # Ajusta tipo produto dos fidcs
        sub_df = df[df['TipoProduto']=='COTAS']
        for idx, row in sub_df.iterrows():
            texto = str(row['NomeProduto']).lower().split(' ')
            if 'fidc' in texto:
                df.loc[idx, 'TipoProduto'] = 'FIDC'
        
        return df    
    
    def oferta_verificar_cliente(self, id_trade, conta_crm_guid=None, conta_crm_guid_fundo=None, titularidade_guid=None):
        """
        Verifica se um cliente já inseriu ordem para um IdTrade, e essa ordem não está cancelada

        Parameters
        ----------
        id_trade : long
        conta_crm_guid : string
        Returns
        -------
        True ou False

        """
        filtro_conta = f"B_CRMGuidConta='{conta_crm_guid}'"
        filtro = filtro_conta
        if conta_crm_guid_fundo:
            filtro = f"B_FundoGuid='{conta_crm_guid_fundo}' OR {filtro_conta}"
        elif titularidade_guid:        
            filtro = f"B_Titularidade='{titularidade_guid}'"
        codsql = f"""SELECT IdComprador 
                    FROM PR_Sec_Buyer 
                    WHERE Cancelado=0 AND QtdeDem - ISNULL(QtdeExecutadaExt,0) > 0 AND IdTrade={id_trade} AND 
                        ({filtro})
                        """
        if self.banco.conta_reg(codsql=codsql) == 0:
            return False
        else:
            return True
        
    def oferta_verificar_id_ordem(self, id_trade, id_ordem):
        """
        Verifica se um cliente já inseriu ordem para um IdTrade, e essa ordem não está cancelada

        Parameters
        ----------
        id_trade : long
        conta_crm_guid : string
        Returns
        -------
        True ou False

        """  
        codsql = f"""SELECT IdComprador 
                    FROM PR_Sec_Buyer 
                    WHERE Cancelado=0 AND IdTrade={id_trade} AND IdOrdem={id_ordem}
                        """
        if self.banco.conta_reg(codsql=codsql) == 0:
            return False
        else:
            return True
    
    def __fundo_enquadramento__(self, conta_crm_guid, tipo_de_titulo):
        # TODO: Desativar quando pretrade entrar no ar
        tipo_titulo = tipo_de_titulo.lower()
        # Fundo é FIC?
        try:
            cnpj = PosicaoDm1(homologacao=self.homologacao).cadastro_fundos(lista_campos=['NomeContaCRM', 'CNPJ'], filtro=f"GuidContaCRM='{conta_crm_guid}'").iloc[0]['CNPJ']
        except:
            raise Exception(f"Conta não encontrada na PO_Cadastro: {conta_crm_guid}")
        try:
            fic = CVM(homologacao=self.homologacao).fundo_cadastro(cnpj=cnpj)['FUNDO_DE_COTAS']
        except:
            raise Exception(f"Fundo não encontrada na CVM_Fundos: {cnpj} (CNPJ: {cnpj})")
        
        if fic == True:
            teste_fic = True
            if tipo_titulo[:5] == 'cotas':
                teste_fic = False
            elif tipo_titulo[:5] == 'fundo':
                teste_fic = False
            elif tipo_titulo[:2] == 'lf':
                teste_fic = False
            elif tipo_titulo[:3] == 'cdb':
                teste_fic = False
            elif tipo_titulo[:4] == 'dpge':
                teste_fic = False
            
            if teste_fic:
                raise Exception(f"O fundo {conta_crm_guid} é um FIC, e portanto não pode investir em {tipo_de_titulo}.")
    
    def oferta_inserir(self, id_trade:int, id_tipo_portfolio:int, conta_crm:str, conta_crm_guid:str, qtde_demanda:float, compra:bool=False, titularidade_guid:str=None, conta_mov_guid:str=None,
                       conta_fundo_excl:str=None, conta_fundo_guid:str=None, observacao:str=None, id_ordem:int=None, motivo_aplicacao:str=None, motivo_resgate:str=None,
                       conta_mov_dest_guid:str=None, conta_mov_entr_fin:str=None, data_validade=None, teste:bool=False) -> bool:
        """
        Insere uma linha na PR_Sec_Buyer com base em uma Ordem, vinda da BL_Ordens

        Parameters
        ----------
        id_trade : int
            IdTrade da PR_Secundario, com o Id da negociação.
        conta_crm : str
            Nome da Conta CRM.
        conta_crm_guid : str
            Guid da conta no CRM.
        qtde_demanda : float
            Quantidade de ativos demandanda.
        compra : bool, optional
            True para Compras, False para Vendas. The default is False.
        titularidade_guid : str, optional
            Como no CRM. The default is None.
        conta_mov_guid : str, optional
            Como no CRM. The default is None.
        conta_fundo_excl : str, optional
            Nome da Conta CRM do Fundo. The default is None.
        conta_fundo_guid : str, optional
            Guid da Conta CRM do Fundo. The default is None.
        observacao : str, optional
            DESCRIPTION. The default is None.
        id_ordem : int, optional
            Id da Ordem da BL_Ordem. The default is None.
        motivo_aplicacao : str, optional
            Conforme lista do CRM. The default is None.
        motivo_resgate : str, optional
            Conforme lista do CRM. The default is None.
        conta_mov_dest_guid : str, optional
            Conta Movimentação a ser utilizada como destino no CRM. The default is None.
        conta_mov_entr_fin : str, optional
            Conta Movimentação Entrada Financeiro a ser utilizada como destino no CRM. The default is None.
        data_validade : datetime, optional
            Data de validade da Ordem.

        Raises
        ------
        Exception
            Faz validações para entender se o registro inserido é valido.

        Returns
        -------
        resultado : TYPE
            resultado da função com_add da classe BaseDados.

        """
        # 1. Verifica se IdTrade existe
        oferta = self.ofertas_buscar_idtrade(id_trade=id_trade)
        if oferta.empty:
            raise Exception(f'Secundario\oferta_inserir: o IdTrade {id_trade} não foi encontrado.')
        oferta = oferta.iloc[0]
        
        # 2. Verifica se a mesma ordem já foi inserida (monitor rodando 2x)
        if self.oferta_verificar_id_ordem(id_trade=id_trade, id_ordem=id_ordem):                    
            return 'Sucesso' # volta como sucesso, para não zoar o upload da ordem
        
        # 3. Verifica se cliente já não está na oferta
        if id_tipo_portfolio == 1:
            if self.oferta_verificar_cliente(id_trade=id_trade, conta_crm_guid=conta_crm_guid, titularidade_guid=titularidade_guid):
                raise Exception(f'Secundario\oferta_inserir: o IdTrade {id_trade} já contem o cliente {conta_crm} ou seu fundo {conta_fundo_excl}.')
        else:
            if self.oferta_verificar_cliente(id_trade=id_trade, titularidade_guid=titularidade_guid):
                raise Exception(f'Secundario\oferta_inserir: o IdTrade {id_trade} já contem o cliente {conta_crm} ou seu fundo {conta_fundo_excl}.')
        # 4. Se oferta é de compra, verifica se não está sendo inserida uma compra
        if oferta['Compra'] == True and compra == True:
            raise Exception(f"Secundario\oferta_inserir: O IdTrade {id_trade} é uma oferta de compra, não é possível inserir compras.")
        
        # 5. Verifica se bid é inferior à quantidade ofertada, no caso das compras
        if compra == True:
            if oferta['QtdeDisponivel']:
                if qtde_demanda > oferta['QtdeDisponivel']:
                    # raise Exception(f"Secundario\oferta_inserir: para o IdTrade {id_trade} existem apenas {oferta['QtdeDisponivel']} papéis disponíveis, mas foram demandados {qtde_demanda}.")
                    # condição removida
                    pass
        
        # 6. Insere a Ordem no Banco
        
        # 6.a. Campos obrigatórios
        txt_compra = 0
        if compra == True:
            txt_compra=1        
        if id_tipo_portfolio == 1:
            txt_fundo = 1
        else:
            txt_fundo = 0
        motivo = 'Realocação'            
        if compra:
            txt_compra = 1
            if motivo_aplicacao:
                motivo = motivo_aplicacao
        else:
            txt_compra = 0
            if motivo_resgate:
                motivo = motivo_resgate 
        txt_observacao = 'NULL'     
        if observacao:
            txt_observacao = self.banco.sql_texto(observacao)
            
        texto = f"""EXEC PR_Add_Ordem @IdTrade={id_trade}, @Compra={txt_compra}, @IdOrdem={id_ordem}, @Fundo={txt_fundo}, @B_ContaCRM={self.banco.sql_texto(conta_crm)},
                                      @B_ContaCRMGuid='{conta_crm_guid}', @Motivo='{motivo}', @Observacao={txt_observacao}, @QtdeDem={qtde_demanda}
                 """
        # 6.b. Campos opcionais
        if titularidade_guid:
            texto += f", @B_Titularidade='{titularidade_guid}'"
        if conta_mov_dest_guid:
            texto += f", @B_ContaMovGuidDest='{conta_mov_dest_guid}'"
        if conta_mov_entr_fin:
            texto += f", @B_ContaMovEntradaFinGuid='{conta_mov_entr_fin}'"        
        if conta_mov_guid:
            banco = Crm(homologacao=self.homologacao).conta_movimento_banco(conta_mov_guid)
            texto += f", @Banco={self.banco.sql_texto(banco)}, @B_ContaMovGuid='{conta_mov_guid}'"                                
        if conta_fundo_excl:
            texto += f", @B_FundoExcl={self.banco.sql_texto(conta_fundo_excl)}, @B_FundoExclGuid='{conta_fundo_guid}'"                    
        if data_validade:
            texto += f", @DataValidadePreBol={self.banco.sql_data(data_validade)}"

        if teste:
            texto += ", @Cancelado=1"

        # 6.c. Inserir ordem
        resultado = self.banco.dataframe(codsql=texto)
        if resultado.empty:
            resultado = False
        else:
            resultado = resultado.iloc[0]['Resultado']
        return resultado                
        
    def compras_em_aberto(self, data_neg_min=None):
        """
        Busca as operações de oferta de compra que ainda não liquidaram no processo interno de passagem
        Para o sys_liquidez isso é importante pois será reservado caixa para todas as ofertas
        Parameters
        ----------
        data_neg_min : Data a partir da qual deve ser buscadas as operações
            DESCRIPTION.

        Returns
        -------
        df : dataframe com os dados
        """
        # TODO: Transformar em PROC
        if not self.compras_em_aberto_set:            
            data_str = self.banco.sql_data(data_neg_min)
            texto = f"DECLARE @DataNeg Datetime={data_str}"
            codsql = texto + """ 
                    SELECT S.IdTrade, S.DataNeg, LOWER(S.GuidProduto) as GuidProduto, S.NomeProduto, S.Rateado, B.Fundo,
                    	   LOWER(REPLACE(REPLACE(B_CRMGuidConta,'}',''),'{','')) as B_CRMGuidConta, B_ContaCRM, LOWER(REPLACE(REPLACE(B_ContaMovGuid,'}',''),'{','')) as B_ContaMovGuid,
                           LOWER(REPLACE(REPLACE(B_Titularidade,'}',''),'{','')) as B_Titularidade,
                    	   ISNULL(B.QtdeRateio,B.QtdeDem) as Quantidade, ISNULL(B.QtdeRateio,B.QtdeDem) * S.PUFonte as Financeiro, S.PUFonte
                    FROM PR_Secundario S INNER JOIN PR_Sec_Buyer B ON S.IdTrade=B.IdTrade
                    WHERE S.Cancelado=0 AND S.Negociado=0 AND (DataNeg IS NULL OR DataNeg>=@DataNeg)
                             AND B.Compra<>0 AND B.Cancelado=0 AND Fundo=0
                    UNION
                    SELECT S.IdTrade, S.DataNeg, LOWER(S.GuidProduto) as GuidProduto, S.NomeProduto, S.Rateado, B.Fundo,
                    		LOWER(REPLACE(REPLACE(ISNULL(B.B_FundoGuid, B_CRMGuidConta),'}',''),'{','')) as B_CRMGuidConta, ISNULL(B.B_FundoExcl, B_ContaCRM), LOWER(REPLACE(REPLACE(B_ContaMovGuid,'}',''),'{','')) as B_ContaMovGuid,
                            NULL as B_Titularidade,
                    		ISNULL(B.QtdeRateio,B.QtdeDem) as Quantidade, ISNULL(B.QtdeRateio,B.QtdeDem) * S.PUFonte as Financeiro, S.PUFonte
                    FROM PR_Secundario S INNER JOIN PR_Sec_Buyer B ON S.IdTrade=B.IdTrade
                    WHERE S.Cancelado=0 AND S.Negociado=0 AND (DataNeg IS NULL OR DataNeg>=@DataNeg)
                             AND B.Compra<>0 AND B.Cancelado=0 AND Fundo<>0
                     """
            df = self.banco.dataframe(codsql=codsql)
            if self.buffer:
                self.compras_em_aberto_df = df.copy()
                self.compras_em_aberto_set = True
        else:
            df = self.compras_em_aberto_df.copy()
        
        return df
    
    def vendas_rateadas(self, data_neg_min=None):
        """
        Busca as operações de oferta de venda que já estão rateadas
        Para o sys_liquidez isso é importante pois será previsto na liquidação dos ativos
        Parameters
        ----------
        data_neg_min : Data a partir da qual deve ser buscadas as operações
            DESCRIPTION.

        Returns
        -------
        df : dataframe com os dados
        """
        # TODO: Transformar em PROC
        if not self.vendas_rateadas_set:
            data_str = self.banco.sql_data(data_neg_min)
            texto = f"DECLARE @DataNeg Datetime={data_str}"
            codsql = texto + """ 
                    SELECT S.IdTrade, S.DataNeg, LOWER(S.GuidProduto) as GuidProduto, S.NomeProduto, S.Rateado, B.Fundo,
                    	   LOWER(REPLACE(REPLACE(B_CRMGuidConta,'}',''),'{','')) as B_CRMGuidConta, B_ContaCRM, REPLACE(REPLACE(B_ContaMovGuid,'}',''),'{','') as B_ContaMovGuid,
                           LOWER(REPLACE(REPLACE(B_Titularidade,'}',''),'{','')) as B_Titularidade,
                    	   ISNULL(B.QtdeRateio,B.QtdeDem) as Quantidade, ISNULL(B.QtdeRateio,B.QtdeDem) * S.PUFonte as Financeiro, S.PUFonte
                    FROM PR_Secundario S INNER JOIN PR_Sec_Buyer B ON S.IdTrade=B.IdTrade
                    WHERE S.Cancelado=0 AND S.Negociado=0 AND (DataNeg IS NULL OR DataNeg>=@DataNeg)
                             AND B.Compra=0 AND B.Cancelado=0 AND Fundo=0 AND B.Rateado<>0
                    UNION
                    SELECT S.IdTrade, S.DataNeg, LOWER(S.GuidProduto) as GuidProduto, S.NomeProduto, S.Rateado, B.Fundo,
                    		LOWER(REPLACE(REPLACE(ISNULL(B.B_FundoGuid, B_CRMGuidConta),'}',''),'{','')) as B_CRMGuidConta, ISNULL(B.B_FundoExcl, B_ContaCRM), REPLACE(REPLACE(B_ContaMovGuid,'}',''),'{','') as B_ContaMovGuid,
                            NULL as B_Titularidade,
                    		ISNULL(B.QtdeRateio,B.QtdeDem) as Quantidade, ISNULL(B.QtdeRateio,B.QtdeDem) * S.PUFonte as Financeiro, S.PUFonte
                    FROM PR_Secundario S INNER JOIN PR_Sec_Buyer B ON S.IdTrade=B.IdTrade
                    WHERE S.Cancelado=0 AND S.Negociado=0 AND (DataNeg IS NULL OR DataNeg>=@DataNeg)
                             AND B.Compra=0 AND B.Cancelado=0 AND Fundo<>0 AND B.Rateado<>0
                     """
            df = self.banco.dataframe(codsql=codsql)
            if self.buffer:
                self.vendas_rateadas_df = df.copy()
                self.vendas_rateadas_set = True
        else:
            df = self.vendas_rateadas_df.copy()
        return df
    
    def limpar_posicoes_zeradas_qtde(self):
        """
        Garante que vendas feitas no book interno não excedem o saldo detido pelo Portfolio
        Olha para os IdTrade da PR_Secundario onde ExportFinanceiro=0

        Returns
        -------
        None.

        """
        lista = []
        data_str = PosicaoDm1(homologacao=self.homologacao).data_posicao_str()
        # 1. Posições de fundos
        texto = "REPLACE(REPLACE(LOWER(ISNULL(B.B_FundoGuid, B.B_CRMGuidConta)), '{', ''), '{', '')"
        codsql = f"""SELECT Sec.GuidProduto, Sec.NomeProduto, Sec.IdTrade, Sec.IdComprador, Sec.B_ContaCRM, Cart.NomeContaCRM, QtdeSec, ISNULL(QtdeTotal,0) as QtdeTotal
                    FROM (
                    		SELECT S.GuidProduto, S.NomeProduto, S.IdTrade, B.B_ContaCRM, B.B_FundoExcl, B.Fundo, IdComprador, {texto}  as GuidConta, QtdeDem - ISNULL(QtdeExecutadaExt,0) as QtdeSec
                    		FROM PR_Secundario S INNER JOIN PR_Sec_Buyer B ON S.IdTrade=B.IdTrade
                    		WHERE S.Cancelado=0 AND S.Rateado=0 AND B.Cancelado=0 AND S.Primario=0 AND S.ExportFinanceiro=0 AND Fundo<>0 AND B.Compra=0
                    	) Sec
                    	 LEFT JOIN (
                    		SELECT Cart.NomeContaCRM, Cad.GuidContaCRM, Cart.GuidProduto, ROUND(SUM(QuantidadeFinal),0) as QtdeTotal
                    		FROM PO_Carteira Cart WITH (NOLOCK) INNER JOIN PO_Cadastro Cad WITH (NOLOCK) ON Cart.NomeContaCRM=Cad.NomeContaCRM 
                    		WHERE Cart.DataArquivo={data_str} AND Cad.Tipo='Fundo' 
                    		GROUP BY Cart.NomeContaCRM, Cad.GuidContaCRM, Cart.GuidProduto
                    		) Cart 
                    		ON Sec.GuidConta=Cart.GuidContaCRM AND Sec.GuidProduto=Cart.GuidProduto
                    WHERE QtdeSec > ISNULL(QtdeTotal,0) 
                    """
        df = self.banco.dataframe(codsql=codsql)
        
        if not df.empty:
            for idx, row in df.iterrows():
                if row['QtdeTotal'] == 0:
                    lista.append({'IdComprador': row['IdComprador'], 'Cancelado': 1, 'QuemC': 'Zero', 'QuandoC': self.banco.hoje()})
                else:
                    lista.append({'IdComprador': row['IdComprador'], 'QtdeDem': row['QtdeTotal'], 'QtdeDemOrig': row['QtdeSec']})
        
        # 2. Posições de pessoas físicas: o vínculo para saber se a posição existe é pela conta movimento.
        codsql = f"""
                SELECT Sec.GuidProduto, Sec.NomeProduto, Sec.IdTrade, Sec.IdComprador, Sec.B_Titularidade, Cart.Titularidade, QtdeSec, ISNULL(QtdeTotal,0) as QtdeTotal
                FROM (
                		SELECT S.GuidProduto, S.NomeProduto, S.IdTrade, B.IdComprador, B.B_Titularidade, LOWER(B.B_ContaMovGuid) as ContaMovGuid, QtdeDem - ISNULL(QtdeExecutadaExt,0) as QtdeSec
                		FROM PR_Secundario S INNER JOIN PR_Sec_Buyer B ON S.IdTrade=B.IdTrade
                		WHERE S.Cancelado=0 AND S.Rateado=0 AND B.Cancelado=0 AND S.Primario=0 AND Fundo=0 AND B.Compra=0 AND S.ExportFinanceiro=0
                	) Sec
                	 LEFT JOIN (
                		SELECT Cad.Titularidade, Cad.TitularidadeGuid, Cart.GuidContaMovimento, Cart.GuidProduto, ROUND(SUM(QuantidadeFinal),0) as QtdeTotal
                		FROM PO_Carteira Cart WITH (NOLOCK) INNER JOIN PO_Cadastro Cad WITH (NOLOCK) ON Cart.TitularidadeGuid=Cad.TitularidadeGuid
                		WHERE Cart.DataArquivo={data_str} AND Cad.Segmento<>'Fundos' 
                		GROUP BY Cad.Titularidade, Cad.TitularidadeGuid, Cart.GuidContaMovimento, Cart.GuidProduto
                		) Cart 
                		ON Sec.ContaMovGuid=Cart.GuidContaMovimento AND Sec.GuidProduto=Cart.GuidProduto
                WHERE QtdeSec > ISNULL(QtdeTotal,0) 
                """
        df = self.banco.dataframe(codsql=codsql)
        if not df.empty:
            for idx, row in df.iterrows():
                if row['QtdeTotal'] == 0:
                    lista.append({'IdComprador': row['IdComprador'], 'Cancelado': 1, 'QuemC': 'Zero', 'QuandoC': self.banco.hoje()})
                else:
                    lista.append({'IdComprador': row['IdComprador'], 'QtdeDem': row['QtdeTotal'], 'QtdeDemOrig': row['QtdeSec']})
        
        # 3. Atualiza a base de dados
        if len(lista) == 0:
            return
        df = pd.DataFrame(lista)
        if 'QtdeDemOrig' in df.columns:
            df1 = df[df['QtdeDemOrig']>0]
            if not df1.empty:
                df1 = df1[['IdComprador', 'QtdeDem', 'QtdeDemOrig']]
                resultado = self.banco.com_edit_df(tabela='PR_Sec_Buyer', dados=df1, campos_filtros=['IdComprador'], 
                                               colunas_comparar=['QtdeDem', 'QtdeDemOrig'])
        if 'Cancelado' in df.columns:
            df1 = df[df['Cancelado']>0]
            if not df1.empty:
                df1 = df1[['IdComprador', 'Cancelado', 'QuemC', 'QuandoC']]
                resultado = self.banco.com_edit_df(tabela='PR_Sec_Buyer', dados=df1, campos_filtros=['IdComprador'], 
                                                   colunas_comparar=['Cancelado', 'QuemC', 'QuandoC'])
        print('concluido')
        
    def limpar_posicoes_zeradas_financeiro(self):
        """
        Garante que vendas feitas no book interno não excedem o saldo detido pelo Portfolio
        Olha para os IdTrade da PR_Secundario onde ExportFinanceiro não é zero.

        Returns
        -------
        None.

        """
        lista = []
        data_str = PosicaoDm1(homologacao=self.homologacao).data_posicao_str()
        # 1. Posições de fundos
        texto = "REPLACE(REPLACE(LOWER(ISNULL(B.B_FundoGuid, B.B_CRMGuidConta)), '{', ''), '{', '')"
        codsql = f"""SELECT Sec.GuidProduto, Sec.NomeProduto, Sec.IdTrade, Sec.IdComprador, Sec.B_ContaCRM, Cart.NomeContaCRM, QtdeSec, ISNULL(QtdeTotal,0) as QtdeTotal
                    FROM (
                    		SELECT S.GuidProduto, S.NomeProduto, S.IdTrade, B.B_ContaCRM, B.B_FundoExcl, B.Fundo, IdComprador, {texto}  as GuidConta, QtdeDem - ISNULL(QtdeExecutadaExt,0) as QtdeSec
                    		FROM PR_Secundario S INNER JOIN PR_Sec_Buyer B ON S.IdTrade=B.IdTrade
                    		WHERE S.Cancelado=0 AND S.Rateado=0 AND B.Cancelado=0 AND S.Primario=0 AND S.ExportFinanceiro<>0 AND Fundo<>0 AND B.Compra=0
                    	) Sec
                    	 LEFT JOIN (
                    		SELECT Cart.NomeContaCRM, Cad.GuidContaCRM, Cart.GuidProduto, ROUND(SUM(FinanceiroFinal),0) as QtdeTotal
                    		FROM PO_Carteira Cart WITH (NOLOCK) INNER JOIN PO_Cadastro Cad WITH (NOLOCK) ON Cart.NomeContaCRM=Cad.NomeContaCRM 
                    		WHERE Cart.DataArquivo={data_str} AND Cad.Tipo='Fundo' 
                    		GROUP BY Cart.NomeContaCRM, Cad.GuidContaCRM, Cart.GuidProduto
                    		) Cart 
                    		ON Sec.GuidConta=Cart.GuidContaCRM AND Sec.GuidProduto=Cart.GuidProduto
                    WHERE QtdeSec > ISNULL(QtdeTotal,0) 
                    """
        df = self.banco.dataframe(codsql=codsql)
        
        if not df.empty:
            for idx, row in df.iterrows():
                if row['QtdeTotal'] == 0:
                    lista.append({'IdComprador': row['IdComprador'], 'Cancelado': 1, 'QuemC': 'Zero', 'QuandoC': self.banco.hoje()})
                else:
                    lista.append({'IdComprador': row['IdComprador'], 'QtdeDem': row['QtdeTotal'], 'QtdeDemOrig': row['QtdeSec']})
        
        # 2. Posições de pessoas físicas: o vínculo para saber se a posição existe é pela conta movimento.
        codsql = f"""
                SELECT Sec.GuidProduto, Sec.NomeProduto, Sec.IdTrade, Sec.IdComprador, Sec.B_Titularidade, Cart.Titularidade, QtdeSec, ISNULL(QtdeTotal,0) as QtdeTotal
                FROM (
                		SELECT S.GuidProduto, S.NomeProduto, S.IdTrade, B.IdComprador, B.B_Titularidade, LOWER(B.B_ContaMovGuid) as ContaMovGuid, QtdeDem - ISNULL(QtdeExecutadaExt,0) as QtdeSec
                		FROM PR_Secundario S INNER JOIN PR_Sec_Buyer B ON S.IdTrade=B.IdTrade
                		WHERE S.Cancelado=0 AND S.Rateado=0 AND B.Cancelado=0 AND S.Primario=0 AND Fundo=0 AND B.Compra=0 AND S.ExportFinanceiro<>0
                	) Sec
                	 LEFT JOIN (
                		SELECT Cad.Titularidade, Cad.TitularidadeGuid, Cart.GuidContaMovimento, Cart.GuidProduto, ROUND(SUM(FinanceiroFinal),0) as QtdeTotal
                		FROM PO_Carteira Cart WITH (NOLOCK) INNER JOIN PO_Cadastro Cad WITH (NOLOCK) ON Cart.TitularidadeGuid=Cad.TitularidadeGuid
                		WHERE Cart.DataArquivo={data_str} AND Cad.Segmento<>'Fundos' 
                		GROUP BY Cad.Titularidade, Cad.TitularidadeGuid, Cart.GuidContaMovimento, Cart.GuidProduto
                		) Cart 
                		ON Sec.ContaMovGuid=Cart.GuidContaMovimento AND Sec.GuidProduto=Cart.GuidProduto
                WHERE QtdeSec > ISNULL(QtdeTotal,0) 
                """
        df = self.banco.dataframe(codsql=codsql)
        if not df.empty:
            for idx, row in df.iterrows():
                if row['QtdeTotal'] == 0:
                    lista.append({'IdComprador': row['IdComprador'], 'Cancelado': 1, 'QuemC': 'Zero', 'QuandoC': self.banco.hoje()})
                else:
                    lista.append({'IdComprador': row['IdComprador'], 'QtdeDem': row['QtdeTotal'], 'QtdeDemOrig': row['QtdeSec']})
        
        # 3. Atualiza a base de dados
        if len(lista) == 0:
            return
        df = pd.DataFrame(lista)
        if 'QtdeDemOrig' in df.columns:
            df1 = df[df['QtdeDemOrig']>0]
            if not df1.empty:
                df1 = df1[['IdComprador', 'QtdeDem', 'QtdeDemOrig']]
                resultado = self.banco.com_edit_df(tabela='PR_Sec_Buyer', dados=df1, campos_filtros=['IdComprador'], 
                                               colunas_comparar=['QtdeDem', 'QtdeDemOrig'])
        if 'Cancelado' in df.columns:
            df1 = df[df['Cancelado']>0]
            if not df1.empty:
                df1 = df1[['IdComprador', 'Cancelado', 'QuemC', 'QuandoC']]
                resultado = self.banco.com_edit_df(tabela='PR_Sec_Buyer', dados=df1, campos_filtros=['IdComprador'], 
                                                   colunas_comparar=['Cancelado', 'QuemC', 'QuandoC'])
        print('concluido')
        
    def pr_secundario_atualizar_precofonte(self, data_pos=None):
        """
        Diariamente rodada para atualizar preços dos deals com base no último preço do ativo na D-1

        Parameters
        ----------
        data_pos : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        None.

        """
        # Pega preços a atualizar
        codsql = """ SELECT IdTrade, GuidProduto, DataFonte, PUFonte
                     FROM PR_Secundario 
                     WHERE AtivoCadastrado<>0 AND Cancelado=0 AND Primario=0 AND ExportFinanceiro=0 AND Negociado=0 AND (DataNeg IS NULL OR DataNeg>=GETDATE()-5)
                 """
        df = self.banco.dataframe(codsql=codsql)
        dm1 = PosicaoDm1()
        for idx, row in df.iterrows():
            data, preco = dm1.produto_preco_atual_pas(row['GuidProduto'], data_pos=data_pos)
            if data and preco:
                dicio = {'DataFonte': data, 'PUFonte': preco}
                resultado = self.banco.com_edit(tabela='PR_Secundario', filtro=f"IdTrade={row['IdTrade']}", campos_valores=dicio)

        print('concluido')
    
    def precifica_id_trade(self, id_trade:int, preco_compra:float, preco_venda:float, taxa_compra:float=None, taxa_venda:float=None, priced:int=1, negociado:bool=False, exportado:bool=False, verifica_validade:bool=True):
        """Função para precificar os trades do secundario interno
        Args:
            id_trade (int): IdTrade a ser precificado
            preco_compra (float): PU Compra
            preco_venda (float): PU Venda
            priced (bool, optional): Mudará a coluna priced para esse valor. Defaults to True.
            negociado (bool, optional): Mudará a coluna negociado para esse valor. Defaults to False.
            exportado (bool, optional): Mudará a coluna exportado da PR_Sec_Buyer para esse valor. Defaults to True.
            verifica_validade (bool, optional): Valida se campos fundamentais da PR_Secundario estão de acordo com o esperado. Defaults to True.

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_
        """
        if verifica_validade:
            df = self.banco.dataframe(f'select * from PR_Secundario where IdTrade={id_trade}')
            
            df = df.iloc[0]
            rateado = df['Rateado']
            priced_val = df['Priced']
            cancelado = df['Cancelado']

            if rateado==True and priced_val==False and cancelado==False:
                print('IdTrade ok para precificar')
            else:
                raise Exception(f'Não é possível precificar o trade. Validação - Rateado: {rateado}, Precificado: {priced}, Cancelado: {cancelado}')
                

        filtro = f'IdTrade={id_trade}'
        
        
        priced_val = 0
        if priced == True or priced==1:
            priced_val = 1
        negociado_val = 0
        if negociado == True or negociado == 1:
            negociado_val = 1
        dicio_campos_valores = {'PrecoCompra':preco_compra,
                                'PrecoVenda':preco_venda,
                                'Priced': priced_val,
                                'Negociado':negociado_val
                            }
        if taxa_compra:
            dicio_campos_valores['TaxaCompra'] = taxa_compra
        if taxa_venda:
            dicio_campos_valores['TaxaVenda'] = taxa_venda
        ret1 = self.banco.com_edit(
            tabela='PR_Secundario',
            campos_valores=dicio_campos_valores,
            filtro=filtro, campo_quem='QuemP', campo_quando='QuandoP'
        )
        exportado_val = 0
        if exportado == True or exportado==1:
            exportado_val = 1
        ret2 = self.banco.com_edit(
            tabela='PR_Sec_Buyer',
            campos_valores={
                'Exportado':exportado
            },
            filtro=filtro
        )

        return ret1, ret2
    
    def pr_limpeza_compras_idtrades(self):
        """
        Limpa na PR_Sec_Buyer operções de compra com mais de 30 dias de idade ou operações com data de validade expirada.

        Returns
        -------
        None.

        """
        # #############################
        # Compras com mais de 30 dias
        # #############################
        # Busca para atualizar as ordens
        bol = Boletador(homologacao=self.homologacao)
        
        codsql = "SELECT B.IdOrdem, O.IdStatusOrdem, A.ValorFlt as IdOrdemOms FROM PR_Sec_Buyer B INNER JOIN PR_Secundario S ON B.IdTrade=S.IdTrade INNER JOIN BL_Ordem O ON B.IdOrdem=O.IdOrdem LEFT JOIN BL_CamposAdicionais A ON O.IdOrdem=A.IdOrdem AND A.IdCampo=103 WHERE B.Cancelado=0 AND B.Compra<>0 AND S.Cancelado=0 AND B.Rateado=0 AND S.Rateado=0 AND S.Primario=0 AND DATEDIFF(day, B.Quando, GETDATE())  > 30"
        df = self.banco.dataframe(codsql)
        for idx, row in df.iterrows():
            bol.ordens_editar_individual(id_ordem=row['IdOrdem'], id_status_anterior=row['IdStatusOrdem'], id_status_novo=100, comentario='Compra com mais de 30 dias de idade', comentario_individual=True, id_ordem_oms=row['IdOrdemOms'])
        
        # Cancela na PR_Secundario
        codsql = """UPDATE PR_Sec_Buyer SET Cancelado=1, QuemC='Limpeza', QuandoC=GETDATE()
                    FROM PR_Sec_Buyer B INNER JOIN PR_Secundario S ON B.IdTrade=S.IdTrade
                    WHERE B.Cancelado=0 AND B.Compra<>0 AND S.Cancelado=0 AND B.Rateado=0 AND S.Rateado=0 AND S.Primario=0 AND DATEDIFF(day, B.Quando, GETDATE())  > 30
                """
        resultado = self.banco.executar_comando(codsql)
        
        # #############################
        # Deals expirados
        # #############################
        # Busca para atualizar as ordens
        codsql = "SELECT B.IdOrdem, O.IdStatusOrdem, A.ValorFlt as IdOrdemOms FROM PR_Sec_Buyer B INNER JOIN PR_Secundario S ON B.IdTrade=S.IdTrade INNER JOIN BL_Ordem O ON B.IdOrdem=O.IdOrdem LEFT JOIN BL_CamposAdicionais A ON O.IdOrdem=A.IdOrdem AND A.IdCampo=103 WHERE B.Cancelado=0 AND S.Cancelado=0 AND B.Rateado=0 AND S.Rateado=0 AND B.DtValidadePreBol<Convert(date, getdate())"
        df = self.banco.dataframe(codsql)
        for idx, row in df.iterrows():
            bol.ordens_editar_individual(id_ordem=row['IdOrdem'], id_status_anterior=row['IdStatusOrdem'], id_status_novo=101, comentario='Data de expiração informada excedida', comentario_individual=True, id_ordem_oms=row['IdOrdemOms'])
        
        # Cancela na PR_Secundario
        codsql = """UPDATE PR_Sec_Buyer SET Cancelado=1, QuemC='Expirado', QuandoC=GETDATE()
                    FROM PR_Sec_Buyer B INNER JOIN PR_Secundario S ON B.IdTrade=S.IdTrade
                    WHERE B.Cancelado=0 AND S.Cancelado=0 AND B.Rateado=0 AND S.Rateado=0 AND B.DtValidadePreBol<Convert(date, getdate())
                """
        resultado = self.banco.executar_comando(codsql)
        
    def trading_desk_book(self, dias_defasagem:int=0) -> pd.DataFrame:
        """
        Traz uma visão sintética do book interno do secundário.
        Criado para trading desk.
        Utiliza a proc PR_Disponivel_TD.

        Parameters
        ----------
        dias_defasagem : int, optional
            Quantos dias de idade tem a oferta antes de trading desk poder atuar. The default is 0.

        Returns
        -------
        pd.DataFrame.

        """
        codsql = f"EXEC PR_Disponivel_TD {dias_defasagem}"
        return self.banco.dataframe(codsql)
    
    def pr_limpa_trades_vazios(self):
        """
        Limpa trades cuja oferta e demanda já foi antendida.
        Intencionalmente não olha para trades totalmente vazios (que pode indicar algo na pedra para ser utilizado por sistemas.)

        Returns
        -------
        None.

        """
        # 1. Secundários
        df = self.trading_desk_book()
        df = df[(df['QuantidadeVendas']==0) & (df['QuantidadeCompras']==0)]
        df = df[~df['NomeProduto'].str.contains('\[Cred\]')]
        df = df[~df['NomeProduto'].str.contains('\[PM\]')]
        df.set_index('IdTrade', inplace=True)
        for idx, row in df.iterrows():
            codsql = f"UPDATE PR_Secundario SET Cancelado=1, QuemC='pr_limpa_trades_vazios', QuandoC=GETDATE() WHERE IdTrade={idx}"
            resultado = self.banco.executar_comando(codsql)
        
        # 2. Primários
        codsql = """
                    SELECT S.IdTrade, S.DataNeg, S.NomeProduto, COUNT(B.IdTrade) as NumOrdens 
                    FROM PR_Secundario S WITH (NOLOCK) LEFT JOIN PR_Sec_Buyer B WITH (NOLOCK)  ON S.IdTrade=B.IdTrade
                    WHERE S.Primario<>0 AND S.Cancelado=0 AND S.Rateado=0 AND S.DtLimiteDem<Convert(date, getdate() - 7)
                    GROUP BY S.IdTrade, S.DataNeg, S.NomeProduto
                    HAVING COUNT(B.IdTrade)=0
                    """
        df = self.banco.dataframe(codsql)
        if df.empty:
            return
        df.set_index('IdTrade', inplace=True)
        for idx, row in df.iterrows():
            codsql = f"UPDATE PR_Secundario SET Cancelado=1, QuemC='pr_limpa_trades_vazios', QuandoC=GETDATE() WHERE IdTrade={idx}"
            resultado = self.banco.executar_comando(codsql)
    
    def pr_sec_buyer_cancela_id_ordem(self, id_ordem:int, quem_cancela:str='MonExec'):
        """
        Cancela uma linha da PR_Sec_Buyer vinculada a um IdOrdem da tabela BL_Ordem.
        Criada como parte do fluxo de ordens, onde o saldo de um grupo de ordens deve ser cancelado após um financeiro
        total ser negociado.
        
        Parameters
        ----------
        id_ordem : int
            DESCRIPTION.
        quem_cancela : str, optional
            DESCRIPTION. The default is 'MonExec'.

        Returns
        -------
        str: resultado do comando.

        """
        codsql = f"UPDATE PR_Sec_Buyer SET Cancelado=1, QuemC='{quem_cancela}', QuandoC=GETDATE() WHERE Cancelado=0 AND IdOrdem={id_ordem}"
        resultado = self.banco.executar_comando(codsql)
        return resultado
    
    def pr_sec_buyer_busca_id_ordem(self, id_ordem_lista):
        """
        Busca uma lista de demandas/ofertas da PR_Sec_Buyer pelo id_da ordem

        Parameters
        ----------
        lista_boletas : TYPE List
            Lista de boletas a marcar como exportada

        Returns
        -------
        retorno : TYPE
            Dicionário de resultados da execução

        """
        lista_boletas = id_ordem_lista
        if not isinstance(lista_boletas, list):
            lista_boletas = [lista_boletas]
        lista_boletas = [str(x) for x in lista_boletas]
        filtro = ','.join(lista_boletas)
        codsql = f"SELECT B.* FROM PR_Sec_Buyer B INNER JOIN PR_Secundario S WITH (NOLOCK) ON B.IdTrade=S.IdTrade WHERE B.Cancelado=0 AND S.Cancelado=0 AND B.IdOrdem IN ({filtro})"
        return self.banco.dataframe(codsql)
    
    def pr_sec_verifica_limite_boletagem(self, id_trade:int, tipo_mov:str) -> float:
        """
        Calcula o financeiro disponível em um idtrade, levando em conta que alguns grupos de ordens têm o financeiro
        total limitado

        Parameters
        ----------
        id_trade : int
            como na tabela PR_Sec_Buyer.
        tipo_mov : str
            'C' ou 'V'.

        Returns
        -------
        float
            Valor disponível.

        """
        # 1. Existe apenas para ordens que tem financeiro como base
        export_fin = self.banco.busca_valor(tabela='PR_Secundario', filtro=f'IdTrade={id_trade}', campo='ExportFinanceiro')
        
        # 2. Busca Linhas do IdTrade junta a IdOrdem
        if tipo_mov == 'C':
            filtro = 'Compra<>0'
        else:
            filtro = 'Compra=0'
        codsql = f""" SELECT Bu.IdOrdem, ISNULL(O.IdOrdemGrupo, O.IdOrdem) as IdOrdemBusca, QtdeDem - ISNULL(Bu.QtdeExecutadaExt,0) as Disponivel
                      FROM PR_Sec_Buyer Bu  WITH (NOLOCK) INNER JOIN BL_Ordem O WITH (NOLOCK) ON Bu.IdOrdem=O.IdOrdem
                      WHERE Bu.Cancelado=0 AND Bu.IdTrade={id_trade} AND {filtro}
                  """
        df = self.banco.dataframe(codsql=codsql)
        if df.empty:
            return 0
        
        lista = df['IdOrdem'].tolist()

        # 3. Busca Limites das ordens que tiverem limites
        df_lim = self.volume_executado_ordens(lista)
        if df_lim.empty:
            # Se nenhuma ordem possui limite, retorna o volume total
            return df['Disponivel'].sum()        
        df_lim['LimDisp'] = df_lim.apply(lambda x: x['ValorLimite'] - x['FinPreBol'], axis=1)
        df_lim.insert(len(df_lim.columns), 'ComLimite', [True] * len(df_lim))
        df_lim.set_index('IdOrdem', inplace=True)
        
        # 4. Preenche o limite no dataframe principal
        df = pd.merge(left=df, left_on='IdOrdemBusca', right=df_lim, right_index=True, how='left')
        df['LimDisp'] = df['LimDisp'].fillna(0)
        df['ComLimite'] = df['ComLimite'].fillna(False)
        
        # 5. Calcula o limite que sobrou
        df['Limite'] = df.apply(lambda x: min(x['Disponivel'], x['LimDisp']) if x['ComLimite'] else x['Disponivel'], axis=1)
        return df['Limite'].sum()
    
    def volume_executado_ordens(self, lista_ordens:list):
        lista = [str(x) for x in lista_ordens]
        filtro = f"({','.join(lista)})"
        codsql = f"""   SELECT B.IdOrdem, B.TipoMov, A.ValorFlt as ValorLimite, SUM(B2.Financeiro) as FinOrdens, SUM(B2.FinPreBol) as FinPreBol, SUM(B3.FinExecutado) as FinExecutado
                        FROM BL_Ordem B INNER JOIN BL_CamposAdicionais A WITH (NOLOCK) ON B.IdOrdem=A.IdOrdem
                        	 LEFT JOIN (
                        				 SELECT ISNULL(B.IdOrdemGrupo, B.IdOrdem) as IdJoin, B.IdOrdem, B.GuidPortfolio, B.NomePortfolio, B.AtivoGuid, B.TipoMov, B.Financeiro, SUM(CASE WHEN NOT P.Financeiro IS NULL THEN P.Financeiro ELSE ISNULL(P.Quantidade,0) * ISNULL(A.PrecoAtual,0) END) as FinPreBol
                          				 FROM BL_Ordem B WITH (NOLOCK) LEFT JOIN BL_PreBoletas P ON B.IdOrdem=P.IdOrdem AND P.PreBoleta<>0 AND P.Deletado=0
                                      	      LEFT JOIN PO_Ativos A WITH (NOLOCK) ON B.AtivoGuid=A.GuidAtivo
                        				 WHERE B.OrdemExcluida=0
                        				 GROUP BY ISNULL(B.IdOrdemGrupo, B.IdOrdem), B.IdOrdem, B.GuidPortfolio, B.NomePortfolio, B.AtivoGuid, B.TipoMov, B.Financeiro
                        	 ) B2 ON B.IdOrdem=B2.IdJoin
                             LEFT JOIN (
                                        SELECT ISNULL(B.IdOrdemGrupo, B.IdOrdem) as IdJoin, B.IdOrdem, B.GuidPortfolio, B.NomePortfolio, B.AtivoGuid, B.TipoMov, B.Financeiro, SUM(ISNULL(P.Financeiro,0)) as FinExecutado
                                        FROM BL_Ordem B WITH (NOLOCK) LEFT JOIN BL_PreBoletas P ON B.IdOrdem=P.IdOrdem AND P.PreBoleta=0 AND P.Execucao<>0 AND P.Deletado=0
                                        WHERE B.OrdemExcluida=0
                                        GROUP BY ISNULL(B.IdOrdemGrupo, B.IdOrdem), B.IdOrdem, B.GuidPortfolio, B.NomePortfolio, B.AtivoGuid, B.TipoMov, B.Financeiro
                            ) B3 ON B.IdOrdem=B3.IdJoin AND B2.IdOrdem=B3.IdOrdem
                        WHERE B.OrdemExcluida=0 AND B.IdOrdemGrupo IS NULL AND A.IdCampo=85 AND B.IdOrdem IN {filtro}
                        GROUP BY B.IdOrdem, B.TipoMov, A.ValorFlt
                    """
        df_lim = self.banco.dataframe(codsql=codsql)
        
        return df_lim
    
    def idtrade_volume_cliente(self, id_trade:int):
        # Busca dados do IdTrade
        codsql = f"""SELECT B.IdTrade, S.PuFonte, S.ExportFinanceiro, IdComprador, B.IdOrdem, B.Compra, B_ContaCRM, B_CRMGuidConta, B_Titularidade, Motivo, QtdeDemOrig, QtdeDem, ISNULL(QtdeExecutadaExt,0) as QtdeExecutadaExt, QtdeDem - ISNULL(QtdeExecutadaExt,0) as QtdeSaldo, O.ValorFlt as LimiteFinOrdem, 0 as FinExecutadoGrupoOrdem, B_ContaMovGuid, B_ContaMovEntradaFinGuid, B_ContaMovDestGuid, DtValidadePreBol
                    FROM PR_Sec_Buyer B INNER JOIN PR_Secundario S WITH (NOLOCK) ON B.IdTrade=S.IdTrade
                    	 LEFT JOIN BL_CamposAdicionais O WITH (NOLOCK) ON B.IdOrdem=O.IdOrdem AND O.IdCampo=85
                    WHERE B.Cancelado=0 AND (QtdeDem - ISNULL(QtdeExecutadaExt,0)) > 0 AND B.IdTrade={id_trade}
                    """
        df = self.banco.dataframe(codsql)
        
        # Se houver ativos que tem financeiro limitado na ordem como um todo, busca os dados
        filtro = df.copy()
        filtro['LimiteFinOrdem'] = filtro['LimiteFinOrdem'].fillna(0)
        filtro = filtro[filtro['LimiteFinOrdem']>0]
        
        if not filtro.empty:
            lista_ordens = list(filtro['IdOrdem'].unique())
            df_executado = self.volume_executado_ordens(lista_ordens=lista_ordens).fillna(0)
            if not df_executado.empty:
                df_executado['FinConsiderar'] = df_executado.apply(lambda x: max(x['FinPreBol'], x['FinExecutado']), axis=1)
                dicio_espaco = df_executado.set_index('IdOrdem')['FinConsiderar'].to_dict()
                df['FinExecutadoGrupoOrdem'] = df['IdOrdem'].map(dicio_espaco)
                df['FinExecutadoGrupoOrdem'] = df['FinExecutadoGrupoOrdem'].fillna(0)
            
        # Calcula o espaço real de ordem
        filtro = df[df['FinExecutadoGrupoOrdem']>0]
        for idx, row in filtro.iterrows():
            saldo = max(0, row['LimiteFinOrdem']-row['FinExecutadoGrupoOrdem']) / row['PuFonte']
            df.loc[idx, 'QtdeSaldo'] = min(row['QtdeSaldo'],saldo)
        
        # Final
        df['FinanceiroSaldo'] = df.apply(lambda x: round(x['QtdeSaldo'] * x['PuFonte'],2), axis=1)
        return df
    
    @staticmethod
    def __idtrade_rateio_passo_aloc__(valor_linha, df_alloc, valor_total):
        total_alocado = 0
        sub_df = df_alloc[df_alloc['Restante'] >= valor_linha]
        if not sub_df.empty:
            for idx, row in sub_df.iterrows():
                df_alloc.loc[idx, 'QtdeAloc'] += valor_linha
                df_alloc.loc[idx, 'Restante'] -= valor_linha
                total_alocado += valor_linha
                if total_alocado >= valor_total:
                    break
        else:
            saldo = valor_total
            for idx, row in df_alloc.iterrows():
                valor_cli = min(row['Restante'], saldo)
                df_alloc.loc[idx, 'QtdeAloc'] += valor_cli
                df_alloc.loc[idx, 'Restante'] -= valor_cli
                total_alocado += valor_cli
                saldo -= valor_cli
                if total_alocado >= valor_total:
                    break
        return df_alloc, total_alocado
    
    def idtrade_rateio_c_v(self, id_trade:int, compra:bool, quantidade_alocar:float, passagem:bool=False, df_idtrade_volume_cliente:pd.DataFrame=pd.DataFrame(), data_movimento=None, filtro_id_comprador:list=[]):
        # A. Busca dados
        casas_dec = len(format(quantidade_alocar, '.8f').split('.')[1].rstrip('0'))
        export_financeiro = self.banco.busca_valor('PR_Secundario', f'IdTrade={id_trade}', 'ExportFinanceiro')
        
        if data_movimento:
            data_mov = data_movimento
        else:
            data_mov = self.banco.hoje()
        
        # B. Busca ordens a alocar
        if df_idtrade_volume_cliente.empty:
            df_alloc = self.idtrade_volume_cliente(id_trade=id_trade)
        else:
            df_alloc = df_idtrade_volume_cliente.copy()
        if len(filtro_id_comprador)>0:
            df_alloc = df_alloc.loc[df_alloc['IdComprador'].isin(filtro_id_comprador)]
        df_alloc = df_alloc[df_alloc['Compra']==compra].sort_values('QtdeSaldo', ascending=False)
        df_alloc['QtdeSaldo'] = df_alloc['QtdeSaldo'].round(casas_dec)
        df_alloc.insert(len(df_alloc.columns), 'QtdeAloc', [0] * len(df_alloc))
        df_alloc.insert(len(df_alloc.columns), 'Restante', df_alloc['QtdeSaldo'])
        saldo_total = df_alloc['QtdeSaldo'].sum()
        
        # C. Processo de alocação        
        if saldo_total <= quantidade_alocar:
            # 1. Quantidade Disponível maior ou igual à demanda
            df_alloc['QtdeAloc'] = df_alloc['QtdeSaldo']
            quantidade_alocada = saldo_total
        else:
            # 2. Demais casos
            a_alocar = quantidade_alocar
            while a_alocar > 0.0:
                # a. alocar inteiramente lote do menor cliente em cada cliente
                valor_linha = round(df_alloc[df_alloc['Restante']>0]['Restante'].min(), casas_dec)
                num_faltantes = len(df_alloc[df_alloc['Restante']>0])                
                valor_total = valor_linha * num_faltantes
                if valor_total > a_alocar:
                    # b. lote minimo viável
                    valor_linha = round(a_alocar / num_faltantes, casas_dec)
                    valor_total = a_alocar
                    if valor_linha * num_faltantes > valor_total:
                        valor_linha -= round(valor_linha * num_faltantes - valor_total, casas_dec)
                    # c. Se valor da linha fica muito pequeno, joga valor em um cliente
                    if valor_linha == 0:
                        valor_linha = valor_total                    
                    
                df_alloc, alocado = self.__idtrade_rateio_passo_aloc__(valor_linha, df_alloc, valor_total)
                a_alocar = round(a_alocar - alocado, casas_dec)
        
        # D. Atualiza os registros        
        if compra:
            c_ou_v = 'C'
        else:
            c_ou_v = 'V'
        if export_financeiro:
            q_ou_f = 'F'
            campo_dest = 'Financeiro'
        else:
            q_ou_f = 'Q'
            campo_dest = 'Quantidade'
        tipo_ordem = 1 # À mercado
        if passagem:
            tipo_ordem = 15 # Passagem RF
        
        sub_df = df_alloc[df_alloc['QtdeAloc']> 0]
        df_alloc.insert(len(df_alloc.columns), '__Resultado__', [None] * len(df_alloc))
        for idx, row in sub_df.iterrows():
            # D.1. Insere a Pré-boleta e atualiza a quantidade executada
            data_str = self.banco.sql_data(data_mov)
            codsql = f"""BEGIN TRANSACTION;
                        
                        DECLARE @DataMov Datetime = {data_str}
                                                
                        INSERT INTO BL_PreBoletas (
                            QouF, 
                            ResgTot, 
                            DataMov, 
                            DataCot, 
                            DataFin, 
                            PreBoleta, 
                            IdTipoOrdem, 
                            AtivoCadastrado, 
                            AtivoGuid, 
                            AtivoNome, 
                            TipoMov, 
                            Quem, 
                            Quando, 
                            ContaCRMGuid, 
                            ContaCRM, 
                            GuidTitularidade, 
                            Titularidade, 
                            GuidContaMovimento, 
                            ContaMovimento, 
                            GuidContaMovimentoOrigem, 
                            ContaMovimentoOrigem, 
                            GuidContaMovimentoDestino, 
                            ContaMovimentoDestino, 
                            CodigoExt, 
                            {campo_dest}, 
                            SecIdComprador, 
                            MotivoAplic, 
                            MotivoResg, 
                            IdOrdem, 
                            DtValidadePreBol
                        )
                        SELECT 
                            '{q_ou_f}', 
                            0, 
                            @DataMov, 
                            @DataMov, 
                            dbo.Workday(@DataMov,1 ,1), 
                            1 AS PreBoleta, 
                            {tipo_ordem} AS TipoOrdem, 
                            1 AS AtivoCad, 
                            CASE WHEN S.GuidProduto IS NULL THEN 'sub:SEC\\' + CAST(S.IdTrade AS VARCHAR(255)) ELSE S.GuidProduto END, 
                            S.NomeProduto, 
                            '{c_ou_v}', 
                            T.Controller, 
                            GETDATE() AS Quando, 
                            T.B_CRMGuidConta, 
                            T.B_ContaCRM, 
                            T.B_Titularidade, 
                            T.B_Titularidade, 
                            T.B_ContaMovEntradaFinGuid, 
                            T.B_ContaMovEntradaFinGuid, 
                            T.B_ContaMovGuid, 
                            T.B_ContaMovGuid, 
                            T.B_ContaMovDestGuid, 
                            T.B_ContaMovDestGuid, 
                            S.CodigoCetip, 
                            {row['QtdeAloc']}, 
                            T.IdComprador, 
                            Motivo, 
                            Motivo, 
                            IdOrdem, 
                            DtValidadePreBol
                        FROM PR_Sec_Buyer T 
                        INNER JOIN PR_Secundario S ON T.IdTrade = S.IdTrade
                        WHERE T.IdComprador = {row['IdComprador']};

                        UPDATE PR_Sec_Buyer SET QtdeExecutadaExt = ISNULL(QtdeExecutadaExt,0) + {row['QtdeAloc']} WHERE IdComprador = {row['IdComprador']};

                        COMMIT TRANSACTION;

                      """
            df_alloc.loc[idx, '__Resultado__'] = self.banco.executar_comando(codsql)            
        
        return df_alloc
    
    def idtrade_rateio_passagem_interna(self, id_trade:int, data_passagem=None, filtro_id_compradores=[]):
        if data_passagem:
            data_mov = data_passagem
        else:
            data_mov = self.banco.hoje()
        df_alloc = self.idtrade_volume_cliente(id_trade=id_trade)
        if len(filtro_id_compradores)>0:
            df_alloc = df_alloc.loc[df_alloc['IdComprador'].isin(filtro_id_compradores)]
        compras = df_alloc[df_alloc['Compra']==True]['QtdeSaldo'].sum()
        vendas = df_alloc[df_alloc['Compra']==False]['QtdeSaldo'].sum()
        valor_passagem = min([compras, vendas])
        print(f'Saldo para passagem {valor_passagem} quantidades.')
        
        compras = self.idtrade_rateio_c_v(id_trade=id_trade, compra=True, quantidade_alocar=valor_passagem, passagem=True, df_idtrade_volume_cliente=df_alloc, data_movimento=data_mov, filtro_id_comprador=filtro_id_compradores)
        vendas = self.idtrade_rateio_c_v(id_trade=id_trade, compra=False, quantidade_alocar=valor_passagem, passagem=True, df_idtrade_volume_cliente=df_alloc, data_movimento=data_mov, filtro_id_comprador=filtro_id_compradores)
        
        quem,quando = self.banco.get_user_and_date()

        campos_valores = {
            'QuemR':quem,
            'QuandoR':quando,
            'Rateado': True
        }

        for idcomprador in df_alloc['IdComprador'].unique().tolist():
            self.banco.com_edit(tabela='PR_Sec_Buyer', campos_valores=campos_valores, filtro=f'IdComprador = {idcomprador}')



        return pd.concat([compras, vendas]), valor_passagem
    
    def bookint_em_aberto_por_cliente(self):
        # Busca dados 
        codsql = """SELECT B.B_ContaCRM, B.Compra, B_Titularidade, S.IdTrade, S.TipoProduto, S.NomeProduto, S.NomeEmissor, S.Indexador, S.PuFonte, A.CodNegociacao, S.PUFonte,
                    		QtdeDemOrig, QtdeDem, ISNULL(QtdeExecutadaExt,0) as QtdeExecutadaExt, QtdeDem - ISNULL(QtdeExecutadaExt,0) as QtdeSaldo, O.ValorFlt as LimiteFinOrdem, 0 as FinExecutadoGrupoOrdem,
                            (B.Quando) as DataIni, (B.Quando) as LastChange, B.IdOrdem, S.QuemN, S.QuandoN
                    FROM PR_Secundario S
                         INNER JOIN PR_Sec_Buyer B ON S.IdTrade=B.IdTrade
                         LEFT JOIN PO_Ativos A ON S.GuidProduto=A.GuidAtivo
                    	 LEFT JOIN BL_CamposAdicionais O WITH (NOLOCK) ON B.IdOrdem=O.IdOrdem AND O.IdCampo=85
                    WHERE S.Cancelado=0 AND S.Primario=0 AND S.Rateado=0 AND B.Cancelado=0
                    ORDER BY B_ContaCRM
                    """
        df = self.banco.dataframe(codsql)
        
        # Se houver ativos que tem financeiro limitado na ordem como um todo, busca os dados
        filtro = df.copy()
        filtro['LimiteFinOrdem'] = filtro['LimiteFinOrdem'].fillna(0)
        filtro = filtro[filtro['LimiteFinOrdem']>0]
        
        if not filtro.empty:
            lista_ordens = list(filtro['IdOrdem'].unique())
            df_executado = self.volume_executado_ordens(lista_ordens=lista_ordens).fillna(0)
            dicio_espaco = df_executado.set_index('IdOrdem')['FinPreBol'].to_dict()
            df['FinExecutadoGrupoOrdem'] = df['IdOrdem'].map(dicio_espaco)
            df['FinExecutadoGrupoOrdem'] = df['FinExecutadoGrupoOrdem'].fillna(0)
            
        # Calcula o espaço real de ordem
        filtro = df[df['FinExecutadoGrupoOrdem']>0]
        for idx, row in filtro.iterrows():
            saldo = max(0, row['LimiteFinOrdem']-row['FinExecutadoGrupoOrdem']) / row['PuFonte']
            df.loc[idx, 'QtdeSaldo'] = min(row['QtdeSaldo'],saldo)
        
        # Final
        df['FinanceiroSaldo'] = df.apply(lambda x: round(x['QtdeSaldo'] * x['PuFonte'],2), axis=1)
        return df
    
    def secundarios_liquidacao(self, data_negociacao:datetime.datetime, corrigir_pu:bool=False, perc_cdi_correcao:float=1.4):
        # Busca as boletas de secundário
        data_str = self.banco.sql_data(data_negociacao)
        codsql = f"""SELECT S.IdTrade, B.IdComprador, S.NomeProduto, S.DataNeg, B.B_ContaCRM, B.B_Titularidade, B.B_ContaMovGuid, B.B_ContaMovEntradaFinGuid, B.Quantidade, S.PUFonte, B.Quantidade * S.PUFonte as Financeiro, B.Compra, S.DataFonte
                    FROM PR_Sec_Boletas B WITH (NOLOCK) INNER JOIN PR_Secundario S WITH (NOLOCK)  ON B.IdTrade=S.IdTrade
                    WHERE S.Cancelado=0 AND S.DataNeg={data_str} AND B.Cancelado=0
                  """
        df = self.banco.dataframe(codsql)
        if df.empty:
            return df
        df['DataFonte'] = pd.to_datetime(df['DataFonte'])
        
        if corrigir_pu:
            # Para corrigir o PU, pega o último valor do CDI anualizado
            codsql = """SELECT N.Valor
                        FROM IN_IndexNAV N WITH (NOLOCK)  INNER JOIN (SELECT MAX(Data) as UltData FROM IN_IndexNAV WITH (NOLOCK) WHERE IndexCod='CDITX') DT
                        	 ON N.Data=DT.UltData
                        WHERE IndexCod='CDITX'
                      """
            df_val = self.banco.dataframe(codsql)
            taxa_ano = df_val.iloc[0]['Valor'] / 100
            taxa_dia = perc_cdi_correcao * ((1 + taxa_ano) ** (1/252) - 1)
            # Conta o número de dias
            ult_data = self.banco.hoje()
            retorno = 0.0
            bawm = Bawm()
            for idx, row in df.sort_values('DataFonte').iterrows():
                if row['DataFonte'] != ult_data:
                    ult_data = row['DataFonte']
                    lista = bawm.dia_trabalho_lista(ult_data, 20)
                    num_dias = len([1 for x in lista if x <= data_negociacao])
                    retorno = (1 + taxa_dia) ** (num_dias)
                df.loc[idx, 'PUFonte'] = row['PUFonte'] * retorno
                df.loc[idx, 'Financeiro'] = round(row['Financeiro'] * retorno, 2)
        
        return df
        
    def secundario_tabela_id_trade(self, data_neg=None, data_rateio=None, rateado:bool=True, cancelado:bool=False, precificado:bool=False):
        '''Baixa um dataframe da PR_Secundario com as informações do secundário interno com os filtros de input

        Args:
            data_neg (_type_, optional): Data_Neg - data comandada para a liquidação desse secundario. Defaults to None.
            data_rateio (_type_, optional): Data em que o middle rateou. Defaults to None.
            rateado (bool, optional): Apenas trades já rateados?. Defaults to True.
            cancelado (bool, optional): Apenas trades cancelados?. Defaults to False.
            precificado (bool, optional): Apenas trades precificados?. Defaults to False.

        Returns:
            _type_: dataframe contendo as informações da PR_Secundario
        '''        
        rateado = self.banco.sql_bool_converter(rateado)
        cancelado = self.banco.sql_bool_converter(cancelado)
        precificado = self.banco.sql_bool_converter(precificado)

        codsql = f'''select IdTrade, Primario, Rateado, Priced, Cancelado, DataNeg, GuidProduto, NomeProduto, CodigoCetip, ContraParte, 
        PrecoCompra, PrecoVenda, TaxaCompra, TaxaVenda, QuemP, QuandoP,QuemC, QuandoC, QuemR, QuandoR from PR_Secundario
        where Rateado={rateado} and Cancelado={cancelado} and Priced={precificado}'''
        
        if data_neg != None:
            data_neg = self.banco.sql_data(data_neg)
            codsql += f" and DataNeg={data_neg}"
        if data_rateio != None:
            data_rateio = self.banco.sql_data(data_rateio)
            codsql += f" and QuandoR={data_rateio}"

        df = self.banco.dataframe(codsql)
        return df

    def secundario_reagenda_trade(self, id_trade, nova_data:datetime.datetime, tira_precificacao:bool=True):
        ''' Muda a data de um IdTrade da PR_Secundario para a nova data definida

        Args:
            id_trade (int): IdTrade a ter a data alterada
            nova_data (datetime.datetime): Nova data desejada para esse IdTrade
            tira_precificacao (bool, optional): Tira a marcação de precificado=1 na PR_Secundario. Defaults to True.
        '''        
        campos_valores = {
            'DataNeg':nova_data
        }
        if tira_precificacao == True:
            campos_valores['Priced'] = 0
            
        result = self.banco.com_edit(tabela='PR_Secundario', campos_valores=campos_valores, filtro=f'IdTrade = {id_trade}')

        df_sec_buyer = self.secundario_boletas_id_trade(id_trade=id_trade)

        campos_valores = {
                'DataMov' : nova_data,
                'DataCot' : nova_data,
                'DataFin' : nova_data, 
            }

        for idx,row in df_sec_buyer.iterrows():
            id_comprador = row['IdComprador']
            self.banco.com_edit(tabela='BL_PreBoletas', campos_valores=campos_valores, filtro=f'SecIdComprador = {id_comprador}')

        return result
    
    def secundario_reagenda_prebols(self, lista_id_realoc, nova_data=None, deletado=None):
        campos_valores = {}
        if nova_data != None:
            campos_valores['DataMov'] = nova_data
            campos_valores['DataCot'] = nova_data
            campos_valores['DataFin'] = nova_data

        if deletado != None:
            if deletado == True:
                campos_valores['Deletado'] = 1
                quem,quando = self.banco.get_user_and_date()
                campos_valores['QuemDel'] = quem
                campos_valores['QuandoDel'] = quando
            elif deletado == False:
                campos_valores['Deletado'] = 0

        for id_comprador in lista_id_realoc:
            result = self.banco.com_edit(tabela='BL_PreBoletas', campos_valores=campos_valores, filtro=f'IdRealocFundos= {id_comprador}')

        return result 
    
    def secundario_pre_bol(self, data_neg=None, deletado:bool=False, exportado:bool=False, execucao:bool=False, preboleta:bool=False):
        codsql = f'''select IdRealocFundos, ContaCRMGuid, ContaCRM, AtivoGuid, AtivoNome, TipoMov, DataMov, DataCot, DataFin, Financeiro, Quantidade, Preco, 
        IdOrdem, Exportado, Deletado from BL_PreBoletas where IdTipoOrdem=15'''
        
        if data_neg != None:
            data_neg = self.banco.sql_data(data_neg)
            codsql += f' and DataMov={data_neg}'

        if deletado == False:
            codsql += ' and Deletado=0'
        elif deletado == True:
            codsql += ' and Deletado=1'
        
        if exportado == False:
            codsql += ' and Exportado=0'
        elif exportado == True:
            codsql += ' and Exportado=1'
        
        if execucao == False:
            codsql += ' and Execucao=0'
        elif execucao == True:
            codsql += ' and Execucao=1'

        if preboleta == False:
            codsql += ' and PreBoleta=0'
        elif preboleta == True:
            codsql += ' and PreBoleta=1'

        df_pre_bols = self.banco.dataframe(codsql)



        return df_pre_bols

    def secundario_cancela_id_trade(self, id_trade:int):
        quem, quando = self.banco.get_user_and_date()
        campos_valores = {
            'Cancelado':True,
            'QuemC':quem,
            'QuandoC':quando
        }
        result = self.banco.com_edit(tabela='PR_Secundario', filtro=f'IdTrade = {id_trade}', campos_valores=campos_valores)

        return result
    
    def secundario_atualiza_boleta(self, id_boleta, exportado=None, cancelado:bool=False):
        campos_valores = {
            'Cancelado':cancelado,
        }

        if exportado != None:
            campos_valores['Exportado'] = exportado

        result = self.banco.com_edit(tabela='PR_Sec_Boletas', campos_valores=campos_valores, filtro=f'IdBoleta = {id_boleta}')

        return result
   
    def sec_buyer_boletas_id_trade(self, id_trade:int):
        df = self.banco.dataframe(f'''select IdBoleta, IdTrade, IdComprador, Compra, Exportado, Cancelado, B_ContaCRM, B_CRMGuidConta,
                                  Quantidade, B_ContaMovEntradaFinGuid, B_ContaMovDestGuid, QuemC, QuandoC, QuandoE, QuemE
                                  from PR_Sec_Buyer where IdTrade={id_trade}''')

        return df

    def secundario_boletas_id_trade(self, id_trade:int):
        df = self.banco.dataframe(f'''select IdTrade, IdComprador, Compra, Exportado, Cancelado, B_ContaCRM, B_ContaMovGuid, B_Titularidade,
                                    Quantidade, B_ContaMovEntradaFinGuid, B_ContaMovDestGuid, QuemC, QuandoC, QuandoE, QuemE
                                    from PR_Sec_Boletas where IdTrade={id_trade} and Cancelado=0''')

        return df

    def secundario_boletas_id_trade_altera_quantidade(self, id_boleta:int, nova_quantidade:int):
        campos_valores = {
            'Quantidade':nova_quantidade,
        }

        result = self.banco.com_edit(tabela='PR_Sec_Boletas', campos_valores=campos_valores, filtro=f'IdBoleta = {id_boleta}')

        return result
    
    def secundario_gera_boletas_boletator(self, lista_id_boletas:list):
        str_tuple = str(tuple(lista_id_boletas))
        if len(lista_id_boletas) == 1:
            str_tuple = str_tuple.replace(',','')
        
        codsql = f'''
                    select
                        Bol.IdTrade, Bol.IdBoleta, Bol.B_CRMGuidConta as ContaCRMGuid, Bol.B_ContaCRM as ContaCRM,  Bol.Fundo,
                        Bol.Quantidade, Bol.Compra, Bol.B_ContaMovGuid, Bol.B_Titularidade as GuidTitularidade,
                        Sec.GuidProduto as AtivoGuid, Sec.NomeProduto as AtivoNome, Sec.PrecoCompra, Sec.PrecoVenda, Sec.DataNeg as DataMov,
                        Buy.IdOrdem, Sec.Contraparte, Sec.CodigoCetip as CodigoExt
                    from 
                        PR_Sec_Boletas Bol
                        LEFT JOIN PR_Secundario Sec on Sec.IdTrade = Bol.IdTrade
                        LEFT JOIN PR_Sec_Buyer Buy on Bol.IdTrade = Buy.IdTrade and Bol.B_CRMGuidConta = Buy.B_CRMGuidConta
                    where
                        bol.IdBoleta in {str_tuple} -- and bol.Exportado = 0 and bol.Cancelado=0
                    '''
        
        df = self.banco.dataframe(codsql)

        if len(df) == 0:
            return 'Não há boletas para serem exportadas'
    
        # cria as colunas necessárias da BL_PreBoletas
        
        df['TipoMov'] = 'V'
        df.loc[df['Compra']==1, 'TipoMov'] = 'C'

        df['Preco'] = df['PrecoVenda']
        df.loc[df['Compra']==1, 'Preco'] = df.loc[df['Compra']==1, 'PrecoCompra']

        df['Financeiro'] = df['Quantidade'] * df['Preco']

        df['DataCot'] = df['DataMov']
        df['DataFin'] = df['DataMov']
        
        df['QouF'] = 'Q'
        df['AtivoCadastrado'] = 1
        df['Secundario'] = 1

        df['MotivoAplic'] = 'Realocação'
        df['MotivoResg'] = 'Realocação'

        #cria as colunas de conta para serem usadas para as titularidades
        for col in ['GuidContaMovimento', 'ContaMovimento', 'GuidContaMovimentoOrigem', 'ContaMovimentoOrigem', 'GuidContaMovimentoDestino', 'ContaMovimentoDestino', 'Titularidade']:
            df[col] = None
        
        for idx, row in df.iterrows():
            tit_guid = row['GuidTitularidade']
            if tit_guid == None:
                continue
            else:
                contas = Crm().contas_movimento_por_titularidade(titularidade_id=tit_guid)
                conta_mov_guid = row['B_ContaMovGuid']
                conta_nome = contas.loc[contas['GuidContaMovimento']==conta_mov_guid,'NomeContaMovimento'].iloc[0]
                titularidade_nome = contas['new_titularidadeidname'].iloc[0]

                for col in ['GuidContaMovimento', 'GuidContaMovimentoOrigem', 'GuidContaMovimentoDestino']:
                    df.loc[idx,col] = conta_mov_guid
                
                for col in ['ContaMovimento', 'ContaMovimentoOrigem', 'ContaMovimentoDestino']:
                    df.loc[idx, col] = conta_nome

                df.loc[idx, 'Titularidade'] = titularidade_nome
        df = df[[
            'ContaCRMGuid', 'ContaCRM', 'AtivoCadastrado', 'AtivoGuid', 'AtivoNome', 'TipoMov', 'QouF', 'DataMov', 'DataCot', 'DataFin',
            'Financeiro', 'Quantidade', 'Preco','Secundario', 'CodigoExt', 'GuidContaMovimento', 'ContaMovimento', 'GuidContaMovimentoOrigem', 
            'ContaMovimentoOrigem', 'GuidContaMovimentoDestino', 'ContaMovimentoDestino', 'IdOrdem', 'Titularidade', 'GuidTitularidade',
            'MotivoAplic','MotivoResg', 'Contraparte'
        ]]

        print(self.banco.com_add_df(tabela='BL_PreBoletas', df=df))

    def secundario_boletas_dia(self, data_boletas:datetime.datetime=datetime.datetime.now()):
        data_boletas = self.banco.sql_data(data_boletas)
        codsql = f'''SELECT 
                    ContaCRM,
                    ContaCRMGuid,
                    Titularidade,
                    GuidTitularidade,
                    TipoMov,
                    SUM(FINANCEIRO) AS Financeiro
                    FROM 
                        BL_PreBoletas
                    WHERE 
                        IdTipoOrdem = 15 AND 
                        DataMov = {data_boletas} AND 
                        -- TipoMov = 'C' AND
                        Execucao = 0 AND
                        deletado = 0
                    GROUP BY 
                    ContaCRM,
                    ContaCRMGuid,
                    Titularidade,
                    GuidTitularidade,
                    TipoMov
                    '''
        df_boletas = self.banco.dataframe(codsql)

        return df_boletas
        
    def secundario_boletas_para_ratear(self):
        codsql = '''SELECT 
                    S.IdTrade, 
                    S.NomeProduto + 
                        CASE 
                            WHEN S.DtLimiteDem IS NULL THEN '' 
                            ELSE ' (lim.dem:' + CONVERT(CHAR(10), S.DtLimiteDem, 103) + ')'
                        END AS NomeProduto, 
                    S.Compra, 
                    S.Primario, 
                    ISNULL(Compras, 0) AS Compras, 
                    ISNULL(Vendas, 0) AS Vendas,

                    CASE 
                        WHEN Primario <> 0 AND Compras <> 0 THEN 1 
                        ELSE 
                            CASE 
                                WHEN Compras <> 0 AND Vendas <> 0 THEN 1 
                                ELSE 0 
                            END
                    END AS Exibir

                FROM 
                    PR_Secundario S WITH (NOLOCK)
                INNER JOIN (
                    SELECT DISTINCT IdTrade 
                    FROM PR_Sec_Buyer WITH (NOLOCK) 
                    WHERE Cancelado = 0
                ) B ON S.IdTrade = B.IdTrade

                LEFT JOIN (
                    SELECT IdTrade, COUNT(IdTrade) AS Compras 
                    FROM PR_Sec_Buyer 
                    WHERE Compra <> 0 AND Cancelado = 0 
                    GROUP BY IdTrade
                ) Cp ON S.IdTrade = Cp.IdTrade

                LEFT JOIN (
                    SELECT IdTrade, COUNT(IdTrade) AS Vendas 
                    FROM PR_Sec_Buyer 
                    WHERE Compra = 0 AND Cancelado = 0 
                    GROUP BY IdTrade
                ) Vd ON S.IdTrade = Vd.IdTrade

                LEFT JOIN PO_Ativos A WITH (NOLOCK) ON S.GuidProduto = A.GuidAtivo

                WHERE 
                    S.Rateado = 0 
                    AND S.Cancelado = 0 
                    AND (A.RatingCRM IS NULL OR A.RatingCRM = 'RECOMENDADO')
                    AND (
                        CASE 
                            WHEN Primario <> 0 AND Compras <> 0 THEN 1 
                            ELSE 
                                CASE 
                                    WHEN Compras <> 0 AND Vendas <> 0 THEN 1 
                                    ELSE 0 
                                END
                        END = 1
                    )

                ORDER BY 
                    S.NomeProduto
                '''
        df = self.banco.dataframe(codsql=codsql)

        return df
    
    

class CreditManagement: 
    """
    Classe criada para intemediar interações com a base BAWM (BAWM), no servidor Aux,
    tratando especificamente de coisas ligadas à gestão dos ativos de crédito.
    
    Ela lida com as tabelas cujo nome começa com PO_LimCred* 
    
    """
    def __init__(self, buffer:bool=False, homologacao:bool=False):
        if homologacao:
            self.banco = BaseSQL(nome_servidor=r'SQLSVRHOM1.GPS.BR\SQL12', nome_banco='BAWM_HOM1')
        else:
            self.banco = BaseSQL(nome_servidor=ambiente.servidor_aux(), nome_banco='BAWM')
            
        self.buffer = buffer
        self.duracao_buffer = 15 * 60
        
        # buffers
        self.df_limcred = pd.DataFrame()
        self.df_limcred_set = datetime.datetime.now()
        self.df_lista_id_tipo = pd.DataFrame()
        self.df_lista_id_tipo_set = datetime.datetime.now()
        self.df_lista_limcredTp = pd.DataFrame()
        self.df_lista_limcredTp_set = datetime.datetime.now()
        self.df_lista_limcred_statement = pd.DataFrame()
        self.df_lista_limcred_statement_set = datetime.datetime.now()
        
        self.df_dados_cadastro = pd.DataFrame()
        self.df_dados_cadastro_set = datetime.datetime.now()
        
    def limites_credito(self):
        codsql = "EXEC PO_LimCredH_LimitesAtuais"
        df = self.banco.dataframe(codsql=codsql).drop_duplicates()
        return df
    
    def limites_excecoes_all(self):
        codsql =   f"""SELECT Tbl.IdLimCred, IdLimCrExc, NomeLimite, GuidLimite, IdTipoLim, Tbl3.TipoExcessao, Tbl3.NomeExcessao, Tbl3.GuidExcessao, ISNULL(tbl3.LimitePerc,0) AS Limite
                    FROM ( 
                    		SELECT L.IdLimCred, GuidLimite, IdTipoLim, L.NomeLimite 
                    		FROM PO_LimCred L WITH (NOLOCK)
                    		WHERE L.Encerrado=0 
                    		UNION 
                    		SELECT L.IdLimCred, Eq.GuidEquiv, IdTipoLim, L.NomeLimite 
                    		FROM PO_LimCredEqui Eq WITH (NOLOCK) INNER JOIN PO_LimCred L WITH (NOLOCK) ON L.IdLimCred=Eq.IdLimCred 
                    		WHERE L.Encerrado=0 
                    	) TBL 
                    	INNER JOIN ( 
                    		SELECT IdLimCrExc, IdLimCred, R.TipoExcessao, T.Tipo as NomeExcessao, LOWER(R.GuidExcessao) as GuidExcessao, R.LimitePerc 
                    		FROM [PO_LimCredEH] R WITH (NOLOCK) INNER JOIN 
                    			(SELECT [IdLimCred] as Codigo, MAX(DataRef) AS Dt FROM [PO_LimCredEH] Lim WITH (NOLOCK) GROUP BY IdLimCred) Tbl2 
                    			ON R.DataRef=Tbl2.Dt AND R.IdLimCred=Tbl2.Codigo
                    			INNER JOIN PO_LimCredTp T WITH (NOLOCK) ON R.TipoExcessao=T.IdTipo
                    		) TBL3 ON tbl.IdLimCred=tbl3.IdLimCred
                    WHERE 0=0
                    """
        df = self.banco.dataframe(codsql=codsql)
        return df
    
    def limites_excecoes(self, guid_conta_crm, guid_mandato):
        codsql =   f"""SELECT Tbl.IdLimCred, IdLimCrExc, NomeLimite, GuidLimite, IdTipoLim, Tbl3.TipoExcessao, Tbl3.NomeExcessao, Tbl3.GuidExcessao, ISNULL(tbl3.LimitePerc,0) AS Limite
                    FROM ( 
                    		SELECT L.IdLimCred, GuidLimite, IdTipoLim, L.NomeLimite 
                    		FROM PO_LimCred L WITH (NOLOCK)
                    		WHERE L.Encerrado=0 
                    		UNION 
                    		SELECT L.IdLimCred, Eq.GuidEquiv, IdTipoLim, L.NomeLimite 
                    		FROM PO_LimCredEqui Eq WITH (NOLOCK) INNER JOIN PO_LimCred L WITH (NOLOCK) ON L.IdLimCred=Eq.IdLimCred 
                    		WHERE L.Encerrado=0 
                    	) TBL 
                    	INNER JOIN ( 
                    		SELECT IdLimCrExc, IdLimCred, R.TipoExcessao, T.Tipo as NomeExcessao, LOWER(R.GuidExcessao) as GuidExcessao, R.LimitePerc 
                    		FROM [PO_LimCredEH] R WITH (NOLOCK) INNER JOIN 
                    			(SELECT [IdLimCred] as Codigo, MAX(DataRef) AS Dt FROM [PO_LimCredEH] Lim WITH (NOLOCK) GROUP BY IdLimCred) Tbl2 
                    			ON R.DataRef=Tbl2.Dt AND R.IdLimCred=Tbl2.Codigo
                    			INNER JOIN PO_LimCredTp T WITH (NOLOCK) ON R.TipoExcessao=T.IdTipo
                    		) TBL3 ON tbl.IdLimCred=tbl3.IdLimCred
                    WHERE NOT Tbl3.TipoExcessao IN (4,5) OR (Tbl3.TipoExcessao=4 AND GuidExcessao='{guid_conta_crm}') OR (Tbl3.TipoExcessao=5 AND GuidExcessao='{guid_mandato}')
                    """
        df = self.banco.dataframe(codsql=codsql)
        return df
    
    def statement_inserir_alterar(self, df_dados:pd.DataFrame, id_lim_cred:int=None, data_competencia=None, idfonte:int=None, id_statement=None):
        """
        Insere ou edita um statement.
        Se não for provido id_statetment cria um novo na tabela

        Parameters
        ----------
        id_lim_cred : int
            DESCRIPTION.
        data_competencia : DateTime
            Data do Balanço.
        idfonte : int
            IdTipo na PO_LimCredTp.
        df_dados : pd.DataFrame
            DataFrame com as colunas: IdTipo, Valor, ValorAjuste (opt), Observacao (opt), QuemAdj (opt), QuandoAdj (Opt).
        id_statetment : TYPE, optional
            Conforme tabela PO_LimCred_Statement, opcional

        Returns
        -------
        resultado : TYPE
            DESCRIPTION.

        """
        # Cria novo Statement na tabela
        if id_statement:
            novo_id = id_statement
        else:
            if not id_lim_cred or not data_competencia or not idfonte:
                raise Exception('Se não for informado um IdStatement existente, é preciso prover id_limite, data_competencia e idfonte')
            # Verifica se já existe combinação de IdLimCred e DataCompetencia
            codsql = f"SELECT IdStatement FROM PO_LimCred_Statement WHERE IdLimCred={id_lim_cred} AND DataCompetencia={self.banco.sql_data(data_competencia)}"
            df_temp = self.banco.dataframe(codsql)
            if not df_temp.empty:
                novo_id = df_temp.iloc[0]['IdStatement']
            else:
                dicio = {'IdLimCred': id_lim_cred, 'DataCompetencia': data_competencia, 'IdTipoFonte': idfonte}
                novo_id = self.banco.com_add('PO_LimCred_Statement', campos_valores=dicio, output_campo='IdStatement')
        
        # Insere ou edita os dados do balanço
        df_dados.insert(0, 'IdStatement', [novo_id]*len(df_dados))
        resultado = self.banco.com_edit_or_add_df(tabela='PO_LimCred_StatementDetalhe', dados=df_dados, campos_filtros=['IdStatement', 'IdTipo'],
                                                  colunas_comparar=['Valor', 'ValorAjuste', 'Observacao', 'QuemAdj', 'QuandoAdj'])        
        return resultado
    
    def statement_busca_valortipo_statement_anterior(self, id_detalhe:int):
        codsql = f"""SELECT TOP 1 D1.IdDetalhe, D1.IdTipo, D1.Valor as ValorAtual, S1.DataCompetencia, S1.IdLimCred, S2.DataCompetencia as DtCompAnterior, S2.IdStatement, S2.Valor as ValorAnt
                      FROM PO_LimCred_StatementDetalhe D1 INNER JOIN PO_LimCred_Statement S1 ON D1.IdStatement=S1.IdStatement
                    	   INNER JOIN (
                    				   SELECT S.IdStatement, S.IdLimCred, S.DataCompetencia, D.IdDetalhe, D.IdTipo, D.Valor FROM PO_LimCred_Statement S INNER JOIN PO_LimCred_StatementDetalhe D ON S.IdStatement=D.IdStatement
                    				   ) S2 ON S1.IdLimCred=S2.IdLimCred AND S2.IdTipo=D1.IdTipo AND S1.DataCompetencia>S2.DataCompetencia
                      WHERE D1.IdDetalhe={id_detalhe}
                      ORDER BY S2.DataCompetencia DESC
                 """
        return self.banco.dataframe(codsql)
    
    def lista_id_tipo(self, campo_filtro:str=None):
        codsql = 'SELECT * FROM PO_LimCredTp WITH (NOLOCK)'
        if self.buffer:
            if self.df_lista_id_tipo.empty or (datetime.datetime.now() - self.df_lista_id_tipo_set).total_seconds() > self.duracao_buffer:
                self.df_lista_id_tipo = self.banco.dataframe(codsql).set_index('IdTipo')
                self.df_lista_id_tipo_set = datetime.datetime.now()
            df = self.df_lista_id_tipo.copy()
        else:            
            df = self.banco.dataframe(codsql).set_index('IdTipo')
        if campo_filtro:
            df = df[df[campo_filtro]==True]
        return df
        
    def lista_limcred(self, id_tipo_limite:int=None):
        codsql = 'SELECT * FROM PO_LimCred WITH (NOLOCK)'
        if self.buffer:
            if self.df_limcred.empty or (datetime.datetime.now() - self.df_limcred_set).total_seconds() > self.duracao_buffer:
                self.df_limcred = self.banco.dataframe(codsql).set_index('IdLimCred')
                self.df_limcred_set = datetime.datetime.now()
            df = self.df_limcred.copy()
        else:            
            df = self.banco.dataframe(codsql).set_index('IdLimCred')
        # Aplica filtros
        if id_tipo_limite:
            df = df[df['IdTipoLim']==id_tipo_limite]
        
        return df        
        
    def lista_rubricasIDs(self):
        codsql = 'SELECT * FROM PO_LimCredTp WITH (NOLOCK)'
        if self.buffer:
            if self.df_lista_limcredTp.empty or (datetime.datetime.now() - self.df_lista_limcredTp_set).total_seconds() > self.duracao_buffer:
                self.df_lista_limcredTp = self.banco.dataframe(codsql)
                self.df_lista_limcredTp = datetime.datetime.now()
            return self.df_lista_limcredTp.copy()
        else:            
            return self.banco.dataframe(codsql)
    
    def lista_PO_LimCred_Statement(self, IdLimCred:int, DtIni=None, DtFim=None):
        codsql = f"""SELECT IdStatement,IdLimCred,DataCompetencia, IdTipoFonte, T.Tipo as NomeFonte, Quem, Quando, Deletado,QuemDel,QuandoDel
                     from PO_LimCred_Statement S WITH (NOLOCK) INNER JOIN PO_LimCredTp T WITH (NOLOCK) ON S.IdTipoFonte=T.IdTipo
                     where IdLimCred = {IdLimCred}"""
        if DtIni:
            codsql += f" and DataCompetencia >= {self.banco.sql_data(DtIni)}"
        if DtFim:
            codsql += f"and DataCompetencia <= {self.banco.sql_data(DtFim)}"
        
        return self.banco.dataframe(codsql)
    
    def lista_PO_LimCred_StatementDetalhe(self, lista):
        codsql = f"""Select t1.*, t2.DataCompetencia from PO_LimCred_StatementDetalhe t1 inner join(select * from PO_LimCred_Statement) t2 on t1.IdStatement = t2.IdStatement and 
        t1.IdStatement in {lista}"""
        return self.banco.dataframe(codsql)
    
    def statement_detalhe(self, id_statement:int):
        codsql = f"""SELECT IdDetalhe, D.IdTipo, T.Tipo as ItemBalaco, Valor, ValorAjuste as ValorAjustado, Observacao as Observação, QuemAdj, QuandoAdj, T.ItemBalanco, T.ItemScoreCred
                    FROM PO_LimCred_StatementDetalhe D WITH (NOLOCK) INNER JOIN PO_LimCredTp T WITH (NOLOCK) ON D.IdTipo=T.IdTipo
                    WHERE IdStatement={id_statement}
                    ORDER BY ItemBalanco, ItemScoreCred"""
        return self.banco.dataframe(codsql)
    
    def statement_detalhe_ajusta_valor(self, id_detalhe:int, valor_adj:float, observacao:str=None):
        lista_user_date = self.banco.get_user_and_date()        
        campos_valores = {'ValorAjuste': valor_adj, 'Observacao': observacao, 'QuemAdj': lista_user_date[0], 'QuandoAdj': lista_user_date[1]}
        resultado = self.banco.com_edit(tabela='PO_LimCred_StatementDetalhe', filtro=f"IdDetalhe={id_detalhe}", campos_valores=campos_valores)
        return resultado
    
    def lista_LimRating(self, IdLimCred):
        codsql = f"""select * from PO_LimCredRating WITH (NOLOCK) where IdLimCred = '{IdLimCred}'"""
        return self.banco.dataframe(codsql)
    
    def lista_PO_LimCred_Statement_2(self, DtFim, IdLimCred):
        codsql = f"""select IdStatement from PO_LimCred_Statement WITH (NOLOCK) where IdLimCred = '{IdLimCred}' and DataCompetencia = '{DtFim}'"""
        return self.banco.dataframe(codsql)
    
    def idlim_cadastro(self, idlim_cred:int):
        codsql = """SELECT IdLimCred, NomeLimite, GuidLimite, T1.SetorCred, ICVM, Responsavel, FreqMonEmi as 'Frenquência Monitoramento Emissão'
                    	   , FreqMonEstr as 'Frenquência Monitoramento Estrutura', Observacao as Observação, Encerrado, DataEnc as 'Data Encerramento'
                    	   , quem, quando
                    FROM PO_LimCred L WITH (NOLOCK) LEFT JOIN PO_LimCredSetor T1 WITH (NOLOCK) ON L.IdCredSet=T1.IdCredSetor"""
        if self.buffer:
            if self.df_dados_cadastro.empty or (datetime.datetime.now() - self.df_dados_cadastro_set).total_seconds() > self.duracao_buffer:            
                self.df_dados_cadastro = self.banco.dataframe(codsql).set_index('IdLimCred')
                self.df_dados_cadastro_set = datetime.datetime.now()    
            return self.df_dados_cadastro.loc[[idlim_cred]]
        else:
            return self.banco.dataframe(codsql + f" WHERE IdlimCred={idlim_cred}")
        

class SolRealocacao:
    """
    Classe criada para intemediar interações com a base BAWM (BAWM), no servidor Aux,
    tratando especificamente de coisas ligadas às Solicitações de Realocação.
    
    Ela lida com as tabelas cujo nome começa com SO_* 
    
    Tabelas são alteradas regularmente ao longo do dia: não usar NO LOCK
    """
    def __init__(self, homologacao=False):
        if homologacao:
            self.banco = BaseSQL(nome_servidor=r'SQLSVRHOM1.GPS.BR\SQL12', nome_banco='BAWM_HOM1')
        else:
            self.banco = BaseSQL(nome_servidor=ambiente.servidor_aux(), nome_banco='BAWM')
        
        # Variáveis gerais        
        self.df_campos = self.banco.dataframe('SELECT * FROM SO_Campos')
        
    def dicionario_tipos_solicitacao(self, id_tipo_solicitacao):
        dicionario = {1: 'Enquadramento PI', 2: 'Ajuste Interclasse', 3: 'Ajuste intraclasse',
                      4: 'Ordem Bolsa', 5: 'Ordem Fundos', 6: 'Ordem RF Soberana',
                      7: 'Providenciar liquidez', 8: 'Aporte relevante', 9: 'Distrato'}
        return dicionario[id_tipo_solicitacao]
    
    def campo_coluna(self, id_campo):
        return self.df_campos[self.df_campos['IdCampo']==id_campo].iloc[0]['TipoValor']
    
    
    def solicitacao_cadastro(self, id_solicitacao=None, guid_solicitacao=None):
        if id_solicitacao:
            codsql = f'SELECT * FROM SO_Solicitacoes WITH (NOLOCK) WHERE IdSolicitacao={id_solicitacao}'
        if guid_solicitacao:
            codsql = f"SELECT * FROM SO_Solicitacoes WITH (NOLOCK) WHERE GuidSolicitacao='{guid_solicitacao}'"
        return self.banco.dataframe(codsql=codsql)
    
    def solicitacao_cadastro_lista(self, lista_id_solicitacao:list):
        lista = [str(x) for x in lista_id_solicitacao]
        filtro = ','.join(lista)
        codsql = f'SELECT * FROM SO_Solicitacoes WITH (NOLOCK) WHERE IdSolicitacao IN ({filtro})'
        return self.banco.dataframe(codsql=codsql)

    def solicitacao_ordens(self, id_solicitacao, solicitacao_em_fundo=False):        
        # Tabela de movimentações
        codsql = f"""
                SELECT IdSolMov, IdSolicitacao, Veiculo, GuidContaMovimento, NomeContaMovimento, TipoMov, Classe, GuidProduto, NomeProduto, FinanceiroDem, QuantidadeDem, PrecoLimDem, ResgTotal, ISNULL(FinanceiroExec, FinanceiroDem) as FinanceiroExec, QuantidadeExec, IdCampoExec, quem, quando, quemPM, quandoPM, deletado, quemdel, quandodel
                FROM SO_SolMovs WITH (NOLOCK)
                WHERE IdSolicitacao={id_solicitacao} AND NOT Veiculo=''
                """
        df = self.banco.dataframe(codsql=codsql)        
        if df.empty:
            return df        
        df.insert(len(df.columns), 'FinanceiroFundoEst', [None] * len(df))
        
        # Supercarteira em questão
        cad = self.solicitacao_cadastro(id_solicitacao=id_solicitacao).iloc[0]
        sc = cad['GuidSupercarteira']
        
        # 1. Cria coluna ajustada para 100% do fundo
        veic_ant = ''
        for idx, row in df.iterrows():
            if solicitacao_em_fundo:
                if row['FinanceiroExec']:
                    df.loc[idx, 'FinanceiroFundoEst'] = row['FinanceiroExec']
                else:
                    df.loc[idx, 'FinanceiroFundoEst'] = row['FinanceiroDem']
            else:
                if row['Veiculo'] not in ['PF', 'CartAdm']:
                    if veic_ant != row['Veiculo']:
                        veic_ant = row['Veiculo']
                        passivo = PosicaoDm1().passivo_fundo_sc_gestao(fundo_conta_crm=row['Veiculo'])
                        try:
                            passivo = passivo[passivo['GuidSupercarteira']==sc].iloc[0]
                        except:
                            raise Exception('Supercarteira não encontrada no passivo do fundo, ou passivo do fundo vazio') # TODO trocar detecção para Guid do fundo
                    if row['FinanceiroExec']:
                        df.loc[idx, 'FinanceiroFundoEst'] = int(row['FinanceiroExec'] / passivo['Share'] / 1000) * 1000
                    else:
                        df.loc[idx, 'FinanceiroFundoEst'] = int(row['FinanceiroDem'] / passivo['Share'] / 1000) * 1000
        
        # 2. Preenche classes dos produtos que não estiverem preenchidos
        df_prod_classe = Crm().product_classes()
        temp = df.isna()
        for idx, row in df.iterrows():
            # 2. a. insere a classe
            if temp.loc[idx, 'Classe']:
                guid_prod = str(row['GuidProduto']).lower().replace('{','').replace('}', '').strip()
                try:
                    classe = df_prod_classe[df_prod_classe['productId']==guid_prod]                    
                except:
                    raise Warning(f'Produto não encontrado no CRM: {guid_prod}')
                df.loc[idx, 'Classe'] = classe.iloc[0]['new_classeidname']
        
        return df
    
    def solicitacao_campo_aux(self, id_solicitacao, id_campo):
        filtro = f'IdSolicitacao={id_solicitacao} AND IdCampo={id_campo}'
        return self.banco.busca_valor('SO_SolAux', filtro, campo=self.campo_coluna(id_campo))
    
    def solicitacao_campo_aux_gravar(self, id_solicitacao, id_campo, valor):
        campo = self.campo_coluna(id_campo)
        df = [{'IdSolicitacao': id_solicitacao, 'IdCampo': id_campo, campo: valor}]
        df = pd.DataFrame(df)
        return self.banco.com_edit_or_add_df(tabela='SO_SolAux', dados=df, 
                                             campos_filtros=['IdSolicitacao', 'IdCampo']
                                             , colunas_comparar=[campo])
    
    def status_inicial(self, data_busca_ini, data_busca_fim=None):
        data_str_ini = self.banco.sql_data(data_busca_ini)
        if data_busca_fim == None:
            data_busca_fim = self.banco.hoje() + relativedelta(days=1)
        
        data_str_fim = self.banco.sql_data(data_busca_fim)

        codsql = f"""SELECT S.IdSolicitacao as IdSol
                FROM SO_Solicitacoes S WITH (NOLOCK) LEFT JOIN (SELECT * FROM SO_SolAux WITH (NOLOCK) WHERE IdCampo=15) A 
                ON S.IdSolicitacao=A.IdSolicitacao
                WHERE (A.IdCampo IS NULL OR A.IdCampo=15) AND S.DataPedido>={data_str_ini} and S.DataPedido<={data_str_fim} AND (A.ValorStr='Em análise' OR A.ValorStr IS NULL)
                """
        return self.banco.dataframe(codsql=codsql)
    
    def status_executar(self, data_busca_ini, data_busca_fim=None):
        data_str_ini = self.banco.sql_data(data_busca_ini)
        if data_busca_fim == None:
            data_busca_fim = self.banco.hoje() + relativedelta(days=1)
        
        data_str_fim = self.banco.sql_data(data_busca_fim)

        codsql = f"""SELECT IdSolicitacao as IdSol FROM SO_SolAux WITH (NOLOCK) WHERE IdCampo=17 AND ValorBool=0 AND quando>={data_str_ini}
                AND quando <= {data_str_fim} 
                EXCEPT
                SELECT IdSolicitacao as IdSol FROM SO_SolAux WITH (NOLOCK) WHERE IdCampo=14 AND ValorBool=1
                ORDER BY IdSol DESC
                """
        return self.banco.dataframe(codsql=codsql)
    
    def status_devolvidas(self, data_busca_ini, data_busca_fim=None):
        data_str_ini = self.banco.sql_data(data_busca_ini)
        if data_busca_fim == None:
            data_busca_fim = self.banco.hoje() + relativedelta(days=1)
        
        data_str_fim = self.banco.sql_data(data_busca_fim)
    
        codsql = f"""SELECT IdSolicitacao as IdSol FROM SO_SolAux WITH (NOLOCK) WHERE IdCampo=17 AND ValorBool<>0 AND quando>={data_str_ini}
                    AND quando <= {data_str_fim}             
                    ORDER BY IdSol DESC
                    """
        return self.banco.dataframe(codsql=codsql)
    
    def status_concluidas(self, data_busca_ini, data_busca_fim=None):
        data_str_ini = self.banco.sql_data(data_busca_ini)
        if data_busca_fim == None:
            data_busca_fim = self.banco.hoje() + relativedelta(days=1)
        
        data_str_fim = self.banco.sql_data(data_busca_fim)

        codsql = f"""
                SELECT IdSolicitacao as IdSol FROM SO_SolAux WITH (NOLOCK) WHERE IdCampo=14 AND ValorBool=1 AND quando>={data_str_ini}
                AND quando <= {data_str_fim}
                EXCEPT
                SELECT IdSolicitacao as IdSol FROM SO_SolAux WITH (NOLOCK) WHERE IdCampo=17 AND ValorBool=1
                ORDER BY IdSol DESC"""
                
        return self.banco.dataframe(codsql=codsql)
    
    def __solicitacoes_busca_campos_adicionais_lista__(self, lista_id_solicitacao:list):
        lista = [str(x) for x in lista_id_solicitacao]
        filtro = ','.join(lista)
        codsql = f"SELECT * FROM SO_SolAux A WITH (NOLOCK) WHERE IdSolicitacao IN ({filtro})"
        return self.banco.dataframe(codsql)
    
    def solicitacoes_executar_dados(self, tipo_lista=1, dias=15):
        data = self.banco.hoje() - relativedelta(days=15)
        if tipo_lista == 1:
            lista = self.status_executar(data_busca_ini=data)
        elif tipo_lista == 2:
            lista = self.status_inicial(data_busca_ini=data)
        elif tipo_lista == 3:
            lista = self.status_devolvidas(data_busca_ini=data)
        elif tipo_lista == 4:
            lista = self.status_concluidas(data_busca_ini=data)
        else:
            raise Exception(f'SolRealocacao\solicitacoes_executar_dados: não há ação cadastrada para o tipo_lista {tipo_lista}')
        
        if lista.empty:
            return pd.DataFrame()
        
        lista = list(lista['IdSol'].unique())
        df = self.solicitacao_cadastro_lista(lista)
        
        df.insert(len(df.columns), 'TipoSolicitacao', [''] * len(df))
        df = df[['GuidSolicitacao', 'IdSolicitacao', 'DataPedido', 'NomeSupercarteira', 'TipoSolicitacao','IdTipoSolicitacao','quem']]
        
        df.insert(len(df.columns), 'Solicitante', [None] * len(df))
        df.insert(len(df.columns), 'Status', [None] * len(df))
        df.insert(len(df.columns), 'Mensagem', [None] * len(df))
        if df.empty:
            return df
        
        df_add = self.__solicitacoes_busca_campos_adicionais_lista__(list(df['IdSolicitacao']))

        lista_quem = []

        for id_sol in df['IdSolicitacao'].tolist():
            df_temp = df_add.loc[df_add['IdSolicitacao']==id_sol]

            if 14 in df_temp['IdCampo'].tolist():
                quem = df_temp.loc[df_temp['IdCampo']==14,'quem'].iloc[0]
            if 17 in df_temp['IdCampo'].tolist():
                quem = df_temp.loc[df_temp['IdCampo']==17,'quem'].iloc[0]
            else:
                quem = df_temp['quem'].iloc[-1]
            
            lista_quem.append(pd.DataFrame({'IdSolicitacao':[id_sol], 'quem':[quem]}))
        df_quem = pd.concat(lista_quem)
        df_add = df_add.set_index(['IdSolicitacao', 'IdCampo'])
            
        def busca_rapida(id_solicitacao, id_campo, df_adicionais):
            try:
                busca = (id_solicitacao, id_campo)
                linha = df_adicionais.loc[[busca]].iloc[0]
                valor = linha[self.campo_coluna(id_campo)]
            except:
                valor = None
            return valor
        
        for idx, row in df.iterrows():                        
            nome = busca_rapida(row['IdSolicitacao'], id_campo=4, df_adicionais=df_add)
            df.loc[idx, 'Solicitante'] = nome
            nome = busca_rapida(row['IdSolicitacao'], id_campo=15, df_adicionais=df_add)
            df.loc[idx, 'Status'] = nome
            nome = busca_rapida(row['IdSolicitacao'], id_campo=16, df_adicionais=df_add)
            df.loc[idx, 'Mensagem'] = nome
                
        df.set_index('GuidSolicitacao', inplace=True)
        for idx, row in df.iterrows():
            df.loc[idx, 'TipoSolicitacao'] = self.dicionario_tipos_solicitacao(row['IdTipoSolicitacao'])
        df.drop('IdTipoSolicitacao', axis=1, inplace=True)
        po_cad = Bawm().po_cadastro_all()
        po_cad = po_cad[['NomeSuperCarteiraCRM','Segmento']].drop_duplicates(subset=['NomeSuperCarteiraCRM'])


        df.reset_index(inplace=True)
        df = df.merge(po_cad, right_on=['NomeSuperCarteiraCRM'], left_on=['NomeSupercarteira'], how='left')
        df = df[['GuidSolicitacao','IdSolicitacao', 'DataPedido', 'NomeSupercarteira', 'TipoSolicitacao','Segmento','Solicitante', 'Status','Mensagem']]
        df = df.merge(df_quem, on='IdSolicitacao', how='left')
        return df.set_index('GuidSolicitacao')
    
    def status_solicitacao_msg(self, data_ini, data_fim):
        data_ini = self.banco.sql_data(data_ini)
        data_fim = self.banco.sql_data(data_fim)
        codsql = f'select IdSolicitacao from SO_Solicitacoes where DataPedido>={data_ini} and DataPedido<={data_fim}'
        df_sols = self.banco.dataframe(codsql)
        lista_sols = df_sols["IdSolicitacao"].tolist()
        codsql = f'select IdSolicitacao, ValorStr as Mensagem from SO_SolAux where IdCampo=16 and IdSolicitacao in {tuple(lista_sols)}'
        df_sols_aux = self.banco.dataframe(codsql)

        return df_sols_aux

    def solicitacao_executar_dados_range_datas(self, data_ini=None, data_fim=None, tipo_lista=1):
        if data_ini == None:
            data_ini = self.banco.hoje() - relativedelta(days=15)
        
        if data_fim == None:
            data_fim = self.banco.hoje() 
        
        # como a data_str é preenchida com hora 00:00:00, colocamos um dia a mais
        data_fim = data_fim + relativedelta(days=1)

        if tipo_lista == 1:
            lista = self.status_executar(data_busca_ini=data_ini, data_busca_fim=data_fim)
        elif tipo_lista == 2:
            lista = self.status_inicial(data_busca_ini=data_ini, data_busca_fim=data_fim)
        elif tipo_lista == 3:
            lista = self.status_devolvidas(data_busca_ini=data_ini, data_busca_fim=data_fim)
        elif tipo_lista == 4:
            lista = self.status_concluidas(data_busca_ini=data_ini, data_busca_fim=data_fim)
        else:
            raise Exception(f'SolRealocacao\solicitacoes_executar_dados: não há ação cadastrada para o tipo_lista {tipo_lista}')
        
        if lista.empty:
            return pd.DataFrame()
        
        lista = list(lista['IdSol'].unique())
        df = self.solicitacao_cadastro_lista(lista)
        
        df.insert(len(df.columns), 'TipoSolicitacao', [''] * len(df))
        df = df[['GuidSolicitacao', 'IdSolicitacao', 'DataPedido', 'NomeSupercarteira', 'TipoSolicitacao','IdTipoSolicitacao']]
        df.insert(len(df.columns), 'Solicitante', [None] * len(df))
        df.insert(len(df.columns), 'Status', [None] * len(df))
        df.insert(len(df.columns), 'Mensagem', [None] * len(df))
        if df.empty:
            return df
        
        for idx, row in df.iterrows():                        
            nome = self.solicitacao_campo_aux(id_solicitacao=row['IdSolicitacao'], id_campo=4)
            df.loc[idx, 'Solicitante'] = nome
            nome = self.solicitacao_campo_aux(id_solicitacao=row['IdSolicitacao'], id_campo=15)
            df.loc[idx, 'Status'] = nome
            nome = self.solicitacao_campo_aux(id_solicitacao=row['IdSolicitacao'], id_campo=16)
            df.loc[idx, 'Mensagem'] = nome
                
        df.set_index('GuidSolicitacao', inplace=True)
        for idx, row in df.iterrows():
            df.loc[idx, 'TipoSolicitacao'] = self.dicionario_tipos_solicitacao(row['IdTipoSolicitacao'])
        df.drop('IdTipoSolicitacao', axis=1, inplace=True)

        po_cad = Bawm().po_cadastro_all()
        po_cad = po_cad[['NomeSuperCarteiraCRM','Segmento']].drop_duplicates(subset=['NomeSuperCarteiraCRM'])
        df = df.merge(po_cad, right_on=['NomeSuperCarteiraCRM'], left_on=['NomeSupercarteira'], how='left')
        df = df[['IdSolicitacao', 'DataPedido', 'NomeSupercarteira', 'TipoSolicitacao','Segmento','Solicitante', 'Status','Mensagem']]
        df.reset_index(inplace=True)
        
        return df

    def validade_pendente(self):
        codsql = """SELECT S.IdSolicitacao
                FROM SO_Solicitacoes S WITH (NOLOCK) LEFT JOIN (SELECT * FROM SO_SolAux WITH (NOLOCK) WHERE IdCampo=15) A 
                ON S.IdSolicitacao=A.IdSolicitacao
                WHERE (A.IdCampo IS NULL OR A.IdCampo=15) AND (A.ValorBool=0 OR A.ValorBool IS NULL)
                """
        return self.banco.dataframe(codsql=codsql)
    
    def movimentacao_atualizar(self, idsolmov, campo, valor):
        tbl = "SO_SolMovs"
        filtro = f"IdSolMov={idsolmov}"
        if valor:
            valores = {campo: valor}            
        else:
            valores = {campo: None}
        teste = self.banco.com_edit(tabela=tbl, filtro=filtro, campos_valores=valores, campo_quem='quemPM', campo_quando='quandoPM')
        return teste
    
    def status_solicitacoes(self, data_ini=None, data_fim=None, usuario_u:str=None, dias:int=7):
        if data_fim:
            data_final = data_fim
        else:
            data_final = self.banco.hoje()
        if data_ini:
            data_inicial = data_ini
        else:
            data_inicial = data_final - relativedelta(days=dias)
        data_ini_str = self.banco.sql_data(data_inicial)
        data_fin_str = self.banco.sql_data(data_final)
        
        filtro_usuario = ""
        if usuario_u:
            filtro_usuario = f" AND (Cad.OfficerUsuarioU='{usuario_u}' OR Cad.ControllerUsuarioU='{usuario_u}' OR Cad.ControllerBKPUsuarioU='{usuario_u}' OR Cad.DPMUsuarioU='{usuario_u}' OR Cad.DeputyDPMUsuarioU='{usuario_u}' OR ResponsavelUsuarioU='{usuario_u}')"
        
        # Status inicial
        codsql = f"""   SELECT S.IdSolicitacao, S.NomeSupercarteira, S.DataPedido, S.IdTipoSolicitacao, St.Status, Msg.Mensagem, S.quando as QuandoPedido, QuandoStatus
                        , Cad.Officer, Cad.OfficerUsuarioU, Cad.Controller, Cad.ControllerUsuarioU, Cad.ControllerBKP, Cad.ControllerBKPUsuarioU, Cad.DPM, Cad.DPMUsuarioU, Cad.DeputyDPM, Cad.DeputyDPMUsuarioU, ResponsavelUsuarioU
                        FROM SO_Solicitacoes S WITH (NOLOCK) INNER JOIN 
                        	 (SELECT IdSolicitacao, ValorStr as Status, quando as QuandoStatus FROM SO_SolAux A WITH (NOLOCK) INNER JOIN SO_Campos C WITH (NOLOCK) ON A.IdCampo=C.IdCampo AND A.IdCampo=15) St ON S.IdSolicitacao=St.IdSolicitacao 
                        	 INNER JOIN
                        	 (SELECT IdSolicitacao, ValorStr as Mensagem, A.quem as ResponsavelUsuarioU FROM SO_SolAux A WITH (NOLOCK) INNER JOIN SO_Campos C WITH (NOLOCK) ON A.IdCampo=C.IdCampo AND A.IdCampo=16) Msg ON S.IdSolicitacao=Msg.IdSolicitacao 
                        	 LEFT JOIN (SELECT DISTINCT GuidSuperCarteira, Cad.Officer, Cad.OfficerUsuarioU, Cad.Controller, Cad.ControllerUsuarioU, Cad.ControllerBKP, Cad.ControllerBKPUsuarioU, Cad.DPM, Cad.DPMUsuarioU, Cad.DeputyDPM, Cad.DeputyDPMUsuarioU FROM PO_Cadastro Cad WITH (NOLOCK)) Cad ON S.GuidSupercarteira=Cad.GuidSuperCarteira
                        WHERE S.DataPedido BETWEEN {data_ini_str} AND {data_fin_str} {filtro_usuario}
                        ORDER BY S.IdSolicitacao DESC"""
        df_base = self.banco.dataframe(codsql)
        
        # Informações completas
        codsql = f"""SELECT	S.IdSolicitacao, O.IdOrdem, O.NomePortfolio, O.AtivoNome, O.TipoMov, O.Quantidade, O.Financeiro, O.IdStatusOrdem, P.Nome as StatusOrdem, O.IdSysDestino, PSD.Nome as SistemaDestino, O.IdStatusQuando, O.Acao, O.Observacao
                    		, B.IdRealocFundos, B.AtivoNome as AtivoBoleta, B.DataMov, B.DataCot, B.DataFin, B.StatusCRM, B.StatusCrmQuando
                    		, B1.AtivoNome as AtivoBoleta2, B1.QtdeExec, B1.PrecoMd, B1.Valor, B1.TaxaMd
                    		, Sec.IdTrade, Sec.NomeProduto, Sec.DtLimiteDem, Sec.DataNeg, Sec.ExportFinanceiro, Sec.QtdeDem, Sec.QtdeRateio, Sec.QtdeExecutadaExt as QtdeExecTD, Sec.StatusCRM as SecStatusCRM, Sec.StatusCrmQuando as SecStatusCRMQuando                    		
                    FROM	BL_Ordem O WITH (NOLOCK) INNER JOIN BL_CamposAdicionais A WITH (NOLOCK) ON O.IdOrdem=A.IdOrdem
                    		INNER JOIN BL_Propriedades P WITH (NOLOCK) ON O.IdStatusOrdem=P.IdCampo
                    		INNER JOIN BL_Propriedades PSD WITH (NOLOCK) ON O.IdSysDestino=PSD.IdCampo
                    		INNER JOIN (SELECT S.*, A.quem as ResponsavelUsuarioU FROM SO_Solicitacoes S WITH (NOLOCK) INNER JOIN SO_SolAux A WITH (NOLOCK) ON S.IdSolicitacao=A.IdSolicitacao AND A.IdCampo=15) S ON CAST(A.ValorStr as INT)=S.IdSolicitacao
                    		LEFT JOIN (SELECT DISTINCT GuidSuperCarteira, Cad.Officer, Cad.OfficerUsuarioU, Cad.Controller, Cad.ControllerUsuarioU, Cad.ControllerBKP, Cad.ControllerBKPUsuarioU, Cad.DPM, Cad.DPMUsuarioU, Cad.DeputyDPM, Cad.DeputyDPMUsuarioU FROM PO_Cadastro Cad WITH (NOLOCK)) Cad ON S.GuidSupercarteira=Cad.GuidSuperCarteira
                    		LEFT JOIN (SELECT * FROM BL_PreBoletas B WITH (NOLOCK) WHERE B.Deletado=0 AND B.PreBoleta=0 AND B.Execucao=0 AND NOT B.IdOrdem IS NULL) B ON O.IdOrdem=B.IdOrdem AND O.IdSysDestino=17
                    		LEFT JOIN (SELECT IdOrdem, AtivoNome, Sum(Quantidade) as QtdeExec, SUM(Quantidade * Preco) as Valor, SUM(Quantidade * Preco)/SUM(Quantidade) as PrecoMd, SUM(Quantidade * B1.Coupom)/SUM(Quantidade) as TaxaMd FROM BL_PreBoletas B1 WITH (NOLOCK) WHERE B1.Deletado=0 AND B1.PreBoleta=0 AND B1.Execucao<>0 AND NOT B1.IdOrdem IS NULL GROUP BY IdOrdem, AtivoNome) B1 ON O.IdOrdem=B1.IdOrdem AND O.IdSysDestino=18
                    		LEFT JOIN (
                    			Select Buy.IdOrdem, S.IdTrade, S.NomeProduto, S.DtLimiteDem, S.DataNeg, S.ExportFinanceiro, Buy.QtdeDem, Buy.QtdeRateio, Buy.QtdeExecutadaExt, Buy.Exportado, B2.StatusCRM, B2.StatusCrmQuando
                    			FROM PR_Sec_Buyer Buy WITH (NOLOCK) INNER JOIN PR_Secundario S WITH (NOLOCK) ON Buy.IdTrade=S.IdTrade 
                    				 LEFT JOIN BL_PreBoletas B2 WITH (NOLOCK) ON Buy.IdComprador=B2.SecIdComprador
                    			WHERE NOT Buy.IdOrdem IS NULL
                    			) Sec ON O.IdOrdem=Sec.IdOrdem AND (O.IdSysDestino=69 OR O.IdSysDestino=19)
                    WHERE S.DataPedido BETWEEN {data_ini_str} AND {data_fin_str} AND O.IdStatusOrdem<>15 AND A.IdCampo=62 AND A.ValorStr<>'0' {filtro_usuario}
                    ORDER BY IdSolicitacao DESC, IdSysDestino, B.DataMov, O.AtivoNome
                  """
        df = self.banco.dataframe(codsql)
        if df.empty:
            return df_base
        
        df.set_index('IdSolicitacao', inplace=True)
        resultado = pd.merge(left=df_base, left_on='IdSolicitacao', right=df, right_index=True, how='left')
        return resultado
    
    def solicitacoes_aportes_resgates(self, ultimos_n_dias:int=7) -> pd.DataFrame:
        codsql = f"""SELECT S.IdSolicitacao, S.DataPedido, S.NomeSupercarteira, LOWER(CAST(S.GuidSolicitacao as VARCHAR(255))) as GuidSolicitacao, C.NomeCampo, C.IdCampo, A.ValorStr, A.ValorFlt, A.ValorDt, A.ValorTxt, A.ValorBool
                    FROM SO_Solicitacoes S WITH (NOLOCK)
                    	 INNER JOIN SO_SolAux A WITH (NOLOCK) ON S.IdSolicitacao=A.IdSolicitacao
                    	 INNER JOIN SO_Campos C WITH (NOLOCK) ON A.IdCampo=C.IdCampo
                    WHERE S.IdTipoSolicitacao IN (7,8) AND S.DataPedido>=GETDATE()-{ultimos_n_dias} AND A.IdCampo IN (1,2,28,26,27,15,16,17)
                    ORDER BY S.IdSolicitacao
                  """
        df = self.banco.dataframe(codsql)
        df['DataPedido'] = pd.to_datetime(df['DataPedido'])
        df['ValorDt'] = pd.to_datetime(df['ValorDt'])
        return df
        
    
class PosicaoAdministrador:
    """
    Classe criada para intemediar interações com a base BAWM (BAWM), no servidor Aux,
    tratando especificamente de coisas ligadas à posição importada dos XMLs dos administradores.
    
    Ela lida com as tabelas cujo nome começa com AD_* 
    """
    def __init__(self, homologacao=False):
        if homologacao:
            self.banco = BaseSQL(nome_servidor=r'SQLSVRHOM1.GPS.BR\SQL12', nome_banco='BAWM_HOM1')
        else:
            self.banco = BaseSQL(nome_servidor=ambiente.servidor_aux(), nome_banco='BAWM')
    
    def posicao_ultima_data(self, codigo_adm, data_pos=None):
        data_str = self.banco.sql_data(data_pos)
        codsql = f"SELECT MAX(DataFonte) DT FROM AD_Carteira WITH (NOLOCK) WHERE DataFonte<={data_str} AND CodigoAdm='{codigo_adm}'"
        data_adm = self.banco.busca_valor_codsql(codsql=codsql, campo='DT')
        return data_adm
    
    def posicao(self, codigo_adm, data_pos=None):        
        data_adm = self.posicao_ultima_data(codigo_adm=codigo_adm, data_pos=data_pos)
        data_str = self.banco.sql_data(data_adm)
        
        # Busca a posição
        codsql = f"SELECT * FROM AD_Carteira WITH (NOLOCK) WHERE DataFonte={data_str} AND CodigoAdm='{codigo_adm}'"
        return self.banco.dataframe(codsql)
    
    def dado_cadastro_conta(self, campo='GuidConta', nome_conta=None, guid_conta=None):
        tabela='AD_Cadastro'
        if nome_conta:
            filtro = f"NomeContaCRM={self.banco.sql_texto(nome_conta)}"
        if guid_conta:
            filtro = f"GuidConta='{guid_conta}'"
        return self.banco.busca_valor(tabela=tabela, filtro=filtro, campo=campo)

    def cadastro_fundos(self, lista_campos=[], filtro=None):
        if len(lista_campos) == 0:
            campos = ['GuidConta', 'NomeContaCRM', 'IdFundo', 'CodigoAdm']
        else:
            campos = lista_campos
        df = self.banco.busca_tabela(tabela='AD_Cadastro', filtro=filtro, lista_campos=campos, filtro_sec="Ativo<>0", no_lock=True)        
            
        return df


class Liquidez:
    """
    Classe criada para interagir com as tabelas de Controle de liquidez na 
    base BAWM (Servidor: SQLAUX)
    Os nomes das tabelas tem o padrão LQ_* 
    """
    def __init__(self, buffer:bool=False, homologacao:bool=False):
        if homologacao:
            self.banco = BaseSQL(nome_servidor=r'SQLSVRHOM1.GPS.BR\SQL12', nome_banco='BAWM_HOM1')
        else:
            self.banco = BaseSQL(nome_servidor=ambiente.servidor_aux(), nome_banco='BAWM')
        
        # Variáveis gerais
        self.dm1 = PosicaoDm1(homologacao=homologacao)        
        self.buffer = buffer
        # Variáveis upload
        self.id_tipo_veic = None
        self.portfolio_guid = None
        self.portfolio_nome = None
        self.data_pos = None
        self.dict_upload = []
        
        # buffer
        if not 'buffer_df_campos_set' in self.__dict__:
            self.buffer_df_campos_set = False
        if self.buffer_df_campos_set:
            self.df_campos = self.buffer_df_campos
        else:
            self.buffer_df_campos = self.banco.dataframe('SELECT * FROM LQ_Campos WITH (NOLOCK)').set_index('IdCampo')
            self.buffer_df_campos_set = True
            self.df_campos = self.buffer_df_campos
        
        self.buffer_liquidezport  = pd.DataFrame()
        self.buffer_liquidezport_set = False
        
    def liquidez_cadastro(self):        
        df = self.dm1.liquidez_cadastro()
        return df
    
    def liquidez_solicitada_portfolio(self, portfolio_guid:str=None, portfolio_nome:str=None, data=None, ativo:bool=True) -> pd.DataFrame:
        """
        Busca a Liquidez Solicitada da tabela LQ_Solicitacao.
        Pode filtrar por portfolio_guid ou portfolio_nome
        Traz apenas linhas sem Data de Expiração ou que expiram após a data informada. Se não for informada data, usa hoje.
        Sempre filtra por deletado = 0.

        Parameters
        ----------
        portfolio_guid : str, optional
            DESCRIPTION. The default is None.
        portfolio_nome : str, optional
            DESCRIPTION. The default is None.
        data : TYPE, optional
            DESCRIPTION. The default is None.
        ativo : bool, optional
            DESCRIPTION. The default is True.

        Returns
        -------
        df : pd.DataFrame
            DataFrame com os dados da tabela.

        """
        data_str = self.banco.sql_data(data)
        filtro_port = ""
        if portfolio_guid:
            filtro_port = f"AND S.PortfolioGuid='{portfolio_guid}'"
        elif portfolio_nome:
            filtro_port = f"AND S.PortfolioNome={self.banco.sql_texto(portfolio_nome)}"
        
        filtro_data = f"AND (DataExpiracao IS NULL OR DataExpiracao>={data_str})"
        
        codsql = f"""SELECT DataSolicitacao, PortfolioNome, PortfolioGuid, IdCampoVeiculo, Financeiro, PercPL, IdCampoTipoPedido, NomeCampo as TipoPedido, DataRequerida, DataExpiracao, 
                            Recorrente, RegraPagamento, TitularNome, TitularGuid, ContaDestinoGuid, ContaDestinoNome, MotivoBoleta
                     FROM LQ_Solicitacao S WITH(NOLOCK) INNER JOIN LQ_Campos C WITH (NOLOCK) ON S.IdCampoTipoPedido=C.IdCampo
                     WHERE S.Deletado=0 {filtro_port} {filtro_data}
                     ORDER BY PortfolioNome, IdCampoTipoPedido, DataExpiracao
                  """
        df = self.banco.dataframe(codsql)
        return df        

    def busca_liquidez_port(self, portfolio_guid=None, portfolio_nome=None, data_pos=None, liquidez=None):
        if not self.buffer:
            return self.__busca_liquidez_port_db__(portfolio_guid=portfolio_guid, portfolio_nome=portfolio_nome, data_pos=data_pos,liquidez=liquidez)
        else:
            if not self.buffer_liquidezport_set:
                data_str = self.dm1.data_posicao_str(data_pos)
                self.buffer_liquidezport = self.banco.dataframe(f"EXEC LQ_BuscaPortfolios {data_str}")
                self.buffer_liquidezport_set = True
            df = self.buffer_liquidezport.copy()
            if portfolio_guid:
                df = df[df['PortfolioGuid']==portfolio_guid]
            elif portfolio_nome:
                df = df[df['PortfolioNome']==portfolio_nome]
            else:
                raise Exception('databases\Liquidez\busca_liquidez_port: função não recebeu parâmetros')
            
            if liquidez == None:
                pass
            else:
                if liquidez:
                    df = df[df['ValorStr'].isnull()]
                else:
                    df = df[df['ValorStr'].notnull()]            
            return df
    
    def __busca_liquidez_port_db__(self, portfolio_guid=None, portfolio_nome=None, data_pos=None, liquidez=None):
        # 1. Identificação do portfólio
        if portfolio_guid:
            filtro = f"@PortfolioGuid='{portfolio_guid}'"
        elif portfolio_nome:
            filtro = f"@PortfolioNome={self.banco.sql_texto(portfolio_nome)}"
        else:
            raise Exception('databases\Liquidez\busca_liquidez_port: função não recebeu parâmetros')
        # 2. Data a buscar
        data_str = self.dm1.data_posicao_str(data_pos)        
        
        # 3. Critério de liquidez a filtrar
        if liquidez == None:
            filtro2 = ''
        else:
            if liquidez:
                filtro2 = "@Liquidez=1"
            else:
                filtro2 = "@Liquidez=0"            
        
        # 4. Busca os dados
        codsql = f"EXEC LQ_BuscaPortfolios @DataArquivo={data_str}, {filtro}, {filtro2}"
        return self.banco.dataframe(codsql=codsql)
    
    def upload_dataframe(self, df):
        resultado = self.banco.com_edit_or_add_df(tabela='LQ_LiquidezPort', dados=df, 
                                campos_filtros=['IdCampoVeiculo', 'DataArquivo', 'PortfolioGuid', 'IdCampo'],
                                colunas_comparar=['ValorStr', 'ValorFlt', 'ValorBool'])
        return resultado
    
    def portfolio_upload_ini(self, id_tipo_veic, portfolio_guid, portfolio_nome, data_pos=None):
        self.id_tipo_veic = id_tipo_veic
        self.portfolio_guid = portfolio_guid
        self.portfolio_nome = portfolio_nome
        self.data_pos = data_pos
        self.dict_upload = []  # Zera os dados
        
    def portfolio_upload_add_campo(self, id_campo, valor, valor_texto_add=None):
        dicio = {'IdCampoVeiculo': self.id_tipo_veic, 'DataArquivo': self.data_pos,
                 'PortfolioNome': self.portfolio_nome, 'PortfolioGuid': self.portfolio_guid,
                 'IdCampo': id_campo, self.df_campos.loc[id_campo, 'TipoValor']: valor}
        if valor_texto_add:
            if self.df_campos.loc[id_campo, 'TipoValor'] != 'ValorStr':
                dicio['ValorStr'] = valor_texto_add
            
        self.dict_upload.append(dicio)
    
    def portfolio_upload_ver_df(self):
        df = pd.DataFrame(self.dict_upload)
        return df
    
    def portfolio_upload_executar(self):
        df = self.portfolio_upload_ver_df()
        resultado = self.banco.com_edit_or_add_df(tabela='LQ_LiquidezPort', dados=df, 
                                campos_filtros=['IdCampoVeiculo', 'DataArquivo', 'PortfolioGuid', 'IdCampo'],
                                colunas_comparar=['ValorStr', 'ValorFlt', 'ValorBool'])
        return resultado
    
    def portfolio_upload_enquadramento(self, df_dados, id_campo, nome_coluna):
        if df_dados.empty:
            return df_dados
        if not nome_coluna in df_dados.columns:
            return df_dados
        colunas = ['ValorStr', 'ValorInt', 'ValorFlt']
        df = df_dados[[nome_coluna]].reset_index().copy()
        df.columns = colunas
        df.insert(0, 'IdCampoVeiculo', [self.id_tipo_veic] * len(df))
        df.insert(1, 'PortfolioNome', [self.portfolio_nome] * len(df))
        df.insert(2, 'PortfolioGuid', [self.portfolio_guid] * len(df))
        df.insert(3, 'DataArquivo', [self.data_pos] * len(df))
        df.insert(4, 'IdCampo', [id_campo] * len(df))
        
        resultado = self.banco.com_edit_or_add_df(tabela='LQ_LiquidezPort', dados=df, 
                                campos_filtros=['IdCampoVeiculo', 'DataArquivo', 'PortfolioGuid', 'IdCampo', 'ValorStr', 'ValorInt'],
                                colunas_comparar=['ValorFlt'])
        return resultado
        
    def busca_liquidez(self, data_pos=None, tipo_veiculo=None, portfolio_guid=None, portfolio_nome=None, liquidez=None):
        data_str = self.dm1.data_posicao_str(data_pos=data_pos)
        codsql = f"EXEC LQ_BuscaPortfolios {data_str}"
        if tipo_veiculo:
            codsql = f"{codsql}, @TipoVeiculo={tipo_veiculo}"
        if portfolio_guid:
            codsql = f"{codsql}, @PortfolioGuid={portfolio_guid}"
        if portfolio_nome:
            codsql = f"{codsql}, @PortfolioNome={portfolio_nome}"
        if not liquidez is None:
            if liquidez:
                txt = 1
            else:
                txt = 0
            codsql = f"{codsql}, @Liquidez={txt}"
        
        return self.banco.dataframe(codsql=codsql)
    
    def edita_sol_liquidez_operacional(self, df_lq_op):
        return self.banco.com_edit_or_add_df(tabela='LQ_Solicitacao', dados=df_lq_op, campos_filtros=['IdSolicitacao'], 
                                       colunas_comparar=['IdTipoCampo','Financeiro','PercPL','DataExpiracao','MotivoBoleta']) 

    def deleta_sol_liquidez_operacional(self, id_sol):
        quem_quando = self.banco.get_user_and_date()
        user = quem_quando[0]
        quando = quem_quando[1]
        df = pd.DataFrame({'IdSolicitacao':[id_sol], 'deletado':[True], 'quemdel':[user], 'quandodel':[quando]})
        return self.banco.com_edit_or_add_df(tabela='LQ_Solicitacao', dados=df, campos_filtros=['IdSolicitacao'], 
                                       colunas_comparar=['deletado']) 
    
    def cadastra_sol_liquidez_operacional(self,portfolio_guid=None, portfolio_nome=None, financeiro=None, perc_pl=None, 
                                          id_campo_tipo_pedido=18, data_expiracao=None, data=datetime.datetime.now(),
                                       id_campo_veiculo=2, recorrente=0, motivo_boleta=None):
        '''
        Função para cadastrar liquidez operacional do cliente.
        '''

        dm1 = PosicaoDm1Pickle(homologacao=False)

        if portfolio_guid==None and portfolio_nome==None:
            raise Exception('É necessário especificar o guid do portfolio ou nome do portfolio')
        else:
            if id_campo_veiculo == 3:
                if portfolio_nome:
                    portfolio_nome = portfolio_nome
                    portfolio_guid = dm1.dado_cadastro_titularidade(campo='TitularidadeGuid', titularidade=portfolio_nome)            
                elif portfolio_guid:
                    portfolio_guid = portfolio_guid
                    self.nome = dm1.dado_cadastro_titularidade(campo='Titularidade', guid_titularidade=portfolio_guid)
            #se for fundos também é pela supercarteira ao invés de cadastrar liquidez operacional no produto
            elif id_campo_veiculo == 2 or id_campo_veiculo == 1:
                if portfolio_nome:
                    portfolio_nome = portfolio_nome
                    portfolio_guid = dm1.dado_cadastro_sc(campo='GuidSuperCarteira', nome_sc=portfolio_nome)         
                elif portfolio_guid:
                    portfolio_guid = portfolio_guid
                    self.nome = dm1.dado_cadastro_sc(campo='NomeSuperCarteiraCRM', guid_sc=portfolio_guid)
           

        if financeiro == None and perc_pl == None:
            raise Exception('É necessário especificar o financeiro ou percentual do PL da liquidez operacional solicitada')
        elif financeiro != None and perc_pl != None:
            raise Exception('Não se pode usar o financeiro e percentual do PL para cadastrar')

        df = pd.DataFrame({
            'PortfolioGuid':[portfolio_guid],
            'PortfolioNome':[portfolio_nome],
            'IdCampoVeiculo':[id_campo_veiculo],
            'IdCampoTipoPedido':[id_campo_tipo_pedido],
            'Recorrente':[recorrente],
            'DataSolicitacao':[data],

        })

        colunas_none = {'DataExpiracao':data_expiracao,'Financeiro':financeiro,'PercPL':perc_pl, 'MotivoBoleta':motivo_boleta}

        for key in colunas_none.keys():
            if colunas_none[key] != None:
                df[key] = colunas_none[key]

        resultado = self.banco.com_add_df(
            tabela='LQ_Solicitacao',
            df=df,
        )

        return resultado
    
    def verificar_sol_liquidez_operacional(self, id_tipo_portfolio=None,  portfolio_guid=None, portfolio_nome=None):
        if portfolio_guid != None or portfolio_nome != None:
            dm1 = PosicaoDm1Pickle(homologacao=False)

            if id_tipo_portfolio == 3:
                if portfolio_nome:
                    portfolio_nome = portfolio_nome
                    portfolio_guid = dm1.dado_cadastro_titularidade(campo='TitularidadeGuid', titularidade=portfolio_nome)            
                elif portfolio_guid:
                    portfolio_guid = portfolio_guid
                    self.nome = dm1.dado_cadastro_titularidade(campo='Titularidade', guid_titularidade=portfolio_guid)
                #se for fundos também é pela supercarteira ao invés de cadastrar liquidez operacional no produto
            elif id_tipo_portfolio == 2 or id_tipo_portfolio == 1:
                if portfolio_nome:
                    portfolio_nome = portfolio_nome
                    portfolio_guid = dm1.dado_cadastro_sc(campo='GuidSuperCarteira', nome_sc=portfolio_nome) 

                codsql = f"select IdSolicitacao, IdCampoVeiculo, PortfolioGuid, PortfolioNome, Financeiro, PercPL, DataExpiracao, MotivoBoleta from LQ_Solicitacao where PortfolioGuid='{portfolio_guid}' and deletado=0"

            else:
                if portfolio_nome:
                    codsql = f"select IdSolicitacao, IdCampoVeiculo, PortfolioGuid, PortfolioNome, Financeiro, PercPL, DataExpiracao, MotivoBoleta from LQ_Solicitacao where PortfolioNome='{portfolio_nome}' and deletado=0"
                elif portfolio_guid:
                    codsql = f"select IdSolicitacao, IdCampoVeiculo, PortfolioGuid, PortfolioNome, Financeiro, PercPL, DataExpiracao, MotivoBoleta from LQ_Solicitacao where PortfolioGuid='{portfolio_guid}' and deletado=0"


        else:
            codsql = f"select IdSolicitacao, IdCampoVeiculo, PortfolioGuid, PortfolioNome, Financeiro, PercPL, DataExpiracao, MotivoBoleta from LQ_Solicitacao where deletado=0"

        df = self.banco.dataframe(codsql)

        return df   

class Reliance_Radar:
    """
    Classe para consultar dados da esteira Reliance.
    """
    def __init__(self, homologacao=False):
        self.homologacao = homologacao
        self.banco = BaseSQL(nome_servidor='EUROPA.GPS.BR', nome_banco='RADAR2')
        self.crm_db_nome = 'RADAR2'

    def fundos_ativos(self):
        codsql = """
                Select Ativo,ISIN,FundoCNPJ from Ativo where AtivoTpID = 3 and FundoStatus =1
                 """
        return self.banco.dataframe(codsql=codsql)        
    
    def parametros_nelson_siegel(self, yield_id:int=1, data=None):
        if data is None:
            filt_dt = ''
        else:
            try:
                data_str = pd.to_datetime(data).strftime('%Y-%m-%d')
            except:
                raise ValueError("'data' deve ser string com formato %Y-%m-%d")
            filt_dt = f"AND Data >= '{data_str}'"
        
        codsql = f"""
                SELECT 
                    Data, beta0, beta1, beta2, beta3, tau1, tau2
                FROM YieldNSS
                WHERE YieldID={yield_id} AND cenarioID=0 {filt_dt}
                ORDER BY Data
                 """
        return self.banco.dataframe(codsql=codsql)   
    
class Reliance_Rel:
    """
    Classe para consultar dados da esteira Reliance.
    """
    def __init__(self, homologacao=False):
        self.homologacao = homologacao
        self.banco = BaseSQL(nome_servidor='EUROPA.GPS.BR', nome_banco='RELIANCE')
        self.crm_db_nome = 'RELIANCE'

    def contas_movimentos(self):
        codsql = """
                Select Ativo,ISIN,FundoCNPJ from Ativo where AtivoTpID = 3 and FundoStatus =1
                 """
        return self.banco.dataframe(codsql=codsql)      
               
class ArquivosPassivo:
    """
    Classe para resgatar dados parciais de movimentação de passivo dos produtos em D0 a partir 
    dos txts gerados em rede.
    """
    
    def __init__(self):
        self.pasta = '//filesvr.gps.br/Aplicativos/PRODUCAO/MovimentacaoDePassivos/RelatorioIntraDay/'
    
    def obter_passivo_data(self, data= None):
        
        if data is None:
            data = datetime.today()
        dfmt = pd.to_datetime(data)
        
        #definir pasta equivalente
        subpasta = str(dfmt.year) + '_' + ('0'+str(dfmt.month))[-2:]
        
        #definir arquivo(s) correto a ser lido
        lt_arq = glob(self.pasta+subpasta+'/*')
        lt_read = [arq for arq in lt_arq if f"ReferenteA_{dfmt.strftime('%Y%m%d')}" in arq]
        
        if len(lt_read) == 0:
            return None
        
        lt_todos = []
        for arq in lt_read:
            sub = pd.read_csv(arq, sep=';')
            lt_todos.append(sub)
            
        df = pd.concat(lt_todos)
        
        #ajustes de base consolidada 
        df = df[~df['Numero'].duplicated(keep= 'last')]
        df = df.loc[~df['StatusDaBoleta'].isin(['Devolvida','Cancelada'])]
        df = df.reset_index(drop= True)
        
        return df


class GameBoyDB:
    """
    Classe criada para interagir com as tabelas de Posição alvo do Gameboy na 
    base BAWM (Servidor: SQLAUX)
    Os nomes das tabelas tem o padrão GB_* 
    """
    def __init__(self, homologacao=False):
        if homologacao:
            self.banco = BaseSQL(nome_servidor=r'SQLSVRHOM1.GPS.BR\SQL12', nome_banco='BAWM_HOM1')
        else:
            self.banco = BaseSQL(nome_servidor=ambiente.servidor_aux(), nome_banco='BAWM')
        self.dm1 = PosicaoDm1(homologacao=homologacao)
        data_pos_dm1 = self.dm1.data_posicao_str()
        
        codsql = f"SELECT DISTINCT GuidProduto, NomeProduto FROM PO_Carteira C WITH (NOLOCK) WHERE DataArquivo={data_pos_dm1}"
        self.produtos = self.banco.dataframe(codsql=codsql).set_index('GuidProduto')['NomeProduto'].to_dict()
        
        codsql = f"""SELECT DISTINCT D.GuidContaCRM as Indice, D.GuidContaCRM, D.CodigoProduto, D.NomeContaCRM, A.AplicPedido as AplicPed, A.AplicCot, A.AplicFin, A.ResgPedido as ResgPed, A.ResgCot, A.ResgFin
                    FROM PO_Carteira C WITH (NOLOCK) INNER JOIN PO_Cadastro D WITH (NOLOCK) ON C.IdProdutoProfitSGI=D.CodigoProduto
                    	 INNER JOIN PO_Ativos A WITH (NOLOCK) ON C.GuidProduto=A.GuidAtivo
                     WHERE C.DataArquivo={data_pos_dm1}
                     ORDER BY NomeContaCRM"""
        self.fundos = self.banco.dataframe(codsql=codsql).set_index('Indice')
    
    def targets_busca_ativos(self) -> pd.DataFrame:
        codsql = 'SELECT IdTarget, NomeTarget, IdTipoVeiculo, GuidPortfolio, DataInicio, DataFinal FROM GB_Target WITH (NOLOCK) WHERE Deletado=0 AND DataInicio<=GETDATE() AND (DataFinal IS NULL OR DataFinal <=GETDATE()) ORDER BY IdTipoVeiculo, NomeTarget'
        return self.banco.dataframe(codsql=codsql)
    
    def target_busca_guid_portfolio(self, guid_portfolio:str, data_pos=None) -> dict:
        id_target = self.banco.busca_valor(tabela='GB_Target', filtro=f"Deletado=0 AND GuidPortfolio='{guid_portfolio}'", campo='IdTarget')
        dicionario = self.target_busca_id_target(id_target=id_target, data_pos=data_pos)
        fundo = self.fundos.loc[guid_portfolio]
        dicionario['Cadastro'] = fundo
        return dicionario
    
    def target_busca_id_target(self, id_target:int, data_pos=None) -> dict:
        data_str = self.banco.sql_data(data_pos)            
        codsql = f'SELECT MAX(DataPort) as DT FROM GB_Posicao WITH (NOLOCK) WHERE Deletado=0 AND DataPort<={data_str}'
        data_alvo = self.banco.busca_valor_codsql(codsql=codsql, campo='DT')
        return self.target_busca_idtarget_datapos(id_target=id_target, data_pos=data_alvo)
                        
    def target_busca_idtarget_datapos(self, id_target:int, data_pos) -> dict:    
        """
        Busca o portfólio e o overlay alvo para um IdTarget e Data Posição.
        Faz a explosão se dentro do Portfolio houver outro IdTarget

        Parameters
        ----------
        id_target : int
            IdTarget, com base no cadastro da tabela GB_Target.
        data_pos : data
            Data da Posição / Overlay.

        Returns
        -------
        Dict: Retorna dicionario com dataframes de overlay e posição

        """
        data_str = self.banco.sql_data(data_pos)                    
        codsql = f"""SELECT G.* 
                FROM GB_Posicao G WITH (NOLOCK)
                WHERE G.Deletado=0 AND G.IdTarget={id_target} AND G.DataPort={data_str}
                """                
        codsql2 = f'SELECT * FROM GB_Overlay WITH (NOLOCK) WHERE IdTarget={id_target} AND DataPort={data_str}'  #TODO Deletado=0 AND 
        df = self.banco.dataframe(codsql)
        if not df.empty:      
            df.insert(4, 'NomeProduto', df['PosGuidProduto'])
            df['NomeProduto'] = df['NomeProduto'].map(self.produtos)
            if 'PosGuidProduto' in df.columns:
                df.rename(columns = {'PosGuidProduto': 'GuidProduto'}, inplace=True)
            
        df_subtgt = df[df['PosTargetId'].notnull()]
        if not df_subtgt.empty:
            df = df[~df['PosTargetId'].notnull()]
            res_pos = []
            res_ovl = []
            for idx, row in df_subtgt.iterrows():
                dicio = self.target_busca_id_target(row['PosTargetId'], data_pos=data_pos)
                s_posicao = dicio['Posicao']
                if not s_posicao.empty:
                    s_posicao['PesoAlvo'] = s_posicao['PesoAlvo'] * row['PesoAlvo'] # Faz a explosão
                    res_pos.append(s_posicao)
                    
                s_overlay = dicio['Overlay']
                if not s_overlay.empty:
                    s_overlay['PesoAlvo'] = s_overlay['PercAlvo'] * row['PesoAlvo'] # Faz a explosão
                    res_ovl.append(s_overlay)
            
            if len(res_pos) > 0:
                res_pos = pd.concat(res_pos).reset_index()
                res_pos.insert(0, 'Adicionar', [1] * len(res_pos))
                for idx, row in res_pos.iterrows():
                    if row['GuidProduto']:
                        if row['GuidProduto'] in df['GuidProduto'].to_list():
                           indice = df[df['GuidProduto']==row['GuidProduto']].index
                           df.loc[indice, 'PesoAlvo'] += row['PesoAlvo']
                           res_pos.loc[idx, 'Adicionar'] = 0
                    # Não compara posições de futuros ou títulos públicos, simplesmente agrupa, eles serão escolhidos no final
                res_pos = res_pos[res_pos['Adicionar']==1]
                df = pd.concat([df, res_pos])
        
        # Não faz explosão do overlay por hora                    
        df2 = self.banco.dataframe(codsql2)
        
        # Retorna
        dicionario = {'Posicao': df, 'Overlay': df2}
        return dicionario
    

class off_Reliance:
    
    """
    Classe criada para consultas na base offshore esteira Reliance, todas as consultas estarão nessa classe.
    carteira_reliance - puxa toda posição que foi processada no sistema internacional
    produtos_off - puxa todos os produtos da base e suas respectivas classificações e categorias
    posicao_classe - puxa a posição por cada classe de ativo
    rentabilidade - traz a rentabilidade em diversas janelas, juntamente com o saldo total e o ganho do periodo(mes)
    
    """
    
    def __init__(self,base):
        if base =='Primeny':
            self.banco = BaseSQL(nome_servidor='europa.gps.br\internacional', nome_banco='Primeny')
        else:
            self.banco = BaseSQL(nome_servidor='europa.gps.br\internacional', nome_banco='CUBO')     

    def carteira_reliance(self, dia):
        data_str = self.banco.sql_data(dia)            
        codsql = f'''SELECT posicao.calculado_em, posicao.dt_posicao as Data_Posicao, cliente.nome_reduzido as Nome_Cliente,posicao.cd_security,ativo.ISIN,ativo.security as Nome_produto,classe.asset_class as Classe,classe_class3.asset_class,categoria.Subcategoria,posicao.estoque as quantidade, posicao.fechamento as PU,cotacao.cotacao,posicao.estoque*posicao.fechamento*cotacao.cotacao as 'SaldoNaData',moeda.moeda
                  FROM TbPosicaoAtivo as posicao inner join tb_account as conta on conta.cd_account=posicao.cd_account inner join tb_cliente_detalhe as cliente on conta.cd_cliente=cliente.cd_cliente 
                  inner join tb_security as ativo on posicao.cd_security=ativo.cd_security
                  inner join tb_asset_class as classe on ativo.cd_asset_class = classe.cd_asset_class
				  inner join tb_asset_class as classe_class3 on ativo.CdAssetClass3 = classe_class3.cd_asset_class
                  inner join tb_moeda as moeda on moeda.cd_moeda = ativo.cd_moeda_security
				  left join TbSubcategoria_Security as ref on ativo.cd_security=ref.cd_security
				  left join TbSubcategoria as categoria on categoria.CdSubcategoria=ref.CdSubcategoria
				  left join tb_moeda_cotacao_emdolar as cotacao on cotacao.cd_moeda=moeda.cd_moeda and cotacao.dt_cotacao=posicao.dt_posicao
                  where posicao.dt_posicao = {data_str}
                  order by posicao.calculado_em'''
                  

        return self.banco.dataframe(codsql=codsql)
    
    def ultima_carteira_validada(self, sc_gestão):
        
        cd_cliente = f'''select cd_cliente from tb_cliente_detalhe where nome_reduzido = '{sc_gestão}' '''
        cd_cliente= self.banco.dataframe(cd_cliente)
        cd_cliente = cd_cliente.iat[0,0]
        cd_cliente = int(cd_cliente)
        cod_sql_1 = f'''select mes_consulta,ano_consulta  from TbConsultaFinalizada where editado_em = (SELECT MAX(editado_em) AS max_data from TbConsultaFinalizada 						 
						 where cd_cliente = {cd_cliente}) and cd_cliente = {cd_cliente}'''
        dia =  self.banco.dataframe(cod_sql_1)
        ano = dia.iat[0,0]
        mes = dia.iat[0,1]
        dia = pd.datetime(mes,ano,1)
        dia = (dia  + relativedelta(months=1)).replace(day=1) - timedelta(days=1)
        data_str = self.banco.sql_data(dia)        
        codsql = f'''set nocount on;IF OBJECT_ID('tempdb..#temp') is not null 
        DROP table #temp 
        CREATE TABLE #temp(Data_Posicao date,
        NomeContaCrm varchar(255),cd_security int,
        NomeDoProduto varchar(255),Rating varchar(255),
        Moeda varchar(255),Classe varchar(255),Subclasse varchar(255),
        Quantidade float,Financeiro float,
        calculado_em datetime)
        Insert into #temp 
        Select posicao.dt_posicao as Data_Posicao,cliente.nome_reduzido as NomeContaCrm,posicao.cd_security,ativo.security as NomeDoProduto,'Sem Rating' as Rating,moeda.moeda as Moeda,classe_class3.asset_class as Classe,classe.asset_class as Subclasse,posicao.estoque as Quantidade,(posicao.estoque*posicao.fechamento*cotacao.cotacao) as Financeiro,convert(datetime,max(posicao.calculado_em))
        FROM TbPosicaoAtivo as posicao 
        inner join tb_account as conta on conta.cd_account=posicao.cd_account 
        inner join tb_cliente_detalhe as cliente on conta.cd_cliente=cliente.cd_cliente  
        inner join tb_security as ativo on posicao.cd_security=ativo.cd_security 
        inner join tb_asset_class as classe_class3 on ativo.CdAssetClass3 = classe_class3.cd_asset_class 
        inner join tb_asset_class as classe on ativo.cd_asset_class = classe.cd_asset_class 
        inner join tb_moeda as moeda on moeda.cd_moeda = ativo.cd_moeda_security 
        left join TbSubcategoria_Security as ref on ativo.cd_security=ref.cd_security 
        left join TbSubcategoria as categoria on categoria.CdSubcategoria=ref.CdSubcategoria 
        left join tb_moeda_cotacao_emdolar as cotacao on cotacao.cd_moeda=moeda.cd_moeda and cotacao.dt_cotacao=posicao.dt_posicao where posicao.dt_posicao = {data_str} and cliente.cd_cliente= {cd_cliente} and classe.asset_class <>'Outros Investimentos' 
        Group by posicao.dt_posicao, cliente.nome_reduzido, posicao.cd_security, ativo.security, moeda.moeda,classe_class3.asset_class,classe.asset_class,posicao.estoque,posicao.estoque*posicao.fechamento*cotacao.cotacao
        set nocount on;
        IF OBJECT_ID('tempdb..#temp_saldos') is not null 
        DROP table #temp_saldos 
        CREATE TABLE #temp_saldos( 
        Data_Posicao date, 
        NomeContaCrm varchar(255), 
        cd_security int, 
        NomeDoProduto varchar(255), 
        Rating varchar(255), 
        Moeda varchar(255), 
        Classe varchar(255), 
        Subclasse varchar(255), 
        Quantidade float, 
        Financeiro float, 
        calculado_em datetime) 
        Insert into #temp_saldos select  DataPeriodo as Data_Posicao,nome_reduzido as NomeContaCrm, 1 as cd_security, 'saldo_caixa' as NomeDoProduto, 'Recomendado' as Rating, 'USD' as Moeda, 'RF CP' as Classe, 'RF CP -' as Subclasse, 
        cla.ValorMercado as Quantidade, cla.ValorMercado as Financeiro, cons.DataProcessamentoFim as calculado_em 
        from TRPT_Performance_Classe  as cla 
        inner join [dbo].[TRPT_Performance] as perf on perf.cdPerformance = cla.cdPerformance 
        inner join [dbo].[TRPT_Consulta] as cons on cons.cdConsulta = perf.cdConsulta 
        inner join tb_cliente_detalhe as cliente on cons.cdCliente=cliente.cd_cliente 
        where NomeClasse='Classe de Ativos: Investimentos de Curto Prazo'and cliente.cd_cliente= {cd_cliente}
        insert into #temp(Data_Posicao,NomeContaCrm,cd_security,NomeDoProduto,Rating,Moeda,Classe,Subclasse,Quantidade,Financeiro,calculado_em)
        select Data_Posicao,NomeContaCrm,cd_security,NomeDoProduto,Rating,Moeda,Classe,Subclasse,Quantidade,Financeiro,calculado_em
        from #temp_saldos where calculado_em  = (SELECT MAX(calculado_em) FROM #temp_saldos) Update #Temp set Financeiro = case when NomeDoProduto like '%FWD%' then 0 Else Financeiro End IF OBJECT_ID('tempdb..#somatudo') is not null DROP table #somatudo create table #somatudo (NomeContaCrm varchar(255), Peso_Carteira float) insert into #somatudo Select NomeContaCrm, Sum(Financeiro) from #temp GROUP BY NomeContaCrm IF OBJECT_ID('tempdb..#somaClasse') is not null DROP table #somaClasse create table #somaClasse (Classe varchar(255), Peso_Intra float) insert into #somaClasse Select Classe, Sum(Financeiro) from #temp GROUP BY Classe select concat('Rel_',T.cd_security) as guidproduto, T.Data_Posicao, T.NomeContaCrm, T.NomeDoProduto, T.Rating, T.Moeda, T.Classe, T.Subclasse, T.Quantidade as Quantidade, T.Financeiro, T.Financeiro/F.Peso_Carteira as Peso_Carteira, T.Financeiro/S.Peso_Intra as IntraClasse From #temp as T inner join #somaClasse as S on T.Classe = S.Classe inner join #somatudo as F on F.NomeContaCrm = T.NomeContaCrm Order by T.Classe,T.Subclasse	
        Select * from #temp'''
                                 

        return self.banco.dataframe(codsql=codsql)



    
    def produtos_isin(self):
        codsql = f'''Select distinct sec.cd_security,sec.security,sec.id_bbrg,sec.ISIN,classe.asset_class,classe_class3.asset_class,categoria.Subcategoria
                        from tb_security as sec
                        inner join tb_asset_class as classe on sec.cd_asset_class = classe.cd_asset_class
                        inner join tb_asset_class as classe_class3 on CdAssetClass3 = classe_class3.cd_asset_class
                        inner join tb_moeda as moeda on moeda.cd_moeda = classe.cd_asset_class
                        left join TbSubcategoria_Security as ref on sec.cd_security=ref.cd_security
                        left join TbSubcategoria as categoria on categoria.CdSubcategoria=ref.CdSubcategoria
                        order by sec.cd_security'''
        return self.banco.dataframe(codsql=codsql)
    
    def rentabilidade(self, dia):
        ''' Parameters
        ----------
        dia : datetime
            Usar sem o primeiro dia de cada mes.'''       
        data_str = self.banco.sql_data(dia) 
        codsql = f'''SELECT consulta.cdConsulta,nome_reduzido,consulta.DataProcessamentoFim,rentabilidade.DataPeriodo,performance.Total_ValorMercado,performance.Total_ValorMercadoAnterior,performance.Total_Ganho_Periodo,performance.PontosRisco,rentabilidade.RetornoLiquido_NoAno,RetornoLiquido_12Meses,RetornoLiquido_24Meses,RetornoLiquido_DesdeInicio,RentabilidadeLiquidaCarteira_JAN,RentabilidadeLiquidaCarteira_FEV,RentabilidadeLiquidaCarteira_MAR,RentabilidadeLiquidaCarteira_ABR,RentabilidadeLiquidaCarteira_MAI,RentabilidadeLiquidaCarteira_JUN,RentabilidadeLiquidaCarteira_JUL,RentabilidadeLiquidaCarteira_AGO,RentabilidadeLiquidaCarteira_SET,RentabilidadeLiquidaCarteira_OUT,RentabilidadeLiquidaCarteira_NOV,RentabilidadeLiquidaCarteira_DEZ
                     FROM [CUBO].[dbo].[TRPT_Consulta] as consulta
                     Inner join [CUBO].[dbo].[tb_cliente_detalhe] as cliente on cliente.cd_cliente=consulta.cdCliente
                     INNER JOIN TRPT_Consolidado as rentabilidade on consulta.cdConsulta=rentabilidade.cdConsulta
                     inner join [CUBO].[dbo].[TRPT_Performance] as performance on consulta.cdConsulta=performance.cdConsulta
                     where rentabilidade.DataPeriodo ={data_str}
                      order by DataPeriodo desc'''
        return self.banco.dataframe(codsql=codsql)
    
    def carteira_por_peso(self, dia):
        ''' Parameters
                ----------
        dia : datetime
        Usar sem o primeiro dia de cada mes.'''
            
        data_str = self.banco.sql_data(dia) 
        codsql = f'''  Select cliente.nome_reduzido,consolidado.DataPeriodo,consulta.DataProcessamentoFim,alocacao.NomeClasse,PcInicioAno
                  from TRPT_Consolidado_AlocacaoCapital as alocacao
                  inner join [CUBO].[dbo].[TRPT_Consolidado] as consolidado on consolidado.cdConsolidado=alocacao.cdConsolidado
                  inner join [CUBO].[dbo].[TRPT_Consulta] as consulta on consulta.cdConsulta=consolidado.cdConsulta
                  Inner join [CUBO].[dbo].[tb_cliente_detalhe] as cliente on cliente.cd_cliente=consulta.cdCliente
                  where consolidado.DataPeriodo={data_str}
                  order by consulta.cdConsulta'''
        
        return self.banco.dataframe(codsql=codsql)  
    
    
    def buscar_saldos_old(self):
        
        codsql = '''CREATE TABLE #temp_saldos(
                    Data_Posicao date,
                    NomeContaCrm varchar(255),
                    cd_security int,
                    NomeDoProduto varchar(255),
                    Rating varchar(255),
                    Moeda varchar(255),
                    Classe varchar(255),
                    Subclasse varchar(255),
                    Quantidade float,
                    Financeiro float,
                    calculado_em datetime)                    
                    Insert into #temp_saldos	                    
                    select  DataPeriodo as Data_Posicao,nome_reduzido as NomeContaCrm, 1 as cd_security, 'saldo_caixa' as NomeDoProduto, 'Recomendado' as Rating, 'USD' as Moeda, 'RF CP' as Classe, 'RF CP -' as Subclasse,
                    cla.ValorMercado as Quantidade, cla.ValorMercado as Financeiro, cons.DataProcessamentoFim as calculado_em
                    from TRPT_Performance_Classe  as cla 
                    inner join [dbo].[TRPT_Performance] as perf on perf.cdPerformance = cla.cdPerformance
                    inner join [dbo].[TRPT_Consulta] as cons on cons.cdConsulta = perf.cdConsulta
                    inner join tb_cliente_detalhe as cliente on cons.cdCliente=cliente.cd_cliente 
                    where NomeClasse='Classe de Ativos: Investimentos de Curto Prazo'                    
                    Select * 
                    from  #temp_saldos 
                    where Data_Posicao  = (SELECT MAX(Data_Posicao) FROM #temp_saldos)'''
        
        return self.banco.dataframe(codsql=codsql) 
        #return print(codsql)

    def buscar_saldos(self):
                
        codsql = '''                    
                    select  cons.DataProcessamentoFim as calculado_em,DataPeriodo as Data_Posicao,nome_reduzido as Nome_Cliente, 1 as cd_security, 'saldo_caixa' as 'Nome_produto', 'Recomendado' as Rating, 'USD' as Moeda, 'RF CP' as Classe, 'RF CP -' as Subclasse,
                    cla.ValorMercado as quantidade, 1 as PU, 1 as cotacao, cla.ValorMercado as SaldoNaData 
                    from TRPT_Performance_Classe  as cla 
                    inner join [dbo].[TRPT_Performance] as perf on perf.cdPerformance = cla.cdPerformance
                    inner join [dbo].[TRPT_Consulta] as cons on cons.cdConsulta = perf.cdConsulta
                    inner join tb_cliente_detalhe as cliente on cons.cdCliente=cliente.cd_cliente 
                    where NomeClasse='Classe de Ativos: Investimentos de Curto Prazo' and DataPeriodo > DATEADD(MONTH, -2, CONVERT(date, GETDATE()))   '''
        
        df =  self.banco.dataframe(codsql=codsql) 
        data = df['Data_Posicao'].max()
        df = df[df['Data_Posicao']==data]
        return df    
    
    def buscar_fundos_jbfo(self):
        
        codsql = '''                    
                   Select distinct * from tb_security where  cd_instituicao_issuer in(2705,1990,99,100,2748)
                   order by security'''
        
        df =  self.banco.dataframe(codsql=codsql) 
        
        return df
    
    def identificar_classe_fwd(self):
        
        codsql = '''
                    Select cd_security, FowardCdSecurityRefHedge,classe_class3.asset_class
                    FROM [Primeny].[dbo].[tb_security] as ativo
                    inner join tb_asset_class as classe_class3 on ativo.CdAssetClass3 = classe_class3.cd_asset_class
                    where security like '%FWD%' and ativo.FowardCdSecurityRefHedge is not null
                    order by dt_inclusao desc'''
        df =  self.banco.dataframe(codsql=codsql) 
        
        return df       
        

class BaseAtivos:
    """
    Classe criada para intemediar interações com a base BAWM (BAWM), no servidor Aux,
    tratando especificamente de coisas ligadas à base de ativos.
    
    Ela lida com as tabelas cujo nome começa com AT_* 
    
    """
    def __init__(self, homologacao=False):
        if homologacao:
            self.banco = BaseSQL(nome_servidor=r'SQLSVRHOM1.GPS.BR\SQL12', nome_banco='BAWM_HOM1')
        else:
            self.banco = BaseSQL(nome_servidor=ambiente.servidor_aux(), nome_banco='BAWM')
    
    def importacao_dados_credito(self, df_dados:pd.DataFrame) -> pd.DataFrame:
        df_current = self.select_isin_cadastro()
        # 1. Transforma os ISINs em IdAtivo
        df_dados.insert(0, 'IdAtivo', [0] * len(df_dados))
        #  1.a. Busca isins já cadastrados na AT_Cadastro
        df_merged = pd.merge(df_dados, df_current, how = "outer", on =['IdAtivo'])
        #  1.b. Cadastra novos ISINS
        df_merged_new_isin = df_merged[df_merged['IdAtivo'].null() == True]
        df_merged_new_isin.insert(0,"NomeAtivo", df_merged_new_isin['ISIN'])
        df_merged_new_isin2 = df_merged_new_isin[['NomeAtivo']]
        #df_merged_new_isin2.to_sql(AT_Cadastro, engine, if_exists = 'fail') #
        self.banco.com_edit_or_add_df(tabela='AT_Cadastro', dados=df_merged_new_isin2, campos_filtros=['NomeAtivo'], colunas_comparar=['NomeAtivo'])
        #  1.c. Tira coluna de ISINS
        df_dados.drop('ISIN', axis=1, inplace=True)
        
        # 2. Sobe os dados
        df = self.banco.com_edit_or_add_df(tabela='AT_Dados', dados=df_dados, campos_filtros=['IdAtivo', 'IdCampo', 'Data', 'Periodo'],
                                           colunas_comparar=['ValorFlt'])
        
    def select_isin_cadastro(self):
        ''' Parameters
                ----------
        dia : datetime
        Usar sem o primeiro dia de cada mes.'''
            
        
        codsql = f'''  Select IdAtivo, NomeAtivo from AT_Cadastro'''
        
        return self.banco.dataframe(codsql=codsql)  


class Teste:
    """
    Classe criada para Testes
    """
    def __init__(self, homologacao:bool=False):
        self.homologacao = homologacao        
        if homologacao:
            self.banco = BaseSQL(nome_servidor='SQLSVRHOM1.GPS.BR', nome_banco='GPS_HOM1')
        else:
            self.banco = BaseSQL(nome_servidor=ambiente.servidor_aux(), nome_banco='BAWM')
    
    def teste(self):
        dicio = {'IdTipoOrdem': 18, 'TipoOrdem': 'Teste1'}
        self.banco.com_add_withcheck('BL_TipoOrdem', dicio, campo_check='TipoOrdem')
        
    def upload_credito(self, emissores):
        planilha = 'C:/Temp/UploadScore.xlsx'
        df = pd.read_excel(planilha)
        
        # Inclui balanços
        if emissores:
            statements = df[['IdLimCred', 'DataCompetencia']].drop_duplicates()
            statements['IdTipoFonte'] = 151        
            retorno = self.banco.com_add_df(tabela='PO_LimCred_Statement', df=statements)
        
        # Busca balanços
        codsql = 'SELECT IdLimCred, DataCompetencia, IdStatement FROM PO_LimCred_Statement'
        df_bal = self.banco.dataframe(codsql).set_index(['IdLimCred', 'DataCompetencia'])
        
        df_up = pd.merge(left=df, left_on=['IdLimCred', 'DataCompetencia'], right=df_bal, right_index=True)
        df_up = df_up[['IdStatement', 'IdTipo', 'Valor', 'ValorAjuste', 'QuemAdj', 'QuandoAdj']]
        retorno = self.banco.com_add_df(tabela='PO_LimCred_StatementDetalhe', df=df_up)
        
        


if __name__ == '__main__':  
    pass
    df = Bawm().risk_score_por_subclasse()
    # # Crm().Produtos_isin_ticker(lista_isin=['BRSTNCNTB0O7', 'BRSTNCNTB674'])
    # df = Secundario().idtrade_volume_cliente(13318)
    # df = Secundario().volume_executado_ordens([151078,151513])
    # print(Crm().conta_movimento_dados('4047a0b5-c228-e911-i8c4a-005056912b96'))
    #sec = Secundario()
    
    # sec.secundario_reagenda_prebols([1441456], nova_data=datetime.datetime(2025,1,20), deletado=False)
    #print(sec.oferta_verificar_cliente(id_trade=15807, conta_crm_guid='847715ea-29eb-eb11-a569-005056912b96', conta_crm_guid_fundo=None, titularidade_guid=None))
    # sec.precifica_id_trade(id_trade=13346, preco_compra=1203.599286, preco_venda=1203.264663, taxa_compra=7.0926, taxa_venda=7.0976, priced=1 )
    # Secundario().idtrade_rateio_c_v(13322, True, 2605528)
    # df = Secundario().secundario_boletas_id_trade(15834)
    # Boletador().pricing_crossovers_enviar_preco(data_mov=datetime.datetime(2025,1,27), ativo_guid='a4813e26-479c-ef11-81b0-005056b17af5', preco_compra=945.8874,preco_venda=945.5721)
    # df = Secundario().volumepr_executado_ordens([93479])
    # df = Secundario().bookint_em_aberto_por_cliente()
    # CreditManagement(buffer=True).idlim_cadastro(1330)
    # df = Boletador().ordem_baixa_log_alteracoes([145903])
    #ret = Secundario().secundario_gera_boletas_boletator([105125])
    #CaixaPF().refresh()
    # df = PosicaoDm1().titularidade_composicao_monta_ex_po_cadastro()
    #Secundario().pr_sec_verifica_limite_boletagem(id_trade=15481, tipo_mov='C')
    # df = Bawm().carteira_ima_b()
    # Teste().upload_credito(emissores=False)    
    # bol = Boletador()
    # df = bol.pricing_crossovers_pendentes(data_ini=datetime.datetime(2025,1,24))
    # ret, msg = bol.pricing_crossovers_enviar_preco(data_mov=datetime.datetime(2025,1,31), ativo_guid='1a78bcd9-be2a-ed11-ac50-005056b17af5', preco_compra=1001, preco_venda=1000.50, taxa_compra=7.98, taxa_venda=7.99)
    #boletas = pd.read_excel('c:/temp/foundation/teste.xlsx')
    #bol.ordens_execucao_trading_desk(boletas)
    #print(OrderMgmtAPI().ordem_atualiza_status(id_ordem_oms=2153, id_status_atual=2, id_status_novo=15))
    
    # data = datetime.datetime(2024,12,30)
    # df = BDS().series_historico([59922307, 10134819], data_ini=data, data_fim=data, lista_idcampos={101: [11], 599: [8], 214: [8]}, campos_por_nome=False, substituir_nomes_campos=['Cota'])
    # df = Crm().contas_movimento_por_titularidade('0a3e33ba-3e3b-e711-acb1-005056912b96')
    # df = Boletador().boletas_por_data(exportado=False)