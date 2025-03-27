import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
from databases import Secundario, CreditManagement, PosicaoDm1Pickle
from objetos import Supercarteira, Ativo, Fundo, Titularidade, CadAtivosMini
from sys_boletador import OrderManager, BookInterno
from cronometro import Cronometro
"""
Arquivo com classes dedicadas à gestão da carteira de crédito
de fundos exclusivos e titularidades. Existe uma classe para supercarteiras
que deve ser descontinuada
"""
        
class SupercarteiraCredito(Supercarteira):
    
    def __init__(self, nome_supercarteira=None, guid_supercarteira=None, data_pos=None, base_dm1=None, homologacao=False):
        super().__init__(nome_supercarteira=nome_supercarteira, guid_supercarteira=guid_supercarteira, data_pos=data_pos, base_dm1=base_dm1, homologacao=homologacao)
                
    def desenq_solucionar(self):
        ordens = []
        lista = self.enquadramento_ativos(apenas_desenquadramentos=True, futuro=True, apenas_rf=True)
        for idx, row in lista.iterrows():
            reducao_necessaria = row['Limite'] / row['PesoFuturo'] - 1
            if row['Classe'] == 'RF':
                # Limite por emissor foi excedido
                emissor = False
                pos = self.posexp[(self.posexp['GuidEmissor']==row['GuidLimite'])].copy()
            else:
                # Limite por produto foi excedido
                emissor = False
                pos = self.posexp[(self.posexp['GuidProduto']==row['GuidProduto'])].copy()
            if not pos.empty and not emissor:
                resposta = self.__desenq_solucionar_execucao__(df_pos=pos, reducao_necessaria=reducao_necessaria)
                for r in resposta:
                    ordens.append(r)
        ordens = pd.DataFrame(ordens)
        if ordens.empty:
            return
        ordens = ordens[ordens['Financeiro']!=0]
        ordens = ordens[~ordens['NomeContaMovimento'].isnull()] # Sem conta movimento seria loucura
        
        # 2. Segue para boletagem, no caso de ordens de fundos
        ordensf = ordens[ordens['QouF']=='F']
        if not ordensf.empty:
            for idx, row in ordensf.iterrows():
                self.bol.adicionar_por_financeiro(guid_conta_crm=row['GuidContaCRM'], guid_produto=row['AtivoGuid'], tipo_mov='V', financeiro=row['Financeiro'])
            self.bol.upload()
        
        # 3. para os demais casos, disponibiliza no secundário interno
        ordensq = ordens[ordens['QouF']=='Q']
        if not ordensq.empty:
            for idx, row in ordensq.iterrows():                
                if 'TitularidadeGuid' in ordensq.columns:
                    try:
                        res = self.sys_sec.disponibilizar_ativo(guid_produto=row['AtivoGuid'], conta_crm=row['NomeContaCRM'], guid_conta_crm=row['GuidContaCRM'], quantidade=row['Quantidade'], 
                                                      titularidade=row['Titularidade'], titularidade_guid=row['TitularidadeGuid'],
                                                      conta_movimento_guid=row['GuidContaMovimento'], motivo='Realocação',
                                                      observacao='Disponibilizado porque o ativo excede o limite de crédito')
                        print(res)
                    except Exception as e:
                        print(str(e))
                else:
                    try:
                        res = self.sys_sec.disponibilizar_ativo(guid_produto=row['AtivoGuid'], conta_crm=row['NomeContaCRM'], guid_conta_crm=row['GuidContaCRM'],
                                                                quantidade=row['Quantidade'], motivo='Realocação',
                                                                observacao='Disponibilizado porque o ativo excede o limite de crédito')
                        print(res)
                    except Exception as e:
                        print(str(e))                        
                
        print('Final')
        
            
    def __desenq_solucionar_execucao__(self, df_pos, reducao_necessaria):        
        ordens = []
        tipo_at_resg = ['COTAS', 'FUNDO']
        min_valor_mov = 50000        
        ratings_exc = ['monitorado', 'monitorado_pf', 'permitido', 'nao_monitorado', 'nao_analisado', 'sem_rating']
        # a variável resgate é verdadeira se o enquadramento é feito via resgate,
        # falsa se é necessário disponibilizar no secundário interno.
        if df_pos.iloc[0]['TipoProduto'] in tipo_at_resg:
            resgate = True
        else:
            resgate = False            
        for idx, row in df_pos.iterrows():
            # 1. dados gerais da movimentação
            nome = row['NomeProduto']
            nome_lista = nome.lower().split(' ')
            if 'fidc' in nome_lista:
                fidc = True
            else:
                fidc = False
            dicionario = {'AtivoGuid': row['GuidProduto'], 'AtivoNome': nome, 'TipoMov': 'V', 'Rating': row['Rating']}
            if reducao_necessaria == -1:
                dicionario['ResgTot'] = True
            else:
                dicionario['ResgTot'] = False
            if resgate and not fidc:
                dicionario['QouF'] = 'F'
                if reducao_necessaria == -1:
                    if str(row['Rating']).lower() in ratings_exc:
                        # Se fundo é permitido, não resgata
                        dicionario['Financeiro'] = 0
                        dicionario['ResgTot'] = False
                    else:
                        dicionario['Financeiro'] = row['FinanceiroFuturo']
                else:
                    dicionario['Financeiro'] = round(abs(reducao_necessaria) * row['FinanceiroFuturo'], -3)
            else:
                dicionario['QouF'] = 'Q'
                if reducao_necessaria == -1:
                    if str(row['Rating']).lower() in ratings_exc:
                        dicionario['Financeiro'] = 0
                        dicionario['Quantidade'] = 0
                    else:
                        dicionario['Financeiro'] = row['FinanceiroFuturo']
                        dicionario['Quantidade'] = row['QuantidadeFinal'] - row['QtdeBloq']
                else:
                    # Boleta tem que ser de no mínimo R$ 50 mil, com pelo menos R$ 50 mil de saldo remanescente
                    # Se saldo remanescente não for de R$ 50 mil, será vendida a posição inteira
                    valor = abs(reducao_necessaria) * row['FinanceiroFuturo']
                    saldo = row['FinanceiroFuturo'] - valor
                    if saldo <= min_valor_mov:
                        # Se o saldo remanescente é baixo, tem que vender posição inteira
                        dicionario['Financeiro'] = row['FinanceiroFuturo']
                        dicionario['Quantidade'] = row['QuantidadeFinal'] - row['QtdeBloq']
                    elif valor < min_valor_mov:
                        # Se o saldo remanescente é baixo, tem que vender posição inteira
                        if (saldo + valor - min_valor_mov) < min_valor_mov:
                            dicionario['Financeiro'] = row['FinanceiroFuturo']
                            dicionario['Quantidade'] = row['QuantidadeFinal'] - row['QtdeBloq']
                        else:
                            valor = min_valor_mov
                            dicionario['Financeiro'] = valor
                            dicionario['Quantidade'] = int((valor / row['FinanceiroFuturo'])  * (row['QuantidadeFinal'] - row['QtdeBloq']))
                    else:
                        dicionario['Financeiro'] = valor
                        dicionario['Quantidade'] = int((valor / row['FinanceiroFuturo'])  * (row['QuantidadeFinal'] - row['QtdeBloq']))
                
            # 2. descobre onde está a posição
            dicionario['Origem'] = row['Origem']
            if row['Origem'] == 'CartAdm':
                # está na carteira adm
                dicionario['NomeContaCRM'] = row['NomeContaCRM']
                dicionario['GuidContaCRM'] = row['GuidContaCRM']
                dicionario['Titularidade'] = row['Titularidade']
                dicionario['TitularidadeGuid'] = row['TitularidadeGuid']
                dicionario['NomeContaMovimento'] = row['NomeContaMovimento']
                dicionario['GuidContaMovimento'] = row['GuidContaMovimento']
            else:
                # está em um fundo
                dicionario['NomeContaCRM'] = row['NomeContaCRM']
                dicionario['GuidContaCRM'] = row['GuidContaCRM']
                dicionario['NomeContaMovimento'] = row['NomeContaMovimento']
            # Adiciona na lista
            ordens.append(dicionario)
        return ordens
    
    def bidar_ofertas(self):
        sys = BookInterno()
        df_ofertas = sys.ofertas_disponiveis()
        self.bidar_ofertas_df_ofertas(df_ofertas=df_ofertas)
    
    def bidar_ofertas_df_ofertas(self, df_ofertas):
        liquidez = self.liquidez_operacional()
        pl = self.pl
        # Verifica limite para cada oferta
        for idx, row in df_ofertas:
            pass


class FundoCredito(Fundo):    
    """
    Classe criada para gerir a carteira de crédito de um fundo, enviando ordens
    que serão tradadas pelo OrderManager (sys_boletador.py)
    Ordens:
        de investimento são tipicamente enviadas para o sistema interno
    de secundário. 
        de desinvestimento podem também ser enviadas diretamente como resgates
    de fundos investidos.
    
    Três funcionalidades básicas:
        - solucionar desenquadramentos: reduz exposições para reenquadrar fundos nos limites de crédito JBFO
        - comprar ativos disponíveis no secundário / primário
        - vender posições, tipicamente para gerar liquidez
    """
    def __init__(self, nome_conta_crm=None, guid_conta_crm=None, data_pos=None, carregar_administrador=False, base_dm1=None, homologacao=False):
        super().__init__(nome_conta_crm=nome_conta_crm, guid_conta_crm=guid_conta_crm, data_pos=data_pos, carregar_administrador=carregar_administrador, 
                         load_posicao_explodida=True, base_dm1=base_dm1, homologacao=homologacao)        
        self.sys = BookInterno(homologacao=homologacao)
    
    def enviar_ordens(self, df_ordens:pd.DataFrame, id_system_origem:int, dias_validade:int=14):
        """
        Envia um dataframe de ordens para o OrderManager

        Parameters
        ----------
        df_ordens : pd.DataFrame
            DataFrame com ordens. Gerado com ajuda da função Fundo.ordem_dicionario_base().
        id_system_origem : int
            Id do sistema originador. Ver na BL_Propriedades.
        dias_validade : int, optional
            Prazo antes que a ordem expire (se não analisada por Cliente ou DPM). The default is 14.

        Returns
        -------
        None.

        """
        om = OrderManager(id_system_origem=id_system_origem, id_tipo_portfolio=1)
        hoje = self.cartdm1.banco.hoje()
        data_val = hoje + relativedelta(days=dias_validade)
        for idx, row in df_ordens.iterrows():
            if row['Ordem']:
                om.inserir_ordem(id_system_destino=row['SysDestino'], guid_portfolio=row['GuidPortfolio'], ativo_guid=row['AtivoGuid'], 
                                 ativo_nome=row['AtivoNome'], tipo_mov=row['TipoMov'], resgate_total=row['ResgTot'],
                                 preboleta=False, data_ordem=hoje, data_validade=data_val, acao=row['Acao'], observacao=row['Observacao'],
                                 q_ou_f=row['QouF'], quantidade=row['Quantidade'], financeiro=row['Financeiro'])
        mesmo_grupo = True
        if id_system_origem == 43:  
            # Ordens de enquadramento no limite de crédito podem ser analisadas separadamente
            mesmo_grupo = False
        om.upload_orders(mesmo_grupo=mesmo_grupo)
    
    def desenq_solucionar(self, enviar_ordens:bool=False) -> pd.DataFrame: 
        """
        Função mapea carteira de fundo e limites de crédito. Ativos que estão
        acima do limite são resgatados (fundos abertos) ou disponibilizados
        no secundário interno

        Parameters
        ----------
        enviar_ordens : bool, optional. The default is False.
            Se True, envia as ordens, sem coordenar com nível superior.

        Returns
        -------
        ordens : DataFrame
            DataFrame com ordens a serem subidas na base.

        """
        ordens = []
        # TODO: Tirar fundos exclusivos do controle de enquadramento
        lista = self.enquadramento_ativos(apenas_desenquadramentos=True, futuro=True, apenas_rf=True)        
        for idx, row in lista.iterrows():
            reducao_necessaria = row['LimiteAlav'] / row['PesoFuturo'] - 1
            if row['Classe'] == 'RF':
                # Limite por emissor foi excedido
                emissor = False
                pos = self.pos[(self.pos['GuidEmissor']==row['GuidLimite'])].copy()
            else:
                # Limite por produto foi excedido
                emissor = False
                pos = self.pos[(self.pos['GuidProduto']==row['GuidProduto'])].copy()
            if not pos.empty and not emissor:
                resposta = self.__desenq_solucionar_execucao__(df_pos=pos, reducao_necessaria=reducao_necessaria, limite_considerado=row['LimiteAlav'], limite_real=row['Limite'])
                for r in resposta:
                    ordens.append(r)
        ordens = pd.DataFrame(ordens)
        if ordens.empty:
            return
        if enviar_ordens:
            self.enviar_ordens(df_ordens=ordens, id_system_origem=43)
        return ordens
    
    def reduzir_exposicao(self, perc_pl:float, classe:str=None, sub_classe:str=None, enviar_ordens:bool=False) -> pd.DataFrame:
        """
        Recebendo um percentual do patrimônio e uma classe ou sub-classe de ativos,
        resgata ou disponibliza (via ordens) ativos em carteira.

        Parameters
        ----------
        perc_pl : float
            Percentual do Patrimônio do Fundo.
        classe : str, optional
            Classe de Ativos. The default is None.
        sub_classe : str, optional
            SubClasse de ativos. The default is None.
        enviar_ordens : bool, optional. The default is False.
            Se True, envia as ordens, sem coordenar com nível superior.

        Returns
        -------
        ordens : DataFrame
            DataFrame com ordens para upload na base

        """
        # TODO: executar venda de desenquadramentos e ajustar matriz de posições
        # a. Pega dados
        limites = self.enquadramento_ativos(apenas_desenquadramentos=False, apenas_rf=True).set_index('GuidLimite')
        limites = limites[limites['IdTipoLim']==2] # apenas limites por produto
        alav = self.multiplicador_limites # calculado no objeto fundo na busca de limites
        ordens = []
        # b. Retira ativos de liquidez e soberanos
        cad_liq = self.cartdm1.liquidez_cadastro()
        cad_liq = cad_liq[cad_liq['Liquidez']==True]
        produtos_cart = list(self.pos['IdProdutoProfitSGI'].unique())
        cad_liq = cad_liq[cad_liq['Valor'].isin(produtos_cart)]
        produtos_cart = self.pos[['IdProdutoProfitSGI', 'GuidProduto']].copy().set_index('IdProdutoProfitSGI')
        # Retira caixa
        cad_liq.insert(0, 'GuidProduto', len(cad_liq) * [None])
        cad_liq['GuidProduto'] = cad_liq['Valor'].apply(lambda x: produtos_cart.loc[x, 'GuidProduto'])
        limites = limites[~limites['GuidProduto'].isin(list(cad_liq['GuidProduto'].unique()))]
        # Retira da conta ativos soberanos
        subcl_soberanas = ['RF Pos Soberano', 'RF Infl Soberano']
        limites = limites[~limites['SubClasse'].isin(subcl_soberanas)]
        
        # c. Classe e sub classe
        if classe:
            limites = limites[limites['Classe']==classe]
        if sub_classe:
            limites = limites[limites['SubClasse']==sub_classe] 
        
        # d. Vende as posições
        limites.sort_values('LimiteAlav', ascending=False, inplace=True)
        total = limites['PesoFuturo'].sum()
        
        if total <= perc_pl:
            # d.1. Se o total disponível para vendas é menor que o necessário, vende tudo
            reducao_necessaria = -1
            for idx, row in limites.iterrows():
                if limites['GuidLimite'] in self.pos['GuidProduto']:
                    pos = self.pos[self.pos['GuidProduto']==limites['GuidLimite']].copy()
                    resposta = self.__desenq_solucionar_execucao__(df_pos=pos, reducao_necessaria=reducao_necessaria, limite_considerado=row['LimiteAlav'], limite_real=row['Limite'], geracao_liquidez=True)                    
                    for r in resposta:
                        ordens.append(r)
        else:
            volume_vender = perc_pl
            for idx, row in limites.iterrows():
                pos = self.pos[self.pos['GuidProduto']==limites['GuidLimite']].copy()
                venda = min(volume_vender, row['PesoFuturo'])
                reducao_necessaria = venda / row['PesoFuturo'] - 1
                volume_vender -= venda
                resposta = self.__desenq_solucionar_execucao__(df_pos=pos, reducao_necessaria=reducao_necessaria, limite_considerado=row['LimiteAlav'], limite_real=row['Limite'], geracao_liquidez=True)                    
                for r in resposta:
                    ordens.append(r)
                if round(volume_vender, 4) <= 0:
                    break
        
        # Final: envia ordens
        if len(ordens) > 0:
            ordens = pd.DataFrame(ordens)
            if enviar_ordens:
                self.enviar_ordens(df_ordens=ordens, id_system_origem=21)
            return ordens
    
    def __desenq_solucionar_execucao__(self, df_pos, reducao_necessaria, limite_considerado, limite_real, geracao_liquidez=False):        
        ordens = []
        tipo_at_resg = ['COTAS', 'FUNDO']
        min_valor_mov = 50000        
        ratings_exc = ['reprovado', 'monitorado', 'monitorado_pf', 'permitido']
        # a variável resgate é verdadeira se o enquadramento é feito via resgate,
        # falsa se é necessário disponibilizar no secundário interno.
        if df_pos.iloc[0]['TipoProduto'] in tipo_at_resg:  # TODO: o que fazer com FIDCs abertos?
            resgate = True
        else:
            resgate = False            
        for idx, row in df_pos.iterrows():
            # 1. dados gerais da movimentação
            nome = row['NomeProduto']
            nome_lista = nome.lower().split(' ')
            if 'fidc' in nome_lista:
                fidc = True
            else:
                fidc = False
            # 2. descobre onde está a posição            
            txt_obs = f'Redução de {round(reducao_necessaria * 100, 2)}% para enquadrar fundo no limite efetivo de {round(limite_considerado * 100, 2)}% O limite real é {round(limite_real * 100, 2)}%.'
            if geracao_liquidez:
                txt_obs = f'Redução de {round(reducao_necessaria * 100, 2)}% para gerar liquidez'
            dicionario = self.ordem_dicionario_base(guid_produto=row['GuidProduto'], nome_produto=nome, tipo_mov='V', observacao=txt_obs)
            dicionario['Rating'] = row['RatingGPS']

            if reducao_necessaria == -1:
                dicionario['ResgTot'] = True
            else:
                dicionario['ResgTot'] = False
            if resgate and not fidc:
                dicionario['QouF'] = 'F'
                if reducao_necessaria == -1:
                    if str(row['RatingGPS']).lower() in ratings_exc:
                        # Se fundo é permitido, não resgata
                        dicionario['Financeiro'] = 0
                        dicionario['ResgTot'] = False
                        dicionario['Acao'] = f"Redução não executada pois ativo é {row['RatingGPS']}"
                        dicionario['Ordem'] = False
                    else:
                        dicionario['Financeiro'] = row['FinanceiroFuturo']
                        dicionario['Acao'] = 'Solicitação de resgate total'
                        dicionario['SysDestino'] = 17  # Cockpit via BL_Prebol
                else:
                    if str(row['RatingGPS']).lower() in ratings_exc:
                        dicionario['Financeiro'] = 0
                        dicionario['ResgTot'] = False
                        dicionario['Acao'] = f"Redução não executada pois ativo é {row['RatingGPS']}"
                        dicionario['Ordem'] = False
                    else:
                        dicionario['Financeiro'] = round(abs(reducao_necessaria) * row['FinanceiroFuturo'], -3)
                        dicionario['Acao'] = 'Solicitação de resgate'
                        dicionario['SysDestino'] = 17  # Cockpit via BL_Prebol
            else:
                dicionario['QouF'] = 'Q'
                if reducao_necessaria == -1:
                    if str(row['RatingGPS']).lower() in ratings_exc:
                        dicionario['Financeiro'] = 0
                        dicionario['Quantidade'] = 0
                        dicionario['Acao'] = f"Redução não executada pois ativo é {row['RatingGPS']}"
                        dicionario['Ordem'] = False
                    else:
                        dicionario['Financeiro'] = row['FinanceiroFuturo']
                        dicionario['Quantidade'] = row['QuantidadeFinal'] - row['QtdeBloq']
                        dicionario['Acao'] = 'Disponibilização total no secundário interno'
                        dicionario['SysDestino'] = 19  # Crossover
                else:
                    # Boleta tem que ser de no mínimo R$ 50 mil, com pelo menos R$ 50 mil de saldo remanescente
                    # Se saldo remanescente não for de R$ 50 mil, será vendida a posição inteira
                    valor = abs(reducao_necessaria) * row['FinanceiroFuturo']
                    saldo = row['FinanceiroFuturo'] - valor
                    if saldo <= min_valor_mov:
                        # Se o saldo remanescente é baixo, tem que vender posição inteira
                        dicionario['Financeiro'] = row['FinanceiroFuturo']
                        dicionario['Quantidade'] = row['QuantidadeFinal'] - row['QtdeBloq']
                        dicionario['Acao'] = 'Disponibilização total no secundário interno, mínimo de movimentação de R$ 50 mil'
                        dicionario['SysDestino'] = 19  # Crossover
                    elif valor < min_valor_mov:
                        # Se o saldo remanescente é baixo, tem que vender posição inteira
                        if (saldo + valor - min_valor_mov) < min_valor_mov:
                            dicionario['Financeiro'] = row['FinanceiroFuturo']
                            dicionario['Quantidade'] = row['QuantidadeFinal'] - row['QtdeBloq']
                            dicionario['Acao'] = 'Disponibilização total no secundário interno, mínimo de movimentação de R$ 50 mil'
                            dicionario['SysDestino'] = 19  # Crossover
                        else:
                            valor = min_valor_mov
                            dicionario['Financeiro'] = valor
                            dicionario['Quantidade'] = int((valor / row['FinanceiroFuturo'])  * (row['QuantidadeFinal'] - row['QtdeBloq']))
                            dicionario['Acao'] = 'Disponibilização no secundário interno, mínimo de movimentação de R$ 50 mil'
                            dicionario['SysDestino'] = 19  # Crossover
                    else:
                        dicionario['Financeiro'] = valor
                        dicionario['Quantidade'] = int((valor / row['FinanceiroFuturo'])  * (row['QuantidadeFinal'] - row['QtdeBloq']))
                        dicionario['Acao'] = 'Disponibilização no secundário interno'
                        dicionario['SysDestino'] = 19  # Crossover
                
            
            # Adiciona na lista
            ordens.append(dicionario)
        return ordens

    def comprar_ofertas(self, perc_pl:float, classe:str=None, sub_classe:str=None, enviar_ordens:bool=False, incluir_cotas=True) -> pd.DataFrame:
        """
        Recebendo um percentual do patrimônio e uma classe ou sub-classe de ativos,
        coloca demanda (via ordens) para ativos.

        Parameters
        ----------
        perc_pl : float
            Percentual do Patrimônio do Fundo.
        classe : str, optional
            Classe de Ativos. The default is None.
        sub_classe : str, optional
            SubClasse de ativos. The default is None.
        enviar_ordens : bool, optional. The default is False.
            Se True, envia as ordens, sem coordenar com nível superior.

        Returns
        -------
        ordens : DataFrame
            DataFrame com ordens para upload na base

        """
        limites = self.enquadramento_ativos(apenas_desenquadramentos=False).set_index('GuidLimite')
        alav = self.limite_credito_alavancagem # calculado no objeto fundo na busca de limites
        # 1. BUSCA OFERTAS E FAZ FILTROS
        # -----------------------------------
        lote_min = 0.0025; perc_lim_min = 0.25; mov_min = 50000
        mov_min_cotas = round(0.001 * self.pl_est, 0)
        df_ofertas = self.sys.ofertas_disponiveis()
        # a. Tira veículos e tipos de produto que são apenas para PF
        lista_pf = ['a0e82c98-0412-e111-a443-d8d385b9752e', '7eb3d5ec-7d48-e611-9f3c-005056912b96', 'b343733d-d315-ea11-a89a-005056912b96', '9829664c-4ae5-e511-9f3c-005056912b96']
        df_ofertas = df_ofertas[~df_ofertas['GuidProduto'].isin(lista_pf)]
        produtos_apenas_pf = ['LCA Pós Exp 252 Isento', 'LCI Pós Exp 252 Isento', 'CRI Pós Exp 252 Isento', 'Fundo Imobiliário','CRA Pós Exp 252 Isento']
        df_ofertas = df_ofertas[~df_ofertas['TipoProduto'].isin(produtos_apenas_pf)]
        # b. Tipo de produto
        if self.tipo_investidor == 'qualificado':
            lista_publico = ['332ebe7c-25e1-e811-afbd-005056912b96', 'e4633edd-4d35-e011-b24e-d8d385b9752e']
        else:
            lista_publico = ['8a9ca84c-ff9f-e411-a411-005056912b96', '13b305cb-0fe2-e811-afbd-005056912b96']
        df_ofertas = df_ofertas[~df_ofertas['GuidProduto'].isin(lista_publico)]
            
        # c. Classe e sub classe
        if classe:
            df_ofertas = df_ofertas[df_ofertas['Classe']==classe]
        if sub_classe:
            df_ofertas = df_ofertas[df_ofertas['SubClasse']==sub_classe]        
        # d. Cotas
        if not incluir_cotas:
            df_ofertas = df_ofertas[~df_ofertas['TipoProduto'].isin(['COTAS'])]
        # e. Tamanho
        df_ofertas.insert(len(df_ofertas.columns), 'OfertaFin', df_ofertas['Oferta'] * df_ofertas['PUFonte'])
        df_ofertas = df_ofertas[df_ofertas['OfertaFin']>=(lote_min * self.pl_est)]
        df_ofertas = df_ofertas[df_ofertas['OfertaFin']>=(df_ofertas['Limite'] * alav * perc_lim_min * self.pl_est)]        
        
        # 2. PARA PRODUTOS DISPONÍVEIS, VERIFICA LIMITE DISPONÍVEL        
        ordens = []
        df_ofertas.insert(len(df_ofertas.columns), 'EspacoAloc', [0] * len(df_ofertas))
        for idx, row in df_ofertas.iterrows():
            # a. descobre o valor da ordem possível            
            limite_real = row['Limite']
            limite_considerado = limite_real * alav
            espaco_prod = limite_considerado ; espaco_emissor = limite_considerado
            if row['GuidEmissor']:
                if row['GuidEmissor'] in limites.index:
                    linha = limites.loc[row['GuidEmissor']]
                    espaco_emissor = max(0, (linha['LimiteAlav'] - max(linha['PesoFinal'], linha['PesoFuturo'])))
            if row['GuidProduto'] in limites.index:
                linha = limites.loc[row['GuidProduto']]
                espaco_prod = max(0, (linha['LimiteAlav'] - max(linha['PesoFinal'], linha['PesoFuturo'])))
                        
            comprar = self.pl_est * min([limite_considerado, espaco_prod, espaco_emissor])
            comprar = min(comprar, row['OfertaFin'])
            if row['TipoProduto'] != 'COTAS':
                if comprar < mov_min:
                    comprar = 0
            else:
                if comprar < mov_min_cotas:
                    comprar = 0
            df_ofertas.loc[idx, 'EspacoAloc'] = comprar
        
        # 3. COLOCA ORDENS DO ATIVO MAIS RESTRITO PARA O MAIS DISPONIVEL
        df_ofertas.sort_values(['OfertaFin', 'EspacoAloc'], inplace=True)
        df_ofertas = df_ofertas[df_ofertas['EspacoAloc']>0]
        volume_alocar = perc_pl * self.pl_est
        for idx, row in df_ofertas.iterrows():
            comprar = row['EspacoAloc']
            comprar = min(comprar, volume_alocar)
            volume_alocar -= comprar
            # b. se valor > 0, faz a ordem
            if comprar > 0:
                texto_obs = f'Investimento de {round(comprar / self.pl_est * 100, 2)}% com base no limite efetivo de {round(row["Limite"] * alav * 100, 2)}% O limite real é {round(row["Limite"] * 100, 2)}%.'
                dicionario = self.dicio_base_ordem(guid_produto=row['GuidProduto'], nome_produto=row['NomeProduto'], tipo_mov='C',
                                               q_ou_f='Q', financeiro=comprar, quantidade=round(comprar / row['PUFonte'],0),
                                               acao='Incluir demanda no secundário', observacao=texto_obs, id_system_destino=19)
                ordens.append(dicionario)
            
        if len(ordens) > 0:
            ordens = pd.DataFrame(ordens)
            if enviar_ordens:
                self.enviar_ordens(df_ordens=ordens, id_system_origem=21)
            return ordens


class EnquadramentoFundos:
    
    def __init__(self, homologacao=False):
        self.dm1 = PosicaoDm1Pickle(load_all_positions=True, homologacao=homologacao)        
        self.om = OrderManager(id_system_origem=43, id_tipo_portfolio=1)  # Sistema: BookInterno - Enquadramento. Enquadramento não tem veto power
        self.df_ordens = pd.DataFrame()
    
    def rodar(self, upload_orders=True, testes=False):
        lista_fundos = self.dm1.lista_fundos_com_idcarteira(apenas_exclusivos=True, gestor_jbfo=True, excluir_infra=True)
        if testes:
            lista_fundos = lista_fundos.sample(n=10)
        relogio = Cronometro(verbose=True, nome_processo='Fundos enquadramento')
        ordens = []
        for idx, row in lista_fundos.iterrows():
            relogio.marca_tempo(f"{row['NomeContaCRM']}, {idx} / {len(lista_fundos)}")
            texto = row['GuidContaCRM']
            try:
                fdo = FundoCredito(guid_conta_crm=texto)    
                if fdo.tem_dm1 and fdo.pl_est > 0:
                    ordens.append(fdo.desenq_solucionar())
            except:
                print('.........Erro no fundo')        
        
        ordens = pd.concat(ordens)                
        self.df_ordens = ordens
        
        if upload_orders:
            relogio.marca_tempo('Enviando ordens.')
            hoje = self.dm1.banco.hoje()
            data_val = hoje + relativedelta(days=14)
            for idx, row in ordens.iterrows():
                if row['Ordem']:
                    self.om.inserir_ordem(id_system_destino=row['SysDestino'], guid_portfolio=row['GuidPortfolio'], ativo_guid=row['AtivoGuid'], 
                                          ativo_nome=row['AtivoNome'], tipo_mov=row['TipoMov'], resgate_total=row['ResgTot'],
                                          preboleta=False, data_ordem=hoje, data_validade=data_val, acao=row['Acao'], observacao=row['Observacao'],
                                          q_ou_f=row['QouF'], quantidade=row['Quantidade'], financeiro=row['Financeiro'])
            
            self.om.upload_orders()
        relogio.concluido()


class TitularidadeCredito(Titularidade):                  
    
    def __init__(self, titularidade=None, guid_titularidade=None, data_pos=None, base_dm1=None, homologacao=False):
        super().__init__(titularidade=titularidade, guid_titularidade=guid_titularidade, data_pos=data_pos, base_dm1=base_dm1, homologacao=homologacao)
        self.sys = BookInterno(homologacao=homologacao)
        
    def desenq_solucionar(self, enviar_ordens:bool=False) -> pd.DataFrame: 
        """
        Função mapea carteira de fundo e limites de crédito. Ativos que estão
        acima do limite são resgatados (fundos abertos) ou disponibilizados
        no secundário interno

        Parameters
        ----------
        enviar_ordens : bool, optional. The default is False.
            Se True, envia as ordens, sem coordenar com nível superior.

        Returns
        -------
        ordens : DataFrame
            DataFrame com ordens a serem subidas na base.

        """
        ordens = []
        # TODO: Tirar fundos exclusivos do controle de enquadramento
        lista = self.enquadramento_ativos(apenas_desenquadramentos=True, futuro=True, apenas_rf=True)        
        for idx, row in lista.iterrows():
            reducao_necessaria = row['LimiteAlav'] / row['PesoFuturo'] - 1
            if row['Classe'] == 'RF':
                # Limite por emissor foi excedido
                emissor = False
                pos = self.pos[(self.pos['GuidEmissor']==row['GuidLimite'])].copy()
            else:
                # Limite por produto foi excedido
                emissor = False
                pos = self.pos[(self.pos['GuidProduto']==row['GuidProduto'])].copy()
            if not pos.empty and not emissor:
                resposta = self.__desenq_solucionar_execucao__(df_pos=pos, reducao_necessaria=reducao_necessaria, limite_considerado=row['LimiteAlav'], limite_real=row['Limite'])
                for r in resposta:
                    ordens.append(r)
        ordens = pd.DataFrame(ordens)
        if ordens.empty:
            return
        if enviar_ordens:
            self.enviar_ordens(df_ordens=ordens, id_system_origem=43)
        return ordens
    
    def comprar_ofertas(self, perc_pl:float, classe:str=None, sub_classe:str=None, enviar_ordens:bool=False, incluir_cotas=True) -> pd.DataFrame:
        """
        Recebendo um percentual do patrimônio e uma classe ou sub-classe de ativos,
        coloca demanda (via ordens) para ativos.

        Parameters
        ----------
        perc_pl : float
            Percentual do Patrimônio do Fundo.
        classe : str, optional
            Classe de Ativos. The default is None.
        sub_classe : str, optional
            SubClasse de ativos. The default is None.
        enviar_ordens : bool, optional. The default is False.
            Se True, envia as ordens, sem coordenar com nível superior.

        Returns
        -------
        ordens : DataFrame
            DataFrame com ordens para upload na base

        """
        limites = self.enquadramento_ativos(apenas_desenquadramentos=False).set_index('GuidLimite')
        alav = 1 # Não há multiplicador de limites na PF
        # 1. BUSCA OFERTAS E FAZ FILTROS
        # -----------------------------------
        lote_min = 0.0025; perc_lim_min = 0.25; mov_min = 50000
        mov_min_cotas = round(0.001 * self.pl_est, 0)
        df_ofertas = self.sys.ofertas_disponiveis()
        # a. Tira veículos e tipos de produto que são apenas para PF
        lista_fundo = ['658cf882-d8e7-e611-9231-005056912b96', '1e9be34d-37fd-e811-af15-005056912b96', '29396cbc-1be6-e011-8193-d8d385b9752e', 'd037e458-0a0d-e011-a374-d8d385b9752e', 'a0e82c98-0412-e111-a443-d8d385b9752e', '98dddd39-ac45-ee11-a6d1-005056b17af5', 'e4a0c67b-a175-e911-8f6a-005056912b96', '760419f0-021b-ec11-a569-005056912b96']
        df_ofertas = df_ofertas[~df_ofertas['GuidProduto'].isin(lista_fundo)]
        produtos_apenas_fundo = ['FIDC', 'DEB Pós Exp 252']
        df_ofertas = df_ofertas[~df_ofertas['TipoProduto'].isin(produtos_apenas_fundo)]
        # b. Tipo de produto
        if self.tipo_investidor == 'qualificado':
            lista_publico = ['332ebe7c-25e1-e811-afbd-005056912b96', 'e4633edd-4d35-e011-b24e-d8d385b9752e']
        else:
            lista_publico = ['8a9ca84c-ff9f-e411-a411-005056912b96', '13b305cb-0fe2-e811-afbd-005056912b96']
        df_ofertas = df_ofertas[~df_ofertas['GuidProduto'].isin(lista_publico)]
            
        # c. Classe e sub classe
        if classe:
            df_ofertas = df_ofertas[df_ofertas['Classe']==classe]
        if sub_classe:
            df_ofertas = df_ofertas[df_ofertas['SubClasse']==sub_classe]        
        # d. Cotas
        if not incluir_cotas:
            df_ofertas = df_ofertas[~df_ofertas['TipoProduto'].isin(['COTAS'])]
        # e. Tamanho
        df_ofertas.insert(len(df_ofertas.columns), 'OfertaFin', df_ofertas['Oferta'] * df_ofertas['PUFonte'])
        df_ofertas = df_ofertas[df_ofertas['OfertaFin']>=(lote_min * self.pl_est)]
        df_ofertas = df_ofertas[df_ofertas['OfertaFin']>=(df_ofertas['Limite'] * alav * perc_lim_min * self.pl_est)]        
        
        # 2. PARA PRODUTOS DISPONÍVEIS, VERIFICA LIMITE DISPONÍVEL        
        ordens = []
        df_ofertas.insert(len(df_ofertas.columns), 'EspacoAloc', [0] * len(df_ofertas))
        for idx, row in df_ofertas.iterrows():
            # a. descobre o valor da ordem possível            
            limite_real = row['Limite']
            limite_considerado = limite_real * alav
            espaco_prod = limite_considerado ; espaco_emissor = limite_considerado
            if row['GuidEmissor']:
                if row['GuidEmissor'] in limites.index:
                    linha = limites.loc[row['GuidEmissor']]
                    espaco_emissor = max(0, (linha['LimiteAlav'] - max(linha['PesoFinal'], linha['PesoFuturo'])))
            if row['GuidProduto'] in limites.index:
                linha = limites.loc[row['GuidProduto']]
                espaco_prod = max(0, (linha['LimiteAlav'] - max(linha['PesoFinal'], linha['PesoFuturo'])))
                        
            comprar = self.pl_est * min([limite_considerado, espaco_prod, espaco_emissor])
            comprar = min(comprar, row['OfertaFin'])
            if row['TipoProduto'] != 'COTAS':
                if comprar < mov_min:
                    comprar = 0
            else:
                if comprar < mov_min_cotas:
                    comprar = 0
            df_ofertas.loc[idx, 'EspacoAloc'] = comprar
        
        # 3. COLOCA ORDENS DO ATIVO MAIS RESTRITO PARA O MAIS DISPONIVEL
        df_ofertas.sort_values(['OfertaFin', 'EspacoAloc'], inplace=True)
        df_ofertas = df_ofertas[df_ofertas['EspacoAloc']>0]
        volume_alocar = perc_pl * self.pl_est
        for idx, row in df_ofertas.iterrows():
            comprar = row['EspacoAloc']
            comprar = min(comprar, volume_alocar)
            volume_alocar -= comprar
            # b. se valor > 0, faz a ordem
            if comprar > 0:
                texto_obs = f'Investimento de {round(comprar / self.pl_est * 100, 2)}% com base no limite efetivo de {round(row["Limite"] * alav * 100, 2)}% O limite real é {round(row["Limite"] * 100, 2)}%.'
                dicionario = self.dicio_base_ordem(guid_produto=row['GuidProduto'], nome_produto=row['NomeProduto'], tipo_mov='C',
                                               q_ou_f='Q', financeiro=comprar, quantidade=round(comprar / row['PUFonte'],0),
                                               acao='Incluir demanda no secundário', observacao=texto_obs, id_system_destino=19)
                ordens.append(dicionario)
            
        if len(ordens) > 0:
            ordens = pd.DataFrame(ordens)
            if enviar_ordens:
                self.enviar_ordens(df_ordens=ordens, id_system_origem=21)
            return ordens
        
    def enviar_ordens(self, df_ordens:pd.DataFrame, id_system_origem:int, dias_validade:int=14):
        """
        Envia um dataframe de ordens para o OrderManager

        Parameters
        ----------
        df_ordens : pd.DataFrame
            DataFrame com ordens. Gerado com ajuda da função Fundo.ordem_dicionario_base().
        id_system_origem : int
            Id do sistema originador. Ver na BL_Propriedades.
        dias_validade : int, optional
            Prazo antes que a ordem expire (se não analisada por Cliente ou DPM). The default is 14.

        Returns
        -------
        None.

        """
        cad_ativos = CadAtivosMini()
        om = OrderManager(id_system_origem=id_system_origem, id_tipo_portfolio=3)
        df_contas = self.contas_movimento_saldo()
        hoje = self.cartdm1.banco.hoje()
        data_val = hoje + relativedelta(days=dias_validade)
        for idx, row in df_ordens.iterrows():
            if row['Ordem']:
                # Dados do ativo
                ativo = cad_ativos.dados_ativo(row['AtivoGuid'])
                if ativo.empty:
                    ativo = Ativo(row['AtivoGuid'])
                    tipo_prod = ativo.tipo_produto
                else:
                    tipo_prod = ativo['TipoProduto']
                
                # Conforme o tipo de produto escolhe uma conta movimento
                linhas = []
                # Bancos generalistas:
                linhas.append(df_contas[df_contas['BancoId']=='e651b603-6f5f-de11-90dd-0003ffe6d283']) # BTG Pactual
                linhas.append(df_contas[df_contas['BancoId']=='f77cc9d5-bc57-df11-8868-0003ffe3d283']) # Banco Alfa
                linhas.append(df_contas[df_contas['BancoId']=='552ace58-de60-e211-a6d9-000c29c91374']) # XP Investimentos
                # Bancos com boletagem por produto
                if tipo_prod == 'COTAS':
                    linhas.append(df_contas[df_contas['BancoId']=='9ddb1d76-cd02-e011-82b1-d8d385b9752e']) # BEM DTVM                    
                elif tipo_prod in ['FII', 'ACAO', 'BOLSA']:
                    linhas.append(df_contas[df_contas['BancoId']=='657bf809-3d7c-de11-b8bd-0003ffe6d283']) # Ágora
                else: # Renda Fixa
                    linhas.append(df_contas[(df_contas['BancoId']=='3de2eeb2-565c-de11-90dd-0003ffe6d283') & (df_contas['new_agencia']=='2856')])
                
                if len(linhas) == 0:
                    campos_add = {}
                else:
                    linhas = pd.concat(linhas).sort_values('Saldo', ascending=False)
                    linha = linhas.iloc[0]
                    campos_add = {'ContaMovimento': linha['NomeContaMovimento'], 'GuidContaMovimento': linha['GuidContaMovimento']}
                
                om.inserir_ordem(id_system_destino=row['SysDestino'], guid_portfolio=row['GuidPortfolio'], ativo_guid=row['AtivoGuid'], 
                                 ativo_nome=row['AtivoNome'], tipo_mov=row['TipoMov'], resgate_total=row['ResgTot'],
                                 preboleta=False, data_ordem=hoje, data_validade=data_val, acao=row['Acao'], observacao=row['Observacao'],
                                 q_ou_f=row['QouF'], quantidade=row['Quantidade'], financeiro=row['Financeiro'],
                                 campos_adicionais=campos_add)
        mesmo_grupo = True
        if id_system_origem == 43:  
            # Ordens de enquadramento no limite de crédito podem ser analisadas separadamente
            mesmo_grupo = False
        om.upload_orders(mesmo_grupo=mesmo_grupo)
    

class EnquadramentoTitularidades:
    
    def __init__(self, homologacao:bool=False):
        self.dm1 = PosicaoDm1Pickle(homologacao=homologacao)
    
    def rodar(self, upload_orders:bool=True):
        lista_tit = self.dm1.titularidade_lista()
        relogio = Cronometro(verbose=True, nome_processo='Titularidades enquadramento')
        ordens = []
        for idx, row in lista_tit.iterrows():
            relogio.marca_tempo(f"{row['Titularidade']}, {idx} / {len(lista_tit)}")
            texto = row['TitularidadeGuid']
            try:
                tit = TitularidadeCredito(guid_titularidade=texto)    
                if tit.tem_dm1 and tit.pl > 0:
                    ordens.append(tit.desenq_solucionar())
            except:
                print('.........Erro na titularidade')
        relogio.marca_tempo('Exportando excel.')
        ordens = pd.concat(ordens)
        
        ordens.to_excel('PFs.xlsx')
        relogio.concluido()


if __name__ == '__main__':           
    # sc = SupercarteiraCredito('MaltaCons')
    # sc = SupercarteiraCredito('LancelotGPS')
    # fd = FundoCredito('_Maranhao FIM CrPr')
    # fd = FundoCredito('_Busca Vida FIM CrPr IE')
    # fd = FundoCredito('_Rhodes FIM CrPr')
    # fd = FundoCredito('_Calix FIM CrPr IE')
    # fd = FundoCredito('_AV5 FIM CrPr IE')
    # obj = FundoCredito(guid_conta_crm='c7ad94b0-55c3-ed11-8103-00505691fa1b');
    # fd.comprar_ofertas(enviar_ordens=True,perc_pl=0.050000,classe='R Fixa Pós',sub_classe='RF Pos Créd')
    # ordens = fd.desenq_solucionar(enviar_ordens=True)
    # ordens = fd.comprar_ofertas(perc_pl=0.1, classe='R Fixa Pós', enviar_ordens=True)
    # ordens = fd.reduzir_exposicao(perc_pl=0.05, classe='R Fixa Pós', enviar_ordens=False)
    # ordens.to_excel('Ordens.xlsx')
    # BookInterno().ofertas_disponiveis()
    enq = EnquadramentoFundos()
    enq.rodar(upload_orders=False)
    enq.df_ordens.to_excel('fundos.xlsx', index=False)
    # enq = EnquadramentoTitularidades()
    # enq.rodar(upload_orders=False)
    # obj = TitularidadeCredito(guid_titularidade='f9a187f3-d765-ed11-80fd-00505691fa1b')
    # obj.comprar_ofertas(enviar_ordens=True,perc_pl=0.050000,classe='R Fixa Pós', incluir_cotas=False)
    # obj = FundoCredito(guid_conta_crm='08026fbd-b4e1-e611-9231-005056912b96')
    # obj.comprar_ofertas(enviar_ordens=True,perc_pl=0.250000,classe='R Fixa Pós',sub_classe='RF Pos Créd', incluir_cotas=True)
    
    print('teste')
    
    