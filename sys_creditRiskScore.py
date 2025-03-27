import pandas as pd
import numpy as np
import xlwings as xw
import datetime
import calendar
import math
from datetime import datetime, timedelta
from databases import CreditManagement as CM
from filemanager import FileMgmt

class ScoreFunctions:
    # Aqui o frontend deveria ter uma seleção da lista dos emissores na Credit Management. Potencialmente até um botão 
    # de adicionar o emissor caso esse não exista (se eu não me engano isso já existe em outro lugar na CM - portanto é adaptar).
    # Isso é necessário, pois o Economatica identifica a empresa pelo Ticker, e não temos essa informação na base da Bawm (temos o
    # codigo CETIP na PO_Carteira, mas estaríamos refém do ticker em questão estar sendo investido por algum cliente. Não acho uma
    # boa via seguir dessa forma). Sendo assim, o código começa entendendo que o usuário selecionou corretamente a empresa.
    def __init__(self, homologacao:bool=False):
        self.path_original_arquivoEconomatica = r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Credito\RiskScore\1. ArquivosEconomatica"
        self.cm = CM(homologacao=homologacao)
    
    def ManuseiaArquivoEconomatica(self, FileNm:str, id_lim_cred:int, idfonte:int=151): #151 'Economatica'
        #Funcao para substituir a macro legado. Basicamente traduz as informações do arquivo oriundo do Economática. 
        
        dfFileOriginal = pd.read_csv(self.path_original_arquivoEconomatica + '\\' + "{}".format(FileNm), on_bad_lines="skip", header = 2, sep=";", encoding='windows-1252')   
        company_ticker = dfFileOriginal.iloc[0][1]
        for col in dfFileOriginal.columns:
            tam = len(col)
            i = 0
            while i in range(tam):
                if col[i] == '|':
                    new_num = i
                    new_column = col[0: new_num]
                    dfFileOriginal = dfFileOriginal.rename(columns={col: new_column})
                i=i+1
        dfFileOriginal.insert(0, 'CompanyTicker', company_ticker)
                
        dfFileOriginal.insert(0, 'IdLimCred', [id_lim_cred] * len(dfFileOriginal)) 
        dfFileOriginal = dfFileOriginal.iloc[1:]
        
        ### prepara dataframes a parte p/ merge ###

        ### datas (economatica padrao data NumeroTrimestreTYYYY, ex: 4T2023, codigo muda para 2023-12-31 ###
        dfFileOriginal['DataFormat1'] = dfFileOriginal['Data'].astype(str).str[0]
        dfFileOriginal['DataFormat2'] = dfFileOriginal['Data'].astype(str).str[2:6]
        dfFileOriginal['DataFormat2b'] = [int((str(x)[0:4])) * 1 for x in dfFileOriginal['DataFormat2']]
        dfFileOriginal['DataFormat3'] = [int((str(x)[0])) * 3 for x in dfFileOriginal['DataFormat1']]
        yr = []
        mth = []
        datas = []
        for i in dfFileOriginal['DataFormat2b']:
            yr.append(i)
        for i in dfFileOriginal['DataFormat3']:
            mth.append(i)
        i = 0
        while i <= len(yr) - 1:
            dtx = datetime(yr[i], mth[i], 1)
            dtx2 = dtx.replace(day=28) + timedelta(days=4)
            dtx3 = dtx2 - timedelta(days = dtx2.day)
            datas.append(dtx3)
            i = i + 1
        dfFileOriginal['DataFormat'] = datas
        dfFileOriginal['Data'] = dfFileOriginal['DataFormat']
        ### RubricaIDs ###
        df_base_RubricaID = self.cm.lista_rubricasIDs()
        df_base_RubricaID = df_base_RubricaID[['IdTipo', 'Tipo']]
        ### deixa o df file original de uma forma mais amigavel p/ merges ###
        dfFileOriginal = dfFileOriginal.melt(id_vars=['IdLimCred', 'Data'], var_name= 'Tipo', value_name= 'Valor')  # id_vars=['GuidLimite', 'Data']
        ### merges ###                
        dfFileOriginal = dfFileOriginal.merge(df_base_RubricaID, on = 'Tipo')
        dfFileOriginal['Data'] = pd.to_datetime(dfFileOriginal['Data'], errors = 'coerce')
        dfFileOriginal['Data'] = dfFileOriginal['Data'].dt.date 
        dfFileOriginal = dfFileOriginal.rename(columns={'Data': 'DataCompetencia'}) 
        dfFileOriginal = dfFileOriginal[['IdLimCred', 'DataCompetencia', 'IdTipo', 'Tipo', 'Valor']]
        datas = dfFileOriginal['DataCompetencia'].unique()
        for data_competencia in datas:            
            df_dados = dfFileOriginal[dfFileOriginal['DataCompetencia'] == data_competencia]
            df_dados = df_dados.drop(columns=['IdLimCred', 'DataCompetencia', 'Tipo'])
            df_dados['Valor'] = df_dados['Valor'].apply(lambda x: x.replace(',', '.'))
            df_dados = df_dados.replace('-', None)
            df_dados['Valor'] = df_dados['Valor'].fillna(0)
            self.cm.statement_inserir_alterar(df_dados=df_dados, id_lim_cred=id_lim_cred, data_competencia=data_competencia, idfonte=idfonte)
        
        try:
            FileMgmt.file_copy(source_filename=self.path_original_arquivoEconomatica + '\\' + FileNm,
                               destination_filename=self.path_original_arquivoEconomatica + '\\Uploads_Realizados\\' + FileNm)
        except:
            print('erro ao mover arquivo')
    
    def CalculaScore(self, id_statement:int, df_statement:pd.DataFrame):
        #a ideia aqui é buscar apenas o que for pertinente na detalhe, por isso o filtro abaixo
        #Via front end o usuario selecionará o papel (que estará linkado com o Idlimcred e a data inicial (automaticamente a datafim deveria ser 3 trimestres p/ frente (ou a inicial 3 para trás)))                
        id_lim_cred = df_statement.iloc[0]['IdLimCred']
        data_fim = df_statement[df_statement['IdStatement']==id_statement].iloc[0]['DataCompetencia']
        
        self.lista_lim_statement = df_statement[df_statement['DataCompetencia']<=data_fim].copy()
        if len(self.lista_lim_statement) < 4:
            raise Exception(f'Número de balanços insuficiente para calcular Score (encontrados {len(self.lista_lim_statement)}).')
        self.lista_lim_statement.sort_values('DataCompetencia', ascending=False)[1:4]        
        data_ini = self.lista_lim_statement['DataCompetencia'].min()
        
        string = '('
        for i in self.lista_lim_statement['IdStatement']:
            string = string + "'" + str(i) + "',"
        string = string[0:-1] + ')'

        df_DRE_adj = self.cm.lista_PO_LimCred_StatementDetalhe(string) 
        df_DRE_adj['ValorAjuste'] = df_DRE_adj['ValorAjuste'].fillna(math.nan) # None gerava problemas
        df_DRE_adj['ValorFinal'] = np.where(df_DRE_adj['ValorAjuste'].apply(lambda x: math.isnan(x)) == True, df_DRE_adj['Valor'], df_DRE_adj['ValorAjuste'])
        # df_DRE_adj['DataCompetencia'] = df_DRE_adj['DataCompetencia'].dt.date 
        DtFim_dtfmt = data_fim # datetime.strptime(data_fim, '%Y-%m-%d').date()
        
        ### métricas do score ###
        #01 Patrimônio líquido
        df_pl = df_DRE_adj[(df_DRE_adj['IdTipo'] == 89) & (df_DRE_adj['DataCompetencia'] == DtFim_dtfmt)]
        PL = df_pl['ValorFinal'].sum()
        
        #02 MarketCap
        df_mktcap = df_DRE_adj[(df_DRE_adj['IdTipo'] == 92) & (df_DRE_adj['DataCompetencia'] == DtFim_dtfmt)]
        MktCap = df_mktcap['ValorFinal'].sum() / 1000
        
        #03 Revenue
        df_revenue = df_DRE_adj[(df_DRE_adj['IdTipo'] == 95)]
        Revenue = df_revenue['ValorFinal'].sum()
        
        #04 Total Debt / (Total Debt + Equity)
        #04a Total Debt
        df_td = df_DRE_adj[(df_DRE_adj['IdTipo'] == 90) & (df_DRE_adj['DataCompetencia'] == DtFim_dtfmt)]
        TotalDebt = df_td['ValorFinal'].sum() / 1000
        #
        td_div_tdpluste = TotalDebt / (TotalDebt + PL)
        
        #05 Total Debt / EBITDA
        #05a EBITDA
        df_ebitda = df_DRE_adj[(df_DRE_adj['IdTipo'] == 103)]
        ebitda = df_ebitda['ValorFinal'].sum() / 1000
        #
        td_div_ebitda = TotalDebt / ebitda
        
        #06 NetDebt / (Total Debt + Equity)
        #06a Divida total líquida
        df_dtl = df_DRE_adj[(df_DRE_adj['IdTipo'] == 91) & (df_DRE_adj['DataCompetencia'] == DtFim_dtfmt)]
        dtl = df_dtl['ValorFinal'].sum() / 1000       
        #
        netdebt_div_tdpluste = dtl / (TotalDebt + PL)
        
        #07 NetDebt / EBITDA
        netdebt_div_ebitda = dtl / ebitda        
        
        #08 EBIT/-NetInterest
        #08a Net Income
        df_netincome = df_DRE_adj[(df_DRE_adj['IdTipo'] == 107)]
        netincome = df_netincome['ValorFinal'].sum()      
        #08b EBT
        df_ebt = df_DRE_adj[(df_DRE_adj['IdTipo'] == 101)]
        ebt = df_ebt['ValorFinal'].sum()
        #08c Taxes
        taxes = netincome - ebt
        #08d EBIT
        df_ebit = df_DRE_adj[(df_DRE_adj['IdTipo'] == 102)]
        ebit = df_ebit['ValorFinal'].sum()        
        #08e NetFinancialResult
        net_financial_result = netincome - ebit - taxes
        #
        EBIT_div_MinusNetInterest =  ebit / (net_financial_result * -1)
        
        #09 CFO / TotalDebtService
        #09a CFO
        df_cfo = df_DRE_adj[(df_DRE_adj['IdTipo'] == 108)]
        cfo = df_cfo['ValorFinal'].sum() / 1000     
        #09b FinPag
        df_finpag = df_DRE_adj[(df_DRE_adj['IdTipo'] == 113)]
        finpag = df_finpag['ValorFinal'].sum() / 1000     
        #09c DesFin
        df_desfin = df_DRE_adj[(df_DRE_adj['IdTipo'] == 105)]
        desfin = df_desfin['ValorFinal'].sum() / 1000   
        #09d TotalDebtService
        totaldebtservice = ((finpag + desfin) * -1)
        #
        cfo_div_totaldebtservice = cfo / totaldebtservice
        
        #10 EBT_div_Revenue
        ebt_div_revenue = (ebt / 1000) / Revenue
        
        #11 Rating
        self.lista_lim_rating = self.cm.lista_LimRating(id_lim_cred)
        listaRating = self.lista_lim_rating
        DtIniMinus2Yrs = data_ini - timedelta(720)
        if len(listaRating) > 0:
            # listaRating['DataRef'] = listaRating['DataRef'].dt.date 
            listaRating = listaRating[(listaRating['DataRef'] >=  DtIniMinus2Yrs)]
            #pegar só o mais recente do período
            listaRating = listaRating.sort_values('DataRef').groupby('IdAgencia').tail(1)
            #buscando o caractere do _
            listaRating['Nota'] = listaRating['Nota'].apply(lambda x: x if '_' in x else f"{x}_[s]" )
            listaRating['Rating1'] = listaRating['Nota'].str.find("_")            
            #separando o rating da perspectiva |  #perspectivas [p] = positiva, [n] = negativa, [s] = stable
            listaRating['Rating'] = listaRating.apply(lambda x: x['Nota'][0:x['Rating1']], axis=1)
            listaRating['Perspectiva'] = listaRating.apply(lambda x: x['Nota'][x['Rating1']+1:], axis=1)
            #traduzindo perspectiva em numero 
            conditions = [listaRating['Perspectiva'] == '[s]', listaRating['Perspectiva'] == '[p]', listaRating['Perspectiva'] == '[n]']
            choices = [0, 0.1, -0.1]
            listaRating['ConversaoPerspectiva'] = np.select(conditions, choices, default = np.nan)
            listaRating['key'] = listaRating['Rating'].astype(str) + "_" + listaRating['IdAgencia'].astype(str)
            #buscando no excel enquanto nao tem tabela na Bawm
            df_ratingNbConverter = pd.read_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Credito\RiskScore\SubirServidor\NewDb_AgRating.xlsx")
            df_ratingNbConverter['key'] = df_ratingNbConverter['Rating'].astype(str) + "_" + df_ratingNbConverter['AgRatingId'].astype(str)
            listaRating = listaRating.merge(df_ratingNbConverter, on = 'key')        
            listaRating['NotaFinal'] = listaRating['Nota_y'] + listaRating['ConversaoPerspectiva']
            qtd_ag = len(listaRating)
            if qtd_ag == 3:
                penalty = 0
            elif qtd_ag == 2:
                penalty = 0.02
            elif qtd_ag == 1:
                penalty = 0.03
            NotaFinal = listaRating['NotaFinal'].mean() - penalty
        else:
            NotaFinal = 0
        
        #consolidando valores em um dataframe final 
        data_to_df = [[PL, MktCap, Revenue, td_div_tdpluste, td_div_ebitda, netdebt_div_tdpluste, netdebt_div_ebitda, EBIT_div_MinusNetInterest, cfo_div_totaldebtservice,
                       ebt_div_revenue]]
        
        df_scorebased = pd.DataFrame(data_to_df, columns=['Equity', 'MarketCap', 'Revenue', 'TotalDebt_div_TotalDebtPlusTotalEquity', 'TotalDebt_div_EBITDA', 'NetDebt_div_TDplusTE',
                                                          'NetDebt_div_EBTIDA', 'EBIT_div_MinusNetInterest', 'CFO_div_TotalDebtService', 'EBT_div_Revenue'])
        
        #criando dataframe de parametro p/ regressao linear (isso aqui eu entendo que poderia ser uma tabela com ligação no front end p/ credito atualizar os valores)
        df_parametro = pd.DataFrame({
                                    'Parametro' : ['Alfa', 'Beta', 'Peso'],
                                    'Equity' : [0.79, 0.66, 0.1],
                                    'MarketCap' : [0.91, -1.36, 0.1],
                                    'Revenue' : [0.91, -0.96, 0.05],
                                    'TotalDebt_div_TotalDebtPlusTotalEquity' : [-3.42, 5.12, 0.05],
                                    'TotalDebt_div_EBITDA' : [-2.09, 9.95, 0.1],
                                    'NetDebt_div_TDplusTE' : [-2.19, 5.46, 0.05],
                                    'NetDebt_div_EBTIDA' : [-1.66, 9.03, 0],
                                    'EBIT_div_MinusNetInterest' : [1.06, 6.01, 0.1],
                                    'CFO_div_TotalDebtService' : [0.82, 7, 0.1],
                                    'EBT_div_Revenue' : [0.78, 9.54, 0.15],
                                    'Rating' : [1,0,0.2]
                                    })
        
        frames = [df_scorebased, df_parametro]
        result = pd.concat(frames, ignore_index = True)
        result = result.T
        result = result[(result.index != 'Parametro')]
        result = result.rename(columns={result.columns[0]: "Valor", result.columns[1]: "Alfa", result.columns[2]: "Beta", result.columns[3]: "Peso"})
        result['Valor'] = result['Valor'].fillna(0)
        result['Index'] = result.index
        #aqui pode buscar a ptax pelo bds (/nova base de ativos) ou deixar que o analista digite isso via tela - deixei como 5.5 por enquanto.
        result['ScoreOriginal'] = np.where(result['Index'].apply(lambda x: x in ('Equity', 'Revenue', 'MarketCap')) == True, np.log(result['Valor'] / float(5.5)) * result['Alfa'] + result['Beta'], np.log(result['Valor']) * result['Alfa'] + result['Beta'])
        result['ScoreOriginal'] = result['ScoreOriginal'].fillna(0)
        result['ScoreMin'] = np.where(result['ScoreOriginal'].apply(lambda x: x < 10) == True, result['ScoreOriginal'], 10)
        result['Score'] = np.where(result['ScoreMin'].apply(lambda x: x > 0) == True, result['ScoreMin'], 0)
        
        result['Tipo'] = result.index
        result.loc[result.Tipo == 'Rating', 'Valor'] = NotaFinal
        result.loc[result.Tipo == 'Rating', 'Score'] = NotaFinal
        result = result[['Tipo', 'Valor', 'Score', 'Peso']]
        
        df_base_RubricaID = self.cm.lista_rubricasIDs()
        df_base_RubricaID = df_base_RubricaID[(df_base_RubricaID['ItemScoreCred'] == 1)] 
        df_base_RubricaID = df_base_RubricaID[['IdTipo', 'Tipo']]
        result = result.merge(df_base_RubricaID, on = 'Tipo')
        scorecalc = result.copy()
        scorecalc['Mult'] = scorecalc['Peso'] * scorecalc['Score']
        score = scorecalc['Mult'].sum()
        score_row = {'Tipo' : 'Score', 'Valor' : score, 'Score': score, 'Peso' : 1, 'IdTipo': 150}
        result.loc[len(result)] = score_row
        
        for i in result['Valor']:
            #criar uma variavel no front end para ver qual eh o idtipofonte
            idfonte = 153 # JBB
            df_dados = result
            df_dados = df_dados.drop(columns=['Score', 'Peso', 'Tipo'])            
            df_dados = df_dados.replace('-', None)
            df_dados = df_dados[['IdTipo', 'Valor']]
            self.cm.statement_inserir_alterar(df_dados=df_dados, id_lim_cred=id_lim_cred, idfonte=idfonte, id_statement=id_statement)

if __name__ == '__main__':  
    # x = ScoreFunctions().ManuseiaArquivoEconomatica('SAPR11.txt', id_lim_cred=1676)                
    x = ScoreFunctions().CalculaScore(560 , CM().lista_PO_LimCred_Statement(1604))