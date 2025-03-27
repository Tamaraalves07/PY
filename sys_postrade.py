import pandas as pd
import datetime
from databases import Bawm, PosicaoDm1Pickle, Crm, PosicaoDm1
from sys_pretrade import RegrasFundo, PreTrade

class RegrasFundosPosT(RegrasFundo):   
    def __init__(self, df_carteira_simulada, pl:float, limite_risk_score:float, multiplicador_credito:float, limites_credito, limites_credito_exc, df_ordens:pd.DataFrame, tipo_investidor=None, obj_portfolio=None, homologacao=False):
        super().__init__(df_carteira_simulada=df_carteira_simulada, pl=pl, limite_risk_score=limite_risk_score, limites_credito=limites_credito, limites_credito_exc=limites_credito_exc,
                         df_ordens=df_ordens, homologacao=homologacao, tipo_investidor=tipo_investidor, obj_portfolio=obj_portfolio)

class PosTrade(PreTrade):
    def __init__(self, data_pos=None, base_dm1=None, homologacao:bool=False):       
        super().__init__(data_pos=data_pos, pos_trade=True, base_dm1=base_dm1, homologacao=homologacao)
        self.data_pos = data_pos
        self.crm = Crm(single_asset=False)
        
    def obter_carteira_simulada_x(self, df_posicao_dm1, id_tipo_portfolio=None, ordens:pd.DataFrame=pd.DataFrame()):
        posicao_adj = super().obter_carteira_simulada(df_posicao_dm1=df_posicao_dm1, id_tipo_portfolio=id_tipo_portfolio, ordens=ordens)   
        # Adicionar tratamentos adicionais
        return posicao_adj
    
    def RodarPosTradeEmLote(self, id_tipo_portfolio=None, guid_portfolio=None):
        x = PosTrade().verificar_enquadramento(id_tipo_portfolio=1, guid_portfolio=guid_portfolio)   
        return x
    
    def rodar(self, testes:bool=False):
        baseCRM = self.dm1.lista_fundos_com_idcarteira(gestor_jbfo=True)
        df = []
        if testes:
            baseCRM = baseCRM.sample(n=10)
        for idx, row in baseCRM.iterrows():
            conta = row['GuidContaCRM']
            print(f"Rodando... {row['NomeContaCRM']}")
            postd = self.verificar_enquadramento(id_tipo_portfolio=1, guid_portfolio = conta)
            df_postd = self.verificacao.to_frame(name='StatusPosTrade')
            df_postd = df_postd.reset_index()
            df_postd = df_postd.rename(columns={'index' : 'Regra'})
            df_Nm = baseCRM[(baseCRM['GuidContaCRM'] == conta)]
            nm = df_Nm['NomeContaCRM'].iloc[0]
            df_postd.insert(0, 'Fundo', nm)
            df.append(df_postd)
        
        df = pd.concat(df).reset_index()
        nome_arquivo = f"RelatPosTrade_{self.data_pos.strftime('%Y-%m-%d')}.xlsx"
        pasta = r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos"
        df.to_excel(pasta + '\\' + nome_arquivo)

if __name__ == '__main__':
    retroativo = True
    if retroativo:
        dm1 = PosicaoDm1()
        data_pos = datetime.datetime.now()
    else:
        dm1 = None
        data_pos = None
    pret = PosTrade(data_pos=data_pos, base_dm1=dm1)  
    
    pret.dm1=dm1
    pret.rodar(testes=False)
    
    