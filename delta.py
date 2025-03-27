import pandas as pd
from databases import BDS, Bawm
from datetime import date,timedelta
bds = BDS()
from funcoes_datas import FuncoesDatas
data = FuncoesDatas()
import xlwings as xw
from datetime import date,datetime

import math
import copy
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import norm

bds = BDS()
bawm = Bawm()


class Option:
    def __init__(self, type, strike, dividend, volatility,
                 stock_price, interest_rate, time_to_maturity,over_foward = True):
        self.type = type
        self.strike = strike
        self.dividend = dividend
        self.volatility = volatility
        self.stock_price = stock_price
        self.interest_rate = interest_rate
        self.time_to_maturity = time_to_maturity
        self.over_foward = over_foward

    ############################################################################
    def __repr__(self):
        return ("Option: {"
            + "\n  type: " + str(self.type)
            + "\n  strike: " + str(self.strike)
            + "\n  dividend: " + str(self.dividend)
            + "\n  volatility: " + str(self.volatility)
            + "\n  stock_price: " + str(self.stock_price)
            + "\n  interest_rate: " + str(self.interest_rate)
            + "\n  time_to_maturity: " + str(self.time_to_maturity)
            + "\n  d1: " + str(self.d1)
            + "\n  d2: " + str(self.d2)
            + "\n  delta: " + str(self.delta)
            + "\n  gamma: " + str(self.gamma)
            + "\n  vega: " + str(self.vega)
            + "\n  theta: " + str(self.theta)
            + "\n  price: " + str(self.price) + "\n}"
        )

    ############################################################################
    def reset(self):
        self._d1 = None
        self._d2 = None
        self._price = None
        self._delta = None
        self._gamma = None
        self._vega = None
        self._theta = None
        self._parity = None

    ############################################################################
    # Plot                                                                     #
    ############################################################################
    @staticmethod
    def plot(options, x_axis, y_axis, x_start, x_end, x_samples, save_to):
        is_inverted = False
        if (x_end < x_start):
            x_start, x_end = x_end, x_start
            is_inverted = True

        x_values = np.linspace(x_start, x_end, x_samples)
        y_values = {}

        for (i, option) in enumerate(options):
            option = copy.copy(option)
            key = str(i+1) + ". " + option.type
            y_values[key] = []
            for x in x_values:
                setattr(option, x_axis, x)
                y = getattr(option, y_axis)
                y_values[key].append(y)

        df = pd.DataFrame(data=y_values, index=x_values)

        fig = df.plot(style=[
            ("-" if (option.type == "CALL") else "--") for option in options
        ])
        if (is_inverted): fig.invert_xaxis()
        plt.title("Options: " + x_axis + " x " + y_axis)
        plt.xlabel(x_axis)
        plt.ylabel(y_axis)
        plt.grid()
        plt.legend(loc=(1.05, 0.5))
        plt.tight_layout()
        plt.savefig(save_to)

    ############################################################################
    # Inputs                                                                   #
    ############################################################################
    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        if (value != "CALL" and value != "PUT"):
            raise ValueError("Invalid option type!")
        self._type = value
        self.reset()

    ############################################################################
    @property
    def strike(self):
        return self._strike

    @strike.setter
    def strike(self, value):
        if (value <= 0):
            raise ValueError("Invalid strike!")
        self._strike = value
        self.reset()

    ############################################################################
    @property
    def dividend(self):
        return self._dividend

    @dividend.setter
    def dividend(self, value):
        if (value < 0):
            raise ValueError("Invalid dividend!")
        self._dividend = value
        self.reset()

    ############################################################################
    @property
    def volatility(self):
        return self._volatility

    @volatility.setter
    def volatility(self, value):
        if (value <= 0):
            raise ValueError("Invalid volatility!")
        self._volatility = value
        self.reset()

    ############################################################################
    @property
    def stock_price(self):
        return self._stock_price

    @stock_price.setter
    def stock_price(self, value):
        if (value < 0):
            raise ValueError("Invalid stock price!")
        self._stock_price = value
        self.reset()

    ############################################################################
    @property
    def interest_rate(self):
        return self._interest_rate

    @interest_rate.setter
    def interest_rate(self, value):
        if (value < 0):
            raise ValueError("Invalid interest rate!")
        self._interest_rate = value
        self.reset()

    ############################################################################
    @property
    def time_to_maturity(self):
        return self._time_to_maturity

    @time_to_maturity.setter
    def time_to_maturity(self, value):
        if (value < 0):
            raise ValueError("Invalid time to maturity!")
        self._time_to_maturity = value
        self.reset()

    ############################################################################
    # Outputs                                                                  #
    ############################################################################
    @property
    def d1(self):
        if (self._d1 is None): self.calculate_d1()
        return self._d1
    
    
    def calculate_d1(self):
        if self.over_foward == False:           
            S = self.stock_price
            K = self.strike
            T = self.time_to_maturity
            r = self.interest_rate
            q = self.dividend
            sigma = self.volatility
            if (S == 0 or T == 0):
                self._d1 = float("NaN")
            else:
                self._d1 = (math.log(S/K) + (r - q + (sigma**2)/2)*T) / (sigma*math.sqrt(T))
        
        if self.over_foward == True:           
            S = self.stock_price
            K = self.strike
            T = self.time_to_maturity
            r = math.log((self.interest_rate+1)**(T/252))/T
            sigma = self.volatility/math.sqrt(252)
            if (S == 0 or T == 0):
                self._d1 = float("NaN")
            else:
                self._d1 = (math.log(S/K) + ((sigma**2)/2)*T) / (sigma*math.sqrt(T))                
    ############################################################################
    @property
    def d2(self):
        if (self._d2 is None): self.calculate_d2()
        return self._d2

    def calculate_d2(self):
        T = self.time_to_maturity
        sigma = self.volatility
        self._d2 = self.d1 - sigma*math.sqrt(T)

    ############################################################################
    @property
    def delta(self):
        if (self._delta is None): self.calculate_delta()
        return self._delta

    def calculate_delta(self):
        S = self.stock_price
        T = self.time_to_maturity
        q = self.dividend
        if (S == 0 or T == 0):
            self._delta = float("NaN")
        else:
            if (self.type == "CALL"):
                self._delta = math.exp(-q*T)*norm.cdf(self.d1)
            else: # PUT
                self._delta = -math.exp(-q*T)*norm.cdf(-self.d1)

    ############################################################################
    @property
    def gamma(self):
        if (self._gamma is None): self.calculate_gamma()
        return self._gamma

    def calculate_gamma(self):
        S = self.stock_price
        T = self.time_to_maturity
        q = self.dividend
        sigma = self.volatility
        if (S == 0 or T == 0):
            self._gamma = float("NaN")
        else:
            self._gamma = math.exp(-q*T)*norm.pdf(self.d1) \
                        / (sigma*S*math.sqrt(T))

    ############################################################################
    @property
    def vega(self):
        if (self._vega is None): self.calculate_vega()
        return self._vega

    def calculate_vega(self):
        S = self.stock_price
        T = self.time_to_maturity
        q = self.dividend
        self._vega = math.exp(-q*T)*S*math.sqrt(T)*norm.pdf(self.d1)

    ############################################################################
    @property
    def theta(self):
        if (self._theta is None): self.calculate_theta()
        return self._theta

    def calculate_theta(self):
        S = self.stock_price
        K = self.strike
        T = self.time_to_maturity
        r = self.interest_rate
        q = self.dividend
        sigma = self.volatility
        if (S == 0 or T == 0):
            self._theta = float("NaN")
        else:
            if (self.type == "CALL"):
                self._theta = -math.exp(-q*T)*S*norm.pdf(self.d1)*sigma \
                            / (2*math.sqrt(T)) \
                            + q*math.exp(-q*T)*S*norm.cdf(self.d1) \
                            - r*K*math.exp(-r*T)*norm.cdf(self.d2)
            else: # PUT
                self._theta = -math.exp(-q*T)*S*norm.pdf(self.d1)*sigma \
                            / (2*math.sqrt(T)) \
                            - q*math.exp(-q*T)*S*norm.cdf(-self.d1) \
                            + r*K*math.exp(-r*T)*norm.cdf(-self.d2)

    ############################################################################
    @property
    def price(self):
        if (self._price is None): self.calculate_price()
        return self._price

    def calculate_price(self):
        S = self.stock_price
        K = self.strike
        T = self.time_to_maturity
        r = self.interest_rate
        q = self.dividend
        if (self.type == "CALL"):
            if (S == 0):
                self._price = 0
            else:
                if (T == 0):
                    self._price = max(S - K, 0)
                else:
                    self._price = math.exp(-q*T)*S*norm.cdf(self.d1) \
                                - math.exp(-r*T)*K*norm.cdf(self.d2)
        else: # PUT
            if (S == 0):
                self._price = K*math.exp(-r*T)
            else:
                if (T == 0):
                    self._price = max(K - S, 0)
                else:
                    self._price = math.exp(-r*T)*K*norm.cdf(-self.d2) \
                                - math.exp(-q*T)*S*norm.cdf(-self.d1)

    ############################################################################
    @property
    def parity(self):
        if (self._parity is None): self.calculate_parity()
        return self._parity

    def calculate_parity(self):
        parity = copy.copy(self)
        parity._type = "PUT" if (self.type == "CALL") else "CALL"
        parity._price = None
        parity._delta = None
        parity._theta = None
        parity._parity = self
        self._parity = parity

#ativo_bds = bds.serie_cadastro('22602225')


@xw.func
def opção_delta(cod,qtd,dia):
    hoje = date.today()
    #dia= data.workday(data = data.hoje(),n_dias = -1,feriados_brasil=True, feriados_eua=False)
    dia = datetime.date(dia)
    #ativo_bds = bds.serie_cadastro('22602225')
    ativos_info = bawm.dados_opcoes(cod)
    cod_bds = ativos_info['BloombergTk'].values[0]
    cod_bds = cod_bds.replace('BDS:','')
    cod_bds = int(cod_bds)
    
    if 'IBOV' in cod:
        preco_vol = bds.preco_vol_opt_rv(cod_bds,dia)
        if preco_vol.empty:
           dia= data.workday(data = dia,n_dias = -1,feriados_brasil=True, feriados_eua=False)
           preco_vol = bds.preco_vol_opt_rv(cod_bds,dia)
        else:
            dia=dia
        ativo_objeto = ativos_info['FutObj'].values[0]
        futuros = bawm.dados_futuro(ativo_objeto,dia)
        over_foward = False
        time_to_maturity = (futuros['DU'].values[0])/252
        volatility = preco_vol['vol'].values[0]
        volatility = volatility/100
        
    if 'DOL' in cod:
        preco_vol = bds.preco_vol_opt_dol(cod_bds,dia)
        ativo_objeto = ativos_info['FutObj'].values[0]
        futuros = bawm.dados_futuro(ativo_objeto,dia)        
        over_foward = True
        time_to_maturity = (futuros['DU'].values[0])
        volatility = preco_vol['vol'].values[0]/100
    
    
    CDI = bawm.in_historico(index_cod ='CDITX',data_ini=dia, data_fim=hoje)
    cdi_atual = CDI['Valor'].values[0]    
    tipo = ativos_info['TipoOpt'].values[0]
    tipo = tipo.strip()
    strike = ativos_info['Strike'].values[0]
    dividend = 0
    stock_price = futuros['Preco'].values[0]
    time_to_maturity = time_to_maturity 
    interest_rate = cdi_atual/100
        
    
    option = Option(
        type = tipo,
        strike = strike,
        dividend = dividend,
        volatility = volatility,
        stock_price = stock_price,
        interest_rate = interest_rate,
        time_to_maturity = (time_to_maturity),
        over_foward = over_foward
        
    )
    
    return (option.delta)*qtd*stock_price

@xw.func
def duration_modificada(cod):
   hoje = date.today()
   hoje = data.workday(data = data.hoje(),n_dias = -1,feriados_brasil=True, feriados_eua=False)
   duration = bawm.futuros_historico(fut_cod =cod, data_ini = hoje, data_fim = hoje)
   duration = (duration['duration_modificada'].iloc[-1]*252)/((taxa_pre(cod,hoje)/100)+1)
   return duration

@xw.func  
def taxa_pre(cod,data):
    taxa = bawm.futuros_historico(fut_cod =cod, data_ini = data, data_fim = data)
    taxa = taxa['Taxa'].iloc[-1]
    return taxa

@xw.func      
def risco_ativo(fundo, index):

    index = index.replace('CDI','CETIPACC').replace('IDA_IPCA','IDA-IPCAIN').replace('IBOVESPA','Ibvsp')
    hoje = data.workday(data = data.hoje(),n_dias = -3,feriados_brasil=True, feriados_eua=False)
    data_inicial = data.workday(data = data.hoje(),n_dias = -252,feriados_brasil=True, feriados_eua=False)    
    bench =bawm.in_historico(index_cod=index, data_ini=data_inicial, data_fim=hoje)
    bench =  bench[['Data','Valor']]
    bench['Rent_bench'] = (bench['Valor']/ bench ['Valor'].shift(1))-1
    
    #Rentabilidade do fundo
    cadastro = bawm.po_cadastro()
    cod_fundo = cadastro.loc[cadastro['NomeContaCRM']==fundo].iat[0,13]
    cota = bds.serie_historico(idserie =cod_fundo ,data_ini= data_inicial, data_fim = hoje)
    cota = cota[['dataser','a8']].rename(columns = {'dataser':'Data','a8':'Cota'})
    cota['Rent_cota'] = (cota['Cota']/ cota['Cota'].shift(1))-1
    
    #Juntando as tabelas
    df = pd.merge(left = cota,right = bench, on = 'Data', how='left')
    
    #Calculando a diferença de retornos
    TE_Fundo = (df['Rent_cota']-df['Rent_bench']).std()
    TE_Fundo = TE_Fundo*(252**0.5)
    Vol_Index = bench['Rent_bench'].std()*(252**0.5)
    Risco_ativo = TE_Fundo / Vol_Index
    
    return Risco_ativo, TE_Fundo


if __name__ == '__main__':
    #print(risco_ativo('_JBFO Alocacao Supra FI Invest Infraestrutura RF','IDAIPCAIN')[0])
      dia=datetime(2023,12,29)  
      oi = opção_delta('IBOVB138_2024',5,dia)
      print(oi)
    
