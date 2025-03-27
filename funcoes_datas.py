import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from databases import Bawm
# Não importar no databases.py!!!!


class FuncoesDatas:

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __init__(self, datas_completas=False, homologacao=False, **kwargs):
        # datas_completas
        # se falso, pega apenas 3 anos para trás e para frente da tabela de feriados
        # 2. Dias da semana
        (MON, TUE, WED, THU, FRI, SAT, SUN) = range(7)
        self.weekends = (SAT, SUN)
        self.param_texto = None
        self.param_data = None
        self.param_feriados_br = True
        self.param_feriados_us = False
        self.param_inc = 0
        self.lista_feriados = []
        self.param_datapagto = False
        # 0. Conecta
        self.dados = Bawm(homologacao=homologacao)
        # 1. Busca feriados da base
        self.feriados_ini = False
        self.datas_completas = datas_completas
        self.feriados_br = pd.DataFrame() #  self.dados.feriados_br()
        self.feriados_us = pd.DataFrame() # self.dados.feriados_us()
        self.feriados = []

    def __iniciar_feriados__(self):
        if self.feriados_ini:
            return        
        self.feriados_br = self.dados.feriados_br(completo=self.datas_completas)
        self.feriados_us = self.dados.feriados_us()
        self.feriados_ini = True

    @staticmethod
    def hoje(retornar_datetime=True):
        if retornar_datetime:
            data = datetime.now()
            data = data.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            data = datetime.now().date()        
        return data

    @staticmethod
    def __forca_date(data):
        c = getattr(data, 'date', None)
        if callable(c):
            valor = data.date()
        else:
            valor = data
        return valor

    def workday(self, data, n_dias, feriados_brasil=True, feriados_eua=False):
        self.__iniciar_feriados__()
        dia = data
        if n_dias == 0:
            return dia
        elif n_dias > 0:
            i = 1
            while i <= n_dias:
                dia = dia + timedelta(days=1)
                if not dia.weekday() in self.weekends:
                    if feriados_brasil or feriados_eua:
                        if feriados_brasil and feriados_eua:
                            if dia not in self.feriados_br.index and dia not in self.feriados_us.index:
                                i = i + 1
                        elif feriados_brasil:
                            if dia not in self.feriados_br.index:
                                i = i + 1
                        elif feriados_eua:
                            if dia not in self.feriados_us.index:
                                i = i + 1
                    else:
                        i = i + 1
        else:
            i = 1
            while i <= np.abs(n_dias):
                dia = dia + timedelta(days=-1)
                if not dia.weekday() in self.weekends:
                    if feriados_brasil or feriados_eua:
                        if feriados_brasil and feriados_eua:
                            if dia not in self.feriados_br.index and dia not in self.feriados_us.index:
                                i = i + 1
                        elif feriados_brasil:
                            if dia not in self.feriados_br.index:
                                i = i + 1
                        elif feriados_eua:
                            if dia not in self.feriados_us.index:
                                i = i + 1
                    else:
                        i = i + 1
                    
        return dia

    def workday_range(self, data_ini, data_fim, feriados_brasil= True, feriados_eua= False):
        datas = [] 
        
        k = 0
        dk = data_ini
        
        while dk.date() <= data_fim:
            datas.append(dk)
            k+=1
            dk = self.workday(data_ini, k, feriados_brasil= feriados_brasil, feriados_eua= feriados_eua)
            datas.append(dk)
            
        datas = sorted(list(set(datas)))
        
        return datas

    def verificar_du(self, data, forcar_du_seguinte:bool=False):
        """
        Recebe uma data, formato datetime.datetime

        Parameters
        ----------
        data : datetime.datetime
            Data a testar.
        forcar_du_seguinte : bool, optional
            Se a data não for dia útil, traz dia seguinte. The default is False.

        Returns
        -------
        Data Ajustada
            Data Ajustada.

        """
        dia = data
        if self.param_datapagto or forcar_du_seguinte:
            return self.workday(dia - timedelta(1), 1, self.param_feriados_br, self.param_feriados_us)
        else:
            return self.workday(dia + timedelta(1), -1, self.param_feriados_br, self.param_feriados_us)

    def __inc_mes(self, data, anos, meses, dia, forcar_du_seguinte=False):
        meses30 = [4, 6, 9, 11]
        # encontra mês e ano
        meses_tot = anos * 12 + meses
        data_fut = data + relativedelta(months=meses_tot)
        # encontra o dia
        # Fevereiro
        if data_fut.month == 2:
            if dia >= 28:
                data_fut = (datetime(data_fut.year, data_fut.month, day=1) + relativedelta(months=1)) + timedelta(
                    days=-1)
            else:
                data_fut = datetime(data_fut.year, data_fut.month, day=dia)
        elif data_fut.month in meses30 and dia == 31:
            data_fut = (datetime(data_fut.year, data_fut.month, day=1) + relativedelta(months=1)) + timedelta(days=-1)
        else:
            data_fut = datetime(data_fut.year, data_fut.month, dia)

        # Verifica qual dia útil deve ser devolvido no resultrado
        return self.verificar_du(data_fut, forcar_du_seguinte)

    def __ler_dc(self):
        mais_menos = self.param_texto[1:2]
        dias = int(self.param_texto[2:])
        du_seg = True
        if mais_menos == '-':
            dias = -dias
            du_seg = False
        data = self.param_data + timedelta(dias)
        # garante que retorno é em dia útil
        #return self.workday(data - timedelta(1), 1, self.param_feriados_br, self.param_feriados_us)  
        return self.verificar_du(data, forcar_du_seguinte=du_seg)

    def __ler_du(self):
        mais_menos = self.param_texto[1:2]
        dias = int(self.param_texto[2:])
        if mais_menos == '-':
            dias = -dias
        return self.workday(self.param_data, dias, self.param_feriados_br, self.param_feriados_us)

    def __ler_parm_long(self):
        param = self.param_texto.split('/')
        texto = param[0][0]

        if param[0][-1] == '+':
            shift = int(param[0][1:-1])
            forcar_du_mais = True
        else:
            shift = int(param[0][1:])
            forcar_du_mais = False
        dia = int(param[1])
        return texto, shift, dia, forcar_du_mais

    def __ler_semana(self, param_data=None):
        # Conversão de dia da semana do padrão mundo real (domingo = 1) para padrão python (segunda = 0)
        semana_conv = {
            1: 6,
            2: 0,
            3: 1,
            4: 2,
            5: 3,
            6: 4,
            7: 5,
        }
        if not param_data:
            param_data = self.param_data

        texto, shift, dia, forcar_du_mais = self.__ler_parm_long()  # parametros

        dia = semana_conv.get(dia)
        dias_tot = shift * 7 + dia - param_data.weekday()
        data = param_data + timedelta(dias_tot)
        # garante que retorno é em dia útil: antes se não for data do pagamento, depois caso contrário
        return self.verificar_du(data, forcar_du_mais)

    def __ler_quinzena(self):
        texto, shift, dia, forcar_du_mais = self.__ler_parm_long()  # parametros
        if dia > 15:  # quinzena só tem 15 dias.
            dia = 15
        shift_dia = dia - 15
        # separa em quinzenas e dias
        meses = int(shift * 15 / 30)
        dias = int(((shift * 15 / 30) - meses) * 30)

        # Adiciona os meses inteiros à data de partida
        data = self.param_data + relativedelta(months=meses)
        # Adiciona as quinzenas à data de partida
        if data.day <= 15 and dias == 0:
            # pegar dia 15 do mês atual
            data = datetime(data.year, data.month, 15)
        elif data.day > 15 and dias == 0:
            # pegar último dia do mês atual
            data = datetime(data.year, data.month, 1) + relativedelta(months=1) - timedelta(1)
        elif data.day <= 15 and dias != 0:
            # estamos no meio do mês, precisa pegar o fim do mês
            data = datetime(data.year, data.month, 1) + relativedelta(months=1) - timedelta(1)
        elif data.day > 15 and dias != 0:
            # estamos no final do mês, precisa pegar dia 15 do mês que vem
            data = datetime(data.year, data.month, 15) + relativedelta(months=1)
        # Dia escolhido da quinzena (ex.: F0/12)
        data = data + timedelta(shift_dia)
        return self.verificar_du(data, forcar_du_mais)

    def __ler_mes(self):
        texto, shift, dia, forcar_du_mais = self.__ler_parm_long()  # parametros
        return self.__inc_mes(self.param_data, 0, shift, dia, forcar_du_mais)

    def __ler_trimestre(self):
        texto, shift, dia, forcar_du_mais = self.__ler_parm_long()  # parametros
        data = self.param_data
        meses = shift * 3
        if data.month % 3 != 0:
            meses = meses + (3 - (data.month % 3))
        return self.__inc_mes(self.param_data, 0, meses, dia, forcar_du_mais)

    def __ler_quadrimestre(self):
        texto, shift, dia, forcar_du_mais = self.__ler_parm_long()  # parametros
        data = self.param_data
        meses = shift * 4
        if data.month % 4 != 0:
            meses = meses + (4 - (data.month % 4))
        return self.__inc_mes(self.param_data, 0, meses, dia, forcar_du_mais)

    def __ler_semestre(self):
        texto, shift, dia, forcar_du_mais = self.__ler_parm_long()  # parametros
        data = self.param_data
        meses = shift * 6
        if data.month % 6 != 0:
            meses = meses + (6 - (data.month % 6))
        return self.__inc_mes(self.param_data, 0, meses, dia, forcar_du_mais)

    def __ler_ano(self):
        texto, shift, dia, forcar_du_mais = self.__ler_parm_long()  # parametros
        data = self.param_data
        meses = shift * 12
        if data.month % 12 != 0:
            meses = meses + (12 - (data.month % 12))
        return self.__inc_mes(self.param_data, 0, meses, dia, forcar_du_mais)

    def __ler_data_exec(self, dia_mov, regra, feriados_br, feriados_us) -> list:
        """Definições
        dia_mov é a data
        regra: lista com 3 valores: Regra para Pedido, Regra para Cotização, Regra para Pagamento
        regras sempre são increntais
        mov: C ou V
        D+n: Data + n dias úteis
        C+n: Data + n dias corridos
        'À partir da daqui, vale a seguinte regra para datas que caem em dia não útil
            para as datas de pedido e cotização: padrão é pegar dia útil anterior
            para data de liquidação: padrão é pegar o próximo dia útil
        Wn/[Dia]: [Dia] de (Semana + n semanas)
        Fn/[Dia]: [Dia] de (Quinzena + n quinzenas)
        Mn/[Dia]: [Dia] de (Mês + n meses)
        Tn/[Dia]: [Dia] de (Trimestre + n trimestres)
        Qn/[Dia]: [Dia] de (Trimestre + n quadrimestres)
        Sn/[Dia]: [Dia] de (Semestre + n semestres)
        * caso queira que uma data de pedido ou cotização trate como liquidação, adicionar + depois de n
            exemplo: se 31/mai é domingo
            pedido=M0/31: 29/mai
            pedido=M0+/31: 1/jun
        "," Separa parâmetros
        """

        if not regra[0] and not regra[1] and not regra[2]:
            return [np.nan]

        self.param_feriados_br = feriados_br
        self.param_feriados_us = feriados_us

        resultados = [None] * 3

        for col in range(3):
            # Separa os parâmetros deste item da movimentação
            self.param_inc = 0
            param = regra[col]
            parametros = param.split(',')

            # no caso da data de pagamento, dia é considerado como depois do feriado
            self.param_datapagto = False
            if col == 2:
                self.param_datapagto = True

            # pega data que vai ser usada nesse loop
            if col == 0:
                # Pega data inicial e garante que é dia útil segundo tabela de feriados do fundo
                data_busca = self.workday(dia_mov - timedelta(1), 1, feriados_br, feriados_us)
            elif col > 0:
                data_busca = resultados[col-1]

            for i in range(len(parametros)):
                self.param_texto = parametros[i].upper()
                self.param_data = data_busca
                func = parametros[i][:1].upper()
                if func == 'C':
                    data_busca = self.__ler_dc()
                elif func == 'D':
                    data_busca = self.__ler_du()
                elif func == 'W':
                    data_busca = self.__ler_semana(dia_mov)
                elif func == 'F':
                    data_busca = self.__ler_quinzena()
                elif func == 'M':
                    data_busca = self.__ler_mes()
                elif func == 'T':
                    data_busca = self.__ler_trimestre()
                elif func == 'Q':
                    data_busca = self.__ler_quadrimestre()
                elif func == 'S':
                    data_busca = self.__ler_semestre()
                elif func == 'A':
                    data_busca = self.__ler_ano()

            resultados[col] = data_busca

        return resultados

    def ler_data(self, dia_mov, regra, feriados_br=True, feriados_us=False, garantir_futuro=True) -> list:
        resultados = self.__ler_data_exec(dia_mov, regra, feriados_br, feriados_us)

        if garantir_futuro and self.__forca_date(resultados[0]) < self.__forca_date(dia_mov):
            data = self.workday(dia_mov, 1, feriados_br, feriados_us)
            while self.__forca_date(resultados[0]) < self.__forca_date(dia_mov):
                resultados = self.__ler_data_exec(data, regra, feriados_br, feriados_us)
                data = self.workday(data, 1, feriados_br, feriados_us)
        resultados = list(map(self.__forca_date, resultados))
        return resultados

if __name__ == '__main__':
    fdt = FuncoesDatas()
    data = datetime(2024,1,29)
    regra = ['Q0/31,M-1/31,C-60','Q0/31,M-1/31','D+2']
    print(fdt.ler_data(data, regra))