import datetime
import pandas as pd
import sys
import warnings
import traceback
import shlex
from subprocess import Popen, PIPE, STDOUT


class Cronometro:
    """
    Classe criada para executar códigos marcando passos e o tempo de execução
    """
    def __init__(self, verbose=False, inicio_print='____', nome_processo=None):
        self.verbose = verbose
        self.inicio_print = inicio_print
        warnings.formatwarning = self.custom_formatwarning
                
        self.__tempos__ = []
        if nome_processo:
            self.nome_processo = nome_processo
            self.marca_tempo(f'{nome_processo}: início')
        else:
            self.nome_processo = ''
            self.marca_tempo('Início')
    
    @staticmethod
    def custom_formatwarning(msg, *args, **kwargs):
        # ignore everything except the message
        return str(msg) + '\n'
    
    def marca_tempo(self, mensagem):
        agora = datetime.datetime.now()
        self.__tempos__.append({'Texto': mensagem, 'Hora': agora})
        if self.verbose:
            warnings.warn(f"{self.inicio_print}{agora}: {mensagem}")
            
    def tempos(self):
        df = pd.DataFrame(self.__tempos__)
        df['Hora'] = pd.to_datetime(df['Hora'])
        if self.verbose:
            df.insert(len(df.columns), 'DeltaMiliSec', [None] * len(df))            
            df.insert(len(df.columns), 'AcumMiliSec', [None] * len(df))            
            df['DeltaMiliSec'] = df['Hora'] - df['Hora'].shift(1)
            df['AcumMiliSec'] = df['Hora'] - df.iloc[0]['Hora']
            df['DeltaMiliSec'] = pd.to_timedelta(df['DeltaMiliSec'])
            df['AcumMiliSec'] = pd.to_timedelta(df['AcumMiliSec'])
            df['DeltaMiliSec'] = df['DeltaMiliSec'].apply(lambda x: round(x.total_seconds() * 1000,0))
            df['AcumMiliSec'] = df['AcumMiliSec'].apply(lambda x: round(x.total_seconds() * 1000,0))
        return df
    
    def tempos_show(self):
        df = self.tempos()
        warnings.warn(f"{self.inicio_print}*** Cronômetro {self.nome_processo} ***")
        for idx, row in df.iterrows():
            variacao = ''
            if self.verbose:
                variacao = f", {row['DeltaMiliSec']}"
            warnings.warn(f"{self.inicio_print}{row['Hora']}: {row['Texto']}{variacao}")

    def concluido(self, exibir_resultados=True):
        if self.nome_processo != '':
            self.marca_tempo(f'{self.nome_processo}: concluído')
        else:
            self.marca_tempo('Concluído')
            
        if exibir_resultados:
            self.tempos_show()
            

class RunWErrorCheck:
    """
    Classe criada para executar códigos com controle de erros e mensagem
    detalhada se houver falha.
    Propriedade .erro pode ser usada pós execução para ver se houve erro.
    
    Passar função e mensagem que indica o que está sendo usado na inicialização.
    """
    def __init__(self, funcao, mensagem:str):
        self.funcao = funcao
        self.mensagem:str = mensagem
        self.erro:bool = False      
        self.mensagem_erro:str = ''
            
    def executar(self, **kwargs):
        try:
            return self.funcao(**kwargs)
        except Exception as e:
            print(f'____{self.mensagem})')
            print('____')
            # Get current system exception
            ex_type, ex_value, ex_traceback = sys.exc_info()
        
            # Extract unformatter stack traces as tuples
            trace_back = traceback.extract_tb(ex_traceback)
        
            # Format stacktrace
            stack_trace = list()
        
            for trace in trace_back:
                stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))
            
            print("Exception type : %s " % ex_type.__name__)
            self.mensagem_erro = "Exception message : %s" %ex_value
            print(self.mensagem_erro)
            print("Stack trace\n____" + "\n____".join(stack_trace))
            print('____')
            self.erro = True
            return None

class Latencia:
    """
    Classe criada para medir a latência de conexão com alguns endereços padrão
    do JBB. Normalmente utilizada em conjunto com execução de rotinas do
    sys_rotinas_lista.py para saber se máquina atual pode ser utilizada para 
    executar uma rotina, tanto pela latência quanto por estar no ambiente nov 
    ou velho.
    """
    def __init__(self):
        self.ambiente_novo = False
        self.temp_last_check = None
        
    @staticmethod
    def get_simple_cmd_output(cmd, stderr=STDOUT):
        """
        Execute a simple external command and get its output.
        """
        args = shlex.split(cmd)
        return Popen(args, stdout=PIPE, stderr=stderr).communicate()[0]
    
    
    def get_ping_time(self, host:str):
        host = host.split(':')[0]
        cmd = "ping {host} -n 10 ".format(host=host)
        try:
            result = str(self.get_simple_cmd_output(cmd))
            texto = "Average ="
            inicio = result.find(texto)
            if inicio == -1:
                texto = "Média ="
                inicio = result.find(texto)
                if inicio == -1:
                    texto = "dia ="
                    inicio = result.find(texto)
            result = result[inicio:]
            final = result.find('ms')
            result = int(result[:final].replace(texto, ''))
            return result
        except:
            return 999999
    
    def teste_velocidade_jb(self, limite:int=10):
        warnings.warn("Executando teste de velocidade de conexão...")
        tempo_jb = self.get_ping_time('infosao01.juliusbaer.com')
        tempo_sql = self.get_ping_time('sqlcore')
        tempo = min(tempo_jb, tempo_sql)
        self.temp_last_check = tempo
        if tempo_jb > tempo_sql:
            self.ambiente_novo = False
        else:
            self.ambiente_novo = True
        warnings.warn(f"Tempo de conexão: {tempo}ms")
        if tempo < limite:
            return True
        else:
            return False
    

if __name__ == '__main__': 
    print(Latencia().teste_velocidade_jb())