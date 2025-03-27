from datetime import date, datetime
import pandas as pd
import subprocess 
from subprocess import Popen
import time
import io, contextlib
from zipfile import ZipFile
from databases import Bawm, Crm, PosicaoDm1,PosicaoAdministrador,Boletador,BDS
from emailer import Email, EmailLer
from funcoes_datas import FuncoesDatas
from filemanager import FileMgmt
from datetime import date, datetime, timedelta
import win32com.client as win32
from databases import Bawm, Crm,PosicaoDm1
from zipfile import ZipFile
import os
import calendar
import numpy as np
import ambiente
from funcoes_datas import FuncoesDatas

"""
Arquivo feito para processar movimentações de zeragem tanto enviadas pelos administradores, quanto feitas por nós
"""
def exportar_bl_preboletas():
    """
    Função para chamar o Boletator, exportação automática, com o usuário atual

    Returns
    -------
    None.

    """
    lista = f"{ambiente.pasta_base_pm()}/08 - Aplicativos/Boletator/bol.bat"
    return_code = subprocess.call(lista,shell=True) # , 


class Zeragem:
    """
    Classe base para processar arquivo do e-mail e gerar boletas
    """
    def __init__(self, texto_busca='X', nome_administrador='Y', homologacao=False, nome_anexo = None, extensao_arquivo = '.txt'):
        self.crm = Crm()
        self.dm1 = PosicaoDm1()
        self.bol = Boletador()
        self.pasta_febe = f'{ambiente.pasta_base_pm()}/16 - Importação Febe/'
        self.bawn= Bawm()
        self.posicao=PosicaoDm1()
        self.texto_busca = texto_busca
        self.nome_administrador = nome_administrador
        self.tem_arq = []
        self.zeragem = pd.DataFrame()                
        self.trades_gps = 0
        self.boletas_reliance = 0
        self.nome_anexo = nome_anexo
        self.extensao_arquivo = extensao_arquivo
        self.funcoes_datas = FuncoesDatas()
    
    def salva_arquivo(self, data_base):
        
        ler = EmailLer(nome_sub_pasta='Zeragem')        
        lista = []
        lista = ler.busca_anexo(self.texto_busca, date.today(), 'C:/Temp/Zeragem/', 'Zeragem',nome_anexo=self.nome_anexo, extensao_arquivo = self.extensao_arquivo)
        
        return lista
    
    def df_arq_email(self):
        # Salva o arquivo do e-mail e monta o dataframe
        hoje = date.today()
        self.tem_arq = self.salva_arquivo(hoje)
        
        return self.df_arq_email_ndo()
    
    def df_arq_email_ndo(self):
        """
        Função que, partindo da lista de anexos do e-mail, retorna um dataframe já tratado com as movimentações
        Aqui está a inteligência de encontrar o arquivo certo e importá-lo num dataframe
        Returns
        -------
        Dataframe de zeragem.

        """
        return pd.DataFrame()

    def email_fundos_nao_identificados(self, df_nao_ids, mensagem_erro=None):
        if df_nao_ids.empty and not mensagem_erro:
            return
        texto = ''
        if not df_nao_ids.empty:
            print(f'Os {len(df_nao_ids)} fundo(s) abaixo não puderam ser identificados:')
            print(df_nao_ids)               
            texto = '''Os fundos abaixo não foram identificados.</h3>{}'''.format(df_nao_ids.to_html())
        
        if mensagem_erro:
            if len(texto) > 0:
                texto = f'{texto}\n\n'
            texto = f'{texto}{mensagem_erro}'
        
        email = Email(subject=f'Fundos do {self.nome_administrador} não integrados via [Zeragem]', 
                      to='portfolio@jbfo.com',
                      text=texto, send=True)
    
    def exportar_febe(self, df_modelo_febe, lista_ids_boletas):
        today = date.today()
        df_modelo_febe.to_excel(f'{self.pasta_febe}Zeragem_{self.nome_administrador}_{today}.xlsx', index = False)
        self.bol.boletas_marcar_exportado(lista_ids_boletas)        
        
    def processar_email(self):
        # 1. Lê do e-mail o arquivo de zeragem
        zeragem = self.df_arq_email()
        if zeragem.empty:
            raise Warning('Nenhum arquivo encontrado!')
        self.zeragem = zeragem
        
        resultado = self.processando_email()
        if not resultado:
            return False
        
        # 8.Executar exportação de boletas
        # self.exportar_febe(self.modelo_febe, self.febe_boletas)
        
        exportar_bl_preboletas()
        
        # 6.#Enviar email p trades da zeragem
        texto = f'''Foram integrados {len(self.layout_blpreboleta)}'''
        email = Email(subject=f'Fundos do {self.nome_administrador} integrados via [Zeragem]', 
                      to='liquidacao@jbfo.com',
                      cc='portfolio@jbfo.com',
                      text=texto, send=True)
        
        return resultado
    

    def processando_email(self):
        """
        Função para tratar o arquivo gerado na funcao df_arq_email
        Aqui está a inteligência pegar o dataframe, e exportar as boletas
        
        Precisa chamar:
            self.email_fundos_nao_identificados()
            self.bol.boletas_importar_df(df=layout_blpreboleta)
            
        Precisa setar as variáveis
            self.mdodelo_febe = df com boletas a exportar para o FEBE
            self.febe_boletas = listas de ids das boletas do FEBE na BL_preBoletas
            self.boletas_reliance = len(n_boletas)           
            self.boletas_gps = len(df_boletar)-(self.boletas_reliance)
        
        Returns
        -------
        bool
            True se fez a exportação corretamete, False se falhou

        """
        return False


class ZeragemBrad(Zeragem):
    
    def __init__(self, texto_busca='ZERAGEM JBB', nome_administrador='Bradesco', homologacao=False):
        super().__init__(texto_busca=texto_busca, nome_administrador=nome_administrador, homologacao=homologacao) 
        self.mensagem_erro_csv = None

    def df_arq_email_ndo(self):
        if len(self.tem_arq) != 0:
            
            # Transforma o txt em data frame
            log = io.StringIO()
            with contextlib.redirect_stderr(log):
                zeragem = pd.read_csv('C:/Temp/Zeragem/zeragem.txt',delimiter = "\t", decimal=",", header = None, on_bad_lines='skip')
            mensagem_erro = log.getvalue()
            if len(mensagem_erro) > 0:
                texto = f'Falha ao ler anexo (read_csv): {mensagem_erro}'
                print(texto)
                self.mensagem_erro_csv = texto
            
            # Faz os ajustes
            zeragem.columns =["data", "cod", "cod_fundo","movimento","cod1","cod2","valor","cod3","cod4","cod5","cod6","cod7","cod8"]
        
            zeragem['valor'] = zeragem['valor'].astype('float')
            zeragem['movimento'] = zeragem['movimento'].replace('A','C')
            zeragem['movimento'] = zeragem['movimento'].replace('R','V')
        
            zeragem.loc[:, "cod"] = zeragem["cod"].map('{:0>6}'.format)
            return zeragem
        else:
            print('arquivo não encontrado!')
            return pd.DataFrame()
    
    def processando_email(self):
        # 2. Faz o cruzamento de cadastro com CRM
        lista_crm = self.crm.fundos_codigo_bradesco()
        df = pd.merge(left=self.zeragem, left_on='cod', right=lista_crm, right_on='CodBradesco', how='left').sort_values('Name')
        df = df.drop_duplicates(subset=['cod','valor'])
        
        df_nao_ids = df[df['Name'].notna()==False]
        df_nao_ids.drop(['cod1','cod2','cod3','cod4','cod5','cod6','cod7','cod8','cod_fundo'],axis =1, inplace = True)         
        self.email_fundos_nao_identificados(df_nao_ids, self.mensagem_erro_csv)
        
        df_boletar = df[df['CodBradesco'].notna()]
        print(len(df) - len(df_nao_ids) - len(df_boletar))
        
        
        # 3. Cria dataframe para subir na BL_PreBoletas
        df_boletar.rename(columns = {'AccountId': "ContaCRMGuid",'Name':'ContaCRM','CodigoProduto':'CodigoProduto'}, inplace=True)
        df_boletar = df_boletar[['data','movimento', 'valor', 'ContaCRMGuid','ContaCRM']]        
        
        layout_blpreboleta = pd.DataFrame(df_boletar['ContaCRMGuid'])        
        layout_blpreboleta['ContaCRM']=df_boletar['ContaCRM']
        layout_blpreboleta["AtivoCadastrado"] = True
        layout_blpreboleta["AtivoGuid"] = '5AA8E514-0FC2-DD11-9886-000F1F6A9C1C'
        layout_blpreboleta["AtivoNome"] = 'BEM FI RF Simples TPF (BRL)'
        layout_blpreboleta["TipoMov"] = df_boletar['movimento']
        layout_blpreboleta["QouF"] = 'F'
        layout_blpreboleta["ResgTot"] = False
        layout_blpreboleta["DataMov"]= date.today()
        layout_blpreboleta["DataCot"]= date.today()
        layout_blpreboleta["DataFin"]= date.today()
        layout_blpreboleta["Financeiro"]= df_boletar['valor']
        layout_blpreboleta["Quantidade"]= None
        layout_blpreboleta["Preco"]= None
        layout_blpreboleta["Secundario"]= False
        layout_blpreboleta["Contraparte"]= None
        layout_blpreboleta["Tracking"]= True
        self.layout_blpreboleta = layout_blpreboleta
    
        #4. Incluir o dataframe no bl_pre_boletas
        self.bol.boletas_importar_df(df=layout_blpreboleta)
        
        # 6.Transformar a zeragem no layout do FEBE   
        de_para_reliance = pd.read_excel(f'{ambiente.pasta_base_pm()}/DE_PARA_RN/cod_bradesco_fundos.xlsx')
        de_para_reliance.loc[:, "cod"] = de_para_reliance["cod"].map('{:0>6}'.format)

        df_reliance= pd.merge(left=self.zeragem, left_on='cod', right=de_para_reliance, right_on='cod', how='left')
        df_reliance = df_reliance.drop_duplicates(subset=['cod','valor'])
        df_reliance = df_reliance.dropna(subset=['Denominação do Fundo'])


        modelo_febe = pd.DataFrame(df_reliance[['movimento','Denominação do Fundo']])
        
        modelo_febe.rename(columns = {'movimento': "TipoMovimento",'Denominação do Fundo':'ContaMovimento'}, inplace=True)
        modelo_febe = modelo_febe.replace('C','Aplicação')
        modelo_febe = modelo_febe.replace('V','Resgate')
        modelo_febe['Fundo']='BEM FUNDO DE INVESTIMENTO RENDA FIXA SIMPLES TPF'
        modelo_febe['BancoNr']= None
        modelo_febe['AgenciaNr']= None
        modelo_febe['Conta']= None
        modelo_febe['FormaLiquida']= 'CETIP'
        modelo_febe['ResgateFinalidade']= 'OUTROS'
        modelo_febe['ValorLiquido']= df_reliance['valor']
        modelo_febe['DATA']= df_reliance['data']
        modelo_febe['OBSERVAÇÃO']= 'ZERAGEM Bradesco'
        modelo_febe['ProvenientAjuste']= 'Não'
        modelo_febe['PessoaContaID']= df_reliance['PessoaContaID']
        modelo_febe.dropna(subset = ['PessoaContaID'])
        self.modelo_febe = modelo_febe
       
        # 6.Separar fundos do Advisory
        fundos_advisory = self.crm.fundos_advisory()
        boletas= self.bol.boletas_lista_datamov().reset_index()
        #layout_blpreboleta_exportados = pd.merge(left=boletas, left_on='ContaCRMGuid', right=fundos_advisory, right_on='AccountId', how='left').sort_values('Name')
        #layout_blpreboleta_exportados = layout_blpreboleta_exportados[layout_blpreboleta_exportados['new_esteira']==2]
        
        
        self.febe_boletas = list()     
        #self.bol.boletas_marcar_exportado(self.febe_boletas)
        exportar_bl_preboletas()
        
        # 7.Linhas finais que devem estar em todos:            
        self.boletas_gps = len(self.layout_blpreboleta)
                
        return True
        # fim das linhas finais
        
class ZeragemSant(Zeragem):
    
    def __init__(self, texto_busca='ZERAGEM GPS', nome_administrador='Santander', homologacao=False,nome_anexo='zeragem_caixa_jbfo',extensao_arquivo='.xlsx'):
        # '.' é o wildcard para buscas no nome_anexo
        super().__init__(texto_busca=texto_busca, nome_administrador=nome_administrador, homologacao=homologacao,nome_anexo=nome_anexo, extensao_arquivo=extensao_arquivo) 
    
    def df_arq_email_ndo(self):
        if len(self.tem_arq) != 0:
             cod_adm = self.crm.fundos_codigo_administrador()
            #Transforma o Excel em data frame:
             zeragem =pd.read_excel('C:/Temp/Zeragem/zeragem.xlsx',sheet_name='FECHAMENTO',header = None)
             zeragem.columns = ['CD Cliente', 'Cliente', 'Papel','Valor Financeiro','Operação','Contra Parte','cod3','cod4','cod5','cod6','cod7','cod8','cod9']
             return zeragem
        else:
            print('arquivo não encontrado!')
            return pd.DataFrame()
    
    # 3. Cria dataframe para subir na BL_PreBoletas
    
    def processando_email(self):
        cod_adm = self.crm.fundos_codigo_administrador()
        self.zeragem.drop(['Papel','cod3','cod4','cod5','cod6','cod7','cod8','cod9'],axis =1, inplace = True)
        zeragem_remove = self.zeragem.loc[(self.zeragem['Cliente'] =='#N/D')  | (self.zeragem['Cliente'] =='Cliente')]
        self.zeragem = self.zeragem.drop(zeragem_remove.index)
        self.zeragem = self.zeragem.dropna(subset=['Valor Financeiro'])
        zeragem_sant = pd.merge(left=self.zeragem, left_on ='CD Cliente',right=cod_adm, right_on='Santander', how ='left')
        # zeragem = zeragem.dropna(subset =['Name'])
        zeragem_sant = zeragem_sant.dropna(subset =['CD Cliente'])
        zeragem_sant.rename(columns = {'AccountId': "ContaCRMGuid"}, inplace = True)
        zeragem_sant['Valor Financeiro'] = zeragem_sant['Valor Financeiro'].astype('float')
        zeragem_sant['Operação'] = zeragem_sant['Operação'].replace('A','C')
        zeragem_sant['Operação'] = zeragem_sant['Operação'].replace('R','V')
        zeragem_sant['Valor Financeiro'] =  abs(zeragem_sant['Valor Financeiro'])
        
        
        layout_blpreboleta = pd.DataFrame(zeragem_sant['ContaCRMGuid'])
        layout_blpreboleta['ContaCRM']=zeragem_sant['Name']
        layout_blpreboleta["AtivoCadastrado"] = True
        layout_blpreboleta["AtivoGuid"] = '838559d4-3f1f-ee11-8d7a-005056b17af5'
        layout_blpreboleta["AtivoNome"] = 'Santander Cash Black FIRF RF DI (BRL)'
        layout_blpreboleta["TipoMov"] = zeragem_sant['Operação']
        layout_blpreboleta["QouF"] = 'F'
        layout_blpreboleta["ResgTot"] = False
        layout_blpreboleta["DataMov"]= date.today()
        layout_blpreboleta["DataCot"]= date.today()
        layout_blpreboleta["DataFin"]= date.today()
        layout_blpreboleta["Financeiro"]= zeragem_sant['Valor Financeiro']
        layout_blpreboleta["Quantidade"]= None
        layout_blpreboleta["Preco"]= None
        layout_blpreboleta["Secundario"]= False
        layout_blpreboleta["Contraparte"]= None
        layout_blpreboleta["Tracking"]= True
        layout_blpreboleta = layout_blpreboleta.dropna(subset=['TipoMov'])
        self.layout_blpreboleta = layout_blpreboleta
        
        #4. Incluir o dataframe no bl_pre_boletas
        self.bol.boletas_importar_df(df=layout_blpreboleta)
        
        # 5.Transformar a zeragem no layout do FEBE   
        de_para_reliance = pd.read_excel(f'{ambiente.pasta_base_pm()}/DE_PARA_RN/cod_sant_fundos.xlsx')
        df_reliance= pd.merge(left=self.zeragem, left_on='CD Cliente', right=de_para_reliance, right_on='cod', how='left')
        df_reliance = df_reliance.drop_duplicates(subset=['CD Cliente','Valor Financeiro'])
        df_reliance = df_reliance.dropna(subset=['Denominação do Fundo'])


        modelo_febe = pd.DataFrame(df_reliance[['Operação','Denominação do Fundo']])                
       
        # 6.Separar fundos do Advisory
        fundos_advisory = self.crm.fundos_advisory()
        boletas= self.bol.boletas_lista_datamov().reset_index()
                
        exportar_bl_preboletas()
        
        # 7.Linhas finais que devem estar em todos:            
        self.boletas_gps = len(self.layout_blpreboleta)
                
        return True
        # fim das linhas finais

class ZeragemBtg(Zeragem):
    
    def __init__(self, texto_busca='ZERAGEM BTG',nome_administrador='BTG', homologacao=False,extensao_arquivo='.xlsx'):
        # '.' é o wildcard para buscas no nome_anexo
        super().__init__(texto_busca=texto_busca, nome_administrador=nome_administrador, homologacao=homologacao,extensao_arquivo=extensao_arquivo) 
    
    def df_arq_email_ndo(self):
        if len(self.tem_arq) != 0:
             zeragem =pd.read_excel('C:/Temp/Zeragem/zeragem.xlsx',sheet_name='Dados',header = 1)
             zeragem = zeragem[['Nome da classe', 'Conta da classe', 'CNPJ da classe', 'Data', 'Lançamento', 'Financeiro (R$)', 'Saldo (R$)']]
             zeragem.columns = ['Nome','Conta','CNPJ','Data','Lançamento','Financeiro','Saldo']
            #  zeragem.drop([0,1], inplace = True)
             return zeragem
        else:
            print('arquivo não encontrado!')
            return pd.DataFrame()
        
    def processando_email(self):
        
        cadastro = self.posicao.lista_fundos()
        self.zeragem['Operação'] = self.zeragem['Financeiro'] .apply(lambda x: "V" if x > 1 else "C")
        self.zeragem['Financeiro'] = abs(self.zeragem['Financeiro'])
        self.zeragem.drop(['Nome','Lançamento','Conta'],axis =1, inplace = True)
        zeragem_btg= pd.merge(left = self.zeragem , left_on = 'CNPJ', right = cadastro, right_on ='CGC', how = 'left')
        zeragem_btg.rename(columns = {'GuidContaCRM': "ContaCRMGuid"}, inplace = True)
        zeragem_btg = zeragem_btg.dropna(subset=['ContaCRMGuid'])

        
        
        layout_blpreboleta = pd.DataFrame(zeragem_btg['ContaCRMGuid'])
        layout_blpreboleta['ContaCRM']=zeragem_btg['NomeContaCRM']
        layout_blpreboleta["AtivoCadastrado"] = True
        layout_blpreboleta["AtivoGuid"] = '8BA7E514-0FC2-DD11-9886-000F1F6A9C1C'
        layout_blpreboleta["AtivoNome"] = 'BTG Pactual Tesouro Selic FI RF (BRL)'
        layout_blpreboleta["TipoMov"] = zeragem_btg['Operação']
        layout_blpreboleta["QouF"] = 'F'
        layout_blpreboleta["ResgTot"] = False
        layout_blpreboleta["DataMov"]= date.today()
        layout_blpreboleta["DataCot"]= date.today()
        layout_blpreboleta["DataFin"]= date.today()
        layout_blpreboleta["Financeiro"]= zeragem_btg['Financeiro']
        layout_blpreboleta["Quantidade"]= None
        layout_blpreboleta["Preco"]= None
        layout_blpreboleta["Secundario"]= False
        layout_blpreboleta["Contraparte"]= None
        layout_blpreboleta["Tracking"]= True
        self.layout_blpreboleta = layout_blpreboleta
        
        # 4. Incluir o dataframe no bl_pre_boletas
        self.bol.boletas_importar_df(df=layout_blpreboleta) 
        
        # 5.Transformar a zeragem no layout do FEBE  
        de_para_reliance = pd.read_excel(f'{ambiente.pasta_base_pm()}/DE_PARA_RN/cod_BTG.xlsx')
        df_reliance= pd.merge(left=self.zeragem, left_on='CNPJ', right=de_para_reliance, right_on='CNPJ', how='left')    
        df_reliance = df_reliance.drop_duplicates(subset=['Fundo','Financeiro'])
        df_reliance = df_reliance.dropna(subset=['Fundo'])


        modelo_febe = pd.DataFrame(df_reliance[['Operação','Fundo']])
        
        modelo_febe.rename(columns = {'Operação': "TipoMovimento",'Fundo':'ContaMovimento'}, inplace=True)
        modelo_febe = modelo_febe.replace('C','Aplicação')
        modelo_febe = modelo_febe.replace('V','Resgate')
        modelo_febe['Fundo']='BTG PACTUAL TESOURO SELIC FI RF REFERENCIADO DI'
        modelo_febe['BancoNr']= None
        modelo_febe['AgenciaNr']= None
        modelo_febe['Conta']= None
        modelo_febe['FormaLiquida']= 'CETIP'
        modelo_febe['ResgateFinalidade']= 'OUTROS'
        modelo_febe['ValorLiquido']= abs(df_reliance['Financeiro'])
        modelo_febe['DATA']= date.today()
        modelo_febe['DATA']= modelo_febe['DATA'].astype(str)
        modelo_febe['OBSERVAÇÃO']= 'ZERAGEM BTG'
        modelo_febe['ProvenientAjuste']= 'Não'
        modelo_febe['PessoaContaID']= df_reliance['Código_Febe']
        #modelo_febe.drop(['index'],inplace = True)
        self.modelo_febe = modelo_febe
        
       
              

        # 6.Separar fundos do Advisory
        fundos_advisory = self.crm.fundos_advisory()
        boletas= self.bol.boletas_lista_datamov().reset_index()
        layout_blpreboleta_exportados = pd.merge(left=boletas, left_on='ContaCRMGuid', right=fundos_advisory, right_on='AccountId', how='left').sort_values('Name')
        layout_blpreboleta_exportados = layout_blpreboleta_exportados[layout_blpreboleta_exportados['new_esteira']==2]
        
        
        self.febe_boletas = list()     
        #self.bol.boletas_marcar_exportado(self.febe_boletas)
        exportar_bl_preboletas()
        
        
        # 7.inhas finais que devem estar em todos:            
        self.boletas_gps = len(self.layout_blpreboleta)
        
        return True
        # fim das linhas finais     

class AjusteFuturos():
    
    def __init__(self, homologacao=False):
        self.crm = Crm(homologacao=homologacao)
        self.fdt = FuncoesDatas(homologacao=homologacao)
        self.bol = Boletador(homologacao=homologacao)
        self.fm = FileMgmt()
        self.hoje = self.bol.banco.hoje()
    
    def caixa_para_ajustes(self):

        email = EmailLer(nome_sub_pasta='BMF')        
        guid = self.crm.account(lista_campos = ['name','accountid'])
        
        #Ajusto a data para encontrar o e-mail do ajuste
        data_BMF = self.fdt.workday(data = self.fdt.hoje(),n_dias = -1,feriados_brasil=True, feriados_eua=False)
        if data_BMF.day < 10:
            dia = str(data_BMF.day)
            dia = '0{}'.format(dia)
        else:
            dia = str(data_BMF.day)
        
        if data_BMF.month < 10:
            mes = str(data_BMF.month)
            mes = '0{}'.format(mes)
        else:
            mes = str(data_BMF.month) 
        year = str(data_BMF.year)
        data_BMF2= year+mes+dia
        assunto = f'MOVIMENTACAO FINANCEIRA BMF DO PREGAO{data_BMF2} JULIUS BAER BRASIL'
        
        
        #Tira o ZIP
        pasta = 'C:/Temp/Ajuste'
        self.fm.create_dir_if_not_exists(pasta)
        email.busca_anexo_zip(assunto_busca = assunto,data_base = data_BMF, pasta_destino =f'{pasta}/',nome_destino = 'ajuste')        
        z = ZipFile(f'{pasta}/ajuste.zip','r')
        z.extractall(path = pasta)
        z.close()
        
        
        #Trata a consulta da conta_movimento para o merge.
        contas_movimento = self.crm.conta_bradesco_corretora(nome_override='Ágora CTVM')
        contas_movimento = contas_movimento.dropna(subset=['new_numeroconta'])
        contas_movimento['new_numeroconta'] = contas_movimento.apply(lambda x: str(x['new_numeroconta']).replace('-',''), axis=1)
        contas_movimento['new_numeroconta'] = contas_movimento['new_numeroconta'].astype(int)
        
        
        #Procura o arquivo correto
        lista = self.fm.files_in_dir(f'{pasta}/','.xlsx')
        if len(lista) == 0:
            raise Exception('Arquivo excel não entrado na pasta C:/Temp/Ajuste')
        arquivo = ''
        cont = 0
        for item in lista:
            data = self.fm.get_file_modified_date(f"{pasta}/{item}")
            if data.year == self.hoje.year and data.month == self.hoje.month and data.day == self.hoje.day:
                arquivo = item
                cont += 1
            else:
                self.fm.delete_file(f"{pasta}/{item}")
        if arquivo == '':
            raise Exception('Nenhum arquivo de excel modificado hoje encontrado na pasta C:/Temp/Ajuste')
        if cont > 1:
            raise Exception('MAIS de UM arquivo de excel modificado hoje encontrado na pasta C:/Temp/Ajuste')
        
        #trata o dataframe ajuste e faz o merge pora encontrar o GUID e o fundo.
        ajuste = pd.read_excel(f"{pasta}/{arquivo}")
        ajuste.columns = ['Data','Cod.','Agente','new_numeroconta','Cliente','Commod','Mercad','Série','C/V','Negócio','Quant.','Preço','Vl. Oper / Ajuste','Corretagem','Outro Custo','Corretagem','Tx. Registro','Emolumento','Custo MC','Vl. Volume','Vl. Líquido','Ori.','Dest.','C/P','Ass.','Nome Ass.','AJ']
        ajuste.drop([0,1,2,3],inplace=True)
        ajuste.drop('Data', axis=1, inplace=True)
        ajuste['Vl. Líquido'] = ajuste['Vl. Líquido'].astype(float)
        ajuste = ajuste.groupby('new_numeroconta').sum()['Vl. Líquido']
        ajuste = pd.DataFrame(ajuste)
        ajuste = ajuste.reset_index()
        ajuste['new_numeroconta'] = ajuste['new_numeroconta'] .astype(int)
        ajuste_v2 = pd.merge(left =ajuste,right = contas_movimento, how ='left', left_on='new_numeroconta', right_on='new_numeroconta')
        
        #Puxar todas as posições e, fundos de liquidez.
        lista = ajuste_v2['new_titularidadeidname']
        lista = list(lista)
        
        from objetos import Fundo
        fundos = {}
        for item in lista:
            fundos[item] = Fundo(nome_conta_crm=item)    
        
            resultado = []
        for item in fundos.keys():
            if fundos[item].tem_dm1:
                resultado.append(fundos[item].liquidez_imediata_por_ativo())
        resultado = pd.concat(resultado).reset_index()
        resultado = resultado[resultado['LiqFinanceiroFinal']>0]
        
        #Filtrar o que é cotas e ver se tem saldo
        resultadov2 = resultado[['NomeContaCRM', 'GuidProduto', 'NomeProduto', 'LiqFinanceiroFinal','TipoProduto']].groupby(['NomeContaCRM', 'GuidProduto', 'NomeProduto','TipoProduto']).sum()
        resultadov2 = pd.DataFrame(resultadov2).reset_index()
        resultadov2 = resultadov2[(resultadov2['TipoProduto']=='FUNDO')].sort_values('LiqFinanceiroFinal',ascending = False)
        ajuste_v2 = pd.merge(left = ajuste_v2, left_on = 'new_titularidadeidname', right =guid, right_on = 'name', how = 'left' )
        
        ajuste_v2['TipoMov'] = ajuste_v2['Vl. Líquido'].apply(lambda x: 'V' if x<0 else 'C')
        boletas = []
        erros = []
        
        for i in lista:
            df = resultadov2[resultadov2['NomeContaCRM']==i] 
            valor = ajuste_v2[ajuste_v2['new_titularidadeidname'] == i]
            if df.empty:
                erros.append(i)
            else:
                for idx, row in valor.iterrows():
                    valor = row['Vl. Líquido']
                    ContaCRMGuid = row['accountid']
                    TipoMov = row['TipoMov']
                    hoje = date.today()
                    for idx, row in df.iterrows():
                        if row['LiqFinanceiroFinal'] > valor:
                            dicio = {'ContaCRMGuid': ContaCRMGuid,'ContaCRM': i,'AtivoCadastrado' : True, 
                                     'AtivoGuid': row['GuidProduto'],'AtivoNome': row['NomeProduto'], 
                                     'TipoMov':TipoMov,'QouF':'F', 'Financeiro': abs(valor),
                                     'ResgTot':False,'DataMov':hoje,'DataCot':hoje,'DataFin':hoje,'Financeiro':abs(valor),'Quantidade':None,
                                     'Preco': None,'Secundario': False,'Contraparte': None,'Tracking':False,'IdSolMov':0}
                            boletas.append(dicio)
                        else:
                            # cria boleta com row['LiqFinanceiroFinal']
                            dicio = {'ContaCRMGuid': ContaCRMGuid,'ContaCRM': i,'AtivoCadastrado' : True, 'AtivoGuid': row['GuidProduto'],'AtivoNome': row['NomeProduto'],
                                     'TipoMov':TipoMov,'QouF':'F', 'Financeiro': abs(valor),
                                     'ResgTot': False,'DataMov': hoje,'DataCot': hoje,'DataFin': hoje,'Financeiro': abs(row['LiqFinanceiroFinal']),'Quantidade': None,
                                     'Preco': None,'Secundario': False,'Contraparte': None,'Tracking': False,'IdSolMov':0}
                            boletas.append(dicio)
                            #valor -= row['LiqFinanceiroFinal']
                        if valor <= 0:
                            break
        
        boletas = pd.DataFrame(boletas)                   
        boletas = boletas.drop_duplicates(subset=['ContaCRM']) 
        boletas.dropna(subset =['AtivoNome'],inplace = True) # ? bug fix?
        boletas = boletas[boletas['AtivoNome'] != 'BNP Paribas Soberano II FIC FI RF Simples (BRL)']
        boletas = boletas[boletas['Financeiro']!= 0] 
        self.bol.boletas_importar_df(df=boletas)

        # 6.Separar fundos do Advisory
        fundos_advisory = self.crm.fundos_advisory()
        boletas= self.bol.boletas_lista_datamov().reset_index()
        layout_blpreboleta_exportados = pd.merge(left=boletas, left_on='ContaCRMGuid', right=fundos_advisory, right_on='AccountId', how='left').sort_values('Name')
        layout_blpreboleta_exportados = layout_blpreboleta_exportados[layout_blpreboleta_exportados['new_esteira']==2]
        
        
        self.febe_boletas = list()     
        self.bol.boletas_marcar_exportado(self.febe_boletas)
        exportar_bl_preboletas()
        

        
        # Envia e-mail avisando quais boletas subiram:

        
       # Envia e-mail avisando quais boletas subiram:

        destino = "DD_JBFO_Portfolio_Management"
        assunto = f"[BMF] Processamento arquivo ajuste - {self.bol.banco.hoje().strftime('%d-%m-%Y')}"
        corpo = '''As seguintes boletas foram importadas após processamento do arquivo de ajustes do bradesco:<br<br>{}'''.format(boletas.to_html())
        email = Email(to=destino, subject=assunto, text=corpo, send=True)        
        resultadov2.to_excel(f'{ambiente.pasta_base_pm()}/16 - Importação Febe/resultadov2.xlsx')
    
        return True
    
  
class Automatizacao:
    
    def importar_rv(self):
        pasta = 'C:/Temp/RV'
        FileMgmt().create_dir_if_not_exists(pasta)
        funcoes_datas = FuncoesDatas()
        bawm = Bawm()
        crm = Crm()
        posicao = PosicaoDm1()
        bds = BDS()
        bol = Boletador()
        hoje = funcoes_datas.hoje()
        d1 = funcoes_datas.workday(hoje,n_dias = -1,feriados_brasil=True, feriados_eua=False)
        
        # Ajuste da data de hoje para localização do arquivo do Bradesco no dia e definir o nome do arquivo
        
        today = date.today()
        data = '{}0{}0{}'.format(today.year, today.month,today.day)
        arquivoRV = 'Consolidado Julius '+ data
        if today.day < 10:
            data = '{}0{}0{}'.format(today.year, today.month,today.day)
        else:
            data = '{}0{}{}'.format(today.year, today.month,today.day)
            
        arquivoRV = 'Consolidado Julius '+ data
        
        #Ajuste da data de liquidação do Ativo (D+2)
        
        DU = timedelta(2)
        dia_util = (today + DU).weekday()
        util = (today + DU).weekday()
        if dia_util == 5:
             data_liquidacao = (today+ timedelta(4))
        elif dia_util == 6:
            data_liquidacao = (today+ timedelta(4))
        else:
            data_liquidacao = (today+ timedelta(2))
            

        def frac(acao):
            if acao[-1]=='F':
                acao = acao[:-1]
            else:
                acao = acao
            return acao    
        
        #Função para encontrar o arquivo do Bradesco corretora, salvar na pasta e tirar do zip
        
        def boletaRV(data_base):
            outlook = win32.Dispatch('outlook.application')
            mapi = outlook.GetNamespace("MAPI")
            inbox = mapi.GetDefaultFolder(6).Folders['RV']
            messages = inbox.Items
            print('Processando ', len(messages), 'emails')
            send_dt = str(date.today())
            achei_arquivo = False
            while achei_arquivo == False:
                for message in messages:
                    message_dt = message.senton.date()
                print('Data do email:', message_dt, type(message_dt))
                assunto = message.subject[7:]
                print(assunto)
                if 'BOVESPA' in assunto:
                    print(data_base, message_dt)
                    if data_base == message_dt:
                        print('entrei')
                        attachments = message.Attachments
                        for att in attachments:
                            att_name = str(att).lower()
                            print(att_name)
                            if '.zip' in att_name:
                                    att.SaveASFile(f'C:/Temp/RV/JULIUS{today}.zip')
                                    achei_arquivo = True
                                    z = ZipFile(f'C:/Temp/RV/JULIUS{today}.zip','r')
                                    z.extractall(path = 'C:/Temp/RV')
                                    z.close()
                
                            else:
                                print('não entrei')
                            return achei_arquivo
                else:
                    continue       
                        
        #Rodar a função acima
        tem_arq = boletaRV(today)  
        data_liquidacao = funcoes_datas.workday(funcoes_datas.hoje(),n_dias = 2,feriados_brasil=True, feriados_eua=False)
        
        #Trazer os números das contas no formato do arquivo do Bradesco
        contas = crm.contas_movimento_CRM_fundos()
        contas = contas[contas['Banco']=='Ágora CTVM']
        contas['Numero'] = contas['Numero'].astype('str')
        
        #contas['Digito']= contas['Digito'].astype('str')
        contas = contas[contas['Numero']!='None']
        #contas['n_conta'] = contas.apply(lambda row:(row['Numero'] + row['Digito']) if row['Digito']!='0' else row['Numero'], axis = 1 ).astype('int')
        #ontas['n_conta'] = (contas['Numero']  + contas['Digito']).astype('int')
        contas['n_conta'] =  contas['Numero'].astype('int') 
        
        #Trazer os produtos de RV
        produtos_bds = bds.consulta_ativos_RV()
        produtos_bds = produtos_bds[['productid','codser','name']]
        produtos_crm = crm.Produtos_isin_ticker().dropna(subset=['new_idsistemaorigem'])
        produtos_crm  = produtos_crm [['productid','new_idsistemaorigem','name']].rename(columns={'new_idsistemaorigem':'codser'})
        produtos = pd.concat([produtos_bds,produtos_crm ])
        produtos = produtos.drop_duplicates()
        produtos['productid'] = produtos['productid'].str.lower()

        
        #Tratamento dos dados que vem da Bradesco corretora para juntar com as outras tabelas
        #df_rv = pd.read_excel(f"C:/Temp/RV/{arquivoRV}.xls")
        df_rv = pd.read_excel('C:/Temp/RV/Bovespa_JBFO_Trade.xls',header = None)
        #df_rv.drop(index=0)
        lin = list(df_rv.loc[0])
        df_rv.columns = lin
        df_rv = df_rv.drop(index=0)
        df_rv = df_rv.groupby(['CODIGO','ATIVO','C/V','PREÇO'])[['QTD']].sum().reset_index()
        df_rv['CODIGO'] = df_rv['CODIGO'].astype('str')
        df_rv['CODIGO'] = df_rv['CODIGO'].apply(lambda x: int(x[:-1]))
        df_rv = pd.merge(right= df_rv, left = contas, left_on ='n_conta', right_on = 'CODIGO', how='right' )
        df_rv['ATIVO']=df_rv['ATIVO'].apply(lambda x: frac(x))
        
        #Trazendo as contas não identificadas e por isso não importadas
        try:
            contas_n_identificadas = df_rv[df_rv['accountid'].isnull()]
            contas_n_identificadas = contas_n_identificadas[['CODIGO','ATIVO','QTD','C/V','PREÇO']]
        
        except:
            pass
            
        #Trazendo o id produto por ticker
        df_rv = df_rv.dropna(subset=['accountid'])
        df_rv = df_rv[['CODIGO','accountid','name','ATIVO','QTD','C/V','PREÇO']]
        df_rv = pd.merge(left = df_rv,left_on='ATIVO', right = produtos,right_on = 'codser', how='left')
        df_rv['accountid']= df_rv['accountid'].str.lower()
        df_rv['productid']= df_rv['productid'].str.lower()   

        #Trazer as operações boletadas, excluindo os futuros e opções
        boletas = bol.boletas_executadas_rv(today)
        # boletas["AtivoNome"] = boletas[boletas["AtivoNome"].apply(lambda x: 'Futuro' not in x)]
        boletas = boletas.loc[~boletas['AtivoNome'].str.contains('Futuro')]
        boletas['ContaCRMGuid']=boletas['ContaCRMGuid'].str.lower()
        boletas['AtivoGuid']=boletas['AtivoGuid'].str.lower()
        boletas.drop_duplicates(inplace=True)
        #Somar as quantidades por produto e conta para encontrar a diferença de operações não boletadas / executadas
        df_rv.drop_duplicates(inplace=True)
        b3_sum = df_rv.groupby(['accountid','name_x','productid','ATIVO'])[['QTD']].sum().reset_index()
        df_rv.drop_duplicates(inplace=True)
        boletas_sum = boletas.groupby(['ContaCRMGuid','ContaCRM','AtivoGuid','CodNegociacao'])[['Quantidade']].sum().reset_index()   
        boletas_sum.drop_duplicates(inplace=True)            
        boletas_exportadas = list(boletas['IdRealocFundos'])     
        bol.boletas_marcar_exportado(boletas_exportadas)
        
       
        bate = pd.merge(left = boletas_sum, right = b3_sum,right_on =['accountid','ATIVO'], left_on=['ContaCRMGuid','CodNegociacao'], how='outer')                                  
        bate['QTD']=bate['QTD'].fillna(0)
        bate['dif']=bate['QTD']-bate['Quantidade']
        
        #Encontrando as diferenças entre a distribuição e o que foi boletado.
        diferenca = bate[bate['dif']!=0]
        diferenca=diferenca[['ContaCRMGuid','ContaCRM','CodNegociacao','Quantidade','QTD','dif']]
        diferenca.columns = ['ContaCRMGuid','ContaCRM','CodNegociacao','Qtd_boletada','qtd_distribuida','dif']
        diferenca = diferenca.dropna(subset=['ContaCRM'])
           
        
        #Ajustando o layout para importação
        layout_importação_rv = pd.merge(left = df_rv, right = boletas, left_on =['accountid','productid'], right_on = ['ContaCRMGuid','AtivoGuid'],how = 'right')
        
        try:   
        
            #Boletas não conciliadas com o bradesco
            boletas_n_conciliadas_b3 = bate[bate['ContaCRM'].isnull()]            
            boletas_n_conciliadas_b3 =boletas_n_conciliadas_b3[['name_x','ATIVO','QTD']]
            boletas_n_conciliadas_b3.columns = ['ContaCRM','CodNegociacao','Quantidade']
            boletas_n_conciliadas_b3['Fonte'] = 'Arquivo B3'
            boletas_n_conciliadas_boletador = bate[bate['name_x'].isnull()]
            boletas_n_conciliadas_boletador = boletas_n_conciliadas_boletador[['ContaCRM','CodNegociacao','Quantidade']]
            boletas_n_conciliadas_boletador['Fonte'] = 'Boletador'
            boletas_n_conciliadas = pd.concat([boletas_n_conciliadas_b3,boletas_n_conciliadas_boletador])
        
        except:
            pass
        
        
        #Ajustando o layout para importação
        layout_importação_rv = layout_importação_rv.dropna(subset = ['accountid']).dropna(subset = ['AtivoGuid'])  
        layout_importação_rv = layout_importação_rv[['accountid','name_x','AtivoGuid','name_y','AtivoNome','CodNegociacao','C/V','QTD','PREÇO','Contraparte','ContraparteExt']]
        layout_importação_rv['Financeiro'] = layout_importação_rv['QTD']*layout_importação_rv['PREÇO']
        layout_blpreboleta = layout_importação_rv
        layout_blpreboleta['AtivoCadastrado'] = True
        layout_blpreboleta["QouF"] = 'Q'
        layout_blpreboleta["DataMov"]= date.today()
        layout_blpreboleta["DataCot"]= date.today()
        layout_blpreboleta["DataFin"]=  data_liquidacao
        layout_blpreboleta["Secundario"]= False
        layout_blpreboleta = layout_blpreboleta[['accountid','name_x','AtivoCadastrado','AtivoGuid','name_y','C/V','QouF','DataMov','DataCot','DataFin','Financeiro','QTD','PREÇO','Contraparte','CodNegociacao','ContraparteExt']]
        layout_blpreboleta.columns = ['ContaCRMGuid','ContaCRM','AtivoCadastrado','AtivoGuid','AtivoNome','TipoMov','QouF','DataMov','DataCot','DataFin','Financeiro','Quantidade','Preco','Contraparte','CodigoExt','ContraparteExt']
        layout_blpreboleta = layout_blpreboleta.drop_duplicates()
        
        # #Importação das boletas no BL Pre_Boletas
        bol.boletas_importar_df(df=layout_blpreboleta)
        lista = f"{ambiente.pasta_base_pm()}/08 - Aplicativos/Boletator/bol.bat"
        return_code = subprocess.call(lista,shell=True)  
        
        layout_blpreboleta.to_excel('layout_blpreboleta.xlsx')
        df_rv.to_excel('df.xlsx')
        produtos.to_excel('produtos.xlsx')
        
        subject='[Boletas RV automáticas] - Validação de posições'
        to=['portfolio@jbfo.com']
        text = '''Prezados,<br>
            
        Não foi possível localizar as contas abaixo no CRM.<br>
        {}<br>
        <br>
        Seguem boletas não conciliadas (Operações boletadas que não foram distribuidas, verificar número da conta ou se foi realizada)).<br>
        {}<br>
        Seguem as diferenças de quantidades (o que foi boletado versus o que foi distribuido via repasse, checar se foi via corretora direto).<br>
        {}<br>
        
        <br>
        <br>
        
        '''.format(contas_n_identificadas.to_html(),boletas_n_conciliadas.to_html(),diferenca.to_html())
            
        email = Email(to = to , subject = subject, text= text,send = False)
        
        texto = f'''Foram integrados {len(layout_blpreboleta)} de RV ao cockpit (Bradesco Corretora)'''
        email = Email(subject=f'Boletas RV Integradas ao cockpit', 
                      to='liquidacao@jbfo.com',
                      cc='portfolio@jbfo.com',
                      text=texto, send=False)
            
class InclusaoSaldos:
    
    def importar_saldos_pf_brad(self):
        import os
        from databases import Crm as crm, BDS, Bawm as bawm, PosicaoDm1 as dm1, Boletador
        import pandas as pd
        import time 
        from funcoes_datas import FuncoesDatas
        from datetime import datetime, timedelta
        import numpy as np
        from emailer import Email, EmailLer
        from pretty_html_table import build_table
        import math
        from scipy import stats 
        from datetime import date
        from objetos import Ativo
        
        caminho_arquivo = 'C:/Temp/saldos/Saldos Custódia.xlsx'
        
        pasta = 'C:/Temp/Saldos'
        FileMgmt().create_dir_if_not_exists(pasta)
        
        bds = BDS()
        crm = crm()
        dm1 = dm1()
        bawm = bawm()
        funcoes_datas = FuncoesDatas()
        bol = Boletador()
        banco = 'Banco Bradesco SA'
        
        #Essa função inclui na D-1 os saldos de PF por titularidade
        
        today = date.today()
        hoje = FuncoesDatas.hoje()
        d1 = funcoes_datas.workday(hoje,n_dias = -1,feriados_brasil=True, feriados_eua=False)
        d3 = funcoes_datas.workday(hoje,n_dias = -3,feriados_brasil=True, feriados_eua=False)
        today = d1 
        
        
        def ver_se_pf(x):
            if 'GPS' not in x and 'REL' not in x:
                valor = 'Fundo'
            else:
                valor ='PF'
            return valor    
                
                #Puxando os codigos dos ADMs, das contas custodias e também as sc que fazem composição da DM1
        cod_adm = crm.pf_codigo_bradesco()
        contas = crm.contas_movimento_por_banco( banco_id_name=banco)
        contas = contas[contas['new_agencia']=='2856']
        contas['new_titularidadeid']=contas['new_titularidadeid'].str.lower()
        pl_sc = dm1.sc_gestao_pl_estimado(data_pos = today)

        saldos = pd.read_excel(caminho_arquivo)
                
                #Ajustando as tabelas de contas  e saldos para trazer as informações necessárias para a importação
        sc = dm1.query_sc_dm1()
        sc['GuidContaCRM']=sc['GuidContaCRM'].str.lower()
        cod_adm['AccountId']=cod_adm['AccountId'].str.lower()
        cod_adm = cod_adm[['AccountId','TitularidadeGuid','Titularidade','NomeContaCRM','CodBradesco','new_codigocarteira']]
        cod_adm = pd.merge(left = cod_adm, right = sc, right_on = 'GuidContaCRM', left_on = 'AccountId', how='left')
        cod_adm =  cod_adm.sort_values(by=['CartIdFonte']).drop_duplicates(subset = ['AccountId'])
        cod_adm = cod_adm.dropna(subset =['CartIdFonte'])
        cod_adm['TitularidadeGuid'] = cod_adm['TitularidadeGuid'].str.lower()
                
        lin = list(saldos.loc[0])
        saldos.columns = lin
        saldos = saldos.drop(index=0)
        saldos = pd.merge(right = saldos, left = cod_adm, left_on = 'CodBradesco',right_on = 'Código Externo', how = 'right')
        saldos['Saldo Abertura'] =saldos['Saldo Abertura'].str.replace(".", "")
        saldos['Saldo Abertura'] =saldos['Saldo Abertura'].str.replace(",", ".").astype('float')
        saldos = saldos[saldos['Saldo Abertura']!=0] 
        saldos['PF']= saldos['Código Externo'].apply(lambda x:ver_se_pf(x))
        saldos = saldos[saldos['PF']=='PF']
        verificar_cadastro  = saldos[saldos['AccountId'].isnull()]
        verificar_cadastro  = verificar_cadastro[['Código Externo','Fundo']] 
        saldos = saldos.dropna(subset=['NomeSuperCarteiraCRM'])
        saldos = pd.merge(right = saldos, left = pl_sc, on = 'NomeSuperCarteiraCRM',how='right')
        saldos['Peso'] = saldos['Saldo Abertura']/saldos['PL']
        saldos['TitularidadeGuid'] =saldos['TitularidadeGuid'].str.lower()
        saldos = pd.merge(left = saldos, right = contas, left_on = 'TitularidadeGuid',right_on = 'new_titularidadeid', how='left')
        saldos['GuidContaMovimento']=saldos['GuidContaMovimento'].fillna(0)
        saldos['Peso']=saldos['Peso'].fillna(0)
        saldos['DataFonte'] = d3
        saldos = saldos[['DataFonte','AccountId','new_titularidadeid','Código Externo','NomeContaCRM_y','NomeSuperCarteiraCRM','Fundo','Saldo Abertura','Peso','Titularidade','TitularidadeGuid','GuidContaMovimento','CartIdFonte']].dropna(subset=['AccountId'])
                
                #Layout da importação e posterior importação para a base bawm
        dados = saldos['DataFonte']
        layout_d1 = pd.DataFrame(data = dados )
        layout_d1['DataFonte'] = d3
        layout_d1['DataArquivo'] = hoje
        layout_d1['NomeContaCRM']=saldos['NomeContaCRM_y']
        layout_d1['NomeSuperCarteiraCRM']=saldos['NomeSuperCarteiraCRM']
        layout_d1['Fonte']='Saldos_PF'
        layout_d1['IdFonte']= saldos['CartIdFonte']
        layout_d1['Classe']='R Fixa Pós'
        layout_d1['SubClasse']='RF Pos Liquidez'
        layout_d1['TipoProduto']='CAIXA'
        layout_d1['GuidProduto']='c6999643-98b7-df11-85ec-d8d385b9752e'
        layout_d1['IdProdutoProfitSGI']='999996'
        layout_d1['IdBDS']='81341250'
        layout_d1['NomeProduto']='Caixa'
        layout_d1['GuidEmissor']=None
        layout_d1['RatingGPS']='SEM_RATING_EI'
        layout_d1['DataEmissao'] = hoje
        layout_d1['DataVencimento'] = hoje
        layout_d1['FinanceiroFinal'] = saldos['Saldo Abertura']
        layout_d1['FinanceiroInicial'] = saldos['Saldo Abertura']
        layout_d1['FinanceiroFuturo'] =saldos['Saldo Abertura']
        layout_d1['FinanceiroCPR'] = 0
        layout_d1['NomeEmissor']=None
        layout_d1['Indexador']='CDI'
        layout_d1['Taxa']=100.00
        layout_d1['Coupon'] =None
        layout_d1['QuantidadeFinal'] =saldos['Saldo Abertura']
        layout_d1['PrecoInicial'] =1.00
        layout_d1['MargemC'] =None
        layout_d1['MargemV'] =None
        layout_d1['QtMd'] =None
        layout_d1['PrecoF'] =None
        layout_d1['PesoFinal'] = saldos['Peso']
        layout_d1['PesoInicial'] = saldos['Peso']
        layout_d1['PesoFuturo'] =saldos['Peso']
        layout_d1['Duration'] =None
        layout_d1['MTH'] =None
        layout_d1['Stress']=None
        layout_d1['TaxaMtM']=None
        layout_d1['CouponMtM']=None
        layout_d1['Isento']=0
        layout_d1['GuidContaMovimento'] =saldos['GuidContaMovimento']
        layout_d1['CodigoCetip'] =None
        layout_d1['NomeContaMovimento'] =None
        layout_d1['NomeBanco'] = banco
        layout_d1['NumeroAgencia'] = None
        layout_d1['BoletasAplicadasNaPosicao'] = None
        layout_d1['QtdeBloq'] = None
        layout_d1['ProdutoID'] = 23044
        layout_d1['Titularidade'] = saldos['Titularidade']
        layout_d1['TitularidadeGuid'] = saldos['TitularidadeGuid'].str.lower()
        layout_d1 = layout_d1[layout_d1['GuidContaMovimento']!=0]
        layout_d1 = layout_d1.drop_duplicates(subset = ['TitularidadeGuid'])
               
        #Importar na Dm1
        dm1.importar_saldos(layout_d1)
        
        # Apaga o arquivo
        FileMgmt().delete_file(caminho_arquivo)
                
        
    def importar_Saldos_banco(self, nome_email,banco):
        from databases import Crm as crm, BDS, Bawm as bawm, PosicaoDm1 as dm1, Boletador
        import pandas as pd
        import time 
        from funcoes_datas import FuncoesDatas
        from datetime import datetime, timedelta
        import numpy as np
        from emailer import Email, EmailLer
        from pretty_html_table import build_table
        import math
        from scipy import stats 
        from datetime import date
        from objetos import Ativo
        from bs4 import BeautifulSoup
        
        pasta = 'C:/Temp/Saldos'
        FileMgmt().create_dir_if_not_exists(pasta)
        
        bds = BDS()
        crm = crm()
        dm1 = dm1()
        bawm = bawm()
        funcoes_datas = FuncoesDatas()
        bol = Boletador()
        
        today = date.today()
        hoje = FuncoesDatas.hoje()
        d1 = funcoes_datas.workday(hoje,n_dias = -1,feriados_brasil=True, feriados_eua=False)
        d3 = funcoes_datas.workday(hoje,n_dias = -3,feriados_brasil=True, feriados_eua=False)
         
        
        cod_adm = crm.pf_codigo_bradesco()
        contas = crm.contas_movimento_por_banco(banco_id_name=banco)
        contas['new_titularidadeid']=contas['new_titularidadeid'].str.lower()
        pl_sc = dm1.sc_gestao_pl_estimado(data_pos = today)
        sc = dm1.query_sc_dm1()
        sc['GuidContaCRM']=sc['GuidContaCRM'].str.lower()
        contas['accountid']=contas['accountid'].str.lower()        
                
        def salva_arquivo(data_base,nome_email):
                    outlook = win32.Dispatch('outlook.application')
                    mapi = outlook.GetNamespace("MAPI")
                    inbox = mapi.GetDefaultFolder(6).Folders['Custodiantes']
                    messages = inbox.Items
                    print('Processando ', len(messages), 'emails')
                    send_dt = str(date.today())
                    achei_arquivo = False
                    for message in messages:
                        message_dt = message.senton.date()
                        print('Data do email:', message_dt, type(message_dt))
                        assunto = message.subject
                        if nome_email in assunto:
                            print(data_base, message_dt)
                            if data_base == message_dt:
                                cc = message.CC
                                body = message.HTMLBody
                                html_body = BeautifulSoup(body,"lxml")
                                html_tables = html_body.find_all('table')
                                df = pd.read_html(str(html_tables))[0]
                                print('entrei')
                    return df    
        
        df = salva_arquivo(today,nome_email)
        
        lin = list(df.loc[0])
        df.columns = lin
        df = df.drop(index=0)
        df['Conta']=df['Conta'].astype('int')
          
        contas['new_numeroconta']=contas['new_numeroconta'].astype('str')
        contas['new_digitoconta']=contas['new_digitoconta'].astype('str')
        contas['n_conta'] = contas.apply(lambda row:(row['new_numeroconta'] + row['new_digitoconta']) if row['new_digitoconta']!='None' else row['new_numeroconta'], axis = 1 ).astype('int')
        contas = pd.merge(left = contas, right = df, left_on = 'n_conta',right_on = 'Conta', how='right')
        contas['Saldo Conta Corrente'] =contas['Saldo Conta Corrente'].str.replace(".", "")
        contas['Saldo Conta Corrente'] =contas['Saldo Conta Corrente'].str.replace(",", ".").str.replace("R", "").str.replace("$", "").astype('float')
        contas = pd.merge(left = contas, right = sc, right_on = 'GuidContaCRM', left_on = 'accountid', how='left')
        contas = contas.sort_values(by=['CartIdFonte']).drop_duplicates(subset = ['accountid'])
        verificar_cadastro  = contas[contas['accountid'].isnull()]
        verificar_cadastro  = verificar_cadastro[['n_conta','Apelido']]
        contas = contas.dropna(subset=['NomeSuperCarteiraCRM'])
        contas = pd.merge(right = contas, left = pl_sc, on = 'NomeSuperCarteiraCRM',how='right')
        contas['Peso'] = contas['Saldo Conta Corrente']/contas['PL']
        contas['new_titularidadeid'] =contas['new_titularidadeid'].str.lower()
        contas['DataFonte'] = d3
                            
        dados = contas['DataFonte']
        layout_d1 = pd.DataFrame(data = dados )
        layout_d1['DataFonte'] = d3
        layout_d1['DataArquivo'] = today
        layout_d1['NomeContaCRM']=contas['NomeContaCRM']
        layout_d1['NomeSuperCarteiraCRM']=contas['NomeSuperCarteiraCRM']
        layout_d1['Fonte']='Saldos_PF'
        layout_d1['IdFonte']= contas['CartIdFonte']
        layout_d1['Classe']='R Fixa Pós'
        layout_d1['SubClasse']='RF Pos Liquidez'
        layout_d1['TipoProduto']='CAIXA'
        layout_d1['GuidProduto']='c6999643-98b7-df11-85ec-d8d385b9752e'
        layout_d1['IdProdutoProfitSGI']='999996'
        layout_d1['IdBDS']='81341250'
        layout_d1['NomeProduto']='Caixa'
        layout_d1['GuidEmissor']=None
        layout_d1['RatingGPS']='SEM_RATING_EI'
        layout_d1['DataEmissao'] = today
        layout_d1['DataVencimento'] = today
        layout_d1['FinanceiroFinal'] = contas['Saldo Conta Corrente']
        layout_d1['FinanceiroInicial'] = contas['Saldo Conta Corrente']
        layout_d1['FinanceiroFuturo'] =contas['Saldo Conta Corrente']
        layout_d1['FinanceiroCPR'] = 0
        layout_d1['NomeEmissor']=None
        layout_d1['Indexador']='CDI'
        layout_d1['Taxa']=100.00
        layout_d1['Coupon'] =None
        layout_d1['QuantidadeFinal'] =contas['Saldo Conta Corrente']
        layout_d1['PrecoInicial'] =1.00
        layout_d1['MargemC'] =None
        layout_d1['MargemV'] =None
        layout_d1['QtMd'] =None
        layout_d1['PrecoF'] =None
        layout_d1['PesoFinal'] = contas['Peso']
        layout_d1['PesoInicial'] = contas['Peso']
        layout_d1['PesoFuturo'] =contas['Peso']
        layout_d1['Duration'] =None
        layout_d1['MTH'] =None
        layout_d1['Stress']=None
        layout_d1['TaxaMtM']=None
        layout_d1['CouponMtM']=None
        layout_d1['Isento']=0
        layout_d1['GuidContaMovimento'] =contas['GuidContaMovimento']
        layout_d1['CodigoCetip'] =None
        layout_d1['NomeContaMovimento'] =None
        layout_d1['NomeBanco'] = banco
        layout_d1['NumeroAgencia'] = None
        layout_d1['BoletasAplicadasNaPosicao'] = None
        layout_d1['QtdeBloq'] = None
        layout_d1['ProdutoID'] = 23044
        layout_d1['Titularidade'] = contas['new_titularidadeidname']
        layout_d1['TitularidadeGuid'] = contas['new_titularidadeid'].str.lower()
        layout_d1 = layout_d1[layout_d1['GuidContaMovimento']!=0]
        layout_d1 = layout_d1.drop_duplicates(subset = ['TitularidadeGuid'])
        dm1.importar_saldos(layout_d1)
 

                             


if __name__ == '__main__':
    
    ##Rodar a zeragem BTG    
    btg = ZeragemBtg()
    btg.processar_email()
    
    ##Rodar Zeragem Santander   
    #ZeragemSant().processar_email()
    
    ##Rodar Zeragem Bradesco    
    #ZeragemBrad().processar_email()   

    
    ##Rodar RV automatico
    # z=Automatizacao()
    # z.importar_rv()
    
    ##Ajuste de futuros
    # z= AjusteFuturos()
    # z.caixa_para_ajustes()
    
    ##Inclusão saldos pessoa física
    #z=InclusaoSaldos()
    #z.importar_Saldos_banco(nome_email = 'Saldos BTG PACTUAL',banco = 'Banco BTG Pactual SA')
    #z.importar_Saldos_banco(nome_email = 'Saldos Banco Alfa',banco = 'Banco Alfa de Investimento SA')
    #z=inclusao_saldos
    #z.importar_saldos_pf_brad()
   


