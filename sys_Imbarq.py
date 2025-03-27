# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 15:50:47 2023

@author: u45285
"""

import os
from databases import Crm as crm, BDS
import pandas as pd
from databases import Bawm as bawm, PosicaoDm1 as dm1
import time 
from emailer import Email 
from funcoes_datas import FuncoesDatas
from datetime import date
import xlwings as xw
from databases import PosicaoDm1, PosicaoDm1Pickle, Crm, Boletador, Secundario, SolRealocacao, Bawm, BDS
import numpy as np

bds = BDS()
crm = crm()
dm1 = dm1()
bawm = bawm()
data = FuncoesDatas()

def imbarq():
    
    #Buscando e tratando o xml baixado da B3
    caminho = "O:/SAO/CICH_All/Renda Variável/IMBARQ - Controle Geral de Clientes e Movimentações/Arquivos IMBARQ/06"
    lista_arquivos = os.listdir(caminho)
    
    lista_datas = []
    for arquivo in lista_arquivos:
        # descobrir a data desse arquivo
        if ".txt" in arquivo:
            datas = os.path.getmtime(f"{caminho}/{arquivo}")
            lista_datas.append((datas, arquivo))    
    lista_datas.sort(reverse=True)
    ultimo_arquivo = lista_datas[0]
    print(ultimo_arquivo[1])
    
    df_rv = pd.read_fwf(f"O:/SAO/CICH_All/Renda Variável/IMBARQ - Controle Geral de Clientes e Movimentações/Arquivos IMBARQ/06/{ultimo_arquivo[1]}", header=None, sep =';')
    
    tipos = ['015265', '025265','065265','165265','175265']
    caminho = 'O:/SAO/CICH_All/Portfolio Management/18-Imbarq/'
    arquivos = [open( caminho + tipo + '.txt', 'w') for tipo in tipos]
    
    with open(f'O:/SAO/CICH_All/Renda Variável/IMBARQ - Controle Geral de Clientes e Movimentações/Arquivos IMBARQ/06/{ultimo_arquivo[1]}', 'r') as arq:
        for linha in arq:
            for tipo, arquivo in zip(tipos, arquivos):
                if tipo in linha:
                    arquivo.write(linha)    
    caminho = "O:/SAO/CICH_All/Renda Variável/IMBARQ - Controle Geral de Clientes e Movimentações/Arquivos IMBARQ/12"
    lista_arquivos = os.listdir(caminho)
    
    lista_datas = []
    for arquivo in lista_arquivos:
        # descobrir a data desse arquivo
        if ".txt" in arquivo:
            datas = os.path.getmtime(f"{caminho}/{arquivo}")
            lista_datas.append((datas, arquivo))    
    lista_datas.sort(reverse=True)
    ultimo_arquivo = lista_datas[0]
    print(ultimo_arquivo[1])

    tipos = ['265265', '285265','305265']
    caminho = 'O:/SAO/CICH_All/Portfolio Management/18-Imbarq/'
    arquivos = [open( caminho + tipo + '.txt', 'w') for tipo in tipos]
    
    with open(f'O:/SAO/CICH_All/Renda Variável/IMBARQ - Controle Geral de Clientes e Movimentações/Arquivos IMBARQ/12/{ultimo_arquivo[1]}', 'r') as arq:
        for linha in arq:
            for tipo, arquivo in zip(tipos, arquivos):
                if tipo in linha:
                    arquivo.write(linha)
    time.sleep(240)  

    def busca_especificacao(nome_especificacao):
        arquivo = 'O:/SAO/CICH_All/Portfolio Management/18-Imbarq/Imbarque_especificacao.xlsx'
        df = pd.read_excel(arquivo)
        df = df[df['Arquivo']==nome_especificacao].sort_values('Ordem')
        return list(df['Lenght'].to_list())
                             
    def busca_coluna(nome_especificacao):
        arquivo = 'O:/SAO/CICH_All/Portfolio Management/18-Imbarq/Imbarque_especificacao.xlsx'
        df = pd.read_excel(arquivo)
        df = df[df['Arquivo']==nome_especificacao].sort_values('Ordem')
        return list(df['Coluna'].to_list())
    
    #Controle de Posições de Opções
    ##015265 -Contêm dados dos mercados: derivativos futuros (2), opções sobre disponível (3), opções sobre futuro (4), opções de compraações (70), opções de venda de ações (80) e Swap Cambial com Ajuste Periódico Baseado em Operações Compromissadas de Um Dia (5) 
    ###larguras_015265 = [2,15,15,15,15,35,35,4,35,12,3,10,30,4,10,10,12,10,26,1,10,1,10,10,1,15,1,15,1,15,15,15,1,15,15,15,1,15,1,15,15,1,15,15,15,15,1,15,20,20,1,20,1,20,1,20,1,20,1,20,1,20,1,20,6,1,23,1,23,1,23,1,23,3,1,23,1,23,1,23,1,23,1,20,13]
    ###controle_posicao_opcoes.columns=['Tipo de Registro','Código do Participante Solicitante','Código do Investidor Solicitante','Código do Participante Solicitado','Código do Investidor Solicitado','Código Instrumento','Código Origem Identificação Instrumento','Código Bolsa Valor','Código de Negociação','ISIN','Mercado','Lote Padrão/Tamanho do contrato','Mercadoria','Código de Vencimento','Indicador Tipo de série','Distribuição da opção','ISIN do Ativo Objeto','Distribuição do ativo-objeto','Preço de Exercício','Tipo de Opção','Fator de cotação','Estilo de Opção','Data de Vencimento','Data da Posição','Natureza Posição Inicial','Valor Posição Inicial','Natureza Posição Vencida','Valor Posição Vencida','Natureza Posição Encerrada por Exercício','Valor Posição Encerrada por Exercício','Quantidade Comprada no Dia','Quantidade Vendida no Dia','Natureza Posição Enviada por Transferência','Valor Posição Enviada por Transferência','Posição Comprada Recebida por Transferência','Posição Vendida Recebida por Transferência','Natureza Posição Encerrada por Entrega Física','Valor Posição Encerrada por Entrega Física','Natureza Posição Atual','Valor Posição Atual','Posição Comprada Bloqueada por Exercício','Natureza Posição Atual após eventos corporativos','Posição atual apos eventos corporativos','Posição Coberta Vendida','Posição Descoberta Vendida','Posição BOX','Natureza Posição Encerrada','Valor Posição Encerrada','Valor Posição Comprada Atual','Valor Posição Vendida Atual','Natureza Ajuste Diário','Valor Ajuste Diário','Natureza Ajuste Diário Relacionado à Posição Inicial','Valor Ajuste Diário Relacionado à Posição Inicial','Natureza Ajuste Diário Referente à Posição Recebida por Transferência','Posição Ajuste Diário Referente à Posição Recebida por Transferência','Natureza Ajuste Diário Relacionado aos Negócios do Dia','Valor Ajuste Diário Relacionado aos Negócios do Dia','Natureza Ajuste Acumulado','Valor Ajuste Acumulado','Natureza Ajuste Acumulado Aplicado sobre a Posição Encerrada','Valor Ajuste Acumulado Aplicado sobre a Posição Encerrada','Natureza Prêmio de Opção','Valor Prêmio de Opção','Código Variável Valor Final','Natureza Valor Consolidado de Negócios do Dia - Variável Valor Final','Valor Consolidado de Negócios do Dia - Variável Valor Final','Natureza Valor Recebido por Transferência no Dia - Variável Valor Final','Valor Recebido por Transferência no Dia - Variável Valor Final','Natureza Valor Enviado por Transferência no Dia - Variável Valor Final','Valor Enviado por Transferência no Dia - Variável Valor Final','Natureza Posição Atualizada do Dia - Variável Valor Final','Valor Posição Atualizada do Dia - Variável Valor Final','Instrumento do Cupom','Natureza Valor Consolidado de Negócios do Dia - Variável Cupom','Valor Consolidado de Negócios do Dia - Variável Cupom','Natureza Valor Recebido por Transferência no Dia - Variável Cupom','Valor Recebido por Transferência no Dia - Variável Cupom','Natureza Valor Enviado por Transferência no Dia - Variável Cupom','Valor Enviado por Transferência no Dia - Variável Cupom','Natureza Posição Atualizada do Dia - Variável Cupom','Valor Posição Atualizada do Dia - Variável Cupom','Natureza Valor de Liquidação','Valor de Liquidação','Reserva']
    larguras_015265 = busca_especificacao('ControleDerivativo')
    larguras_015265 = [int(x) for x in larguras_015265]
    colunas = busca_coluna('ControleDerivativo')
    controle_posicao_opcoes = pd.read_fwf('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/015265.txt', header=None,widths =larguras_015265)
    controle_posicao_opcoes.columns = colunas
    controle_posicao_opcoes.to_excel('O:/SAO/CICH_All/Portfolio Management/18-Imbarq/controle_posicao_opcoes.xlsx')

    #Controle de Posições do Mercado à Vista
    #025265 - Contêm dados dos mercados: ações (10), disponível (1), ETF Primário (8), exercício de opções de Call (12), exercício de opções de Put (13), fracionário (20), leilão (17) e fixed income (5)
    ###larguras_025265 = [2,15,15,15,15,15,15,35,35,4,12,35,5,3,10,10,10,35,15,20,26,15,20,26,26,26,540]
    
    larguras_025265 = busca_especificacao('Mercadoavista')
    larguras_025265 = [int(x) for x in larguras_025265]
    colunas = busca_coluna('Mercadoavista')
    controle_posicao_avista = pd.read_fwf('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/025265.txt', header=None,widths =larguras_025265)
    controle_posicao_avista.columns = colunas
    controle_posicao_avista.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/controle_posicao_avista.xlsx')    

    #Resultados líquidos liquidados
    #175265 - Contêm dados dos resultados das liquidações em ativos
    # larguras_025265 = [2,15,15,15,15,10,35,35,4,12,35,35,1,35,35,4,12,10,3,35,10,10,10,35,26,10,15,15,26,15,15,26,15,20,1,1,1,26,10,15,1,1,1,64,15,15,238]
    #controle_emprestimo_ativos.columns=['Tipo de Registro','Código do Participante Solicitante','Código do Investidor Solicitante','Código do Participante Solicitado','Código do Investidor Solicitado','Código Custodiante','Código do Investidor no Custodiante','Código Instrumento','Código Origem Identificação Instrumento','Código Bolsa Valor','ISIN','Código de Negociação','Distribuição','Mercado','Fator de cotação','Data do Pregão','Data de Liquidação','Código da Carteira','Quantidade Comprada','Volume Financeiro Comprado','Preço Médio Compra','Quantidade Vendida','Volume Financeiro Vendido','Preço Médio Venda','Posição Coberta Vendida','Posição Descoberta Vendida','Reserva']
    larguras_175265 = busca_especificacao('ativos_a_liquidar')
    larguras_175265 = [int(x) for x in larguras_175265]
    try:
        ativos_a_liquidar = pd.read_fwf('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/175265.txt', header=None,widths = larguras_175265)
        ativos_a_liquidar.columns = busca_coluna('ativos_a_liquidar')
        ativos_a_liquidar.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/ativos_a_liquidar.xlsx')
    except:
        print('Arquivo em branco')    

    #Resultados líquidos liquidados
    #165265 - Contêm dados dos resultados das liquidações em ativos
    # larguras_025265 = [2,15,15,15,15,10,35,35,4,12,35,35,1,35,35,4,12,10,3,35,10,10,10,35,26,10,15,15,26,15,15,26,15,20,1,1,1,26,10,15,1,1,1,64,15,15,238]
    #controle_emprestimo_ativos.columns=['Tipo de Registro','Código do Participante Solicitante','Código do Investidor Solicitante','Código do Participante Solicitado','Código do Investidor Solicitado','Código Custodiante','Código do Investidor no Custodiante','Código Instrumento','Código Origem Identificação Instrumento','Código Bolsa Valor','ISIN','Código de Negociação','Distribuição','Mercado','Fator de cotação','Data do Pregão','Data de Liquidação','Código da Carteira','Quantidade Comprada','Volume Financeiro Comprado','Preço Médio Compra','Quantidade Vendida','Volume Financeiro Vendido','Preço Médio Venda','Posição Coberta Vendida','Posição Descoberta Vendida','Reserva']
    larguras_165265 = busca_especificacao('Liquidacao_ativos')
    larguras_165265 = [int(x) for x in larguras_165265]
    try:
        ativos_liquidados = pd.read_fwf('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/165265.txt', header=None,widths = larguras_165265)
        ativos_liquidados.columns = busca_coluna('Liquidacao_ativos')
        ativos_liquidados.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/ativos_liquidados.xlsx')
    except:
        print('Arquivo em branco')
    
    #Posições de empréstimo de ativos
    #065265 - Contêm dados dos mercados: empréstimo de ativos (91, 92, 93 e 94
    # larguras_025265 = [2,15,15,15,15,10,35,35,4,12,35,35,1,35,35,4,12,10,3,35,10,10,10,35,26,10,15,15,26,15,15,26,15,20,1,1,1,26,10,15,1,1,1,64,15,15,238]
    #controle_emprestimo_ativos.columns=['Tipo de Registro','Código do Participante Solicitante','Código do Investidor Solicitante','Código do Participante Solicitado','Código do Investidor Solicitado','Código Custodiante','Código do Investidor no Custodiante','Código Instrumento','Código Origem Identificação Instrumento','Código Bolsa Valor','ISIN','Código de Negociação','Distribuição','Mercado','Fator de cotação','Data do Pregão','Data de Liquidação','Código da Carteira','Quantidade Comprada','Volume Financeiro Comprado','Preço Médio Compra','Quantidade Vendida','Volume Financeiro Vendido','Preço Médio Venda','Posição Coberta Vendida','Posição Descoberta Vendida','Reserva']
    larguras_065265 = busca_especificacao('EmprestimoAtivos')
    larguras_065265 = [int(x) for x in larguras_065265]
    try:
        controle_emprestimo_ativos = pd.read_fwf('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/065265.txt', header=None,widths = larguras_065265)
        controle_emprestimo_ativos.columns = busca_coluna('EmprestimoAtivos')
        controle_emprestimo_ativos.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/controle_emprestimo_ativos.xlsx')
    except:
        print('Arquivo em branco')   
        
    #Saldo de custódia
    #285265 - Contêm o saldo geral de investidores em um Agente de Custódia
    
    larguras_285265 = busca_especificacao('saldo_custodia')
    larguras_285265 = [int(x) for x in larguras_285265]
    try:
        saldo_custodia = pd.read_fwf('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/285265.txt', header=None,widths = larguras_285265)
        saldo_custodia.columns = busca_coluna('saldo_custodia')
        saldo_custodia['Código de negociação'].replace('000','')
        saldo_custodia.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/saldo_custodia.xlsx')
        saldo_custodia['Código de negociação'] = saldo_custodia['Código de negociação'].apply(lambda x: x.replace('000',''))
    except:
        print('Arquivo em branco')   

    #Identificação do Saldo Analítico
    #305265 - Contêm o saldo analítico para ISIN com característica de renda fixa de investidores em um Agente de Custódia
    
    larguras_305265 = busca_especificacao('saldo_analitico')
    larguras_305265 = [int(x) for x in larguras_305265]
    try:
        saldo_analitico = pd.read_fwf('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/285265.txt', header=None,widths = larguras_305265)
        saldo_analitico.columns = busca_coluna('saldo_analitico')
        saldo_analitico.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/saldo_analitico.xlsx')
    except:
        print('Arquivo em branco')   
        
    #Puxando as contas do CRM
    contas = crm.cod_imbarq()
    contas= contas.dropna(subset = ['new_numeroconta'])
    contas = contas[contas['new_numeroconta'] != '328196-4']
    contas['new_numeroconta'] = contas['new_numeroconta'].astype(int)
    contas['accountid']=contas['accountid'].str.lower()
    titularidades = contas[['accountid','new_esteiraname','accountcategorycodename']]
    
    #Retirando contas que estão duplicadas no CRM
    contas_duplicadas = contas[contas.new_numeroconta.duplicated()]
    contas_duplicadas = contas_duplicadas.drop(columns = ['new_titularidadeid'])
    
    #Puxando os produtos do CRM, da D1 e os Ticker e ISINs do Imbarq
    produtos = crm.Produtos_isin_ticker()
    produtos['productid']= produtos['productid'].str.lower()
    produtos = produtos.drop_duplicates(subset =['new_isin'],keep = 'first')
    posicao= dm1.posicao_total(trazer_colunas='GuidContaCRM',data_pos=data.hoje())
    posicao['GuidProduto']= posicao['GuidProduto'].str.lower()
    posicao['GuidContaCRM']= posicao['GuidContaCRM'].str.lower()
    ticker_isin = saldo_custodia[['Código ISIN','Código de negociação']]
    
    
    #Ativos_BDS
    lista_ativos_rv = bds.consulta_ativos_RV()
    lista_ativos_rv['productid'] = lista_ativos_rv['productid'].str.lower()
    lista_ativos_rv.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/lista_ativos_rv.xlsx')
    contas.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/contas.xlsx')
    #pf.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/pf.xlsx')
    
    #Gestores dos fundos 
    
    gestor_fundos = bawm.fundo_gestor()

    #Incluindo o ISIN na Posicao b3
    posicao = pd.merge(left = posicao, right = produtos, left_on = 'GuidProduto', right_on ='productid', how ='left')    
    
        
    #Ajuste da Plan Posição
    posicao= posicao[['GuidContaCRM','NomeContaCRM','new_isin','name','QuantidadeFinal']]
    posicao = posicao.groupby(['GuidContaCRM','NomeContaCRM','new_isin','name']).sum()
    posicao = posicao.reset_index()
    posicao['GuidContaCRM'] = posicao['GuidContaCRM'].str.lower()
    posicao.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/posicao_dm1.xlsx')
    
    #Configurando o saldo em custódia com as informações necessárias para concatenar com as movimentações.
    
    saldo_custodia_d2 = saldo_custodia[['Código do Investidor Solicitado','Código ISIN']]
    saldo_custodia_d2['qtd_v1'] = saldo_custodia['Quantidade de ações em custódia'] + saldo_custodia['Quantidade total de ações bloqueadas']
    saldo_custodia_d2 = pd.merge(left = saldo_custodia_d2,left_on = 'Código do Investidor Solicitado', right = contas, right_on = 'new_numeroconta',how = 'left') 
 
      saldo_custodia_d2 = saldo_custodia_d2.drop(columns = ['new_numeroconta','new_titularidadeid','new_bancoidname','new_digitoconta'])
    saldo_custodia_d2.columns = ['cod_investidor', 'isin','qtd','accountid','NomeCRM','esteira','nome_esteira','tipo','categoria']
    saldo_custodia_d2['cod_investidor'] = saldo_custodia_d2 ['cod_investidor'] .astype(int)
    saldo_custodia_d2 = saldo_custodia_d2.drop_duplicates()
    saldo_custodia_d2.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/saldo_custodia_d2.xlsx')    
    
    #Deixando as movimentações no mesmo padrão dos dados da custódia paea concatenar os dados.
    
    Movimentos_d2 = ativos_a_liquidar[['Código do Investidor Solicitado','ISIN','Natureza da Operação','Quantidade Total da Instrução de Liquidação']]
    Movimentos_d2.columns = ['cod_investidor','isin','operacao','qtd']
    Movimentos_d2  = pd.merge(left = Movimentos_d2 ,left_on = 'cod_investidor', right = contas, right_on = 'new_numeroconta',how = 'left')
    Movimentos_d2['qtd_v1'] = Movimentos_d2.apply(lambda x: x.qtd if x.operacao	 == 'C' else (x.qtd *-1), axis = 1)
    Movimentos_d2 = Movimentos_d2.drop(columns = ['qtd','operacao','new_titularidadeid','new_numeroconta','new_bancoidname'])
    Movimentos_d2['cod_investidor'] = Movimentos_d2['cod_investidor'].astype(int)
    Movimentos_d2 = Movimentos_d2[['cod_investidor','isin','qtd_v1','new_titularidadeidname','accountid','new_esteira','new_esteiraname','businesstypecodename','accountcategorycodename']]
    Movimentos_d2.columns = ['cod_investidor','isin','qtd','NomeCRM','accountid','esteira','nome_esteira','tipo','categoria']
    Movimentos_d2.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/Movimentos_d2.xlsx')    
        
    #POsição para cruzamento D-1 e B3 (diferença é o guid produto)
    posicao_consolidada = pd.concat([saldo_custodia_d2,Movimentos_d2])
    posicao_consolidada = posicao_consolidada[posicao_consolidada['nome_esteira'].notnull()]
    posicao_consolidada = posicao_consolidada.groupby(['isin','NomeCRM','accountid','nome_esteira','categoria']).sum()
    posicao_consolidada = posicao_consolidada.reset_index()
    posicao_consolidada = pd.merge(left = posicao_consolidada, right = produtos, left_on = 'isin', right_on ='new_isin', how ='left')
    posicao_consolidada['NomeCRM']= posicao_consolidada['NomeCRM'].str.lower()
    posicao_consolidada = posicao_consolidada[['new_isin','NomeCRM','accountid','name','qtd','nome_esteira','categoria']]
    posicao_consolidada.columns = ['cod','NomeCRM','accountid','produto','qtd','esteira','tipo']
    posicao_consolidada.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/posicao_consolidada.xlsx')

    #Tirando o Vic Desenvolvimento, pois os recibos não são tratados no sistema
    vic_dev = ['BRVDSVR01M15','BRVIDSR01M12','BRVIDSR02M11','BRVIDSR03M10','BRVIDSR04M19','BRVIDSR05M18','BRVIDSR06M17','BRVIDSR07M16','BRVIDSR08M15','BRVIDSR09M14','BRVIDSR10M11','BRVIDSR11M10','BRVIDSR12M19','BRVIDSR13M18','BRVDSVR01M15','BRVDSVR02M14','BRVDSVR01M15','BRVDSVR02M14','BRVIDSR04M19','BRVIDSR09M14','BRVIDSR10M11','BRVIDSR11M10','BRVIDSR12M19','BRVDSVR03M13','BRVIDSR13M18','BRVIDSR05M18','BRVIDSR08M15','BRVIDSR07M16','BRVIDSR02M11','BRVIDSR03M10']

    #Fundos que não estão cadastrados no CRM e precisam ser identificados
    cod_fora_CRM = saldo_custodia_d2[saldo_custodia_d2['NomeCRM'].isnull()]
    cod_fora_CRM =cod_fora_CRM['cod_investidor'].drop_duplicates().reset_index()  

    #Ativos que estão no IMBARQ e não estão no CRM:
    ativos_imbarq_mov = ativos_a_liquidar['ISIN'].drop_duplicates()
    ativos_imbarq_mov = pd.merge(left = ativos_imbarq_mov,left_on = 'ISIN',right = ticker_isin,right_on ='Código ISIN',how ='left')
    ativos_imbarq_mov = ativos_imbarq_mov.drop(columns=['ISIN'])
    ativos_imbarq_posicao = saldo_custodia[['Código ISIN','Código de negociação']]
    ativos_imbarq = pd.concat([ativos_imbarq_mov,ativos_imbarq_posicao]).drop_duplicates()
    ativos_imbarq_semIsin = pd.merge(left =ativos_imbarq,left_on ='Código ISIN',right = produtos, right_on ='new_isin',how='left')
    ativos_imbarq_semIsin = ativos_imbarq_semIsin[ativos_imbarq_semIsin['new_isin'].isnull()]
    ativos_imbarq_semIsin = ativos_imbarq_semIsin.drop(columns = ['new_idbds','new_isin','new_tickerid','new_onshore','name','new_nomedaacao','productid','new_idsistemaorigem'])
    ativos_imbarq_semIsin.columns = ['cod_ISIN','ticker']
    ativos_imbarq_semIsin['cod_ISIN'] = ativos_imbarq_semIsin['cod_ISIN']
    ativos_imbarq_semIsin = ativos_imbarq_semIsin[~ativos_imbarq_semIsin.cod_ISIN.isin(vic_dev)]
    ativos_imbarq_semIsin_lista = ativos_imbarq_semIsin['cod_ISIN'].to_list()
    ativos_imbarq_semIsin= ativos_imbarq_semIsin[['ticker','cod_ISIN']]
    ativos_imbarq_semIsin.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/ativos_imbarq_semIsin.xlsx')

    #Fundos sem posição na D-1
    fundos_dm1 = posicao[['NomeContaCRM','GuidContaCRM']]
    fundos_imbarq = posicao_consolidada[['NomeCRM','accountid']]
    fundos_sem_dm1 = pd.merge(left = fundos_imbarq , left_on = 'accountid',right = fundos_dm1,right_on = 'GuidContaCRM',how='left')
    fundos_sem_dm1 = fundos_sem_dm1.drop_duplicates()
    fundos_sem_dm1 = fundos_sem_dm1[fundos_sem_dm1['NomeContaCRM'].isnull()]
    fundos_sem_dm1_lista = fundos_sem_dm1['NomeCRM'].to_list()
    fundos_sem_dm1.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/fundos_sem_dm1_lista.xlsx')          
    
    #Fazendo o batimento da Dm1 com o arquivo da B3, após as configurações para o merge dar certo.
    bate_b3 = pd.merge(left = posicao_consolidada, left_on = ['accountid','cod'], right = posicao, right_on = ['GuidContaCRM','new_isin'], how = 'left')
    fundos_n_identificados_b3 = bate_b3[bate_b3['NomeContaCRM'].isnull()]
    bate_b3 = bate_b3[~bate_b3.new_isin.isin(ativos_imbarq_semIsin_lista)]
    bate_b3 = bate_b3[~bate_b3.NomeCRM.isin(fundos_sem_dm1_lista)]
    bate_b3 = bate_b3[~bate_b3.new_isin.isin(vic_dev)]
    bate_b3['QuantidadeFinal'] = bate_b3['QuantidadeFinal'].fillna(0)
    bate_b3 = bate_b3.groupby(['cod','NomeContaCRM','accountid','esteira','tipo','name']).sum()
    bate_b3 = bate_b3.reset_index()
    bate_b3 = pd.merge(left = bate_b3, right = lista_ativos_rv, left_on = 'cod', right_on ='new_isin', how ='left')
    bate_b3 = bate_b3[['cod','NomeContaCRM','Nome','codser','qtd','QuantidadeFinal','esteira','tipo','name_x']]
    bate_b3.columns = ['cod','Cliente','produto','ticker','qtd_b3','qtd_d1','esteira','tipo','nome_produto_dm1']
    # bate_b3.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/bate_b3.xlsx') 
    
    #Ajustando o batimento da B3 e gerando o excel
    
    bate_b3['diferença'] = bate_b3['qtd_b3']-bate_b3['qtd_d1']
    batimento_b3= bate_b3
    batimento_b3.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/batimento_b3.xlsx')    
    divergencia_b3 = bate_b3[(bate_b3['diferença'] > 1) | (bate_b3['diferença'] < -1) ]
    divergencia_b3 = divergencia_b3.reset_index(drop = True)
    divergencia_b3 = pd.merge(left = divergencia_b3, left_on = 'cod', right = lista_ativos_rv, right_on = 'new_isin' , how = 'left')
    divergencia_b3 = divergencia_b3[['cod','Cliente','ticker','qtd_b3','qtd_d1','diferença','nome_produto_dm1','esteira','tipo']]
    divergencia_b3 = divergencia_b3.drop_duplicates()
    divergencia_b3.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/diferença_b3.xlsx')    
    
    
   
    # #Puxando todos os ativos de RV da DM-1
    ativos_rv_dm1 = pd.merge(left = lista_ativos_rv, left_on = 'new_isin', right = posicao, right_on = 'new_isin', how = 'right')
    # #Ativos que estão na D1 e não possuem ISIN, excluir eles do batimento
    
    ativos_dm1_sem_ISIN =  ativos_rv_dm1.loc[ativos_rv_dm1['new_isin'].isnull()]
    ativos_dm1_semISIN_email = ativos_rv_dm1[ativos_rv_dm1['new_isin'].isnull()].drop_duplicates()
    ativos_dm1_semISIN_email = ativos_dm1_semISIN_email.drop(columns = ['new_isin','NomeContaCRM','QuantidadeFinal'])
    prodtos_isin = lista_ativos_rv[['new_isin','codser']]
    ativos_dm1_semISIN_email = pd.merge(left = ativos_dm1_semISIN_email , right = prodtos_isin, how = 'left')
    ativos_dm1_semISIN_email = ativos_dm1_semISIN_email.dropna(subset=['new_isin']).drop_duplicates()
    ativos_dm1_semISIN_email = ativos_dm1_semISIN_email[['codser','new_isin']]
    ativos_dm1_semISIN_email.columns = ['ticker','cod_ISIN']
    ativos_rv_dm1 = ativos_rv_dm1.drop(ativos_dm1_sem_ISIN.index)   
    ativos_rv_dm1 = ativos_rv_dm1[['NomeContaCRM','GuidContaCRM','name_x','codser','QuantidadeFinal','new_isin','productid']]
    ativos_rv_dm1.columns = ['Nome_CRM','GuidContaCRM','produto','ticker','qtd_final','isin','productid']
    ativos_rv_dm1 = ativos_rv_dm1.dropna(subset = ['productid'])    
    
    #Fundos da D-1 que não estão no Imbarq
    fundos_imbarq = posicao_consolidada[['accountid','NomeCRM']]     
    fundos_dm1 = ativos_rv_dm1[['GuidContaCRM','Nome_CRM']]  
    fundos_fora_imbarq = pd.merge(left = fundos_dm1, left_on ='GuidContaCRM',right = fundos_imbarq , right_on = 'accountid', how ='left')
    fundos_fora_imbarq = fundos_fora_imbarq[fundos_fora_imbarq['NomeCRM'].isnull()].drop_duplicates()
    monitorados = fundos_fora_imbarq.loc[fundos_fora_imbarq['Nome_CRM'].str.contains('monitorado')]
    fundos_fora_imbarq = fundos_fora_imbarq.drop(monitorados.index).drop(columns = ['NomeCRM']).reset_index(drop = True).drop_duplicates()
    fundos_fora_imbarq_lista = fundos_fora_imbarq['Nome_CRM'].to_list()
    fundos_fora_imbarq.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/fundos_fora_imbarq.xlsx')

    #Cruzando a DM1 com o Imbarq
    bate_dm1_imbarq = pd.merge(left = ativos_rv_dm1, left_on = ['GuidContaCRM','isin'], right = posicao_consolidada, right_on =['accountid','cod'], how = 'left')
    fundos_n_identificados_d1 = bate_dm1_imbarq[bate_dm1_imbarq['NomeCRM'].isnull()]
    bate_dm1_imbarq= bate_dm1_imbarq.dropna(subset = ['Nome_CRM'])
    bate_dm1_imbarq = bate_dm1_imbarq[['GuidContaCRM','Nome_CRM','produto_x','cod','isin','ticker','qtd_final','qtd','esteira','tipo']]
    bate_dm1_imbarq = bate_dm1_imbarq[~bate_dm1_imbarq.cod.isin(vic_dev)]
    bate_dm1_imbarq = bate_dm1_imbarq[~bate_dm1_imbarq.Nome_CRM.isin(fundos_fora_imbarq_lista)]
    bate_dm1_imbarq['qtd'] = bate_dm1_imbarq['qtd'].fillna(0)
    bate_dm1_imbarq['diferença'] = bate_dm1_imbarq['qtd_final']-bate_dm1_imbarq['qtd']
    batimento_dm1_b3 =  bate_dm1_imbarq[['Nome_CRM','produto_x','isin','ticker','qtd_final','qtd','esteira','diferença']]
    batimento_dm1_b3.columns = ['Nome_CRM','nome_produto','isin','ticker','qtd_dm1','qtd_b3','esteira','diferença']
    batimento_dm1_b3.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/batimento_dm1_b3.xlsx')
    divergencia_dm1 = bate_dm1_imbarq[(bate_dm1_imbarq['diferença'] > 1) | (bate_dm1_imbarq['diferença'] < -1) ]  
    divergencia_dm1 = divergencia_dm1[~divergencia_dm1.cod.isin(vic_dev)]
    divergencia_dm1 = divergencia_dm1 [['GuidContaCRM','Nome_CRM','ticker','isin','qtd_final','qtd','diferença']]
    divergencia_dm1 = pd.merge(left = divergencia_dm1, left_on = 'GuidContaCRM',right = titularidades,right_on ='accountid', how='left')
    divergencia_dm1 = divergencia_dm1 [['Nome_CRM','ticker','isin','qtd_final','qtd','diferença','new_esteiraname','accountcategorycodename']]
    divergencia_dm1.columns = ['Cliente','ticker','cod','qtd_d1','qtd_b3','diferença','esteira','tipo']
    divergencia_dm1.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/divergencia_dm1.xlsx')
        
    #Juntando as tabelas
    diferencas_concat = pd.concat([divergencia_dm1,divergencia_b3])
    diferencas_concat = diferencas_concat[['Cliente','cod','ticker','qtd_d1','qtd_b3','diferença','esteira','tipo']]
    diferencas_fundos= diferencas_concat[diferencas_concat['tipo']=='Fundos']
    diferencas_fundos= diferencas_fundos.drop(columns = ['cod','tipo']).drop_duplicates().sort_values(by=['diferença'])
    diferencas_pf= diferencas_concat[diferencas_concat['tipo']!='Fundos']
    diferencas_pf= diferencas_pf.drop(columns = ['cod','tipo']).drop_duplicates().sort_values(by=['diferença'])
    diferencas_concat = divergencia_dm1.reset_index(drop = True)
    diferencas_fundos['diferença']= diferencas_fundos['diferença'].apply(lambda x : abs(x))
    diferencas_pf['diferença']=  diferencas_pf['diferença'].apply(lambda x : abs(x))
    diferencas_fundos = diferencas_fundos.drop_duplicates()
    diferencas_pf = diferencas_pf.drop_duplicates()
    #diferencas_pf['qtd_b3'] = diferencas_pf['qtd_b3'].map('{:.1f}'.format)
    #diferencas_pf['qtd_d1'] = diferencas_pf['qtd_d1'].map('{:.1f}'.format)
    #diferencas_pf['diferença'] = diferencas_pf['diferença'].map('{:.1f}'.format)
    #diferencas_fundos['qtd_b3'] = diferencas_fundos['qtd_b3'].map('{:.1f}'.format)
    #diferencas_fundos['qtd_d1'] = diferencas_fundos['qtd_d1'].map('{:.1f}'.format)
    #diferencas_fundos['diferença'] = diferencas_fundos['diferença'].map('{:.1f}'.format)
    diferencas_fundos = diferencas_fundos.reset_index(drop = True)
    diferencas_pf = diferencas_pf.reset_index(drop = True)

 #Verificar a quantidade de ativos totais

    fundos_n_identificados_b3 = fundos_n_identificados_b3[['cod','qtd']]
    fundos_n_identificados_d1 = fundos_n_identificados_d1[['isin','qtd_final']]
    fundos_n_identificados_d1.columns = ['cod','qtd']
    fundos_n_identificados = pd.concat([fundos_n_identificados_b3,fundos_n_identificados_d1])
    fundos_n_identificados= fundos_n_identificados_d1.groupby(['cod']).sum().reset_index()
    agrupamento_ativos_b3 = posicao_consolidada[['cod','qtd']]
    agrupamento_ativos_dm1 = posicao[['new_isin','QuantidadeFinal']]
    agrupamento_ativos_b3 = agrupamento_ativos_b3.groupby(['cod']).sum().reset_index()
    agrupamento_ativos_dm1 = agrupamento_ativos_dm1.groupby(['new_isin']).sum().reset_index()
    verificacao_ativos = pd.merge(left = agrupamento_ativos_b3, right =agrupamento_ativos_dm1, left_on = 'cod', right_on = 'new_isin', how = 'left')
    verificacao_ativos['diferença']= verificacao_ativos['qtd']-verificacao_ativos['QuantidadeFinal']
    verificacao_ativos = pd.merge(left = verificacao_ativos,right = lista_ativos_rv, left_on = 'cod',right_on ='new_isin',how = 'left')
    verificacao_ativos = pd.merge(left = verificacao_ativos,right = fundos_n_identificados, on = 'cod',how = 'left')
    verificacao_ativos = verificacao_ativos.dropna(subset = ['new_isin_x'])
    verificacao_ativos = verificacao_ativos[['codser','qtd_x','QuantidadeFinal','diferença','qtd_y']]
    verificacao_ativos= verificacao_ativos.drop_duplicates()
    verificacao_ativos = verificacao_ativos[verificacao_ativos['diferença']!=0]
    verificacao_ativos.columns = ['Ticker','Qtd_B3','Qtd_D1','Diferença','qtd_fora_batimento']
    verificacao_ativos['Qtd_B3'] = verificacao_ativos['Qtd_B3'].astype(float)
    verificacao_ativos['Qtd_B3'] = verificacao_ativos['Qtd_B3'].map('{:.1f}'.format)
    verificacao_ativos['Qtd_D1'] = verificacao_ativos['Qtd_D1'].astype(float)
    verificacao_ativos['Qtd_D1'] = verificacao_ativos['Qtd_D1'].map('{:.1f}'.format)
    verificacao_ativos['Diferença'] = verificacao_ativos['Diferença'].astype(float)
    verificacao_ativos['Diferença'] = verificacao_ativos['Diferença'].map('{:.1f}'.format)
    verificacao_ativos['qtd_fora_batimento'] = verificacao_ativos['qtd_fora_batimento'].fillna(0)
    verificacao_ativos['qtd_fora_batimento'] = verificacao_ativos['qtd_fora_batimento'].astype(float)
    verificacao_ativos['qtd_fora_batimento'] = verificacao_ativos['qtd_fora_batimento'].map('{:.1f}'.format)
    verificacao_ativos = verificacao_ativos.reset_index(drop = True)
     # # ativos a incluir verificacao_ativos[verificacao_ativos['new_isin_x'].isnull()]_ativos['qtd_que_só_tem_na_B3'].fillna(0)    
    
    #Gerando excel para e-mail:
    with pd.ExcelWriter('C:/Temp/Foundation/imbarq.xlsx') as writer:  
        diferencas_fundos.to_excel(writer, sheet_name='diferencas_fundos')
        diferencas_pf.to_excel(writer, sheet_name='diferencas_pf')
        verificacao_ativos.to_excel(writer, sheet_name='verificacao_ativos')    
            
    #Configurando o e-mail 
    ativos_sem_isin = pd.concat([ativos_dm1_semISIN_email,ativos_imbarq_semIsin])
    ativos_imbarq_semIsin.to_excel('O:/SAO\CICH_All/Portfolio Management/18-Imbarq/ativos_sem_isin.xlsx')
    
    #Emails
    
    subject='Ativos sem ISIN no CRM'
    to=['processamento@jbfo.com','fundos@jbfo.com','portfolio@jbfo.com','mariana.drumond@jbfo.com','rafael.ribeiro@jbfo.com','joao.freitas@jbfo.com','guilherme.liberatore@jbfo.com','bernard.ledoux@jbfo.com']
    text = '''Favor cadastrar os ativos abaixo no CRM.</h3><br>
    {}
    
    '''.format(ativos_sem_isin.to_html())
    
    email = Email(to = to , subject = subject, text= text,send = False)
    
    subject='Fundos fora do Imbarq'
    to=['portfolio@jbfo.com']
    text = '''Favor incluir esses clientes na posição do IMBARQ .</h3><br>
    {}
    
    '''.format(fundos_fora_imbarq.to_html())
    email = Email(to = to , subject = subject, text= text,send = False)
    
    subject='[Imbarq] - Validação de posições'
    to=['processamento@jbfo.com','portfolio@jbfo.com','victor.vega@jbfo.com','bruno.vigorito@jbfo.com']
    anexo = 'O:/SAO/CICH_All/Portfolio Management/18-Imbarq/imbarq.xlsx'
    text = '''Prezados,<br>
    
    Fizemos um batimento entre a posição da B3 e a posição da D-1, encontramos as diferenças que estão na planilha anexa. É possível que o PAS/BOLETAS do cockpit precisem de ajuste ou que existam contas corretoras não cadastradas no CRM..<br>
    <br>
    <br>
    '''
    
    email = Email(to = to , subject = subject, text= text,send = True, attachments = 'C:/Temp/Foundation/imbarq.xlsx')
    

###### adicionando funcoes de Equity ######    
 
    
@xw.func
def Imbarq(Nm06like, Nm12like):
        
    def findfiles(path_folder, fileNmLike):
        files = os.listdir(path_folder)
        for i in files:
            if i[0:26] == fileNmLike:
                if i[0:9] == 'IMBARQ006':
                    df_imbarq06_ori = pd.read_csv(path_folder + "/" + i)
                    df_out = AjustaImbarq06(df_imbarq06_ori)
                    return df_out
                elif i[0:9] == 'IMBARQ012':
                    df_imbarq12_ori = pd.read_csv(path_folder + "/" + i)
                    df_out = AjustaImbarq12(df_imbarq12_ori)
                    return df_out
                
    def AjustaImbarq06(df_imbarq06_ori):
        lista_02 = []
        df_imbarq06_temp = df_imbarq06_ori.copy()
        df_imbarq06_temp = df_imbarq06_temp.rename(columns={df_imbarq06_temp.columns[0]: "All" })
        for i in df_imbarq06_temp['All']:
            if i[0:2] == '02':
                TipoRegistro = i[0:2].rstrip()
                CodigoParticipanteSolicitante = i[2:17].rstrip()
                CodigoInvestidorSolicitante = i[17:32].rstrip()
                CodigoParticipanteSolicitado = i[32:47].rstrip()
                CodigoInvestidorSolicitado = i[47:62].rstrip()
                CodigoCustodiante = i[62:77]
                CodigoInvestidorNoCustodiante = i[77:92]
                CodigoInstrumento = i[92:127]
                CodigoOrigemIdentInstrumento = i[127:162]
                CodigoBolsaValor = i[162:166]
                ISIN = i[166:178]
                CodigoNegociacao = i[178:213].rstrip()
                Distribuicao = i[213:218]
                Mercado = i[218:221]
                FatorCotacao = int(i[221:231])
                DataPregao = i[231:241]
                DataLiquidacao = i[241:251]
                CodigoCarteira = i[251:286]
                QtdComprada = int(i[286:301])
                VolumeFinComprado = int(i[301:321])/100
                PrecoMedioCompra = int(i[321:347])/10000000
                QtdVendida = int(i[347:362])
                VolumeFinanceiroVendido = int(i[362:382])/100
                PrecoMedioVenda = int(i[382:408])/10000000
                PosicaoCobertaVendida = int(i[408:434])
                PosicaoDescobertaVendida = int(i[434:460])/10000000
                Reserva = i[460:1000]
            
                lista_02.append([TipoRegistro,CodigoParticipanteSolicitante,CodigoInvestidorSolicitante,CodigoParticipanteSolicitado, CodigoInvestidorSolicitado,CodigoCustodiante, 
                               CodigoInvestidorNoCustodiante, CodigoInstrumento, CodigoOrigemIdentInstrumento, CodigoBolsaValor, ISIN, CodigoNegociacao, Distribuicao, Mercado, FatorCotacao,
                               DataPregao, DataLiquidacao, CodigoCarteira, QtdComprada, VolumeFinComprado, PrecoMedioCompra,QtdVendida, VolumeFinanceiroVendido, PrecoMedioVenda, PosicaoCobertaVendida,
                               PosicaoDescobertaVendida,Reserva])
                
        df_02_imbarq06 = pd.DataFrame(lista_02, columns=['Tipo_de_Registro', 'Codigo_do_Participante_Solicitante', 'Codigo_do_Investidor_Solicitante', 'Codigo_do_Participante_Solicitado',
                        'Codigo_do_Investidor_Solicitado', 'Codigo_Custodiante', 'Codigo_do_Investidor_no_Custodiante', 'Codigo_Instrumento', 
                        'Codigo_Origem_Identificacao_Instrumento', 'Codigo_Bolsa_Valor', 'ISIN', 'Codigo_de_Negociacao', 'Distribuicao', 'Mercado', 'Fator_de_Cotacao',
                        'Data_do_Pregao', 'Data_de_Liquidacao', 'Codigo_da_Carteira', 'Quantidade_Comprada','Volume_Financeiro_Comprado','Preco_Medio_Compra',
                        'Quantidade_Vendida', 'Volume_Financeiro_Vendido', 'Preco_Medio_Venda','Posicao_Coberta_Vendida','Posicao_Descoberta_Vendida','Reserva'])
        
        return df_02_imbarq06
        
    def AjustaImbarq12(df_imbarq12_ori):
        lista_28 = []
        df_imbarq12_temp = df_imbarq12_ori.copy()
        df_imbarq12_temp = df_imbarq12_temp.rename(columns={df_imbarq12_temp.columns[0]: "All" })    
        for i in df_imbarq12_temp['All']:
            if i[0:2] == '28':
                TipoRegistro = i[0:2].rstrip()
                CodigoParticipanteSolicitante = i[2:17].rstrip()
                CodigoInvestidorSolicitante = i[17:32].rstrip()
                DigitoInvestidorSolicitante = i[32:33].rstrip()
                CodigoParticipanteSolicitado = i[33:48].rstrip()
                CodigoInvestidorSolicitado = i[48:63].rstrip()
                CodigoCarteira = i[63:68]
                DescricaoCarteira = i[68:83]
                ISIN = i[83:95]
                Distribuicao = i[95:98]
                NomeEmissora = i[98:110]
                Especificacao = i[110:120].rstrip()        
                QtdAcoesCustodia = int(i[120:138])/1000
                QtdAcoesBloqueadas = int(i[138:153])/1000
                CodigoNegociacao = i[153:165].rstrip()
                IndicadorSaldoAnalitico = i[165:166]
                TipoAtivo = i[166:169]
                Reserva = i[169:1000]
                        
                lista_28.append([CodigoParticipanteSolicitante, TipoRegistro, CodigoInvestidorSolicitante, DigitoInvestidorSolicitante, CodigoParticipanteSolicitado,
                                 CodigoInvestidorSolicitado, CodigoCarteira, DescricaoCarteira, ISIN, Distribuicao, NomeEmissora, Especificacao, QtdAcoesCustodia,
                                 QtdAcoesBloqueadas, CodigoNegociacao, IndicadorSaldoAnalitico, TipoAtivo, Reserva])
                
        df_28_imbarq12 = pd.DataFrame(lista_28, columns=['Tipo_de_Registro', 'Codigo_do_Participante_Solicitante', 'Codigo_do_Investidor_Solicitante', 'Digito_do_Investidor_Solicitante',
                                                 'Codigo_do_Participante_Solicitado', 'Codigo_do_Investidor_no_Custodiante', 'Codigo_da_Carteira', 'Descricao_da_Carteira', 'Codigo_ISIN',
                                                 'Distribuicao', 'Nome_da_sociedade_emissora', 'Especificacao', 'Quantidade_de_acoes_em_custodia', 'Quantidade_total_de_acoes_bloqueadas', 
                                                 'Codigo_de_Negociacao', 'Indicador_de_saldo_analitico', 'Tipo_de_ativo', 'Reserva'])
        
        return df_28_imbarq12
            
    path_06 = r"O:\SAO\CICH_All\Renda Variável\IMBARQ - Controle Geral de Clientes e Movimentações\Arquivos IMBARQ\06"
    path_12 = r"O:\SAO\CICH_All\Renda Variável\IMBARQ - Controle Geral de Clientes e Movimentações\Arquivos IMBARQ\12"
    
    df_02_imbarq06 = findfiles(path_folder = path_06, fileNmLike = Nm06like)
    df_28_imbarq12 = findfiles(path_folder = path_12, fileNmLike = Nm12like)
    
    BasePOCadastro = Bawm().po_cadastro_all()
    BaseCadastralEquitie = pd.read_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\CadastroCliente.xlsx")
    BasePOCadastro = BasePOCadastro[['GuidContaCRM', 'NomeContaCRM', 'Segmento', 'VetoPowerAcoes', 'DPM', 'DeputyDPM']]

    BaseEquity = BaseCadastralEquitie.merge(BasePOCadastro, on = 'NomeContaCRM')
    BaseEquity = BaseEquity.drop_duplicates(subset = ['Codigo_do_Investidor_no_Custodiante'])
    BaseEquity['Codigo_do_Investidor_no_Custodiante'] = BaseEquity['Codigo_do_Investidor_no_Custodiante'].astype(int)
    
    df_02_imbarq06['Codigo_do_Investidor_no_Custodiante'] = df_02_imbarq06['Codigo_do_Investidor_no_Custodiante'].astype(int)
    df_28_imbarq12['Codigo_do_Investidor_no_Custodiante'] = df_28_imbarq12['Codigo_do_Investidor_no_Custodiante'].astype(int)
    
    BaseEquityPosDia = BaseEquity.merge(df_28_imbarq12, on = 'Codigo_do_Investidor_no_Custodiante')
    BaseEquityMovsDia = BaseEquity.merge(df_02_imbarq06, on = 'Codigo_do_Investidor_no_Custodiante')
    
    BaseEquityMovPos = pd.merge(BaseEquityPosDia, BaseEquityMovsDia, how = "outer", left_on =['Codigo_do_Investidor_no_Custodiante', 'Codigo_de_Negociacao'], right_on =['Codigo_do_Investidor_no_Custodiante', 'Codigo_de_Negociacao'])
    BaseEquityMovPos['F_check'] = [x.strip()[-1] for x in BaseEquityMovPos['Codigo_de_Negociacao']]
    BaseEquityMovsDiaCOMF = BaseEquityMovPos[BaseEquityMovPos['F_check'].isin(["F"])]
    BaseEquityMovsDiaSEMF = BaseEquityMovPos[~BaseEquityMovPos['F_check'].isin(["F"])]
    BaseEquityMovsDiaCOMF['temp_negociacao'] = BaseEquityMovsDiaCOMF['Codigo_de_Negociacao'].str[:-1] 
    BaseEquityMovsDiaCOMF = BaseEquityMovsDiaCOMF[['Codigo_do_Investidor_no_Custodiante','Codigo_do_Investidor_Solicitado', 'Data_do_Pregao', 'Data_de_Liquidacao', 'Quantidade_Comprada', 'Quantidade_Vendida', 'Posicao_Descoberta_Vendida', 'temp_negociacao']]
    BaseEquityMovsDiaCOMF = BaseEquityMovsDiaCOMF.rename(columns={'temp_negociacao' : 'Codigo_de_Negociacao'}) 
    
    BaseConsolidada = pd.merge(BaseEquityMovsDiaSEMF, BaseEquityMovsDiaCOMF, how = "outer", left_on =['Codigo_do_Investidor_no_Custodiante', 'Codigo_de_Negociacao'], right_on =['Codigo_do_Investidor_no_Custodiante', 'Codigo_de_Negociacao'])
    BaseConsolidada = BaseConsolidada.drop('F_check', axis=1)
    BaseConsolidada['Quantidade_Final'] = BaseConsolidada['Quantidade_de_acoes_em_custodia'].fillna(0) + BaseConsolidada['Quantidade_Comprada_x'].fillna(0) + BaseConsolidada['Quantidade_Comprada_y'].fillna(0) - BaseConsolidada['Quantidade_Vendida_x'].fillna(0) - BaseConsolidada['Quantidade_Vendida_y'].fillna(0)
    BaseConsolidada = BaseConsolidada[~BaseConsolidada['Especificacao'].isin(["CI", "CI EA", "CI ER", "CI  ER", "CI ES","F11","REC"])]
    BaseConsolidada.to_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\PrimeiraParte_{}.xlsx".format(Nm06like[18:26]))
    
    tickers = BaseConsolidada['Codigo_de_Negociacao'].unique()
    TickerToPrice = pd.DataFrame(tickers, columns=['Codigo_de_Negociacao'])
    TickerToPrice.to_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\SegundaParte_{}.xlsx".format(Nm06like[18:26]))

@xw.func
def EquityPosImbarq(dataText):
    df_px_bbg = pd.read_excel(r"I:\Shared\SAO_Investimentos\Renda Variavel\DailyFiles\SegundaParte_{}.xlsx".format(dataText))
    df_px_bbg = df_px_bbg.rename(columns={df_px_bbg.columns[3]: "LastPx" })    
    df_px_bbg = df_px_bbg[['Codigo_de_Negociacao', 'LastPx']]
    df_primeira_parte = pd.read_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\PrimeiraParte_{}.xlsx".format(dataText))
    df_PosMaisRecente = pd.merge(df_primeira_parte, df_px_bbg, how = "outer", on = "Codigo_de_Negociacao")
    df_PosMaisRecente['Financeiro'] = df_PosMaisRecente['LastPx'] * df_PosMaisRecente['Quantidade_Final']
    total_financeiro = df_PosMaisRecente.groupby('NomeContaCRM_x')['Financeiro'].transform('sum')
    df_PosMaisRecente['Porcentagem_PL'] = df_PosMaisRecente['Financeiro'] / total_financeiro * 100
    df_PosMaisRecente = df_PosMaisRecente[['NomeContaCRM_x', 'Segmento_x', 'Codigo_de_Negociacao', 'Quantidade_Final', 'LastPx', 'Financeiro', 'Porcentagem_PL', 'Codigo_da_Carteira_x', 'Codigo_ISIN',
                                           'GuidContaCRM_x', 'VetoPowerAcoes_x', 'Quantidade_de_acoes_em_custodia', 'Quantidade_total_de_acoes_bloqueadas', 'Data_do_Pregao_x', 
                                           'Quantidade_Comprada_x', 'Quantidade_Vendida_x', 'Data_do_Pregao_y', 'Quantidade_Comprada_y', 'Quantidade_Vendida_y', 'DPM_x', 'DeputyDPM_x']]
    
    df_PosMaisRecente = df_PosMaisRecente.rename(columns={'NomeContaCRM_x' : 'NomeContaCRM', 'Segmento_x' : 'Segmento', 'Codigo_de_Negociacao' : 'Ticker', 
                                                          'Quantidade_Final' : 'Quantidade_Acoes', 'Codigo_da_Carteira_x' : 'Carteira', 'Codigo_ISIN' : 'ISIN',
                                                          'GuidContaCRM_x' : 'GuidContaCRM', 'VetoPowerAcoes_x' : 'VetoPowerAcoes', 'Quantidade_Comprada_y' : 'Quantidade_Comprada_Fracionada',
                                                          'Quantidade_Vendida_y' : 'Quantidade_Vendida_Fracionada', 'Data_do_Pregao_y' : 'Pregao_Fracionado', 'Data_do_Pregao_x' : 'Pregao_Lote',
                                                          'Quantidade_Comprada_x' : 'Quantidade_Comprada_Lote', 'Quantidade_Vendida_x' : 'Quantidade_Vendida_Lote', 'DPM_x': 'DPM',
                                                          'DeputyDPM_x' : 'DeputyDPM'})
    
    df_PosMaisRecente.to_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\TerceiraParte_{}.xlsx".format(dataText))
    

@xw.func
def EquityPosMovTeorica(dataText, percent_cash):   
    df_PosClientesAgora = pd.read_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\TerceiraParte_{}.xlsx".format(dataText))
    df_newAlocacao = pd.read_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\QuartaParte_{}.xlsx".format(dataText))    
    df_newAlocacao = df_newAlocacao[['Ticker', 'NewAloc']]
    df_segueCarteira = pd.read_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\CadastroCliente.xlsx")
    df_segueCarteira = df_segueCarteira[df_segueCarteira['Estrategia'] == 'Segue_estrategia_carteira']
    df_segueCarteira = df_segueCarteira['NomeContaCRM'].unique()
    
    def custom_round(x, base=100):
        return int(base * round(float(x)/base))
    
    df_cliente3 = pd.DataFrame()
    for i in df_segueCarteira:
        df_cliente = df_PosClientesAgora[df_PosClientesAgora['NomeContaCRM'] == i]
        df_cliente['Aloc'] = df_cliente['Financeiro'] / df_cliente['Financeiro'].sum()
        df_cliente2 = df_cliente.merge(df_newAlocacao, on = 'Ticker')
        df_cliente2 = df_cliente2.rename(columns={'NewAloc' : 'AlocFundoJBFO'})
        df_cliente2['NewFin'] = df_cliente2['AlocFundoJBFO'] * df_cliente['Financeiro'].sum()
        df_cliente2['NewQuantity'] = df_cliente2['NewFin'] / df_cliente2['LastPx']
        df_cliente2['MovSugerida'] = df_cliente2['Quantidade_Acoes'] - df_cliente2['NewQuantity']
        df_cliente2.insert(0, "TipoMov", np.where(df_cliente2['MovSugerida'].apply(lambda x: x < 0) == True, "Compra", "Venda"))
        df_cliente2.insert(0, 'MovSugeridaGeraCaixa', np.where(df_cliente2['TipoMov'].apply(lambda x: x) == "Compra", df_cliente2['MovSugerida'] * (1 - percent_cash), df_cliente2['MovSugerida']))
        df_cliente2.insert(0, "MovArredondada", df_cliente2['MovSugeridaGeraCaixa'].apply(lambda x: custom_round(abs(x), base = 100)))
        df_cliente2 = df_cliente2[['NomeContaCRM', 'TipoMov', 'Ticker', 'MovArredondada']]
        df_cliente3 = pd.concat([df_cliente3, df_cliente2])
        #df_cliente2.to_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\Movs\{}_{}_Sugestao.xlsx".format(dataText, i))
    df_cliente3.to_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\Movs\BolConsSegueCarteira_{}.xlsx".format(dataText))      

@xw.func 
def ConsBoletaEquity(dataText):
    files = os.listdir(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\Movs")
    dfConsolidadorBoletas = pd.DataFrame()
    for i in files:    
        if i == 'BolConsSegueCarteira_{}.xlsx'.format(dataText):
            df_BolConsSegueCarteira = pd.read_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\Movs\{}".format(i))
            df_BolConsSegueCarteira = df_BolConsSegueCarteira.rename(columns={'MovArredondada' : 'QuantidadeRounded'})
            df_BolConsSegueCarteira = df_BolConsSegueCarteira[['NomeContaCRM', 'TipoMov', 'Ticker', 'QuantidadeRounded']]
            dfConsolidadorBoletas = pd.concat([dfConsolidadorBoletas, df_BolConsSegueCarteira])
        #elif i == 'BolConsCustomizados_{}.xlsx'.format(dataText):
            #df_BolConsCustomizados = pd.read_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\Movs\{}".format(i))
            #dfConsolidadorBoletas = pd.concat([dfConsolidadorBoletas, df_BolConsCustomizados])
        elif i == 'BolConsExcecoes_{}.xlsx'.format(dataText):
            df_BolConsExcecoes = pd.read_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\Movs\{}".format(i))
            dfConsolidadorBoletas = pd.concat([dfConsolidadorBoletas, df_BolConsExcecoes])
    
    dfConsolidadorBoletas = dfConsolidadorBoletas[['NomeContaCRM', 'TipoMov', 'Ticker', 'QuantidadeRounded']]
    BasePOCadastro = Bawm().po_cadastro_all()
    BaseCadastralEquitie = pd.read_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\CadastroCliente.xlsx")
    BaseCadastralEquitie = BaseCadastralEquitie[['NomeContaCRM', 'Estrategia']]
    BaseCadastralEquitie = BaseCadastralEquitie.drop_duplicates(subset = ['NomeContaCRM', 'Estrategia'])
    BasePOCadastro = BasePOCadastro[['GuidContaCRM', 'NomeContaCRM', 'Segmento', 'VetoPowerAcoes', 'DPM', 'DeputyDPM', 'CNPJ']]
    BaseEquity = BaseCadastralEquitie.merge(BasePOCadastro, on = 'NomeContaCRM')
        
    dfConsolidadorBoletasFinal = dfConsolidadorBoletas.merge(BaseEquity, on = 'NomeContaCRM')
    dfConsolidadorBoletasFinal['Temp'] = dfConsolidadorBoletasFinal['NomeContaCRM'] + "_" + dfConsolidadorBoletasFinal['Ticker'] + "_" + dfConsolidadorBoletasFinal['QuantidadeRounded'].apply(str)
    dfConsolidadorBoletasFinal = dfConsolidadorBoletasFinal.drop_duplicates(subset = ['Temp'])
    dfConsolidadorBoletasFinal = dfConsolidadorBoletasFinal.drop(columns=['Temp'])
    dfConsolidadorBoletasFinal = dfConsolidadorBoletasFinal[dfConsolidadorBoletasFinal['QuantidadeRounded'] > 0]
    return dfConsolidadorBoletasFinal.to_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\Movs\BolConsolidaGeral_{}.xlsx".format(dataText))

@xw.func
def ConsBoletaEquityCust(dataText):
    files = os.listdir(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\Movs")
    for i in files:  
        if i == 'BolConsCustomizados_{}.xlsx'.format(dataText):
            df_BolConsCustomizados = pd.read_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\Movs\{}".format(i))
         
    BasePOCadastro = Bawm().po_cadastro_all()
    BaseCadastralEquitie = pd.read_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\CadastroCliente.xlsx")
    BaseCadastralEquitie = BaseCadastralEquitie[['NomeContaCRM', 'Estrategia']]
    BaseCadastralEquitie = BaseCadastralEquitie.drop_duplicates(subset = ['NomeContaCRM', 'Estrategia'])
    BasePOCadastro = BasePOCadastro[['GuidContaCRM', 'NomeContaCRM', 'Segmento', 'VetoPowerAcoes', 'DPM', 'DeputyDPM', 'CNPJ']]
    BaseEquity = BaseCadastralEquitie.merge(BasePOCadastro, on = 'NomeContaCRM')
        
    df_BolConsCustomizados = df_BolConsCustomizados.merge(BaseEquity, on = 'NomeContaCRM')
    df_BolConsCustomizados['Temp'] = df_BolConsCustomizados['NomeContaCRM'] + "_" + df_BolConsCustomizados['Ticker'] + "_" + df_BolConsCustomizados['QuantidadeRounded'].apply(str)
    df_BolConsCustomizados = df_BolConsCustomizados.drop_duplicates(subset = ['Temp'])
    df_BolConsCustomizados = df_BolConsCustomizados.drop(columns=['Temp'])
    df_BolConsCustomizados = df_BolConsCustomizados[df_BolConsCustomizados['QuantidadeRounded'] > 0]
    return df_BolConsCustomizados.to_excel(r"O:\SAO\CICH_All\Investment Solutions\02. Projetos\1. Geral Investimentos\Equities\DailyFiles\Movs\BolConsolidaCust_{}.xlsx".format(dataText))

if __name__ == '__main__':
    
    imbarq() 
    #btg = ZeragemBtg()
    #btg.processar_email()

