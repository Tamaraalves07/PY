import os
from databases import Crm as crm, BDS
import pandas as pd
from databases import Bawm as bawm, PosicaoDm1 as dm1
import time 
from emailer import Email 


bds = BDS()
crm = crm()
dm1 = dm1()
bawm = bawm()

class conciliacao():
    

    def imbarq(self):
        caminho = "O:/SAO/CICH_All/Renda Variável/IMBARQ - Controle Geral de Clientes e Movimentações/Arquivos JBFO/06"
        lista_arquivos = os.listdir(caminho)
        
        lista_datas = []
        for arquivo in lista_arquivos:
            # descobrir a data desse arquivo
            if ".txt" in arquivo:
                data = os.path.getmtime(f"{caminho}/{arquivo}")
                lista_datas.append((data, arquivo))    
        lista_datas.sort(reverse=True)
        ultimo_arquivo = lista_datas[0]
        print(ultimo_arquivo[1])
    
        df_rv = pd.read_fwf(f"O:/SAO/CICH_All/Renda Variável/IMBARQ - Controle Geral de Clientes e Movimentações/Arquivos JBFO/06/{ultimo_arquivo[1]}", header=None, sep =';')
        tipos = ['015265', '025265','065265','165265','175265']
        caminho = 'O:/SAO/CICH_All/Portfolio Management/18-Imbarq/'
        arquivos = [open( caminho + tipo + '.txt', 'w') for tipo in tipos]
        
        with open(f'O:/SAO/CICH_All/Renda Variável/IMBARQ - Controle Geral de Clientes e Movimentações/Arquivos JBFO/06/{ultimo_arquivo[1]}', 'r') as arq:
            for linha in arq:
                for tipo, arquivo in zip(tipos, arquivos):
                    if tipo in linha:
                        arquivo.write(linha)    
                        
        caminho = "O:/SAO/CICH_All/Renda Variável/IMBARQ - Controle Geral de Clientes e Movimentações/Arquivos JBFO/12"
        lista_arquivos = os.listdir(caminho)
        
        lista_datas = []
        for arquivo in lista_arquivos:
            # descobrir a data desse arquivo
            if ".txt" in arquivo:
                data = os.path.getmtime(f"{caminho}/{arquivo}")
                lista_datas.append((data, arquivo))    
        lista_datas.sort(reverse=True)
        ultimo_arquivo = lista_datas[0]
        print(ultimo_arquivo[1])
        
        tipos = ['265265', '285265','305265']
        caminho = 'O:/SAO/CICH_All/Portfolio Management/18-Imbarq/'
        arquivos = [open( caminho + tipo + '.txt', 'w') for tipo in tipos]
        
        with open(f'O:/SAO/CICH_All/Renda Variável/IMBARQ - Controle Geral de Clientes e Movimentações/Arquivos JBFO/12/{ultimo_arquivo[1]}', 'r') as arq:
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
        controle_posicao_opcoes = pd.read_fwf('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/015265.txt', header=None,widths =larguras_015265)
        controle_posicao_opcoes.columns = colunas
        controle_posicao_opcoes.to_excel('C:/Temp/Imbarq/controle_posicao_opcoes.xlsx')    
        
        #Controle de Posições do Mercado à Vista
        #025265 - Contêm dados dos mercados: ações (10), disponível (1), ETF Primário (8), exercício de opções de Call (12), exercício de opções de Put (13), fracionário (20), leilão (17) e fixed income (5)
        ###larguras_025265 = [2,15,15,15,15,15,15,35,35,4,12,35,5,3,10,10,10,35,15,20,26,15,20,26,26,26,540]
        
        larguras_025265 = busca_especificacao('Mercadoavista')
        larguras_025265 = [int(x) for x in larguras_025265]
        colunas = busca_coluna('Mercadoavista')
        controle_posicao_avista = pd.read_fwf('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/025265.txt', header=None,widths =larguras_025265)
        controle_posicao_avista.columns = colunas
        controle_posicao_avista.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/controle_posicao_avista.xlsx')        
                    
        #Resultados líquidos liquidados
        #175265 - Contêm dados dos resultados das liquidações em ativos
        # larguras_025265 = [2,15,15,15,15,10,35,35,4,12,35,35,1,35,35,4,12,10,3,35,10,10,10,35,26,10,15,15,26,15,15,26,15,20,1,1,1,26,10,15,1,1,1,64,15,15,238]
        #controle_emprestimo_ativos.columns=['Tipo de Registro','Código do Participante Solicitante','Código do Investidor Solicitante','Código do Participante Solicitado','Código do Investidor Solicitado','Código Custodiante','Código do Investidor no Custodiante','Código Instrumento','Código Origem Identificação Instrumento','Código Bolsa Valor','ISIN','Código de Negociação','Distribuição','Mercado','Fator de cotação','Data do Pregão','Data de Liquidação','Código da Carteira','Quantidade Comprada','Volume Financeiro Comprado','Preço Médio Compra','Quantidade Vendida','Volume Financeiro Vendido','Preço Médio Venda','Posição Coberta Vendida','Posição Descoberta Vendida','Reserva']
        larguras_175265 = busca_especificacao('ativos_a_liquidar')
        larguras_175265 = [int(x) for x in larguras_175265]
        try:
            ativos_a_liquidar = pd.read_fwf('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/175265.txt', header=None,widths = larguras_175265)
            ativos_a_liquidar.columns = busca_coluna('ativos_a_liquidar')
            ativos_a_liquidar.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/ativos_a_liquidar.xlsx')
        except:
            print('Arquivo em branco')    
            
        #Resultados líquidos liquidados
        #165265 - Contêm dados dos resultados das liquidações em ativos
        # larguras_025265 = [2,15,15,15,15,10,35,35,4,12,35,35,1,35,35,4,12,10,3,35,10,10,10,35,26,10,15,15,26,15,15,26,15,20,1,1,1,26,10,15,1,1,1,64,15,15,238]
        #controle_emprestimo_ativos.columns=['Tipo de Registro','Código do Participante Solicitante','Código do Investidor Solicitante','Código do Participante Solicitado','Código do Investidor Solicitado','Código Custodiante','Código do Investidor no Custodiante','Código Instrumento','Código Origem Identificação Instrumento','Código Bolsa Valor','ISIN','Código de Negociação','Distribuição','Mercado','Fator de cotação','Data do Pregão','Data de Liquidação','Código da Carteira','Quantidade Comprada','Volume Financeiro Comprado','Preço Médio Compra','Quantidade Vendida','Volume Financeiro Vendido','Preço Médio Venda','Posição Coberta Vendida','Posição Descoberta Vendida','Reserva']
        larguras_165265 = busca_especificacao('Liquidacao_ativos')
        larguras_165265 = [int(x) for x in larguras_165265]
        try:
            ativos_liquidados = pd.read_fwf('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/165265.txt', header=None,widths = larguras_165265)
            ativos_liquidados.columns = busca_coluna('Liquidacao_ativos')
            ativos_liquidados.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/ativos_liquidados.xlsx')
        except:
            print('Arquivo em branco')    
            
        #Posições de empréstimo de ativos
        #065265 - Contêm dados dos mercados: empréstimo de ativos (91, 92, 93 e 94
        # larguras_025265 = [2,15,15,15,15,10,35,35,4,12,35,35,1,35,35,4,12,10,3,35,10,10,10,35,26,10,15,15,26,15,15,26,15,20,1,1,1,26,10,15,1,1,1,64,15,15,238]
        #controle_emprestimo_ativos.columns=['Tipo de Registro','Código do Participante Solicitante','Código do Investidor Solicitante','Código do Participante Solicitado','Código do Investidor Solicitado','Código Custodiante','Código do Investidor no Custodiante','Código Instrumento','Código Origem Identificação Instrumento','Código Bolsa Valor','ISIN','Código de Negociação','Distribuição','Mercado','Fator de cotação','Data do Pregão','Data de Liquidação','Código da Carteira','Quantidade Comprada','Volume Financeiro Comprado','Preço Médio Compra','Quantidade Vendida','Volume Financeiro Vendido','Preço Médio Venda','Posição Coberta Vendida','Posição Descoberta Vendida','Reserva']
        larguras_065265 = busca_especificacao('EmprestimoAtivos')
        larguras_065265 = [int(x) for x in larguras_065265]
        try:
            controle_emprestimo_ativos = pd.read_fwf('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/065265.txt', header=None,widths = larguras_065265)
            controle_emprestimo_ativos.columns = busca_coluna('EmprestimoAtivos')
            controle_emprestimo_ativos.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/controle_emprestimo_ativos.xlsx')
        except:
            print('Arquivo em branco')    
    
        #Saldo de custódia
        #285265 - Contêm o saldo geral de investidores em um Agente de Custódia
        
        larguras_285265 = busca_especificacao('saldo_custodia')
        larguras_285265 = [int(x) for x in larguras_285265]
        try:
            saldo_custodia = pd.read_fwf('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/285265.txt', header=None,widths = larguras_285265)
            saldo_custodia.columns = busca_coluna('saldo_custodia')
            saldo_custodia['Código de negociação'].replace('000','')
            saldo_custodia.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/saldo_custodia.xlsx')
            saldo_custodia['Código de negociação'] = saldo_custodia['Código de negociação'].apply(lambda x: x.replace('000',''))
        except:
            print('Arquivo em branco')    

        #Identificação do Saldo Analítico
        #305265 - Contêm o saldo analítico para ISIN com característica de renda fixa de investidores em um Agente de Custódia
        
        larguras_305265 = busca_especificacao('saldo_analitico')
        larguras_305265 = [int(x) for x in larguras_305265]
        try:
            saldo_analitico = pd.read_fwf('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/285265.txt', header=None,widths = larguras_305265)
            saldo_analitico.columns = busca_coluna('saldo_analitico')
            saldo_analitico.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/saldo_analitico.xlsx')
        except:
            print('Arquivo em branco')            
                    
        #Puxando as contas do CRM
        contas = crm.cod_imbarq()
        contas= contas.dropna(subset = ['new_numeroconta'])
        contas = contas[contas['new_numeroconta'] != '328196-4']
        contas['new_numeroconta'] = contas['new_numeroconta'].astype(int)
        
        #Retirando contas que estão duplicadas no CRM
        contas_duplicadas = contas[contas.new_numeroconta.duplicated()]
        contas_duplicadas = contas_duplicadas.drop(columns = ['new_titularidadeid'])
        
        #Puxando os produtos do CRM, da D1 e os Ticker e ISINs do Imbarq
        produtos = crm.Produtos_isin_ticker()
        produtos['productid']= produtos['productid'].str.lower()
        posicao= dm1.posicao_fundos()
        posicaoB3 = posicao
        posicao['GuidProduto']= posicao['GuidProduto'].str.lower()
        posicao['NomeContaCRM']= posicao['NomeContaCRM'].str.lower()
        ticker_isin = saldo_custodia[['Código ISIN','Código de negociação']]
        
        #Ativos_BDS
        lista_ativos_rv = bds.consulta_ativos_RV()
        lista_ativos_rv['productid'] = lista_ativos_rv['productid'].str.lower()
        lista_ativos_rv.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/lista_ativos_rv.xlsx')
        
        #Gestores dos fundos 
        
        gestor_fundos = bawm.fundo_gestor()
                            
 
        #Incluindo o ISIN na Posicao
        posicaoB3 = pd.merge(left = posicao, right = produtos, left_on = 'GuidProduto', right_on ='productid', how ='left')
        posicao['GuidProduto']=posicao['GuidProduto'].str.lower()            
            
         #Configurando o saldo em custódia com as informações necessárias para concatenar com as movimentações.
        
        saldo_custodia_d2 = saldo_custodia[['Código do Investidor Solicitado','Código ISIN']]
        saldo_custodia_d2['qtd_v1'] = saldo_custodia['Quantidade de ações em custódia'] + saldo_custodia['Quantidade total de ações bloqueadas']
        saldo_custodia_d2 = pd.merge(left = saldo_custodia_d2,left_on = 'Código do Investidor Solicitado', right = contas, right_on = 'new_numeroconta',how = 'left') 
        saldo_custodia_d2 = saldo_custodia_d2.drop(columns = ['new_numeroconta','new_titularidadeid','new_bancoidname'])
        saldo_custodia_d2.columns = ['cod_investidor', 'isin','qtd','NomeCRM','esteira','nome_esteira','tipo','categoria']
        saldo_custodia_d2['cod_investidor'] = saldo_custodia_d2 ['cod_investidor'] .astype(int)
        saldo_custodia_d2 = saldo_custodia_d2.drop_duplicates()
        saldo_custodia_d2.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/saldo_custodia_d2.xlsx')       
        
                
        #Deixando as movimentações no mesmo padrão dos dados da custódia paea concatenar os dados.
        
        Movimentos_d2 = ativos_a_liquidar[['Código do Investidor Solicitado','ISIN','Natureza da Operação','Quantidade Total da Instrução de Liquidação']]
        Movimentos_d2.columns = ['cod_investidor','isin','operacao','qtd']
        Movimentos_d2  = pd.merge(left = Movimentos_d2 ,left_on = 'cod_investidor', right = contas, right_on = 'new_numeroconta',how = 'left')
        Movimentos_d2['qtd_v1'] = Movimentos_d2.apply(lambda x: x.qtd if x.operacao	 == 'C' else (x.qtd *-1), axis = 1)
        Movimentos_d2 = Movimentos_d2.drop(columns = ['qtd','operacao','new_titularidadeid','new_numeroconta','new_bancoidname'])
        Movimentos_d2['cod_investidor'] = Movimentos_d2['cod_investidor'].astype(int)
        Movimentos_d2 = Movimentos_d2[['cod_investidor','isin','qtd_v1','new_titularidadeidname','new_esteira','new_esteiraname','businesstypecodename','accountcategorycodename']]
        Movimentos_d2.columns = ['cod_investidor','isin','qtd','NomeCRM','esteira','nome_esteira','tipo','categoria']
        Movimentos_d2.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/Movimentos_d2.xlsx')        
                
           #POsição cruzar com a B3 (arquivo D-1)  
        posicao_consolidada = pd.concat([saldo_custodia_d2,Movimentos_d2])
        posicao_consolidada = posicao_consolidada[posicao_consolidada['nome_esteira'].notnull()]
        posicao_consolidada = posicao_consolidada.groupby(['isin','NomeCRM','nome_esteira','categoria']).sum()
        posicao_consolidada = posicao_consolidada.reset_index()
        posicao_consolidada = pd.merge(left = posicao_consolidada, right = produtos, left_on = 'isin', right_on ='new_isin', how ='left')
        posicao_consolidada['NomeCRM']= posicao_consolidada['NomeCRM'].str.lower()
        posicao_consolidada['productid']= posicao_consolidada['productid'].str.lower()
        posicao_consolidada = posicao_consolidada[['new_isin','NomeCRM','name','qtd','productid','nome_esteira','categoria']]
        posicao_consolidada.columns = ['cod','NomeCRM','produto','qtd','productid','esteira','tipo']
        posicao_consolidada.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/posicao_consolidada.xlsx')       
                
        posicao_consolidadaB3 = pd.concat([saldo_custodia_d2,Movimentos_d2])
        posicao_consolidadaB3 = posicao_consolidadaB3[posicao_consolidadaB3['nome_esteira'].notnull()]
        posicao_consolidadaB3 = posicao_consolidadaB3.groupby(['isin','NomeCRM','nome_esteira','categoria']).sum()
        posicao_consolidadaB3 = posicao_consolidadaB3.reset_index()
        posicao_consolidadaB3 = pd.merge(left = posicao_consolidadaB3, right = produtos, left_on = 'isin', right_on ='new_isin', how ='left')
        posicao_consolidadaB3['NomeCRM']= posicao_consolidadaB3['NomeCRM'].str.lower()
        posicao_consolidadaB3 = posicao_consolidadaB3[['isin','NomeCRM','name','qtd','nome_esteira','categoria']]
        posicao_consolidadaB3.columns = ['cod','NomeCRM','produto','qtd','esteira','tipo']
        posicao_consolidadaB3.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/posicao_consolidadaB3.xlsx')        
                
        
        vic_dev = ['BRVIDSR01M12','BRVIDSR02M11''BRVIDSR03M10''BRVIDSR04M19''BRVIDSR05M18''BRVIDSR06M17''BRVIDSR07M16''BRVIDSR08M15''BRVIDSR09M14''BRVIDSR10M11''BRVIDSR11M10''BRVIDSR12M19''BRVIDSR13M18''BRVDSVR01M15''BRVDSVR02M14','BRVDSVR01M15','BRVDSVR02M14','BRVIDSR04M19','BRVIDSR09M14','BRVIDSR10M11','BRVIDSR11M10','BRVIDSR12M19','BRVDSVR03M13','BRVIDSR13M18','BRVIDSR05M18','BRVIDSR08M15','BRVIDSR07M16','BRVIDSR02M11','BRVIDSR03M10']
        
        #Configurando os arquivos (posição consolidada da B3 e a posição da D1)

        #Agrupando os produtos pelo guid
        posicaoB3 = posicaoB3.groupby(['NomeContaCRM','new_isin','name']).sum()
        posicaoB3 = posicaoB3.reset_index()
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
        ativos_imbarq_semIsin = ativos_imbarq_semIsin.drop(columns = ['new_idbds','new_isin','new_tickerid','new_onshore','name','new_nomedaacao','productid'])
        ativos_imbarq_semIsin.columns = ['cod_ISIN','ticker']
        ativos_imbarq_semIsin['cod_ISIN'] = ativos_imbarq_semIsin['cod_ISIN']
        ativos_imbarq_semIsin = ativos_imbarq_semIsin[~ativos_imbarq_semIsin.cod_ISIN.isin(vic_dev)]
        ativos_imbarq_semIsin_lista = ativos_imbarq_semIsin['cod_ISIN'].to_list()
        ativos_imbarq_semIsin= ativos_imbarq_semIsin[['ticker','cod_ISIN']]
        ativos_imbarq_semIsin.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/ativos_imbarq_semIsin.xlsx')
        
        #Fundos sem posição na D-1
        fundos_dm1 = posicao['NomeContaCRM']
        fundos_imbarq = posicao_consolidada['NomeCRM']
        fundos_sem_dm1 = pd.merge(left = fundos_imbarq , left_on = 'NomeCRM',right = fundos_dm1,right_on = 'NomeContaCRM',how='left')
        fundos_sem_dm1 = fundos_sem_dm1.drop_duplicates()
        fundos_sem_dm1 = fundos_sem_dm1[fundos_sem_dm1['NomeContaCRM'].isnull()]
        fundos_sem_dm1_lista = fundos_sem_dm1['NomeCRM'].to_list()
        fundos_sem_dm1.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/fundos_sem_dm1_lista.xlsx')
        
        #Fazendo o batimento da Dm1 com o arquivo da B3, após as configurações para o merge dar certo.
        bate_b3 = pd.merge(left = posicao_consolidadaB3, left_on = ['NomeCRM','cod'], right = posicaoB3, right_on = ['NomeContaCRM','new_isin'], how = 'left')
        bate_b3 = bate_b3[~bate_b3.new_isin.isin(ativos_imbarq_semIsin_lista)]
        bate_b3 = bate_b3[~bate_b3.NomeCRM.isin(fundos_sem_dm1_lista)]
        bate_b3 = bate_b3[~bate_b3.new_isin.isin(vic_dev)]
        bate_b3['QuantidadeFinal'] = bate_b3['QuantidadeFinal'].fillna(0)
        bate_b3 = bate_b3.groupby(['cod','NomeContaCRM','esteira','tipo','name']).sum()
        bate_b3 = bate_b3.reset_index()
        bate_b3 = pd.merge(left = bate_b3, right = lista_ativos_rv, left_on = 'cod', right_on ='new_isin', how ='left')
        bate_b3 = bate_b3[['cod','NomeContaCRM','Nome','codser','qtd','QuantidadeFinal','esteira','tipo','name']]
        bate_b3.columns = ['cod','Cliente','produto','ticker','qtd_b3','qtd_d1','nome_esteira','tipo','nome_produto_dm1']
        bate_b3.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/bate_b3.xlsx')
        
        #Ajustando o batimento da B3 e gerando o excel

        bate_b3['diferença'] = bate_b3['qtd_b3']-bate_b3['qtd_d1']
        divergencia_b3 = bate_b3[(bate_b3['diferença'] > 1) | (bate_b3['diferença'] < -1) ]
        divergencia_b3 = divergencia_b3.reset_index(drop = True)
        divergencia_b3 = pd.merge(left = divergencia_b3, left_on = 'cod', right = lista_ativos_rv, right_on = 'new_isin' , how = 'left')
        divergencia_b3 = divergencia_b3[['cod','Cliente','ticker','qtd_b3','qtd_d1','diferença','nome_produto_dm1']]
        divergencia_b3 = divergencia_b3.drop_duplicates()
        divergencia_b3.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/diferença_b3.xlsx')
                                        
                
       # #Puxando todos os ativos de RV da DM-1
        ativos_rv_dm1 = pd.merge(left = lista_ativos_rv, left_on = 'productid', right = posicao, right_on = 'GuidProduto', how = 'right')
        ativos_rv_dm1.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/ativos_rv_dm1.xlsx')
        # Ativos que estão na D1 e não possuem ISIN, excluir eles do batimento
        ativos_dm1_sem_ISIN = ativos_rv_dm1.loc[ativos_rv_dm1['new_isin'].isnull()]
        ativos_dm1_semISIN_email = ativos_rv_dm1[ativos_rv_dm1['new_isin'].isnull()].drop_duplicates()
        ativos_dm1_semISIN_email = ativos_dm1_semISIN_email.drop(columns = ['new_isin','NomeContaCRM','QuantidadeFinal','Nome'])
        prodtos_isin = lista_ativos_rv[['new_isin','codser']]
        ativos_dm1_semISIN_email = pd.merge(left = ativos_dm1_semISIN_email , right = prodtos_isin, how = 'left')
        ativos_dm1_semISIN_email = ativos_dm1_semISIN_email.dropna(subset=['new_isin','productid']).drop_duplicates()
        ativos_dm1_semISIN_email = ativos_dm1_semISIN_email[['productid','codser','new_isin']]
        ativos_dm1_semISIN_email.columns = ['id_crm','ticker','cod_ISIN']
        ativos_rv_dm1 = ativos_rv_dm1.drop(ativos_dm1_sem_ISIN.index)
        ativos_rv_dm1 = ativos_rv_dm1[['NomeContaCRM','NomeProduto','Nome','QuantidadeFinal','productid']]
        ativos_rv_dm1.columns = ['Nome_CRM','produto','ticker','qtd_final','productid']         
                
        #Fundos da D-1 que não estão no Imbarq
        fundos_imbarq = posicao_consolidada['NomeCRM']     
        fundos_dm1 = ativos_rv_dm1['Nome_CRM']  
        fundos_fora_imbarq = pd.merge(left = fundos_dm1, left_on = 'Nome_CRM',right = fundos_imbarq, right_on = 'NomeCRM', how ='left')
        fundos_fora_imbarq = fundos_fora_imbarq[fundos_fora_imbarq['NomeCRM'].isnull()].drop_duplicates()
        monitorados = fundos_fora_imbarq.loc[fundos_fora_imbarq['Nome_CRM'].str.contains('monitorado')]
        fundos_fora_imbarq = fundos_fora_imbarq.drop(monitorados.index).drop(columns = ['NomeCRM']).reset_index(drop = True).drop_duplicates()
        fundos_fora_imbarq.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/fundos_fora_imbarq.xlsx')
        
                
        bate_dm1_imbarq = pd.merge(left = ativos_rv_dm1, left_on = ['Nome_CRM','productid'], right = posicao_consolidada, right_on =['NomeCRM','productid'], how = 'left')
        bate_dm1_imbarq= bate_dm1_imbarq.dropna(subset = ['Nome_CRM'])
        bate_dm1_imbarq = bate_dm1_imbarq[['Nome_CRM','produto_x','cod','ticker','qtd_final','qtd','esteira','tipo']]
        bate_dm1_imbarq = bate_dm1_imbarq[~bate_dm1_imbarq.cod.isin(vic_dev)]
        bate_dm1_imbarq['qtd'] = bate_dm1_imbarq['qtd'].fillna(0)
        bate_dm1_imbarq['diferença'] = bate_dm1_imbarq['qtd_final']-bate_dm1_imbarq['qtd']
        bate_dm1_imbarq.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/bate_dm1_imbarq 1.xlsx')
        divergencia_dm1 = bate_dm1_imbarq[(bate_dm1_imbarq['diferença'] > 1) | (bate_dm1_imbarq['diferença'] < -1) ]    
        divergencia_dm1 = divergencia_dm1 [['Nome_CRM','ticker','cod','qtd_final','qtd','diferença','esteira','tipo']]
        divergencia_dm1.columns = ['NomeCRM','ticker','isin','qtd_dm-1','qtd_b3','diferença','esteira','tipo']
        divergencia_dm1.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/divergencia_dm1.xlsx')
        divergencia_dm1 = divergencia_dm1[(divergencia_dm1['esteira']=='Legado GPS') & (divergencia_dm1['tipo']=='Fundos')]
        
        #Configurando o e-mail 
        ativos_sem_isin = pd.concat([ativos_dm1_semISIN_email,ativos_imbarq_semIsin])
        ativos_imbarq_semIsin.to_excel('O:/SAO\CMHA_All/Portfolio Management/18-Imbarq/ativos_sem_isin.xlsx')
        
        divergencia_dm1 = divergencia_dm1.reset_index(drop = True)
        divergencia_dm1['qtd_b3'] = divergencia_dm1['qtd_b3'].map('{:.1f}'.format)
        divergencia_dm1['qtd_dm-1'] = divergencia_dm1['qtd_dm-1'].map('{:.1f}'.format)
        divergencia_dm1['diferença'] = divergencia_dm1['diferença'].map('{:.1f}'.format)
        
        #divergencia_b3 = divergencia_b3[(divergencia_b3['nome_esteira']=='Legado GPS') & (divergencia_b3['tipo']=='Fundos')]
        divergencia_b3 = divergencia_b3.reset_index(drop = True)
        divergencia_b3['qtd_b3'] = divergencia_b3['qtd_b3'].astype(float)
        divergencia_b3['qtd_d1'] = divergencia_b3['qtd_d1'].astype(float)
        divergencia_b3['diferença'] = divergencia_b3['diferença'].astype(float)
        divergencia_b3['qtd_b3'] = divergencia_b3['qtd_b3'].map('{:.1f}'.format)
        divergencia_b3['qtd_d1'] = divergencia_b3['qtd_d1'].map('{:.1f}'.format)
        divergencia_b3['diferença'] = divergencia_b3['diferença'].map('{:.1f}'.format)
                
       # #Deixando apenas os fundos que são Julius Baer
# fundos_fora_imbarq = pd.merge(left = fundos_fora_imbarq,left_on = 'Nome_CRM',right = gestor_fundos,right_on = 'NomeContaCRM',how = 'left')
# fundos_fora_imbarq = (fundos_fora_imbarq[fundos_fora_imbarq['GestorNome']=='Julius Baer Brasil Gestão de Patrimônio e Consultoria de Valores Mobiliários Ltda.']).drop(columns = ['GestorNome']) 
                        
        
        
        subject='Ativos sem ISIN no CRM'
        to=['fundos@jbfo.com','portfolio@jbfo.com']
        text = '''Favor verificar os ISINs dos ativos abaixo no CRM.</h3><br>
        {}
        
        '''.format(ativos_sem_isin.to_html())
        
        email = Email(to = to , subject = subject, text= text,send = False)
        
        
        subject='Fundos fora do Imbarq'
        to=['portfolio@jbfo.com']
        text = '''Favor incluir esses fundos na posição do IMBARQ .</h3><br>
        {}
        
        '''.format(fundos_fora_imbarq.to_html())
        email = Email(to = to , subject = subject, text= text,send = False)
        
        
        subject='Posições divergentes entre a B3 e a DM1'
        to=['processamento@jbfo.com','portfolio@jbfo.com']
        text = '''Prezados,<br>
        
        Encontramos as diferenças abaixo entre a posição da B3 e a D-1. É possível que o PAS/BOLETAS do cockpit precisem de ajuste.<br>
        <br>
        <br>
        As posições abaixo estão divergentes entre B3 e a DM1:</h3><br>
        {}<br>
        As posições abaixo estão divergentes entre a DM1 e a B3:</h3><br>
        {}<br>
        '''.format(divergencia_b3.to_html(),divergencia_dm1.to_html())
        
        email = Email(to = to , subject = subject, text= text,send = False)


        
        
        
        