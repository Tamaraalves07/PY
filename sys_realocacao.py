import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
import xlwings as xw
import traceback
import sys
import math
import locale
import warnings
import os

# locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
from databases import PosicaoDm1, PosicaoDm1Pickle, SolRealocacao, Crm, Bawm, Boletador, CW
from funcoes import CalculosRiscoPort
from objetos import Supercarteira, Fundo, Titularidade, Ativo
from emailer import Email
from emailer import EmailLer
from sys_boletador import OrderManager
from funcoes_datas import FuncoesDatas
from sys_pretrade import PreTrade
from pretty_html_table import build_table

# import matplotlib.pyplot as plt


class Solicit:
    
    def __init__(self, id_solicitacao=None, guid_solicitacao=None, carregar_sc=False, homologacao=False):
        # Classes
        self.sol = SolRealocacao(homologacao=homologacao)
        self.dm1 = PosicaoDm1Pickle(homologacao=homologacao)
        self.homologacao = homologacao
        self.dicio_tipos = {1: 'Enquadramento PI', 2: 'Ajuste interclasse', 3: 'Ajuste intraclasse', 
                            4: 'Bolsa', 5: 'Fundos', 6: 'Renda Fixa', 7: 'Pedido Resgate', 8: 'Aporte', 9: 'Distrato'}
        
        # Variáveis descritivas
        if not id_solicitacao and not guid_solicitacao:
            raise Warning('Id ou Guid da solicitação deve ser passado na inicialização')
        if id_solicitacao:
            self.id_solicitacao = id_solicitacao
            self.solcad = self.sol.solicitacao_cadastro(id_solicitacao=id_solicitacao).iloc[0]
            self.guid_solicitacao = self.solcad['GuidSolicitacao']
        elif guid_solicitacao:
            self.guid_solicitacao = self.guid_solicitacao
            self.solcad = self.sol.solicitacao_cadastro(guid_solicitacao=guid_solicitacao).iloc[0]
            self.id_solicitacao = self.solcad['IdSolicitacao']
        
        self.solicitacao_em_fundo = False
        
        self.solmov = self.sol.solicitacao_ordens(id_solicitacao=id_solicitacao, solicitacao_em_fundo=self.solicitacao_em_fundo)
        self.sc_set = False
        self.sc = None     
        if carregar_sc:
            self.__sc_load__()
        
    def __sc_load__(self):
        if self.sc_set == False:
            self.sc = Supercarteira(guid_supercarteira=self.solcad['GuidSupercarteira'], base_dm1=self.dm1, homologacao=self.homologacao)   
            self.sc_set = True

    def status(self):
        return self.campo_aux(15)
    
    def email_enquadramento(self, texto_gravar:str=None):
        if not texto_gravar:
            return self.campo_aux(30)
        else:
            self.sol.solicitacao_campo_aux_gravar(id_solicitacao=self.id_solicitacao, id_campo=30, valor=texto_gravar)
    
    def status_mensagem(self):
        return self.campo_aux(16)
    
    def status_completo(self):
        return f"{self.status()}: {self.status_mensagem()}"
    
    def nome_sc(self):
        return self.solcad['NomeSupercarteira']
    
    def data_pedido(self):
        return self.solcad['DataPedido']
    
    def data_pagamento(self):
        return self.campo_aux(2)
    
    def tipo_solicitacao(self):
        return self.solcad['IdTipoSolicitacao']
    
    def tipo_solicitacao_nome(self):
        return self.dicio_tipos[self.tipo_solicitacao()]
    
    def aporte(self):
        return self.campo_aux(1)
    
    def conta_movimento_escolhida_guid(self):
        return self.campo_aux(26)
    
    def conta_movimento_escolhida_nome(self):
        return self.campo_aux(27)
    
    def motivo_mov(self):
        return self.campo_aux(28)    
    
    def portfolio_explodido(self):
        valor = self.campo_aux(29)    
        if valor is None:
            valor = True
        else:
            valor = bool(valor)
    
    def pl_projetado(self):
        self.__sc_load__()
        novo_pl = self.sc.pl_fut + self.aporte()
        return novo_pl
    
    def campo_aux(self, id_campo):
        return self.sol.solicitacao_campo_aux(id_solicitacao=self.id_solicitacao, id_campo=id_campo)
    
    def status_gravar(self, status, mensagem=None):
        self.sol.solicitacao_campo_aux_gravar(id_solicitacao=self.id_solicitacao, id_campo=15, valor=status)
        if mensagem:
            self.sol.solicitacao_campo_aux_gravar(id_solicitacao=self.id_solicitacao, id_campo=16, valor=mensagem)
            
    def devolver(self):
        self.sol.solicitacao_campo_aux_gravar(id_solicitacao=self.id_solicitacao, id_campo=17, valor=True)    
    
    def email_destinatarios_comercial(self):
        dest_1 = ''
        if self.campo_aux(12):
            dest_1 = self.campo_aux(12)
        dest_2 = ''
        if self.campo_aux(13):
            dest_2 = self.campo_aux(13)
        if self.campo_aux(12) != self.campo_aux(4) and self.campo_aux(13) != self.campo_aux(4):
            cc_extra = self.campo_aux(4)
        else:
            cc_extra  = ''
        return f"{dest_1}; {dest_2}; {cc_extra}"
    
    def email_destinatarios_cc(self):
        return "portfolio@jbfo.com; risk.management@jbfo.com"
    
    def email_assunto(self, mensagem):
        return f"[SolRealocacao]{self.nome_sc()} - {self.data_pedido().strftime('%d/%b/%Y')}: {mensagem}"
    
    def email_guid_solicitacao(self):
        return f'<br><p style="color:#FFFFFF";> {self.solcad["GuidSolicitacao"]} </p>'
    
    def email_envia(self, assunto, texto_mensagem):
        texto = f'Prezados,<br><br>    {texto_mensagem}'
        texto = texto + self.email_guid_solicitacao()
        
        email = Email(subject=self.email_assunto(assunto),
                          to=self.email_destinatarios_comercial(),
                          cc=self.email_destinatarios_cc(),
                          text=texto, send=True)
    
    def verifica_validade(self):
        # Verifica se pedido de solicitação é válido
        seguir = True
        status = 'Válida'
        mensagem = 'Solicitação validada.'
        self.__sc_load__()
        # 1. Verifica se a supercarteira selecionada é a de gestão (pode ter mudado)   
        if self.sc.tem_dm1:
            if not bool(self.sc.po_cad_atual['SCGestao']):
                seguir = False
                status = 'Anulada'
                mensagem = 'Supercarteira não é a de gestão.'
        
        # 2. Verifica se a solicitação tem as ordens que deveria ter (pode ocorrer erro ao criar uma solicitação)
        if not self.tipo_solicitacao() in [1, 7, 8]:  # Se for 1, 7 ou 8 não precisa ter movimentações
            if self.solmov.empty:
                seguir = False
                status = 'Anulada'
                mensagem = 'Solicitação foi criada sem ordens ou sem ordens válidas.'
                
        # 3. Verifica se a Supercarteira tem participação relevante no fundo
        if seguir and not self.solicitacao_em_fundo:
            # TODO Implementar teste de maior cotista do fundo, se nenhuma supercarteira superar 20%
            part_min_relevancia = 10 / 100
            if self.tipo_solicitacao() != 7 and self.tipo_solicitacao() != 1:  # 7 = Providenciar liquidez
                movs_fundo = self.solmov[~self.solmov['Veiculo'].isin(['PF', 'CartAdm'])]['Veiculo'].unique()
                if len(movs_fundo) > 0:
                    veic = self.sc.veiculos_participacao()
                    for fundo in movs_fundo:
                        part = veic[veic['NomeContaFundo']==fundo]
                        if part.empty:
                            passivo = self.dm1.passivo_fundo_sc_gestao(fundo_conta_crm=fundo)
                            if passivo.empty:
                                raise Warning(f'Posição no fundo {fundo} não encontrada na supercarteira {self.nome_sc()}!')
                            else:
                                passivo = passivo[passivo['NomeSupercarteira']==self.nome_sc()].iloc[0]
                                perc = max(passivo['Share'], passivo['ShareFuturo'])
                        else:
                            perc = max(part.iloc[0]['PartPLFinal'], part.iloc[0]['PartPLFuturo'])
                        if perc < part_min_relevancia:
                            seguir = False
                            mensagem = f'Supercarteira da solicitação detém menos de 10% do fundo {fundo}. Por favor refaça a solicitação partindo de uma Supercarteira mais relevante.'
                                    
        # 7. Se solicitação foi anulada, envia e-mail
        if not seguir:
            texto = f'Prezados,<br><br>    a solicitação de realocação foi <b>anulada</b>.<br><b>Motivo:</b> {mensagem}<br>'
            texto = texto + self.email_guid_solicitacao()
            
            try:
                self.responde_email_analise_enquadramento(texto=texto, send=True, subject=self.email_assunto("Validação da solicitação"))
            except:
                email = Email(subject=self.email_assunto("Validação da solicitação"),
                                to=self.email_destinatarios_comercial(),
                                cc=self.email_destinatarios_cc(),
                                text=texto, send=True)
        else:
            self.sol.solicitacao_campo_aux_gravar(id_solicitacao=self.id_solicitacao, id_campo=17, valor=False)
            
        return seguir, status, mensagem
    
    def verifica_enquadramento(self):
        self.__sc_load__()
        #1. Se for ordem de enquadrar na PI, não há problema de enquadramento
        if self.tipo_solicitacao() == 1:
            if self.sc.perfil == 'N/A':
                return False, 'Inválida', 'Cliente sem PI.'
            else:
                texto = 'Prezados,<br><br>solicitações de ajuste à PI sempre tem o enquadramento aprovado à priori <b>aprovado</b>.'
                email = Email(subject=self.email_assunto("Aprovação do Enquadramento") ,
                        to=self.email_destinatarios_comercial(),
                        cc=self.email_destinatarios_cc(),
                        text=texto, send=True)
                return True, 'Enquadrada', 'Enquadramento OK.'
        
        # 2. Se o fundo tiver um mandato, controla esse mandato
        if self.solicitacao_em_fundo:
            # TODO se fundo tiver um mandato, tenta controlá-lo
            if self.sc.perfil and self.sc.perfil != "N/A":
                pass  
        else:
            fundos_mov = self.solmov[~self.solmov['Veiculo'].isin(['PF', 'CartAdm'])]['Veiculo'].unique()
            # TODO se fundo tiver um mandato, tenta controlá-lo
        
        # 3. Consolida o passivo dos fundos movimentados        
        fundos_mov = self.solmov[~self.solmov['Veiculo'].isin(['PF', 'CartAdm'])]['Veiculo'].unique()
        pfs_mov = self.solmov[self.solmov['Veiculo'].isin(['PF', 'CartAdm'])]
        lista_scs = []
        if len(fundos_mov) == 0 and len(pfs_mov) == 0:
            seguir = True
            status = 'Enquadrada'
            mensagem = 'Enquadramento SC não calculado: solicitação sem movimentações'
            return seguir, status, mensagem
        
        lista = []
        if len(fundos_mov) > 0:
            for fundo in fundos_mov:
                passivo = self.dm1.passivo_fundo_sc_gestao(fundo_conta_crm=fundo)
                if not passivo.empty:
                    passivo.insert(0, 'Fundo', [fundo] * len(passivo))
                    lista.append(passivo)
        if len(lista) > 0:
            passivo = pd.concat(lista)                
            lista_scs = list(passivo['NomeSupercarteira'].unique())
        else:
            passivo = pd.DataFrame()
        
        if len(pfs_mov) > 0:
            if not self.sc.nome in lista_scs:
                lista_scs = lista_scs + [self.sc.nome]
        
        # 4. Lista de supercarteiras a controlar
        seguir = True
        
        lista_enq = []
        lista_seguir = []
        lista_mandato = []
        alertas = []
        for sc in lista_scs:
            teste, perfil, df = self.__verifica_enquadramento_sc__(sc, self.solmov, passivo)
            lista_enq.append(df)
            lista_seguir.append(teste)
            lista_mandato.append(perfil)
            if teste == 0:
                seguir = False
            elif teste == 2:
                alertas.append(f'Não foi possível calcular o enquadramento do portfólio {sc}.')
        
        # 5. Totaliza enquadramento
        if seguir:            
            texto = 'Prezados,<br><br>conforme a(s) tabela(s) abaixo a solicitação teve o enquadramento por classe de ativos <b>aprovado</b>.<br>Porém ainda há outras etapas necessárias que podem impedir a execução, como <b>Suitability</b>, o <b>enquadramento dos fundos exclusivos a seus respectivos regulamentos</b>, entre outros.'
            if len(alertas) > 0:
                for alerta in alertas:
                    txt = '<p style="color:#FF0000";> {alerta} </p>'
                    texto = f"{texto}<br>{alerta}"
                texto = f"{texto}<br>"
            status = 'Enquadrada'
            mensagem = 'Enquadramento OK'
        else:
            texto = 'Prezados,<br><br>conforme a(s) tabela(s) abaixo a solicitação teve o enquadramento por classe de ativos <b>reprovado</b>.'
            status = 'Reprovada'
            mensagem = 'Solicitação causaria desenquadramento'
        
        if not seguir:
            if self.tipo_solicitacao() == 9:  # DISTRATO
                seguir = True
                texto = f"{texto}<br>Ela será executada por se tratar de <b>distrato</b>!"
                status = 'Aprovada'
                mensagem = 'Aprovada (distrato)'
            else:
                texto = f"{texto}<br>A solicitação não será executada."
        
        texto = texto + '<br>'
        i = 0
        for nome_sc in lista_scs:
            if lista_seguir[i]:
                enquad = 'Aprovado'
            else:
                enquad = 'Reprovado'
            texto = texto +f'<br><b>{nome_sc} - Mandato atual: {lista_mandato[i]}</b> - Enquadramento: {enquad}<br>' + lista_enq[i].to_html()
            i += 1
            
        texto = texto + self.email_guid_solicitacao()
        
        # 6. Envia e-mail confirmando enquadramento
        found = False
        
        # Grava resposta para envio posterior
        self.email_enquadramento(texto_gravar=texto)
        return seguir, status, mensagem
    
    def envio_email_enquadramento(self):
        found = False
        texto = self.email_enquadramento()
        try:
            found = self.responde_email_analise_enquadramento(texto=texto, send=False, subject=self.email_assunto("Aprovação do Enquadramento"))
        except:
            email = Email(subject=self.email_assunto("Aprovação do Enquadramento") ,
                            to=self.email_destinatarios_comercial(),
                            cc=self.email_destinatarios_cc(),
                            text=texto, send=False)
        if found == False:
            email = Email(subject=self.email_assunto("Aprovação do Enquadramento") ,
                        to=self.email_destinatarios_comercial(),
                        cc=self.email_destinatarios_cc(),
                        text=texto, send=False)
    
    def responde_email_analise_enquadramento(self, texto, send, subject):
        nome_sc = self.nome_sc()
        data_pedido = self.data_pedido()
        days_ago = datetime.datetime.now() - data_pedido
        days_ago = math.ceil(abs(days_ago.days))
        days_ago += 1
        data_str = data_pedido.strftime('%d/%b/%Y').lower()

        mes_converter = {
            'fev':'feb',
            'abr':'apr',
            'mai':'may',
            'ago':'aug',
            'set':'sep',
            'out':'oct',
            'dez':'dec'
        }
        mes_converter_inv = {v: k for k, v in mes_converter.items()}

        conver = False

        for key in mes_converter.keys():
            if key in data_str:
                data_str2 = data_str.replace(key,mes_converter[key])
                conver = True
                break
        
        if conver == False:
            for key in mes_converter_inv.keys():
                if key in data_str:
                    data_str2 = data_str.replace(key, mes_converter_inv[key])
                break
        
        if conver == False:
            data_str2 = data_str
        guid_sol = self.guid_solicitacao

        lista_assuntos = [
            f'[SolRealocacao] {nome_sc} - {data_str}',
            f'[SolRealocacao] {nome_sc} - {data_str2}',
            f'[SolRealocacao]{nome_sc} - {data_str}',
            f'[SolRealocacao]{nome_sc} - {data_str2}',
            f'[SolRealocacao][DPM] {nome_sc} - {data_str}',
            f'[SolRealocacao][DPM] {nome_sc} - {data_str2}',
            f'[SolRealocacao] [DPM]{nome_sc} - {data_str}',
            f'[SolRealocacao] [DPM]{nome_sc} - {data_str2}',
        ]
        
        
        emailer = EmailLer(nome_sub_pasta='SolRealocacao')
        for assunto in lista_assuntos:
            found = emailer.reply_email(text=texto, content_key_to_reply=guid_sol, subject_to_reply=assunto, send=send, days_ago=days_ago, assunto_resp=subject)
            if found == True: 
                return True
        return False


    def __verifica_enquadramento_sc__(self, nome_sc, df_movs, passivo):
        print(nome_sc)
        if not passivo.empty:
            df_pas = passivo[passivo['NomeSupercarteira']==nome_sc].copy()
        else:
            df_pas = passivo
        movs = df_movs.copy()
        
        # 1. DataFrame com Alocação por classe
        o_sc = Supercarteira(nome_supercarteira=nome_sc)
        df = o_sc.asset_class_allocation()
        df.insert(len(df.columns), 'Movs', [0] * len(df))
        df.insert(len(df.columns), 'PesoProjetado', [0] * len(df))
        df.insert(len(df.columns), 'EnqAtual', [False] * len(df))
        df.insert(len(df.columns), 'EnqFuturo', [False] * len(df))
        df.insert(len(df.columns), 'EnqProjetado', [False] * len(df))
        df.insert(len(df.columns), 'Mensagem', [None] * len(df))
        df.insert(len(df.columns), 'Aprovada', [True] * len(df))
        df.insert(len(df.columns), 'MovEstFundo', [0] * len(df))
        df.set_index('Classe', inplace=True)
        
        if nome_sc == self.nome_sc():
            aporte = self.campo_aux(1)
        else:
            aporte = 0
        novo_pl = o_sc.pl_fut + aporte
                
        # 3. Movimentação por classe de ativo
        for idx, row in movs.iterrows():
            valor = 0
            
            if row['Veiculo'] in ['PF', 'CartAdm'] and nome_sc == self.nome_sc():
                # Só adiciciona movimentações na supercarteira para cálculo do enquadramento se estivermos na SC principal
                if row['TipoMov'] == 'C':
                    if row['FinanceiroExec']:
                        valor = row['FinanceiroExec']
                    else:
                        valor = row['FinanceiroDem']
                else:
                    if row['FinanceiroExec']:
                        valor = -row['FinanceiroExec']
                    else:
                        valor = -row['FinanceiroDem']
            elif not row['Veiculo'] in ['PF', 'CartAdm']:
                df_share = df_pas[df_pas['Fundo']==row['Veiculo']]
                if len(df_share) > 0:
                    share = df_share.iloc[0]['Share']
                    if row['TipoMov'] == 'C':
                        valor = int(row['FinanceiroFundoEst'] * share * 1000) / 1000
                    else:
                        valor = -int(row['FinanceiroFundoEst'] * share * 1000) / 1000
            
            # 4. Coloca movimentações na alocação            
            dicio = {row['Classe']: valor}            
            if row['Classe'] not in df.index:
                # 4.a. Verifica se há necessidade de explosão
                if row['GuidProduto']:
                    ativo = Ativo(id_ativo=row['GuidProduto'])
                    if not ativo.crm_cadastro_fundo.empty:
                        if ativo.crm_cadastro_fundo['new_gestorid'].lower() == '85475fa5-e062-ea11-8e83-005056912b96':
                            # Gestão JBFO
                            fundo = Fundo(codigo_produto=ativo.id_origem, base_dm1=self.dm1, load_posicao_explodida=True)
                            df_temp = fundo.asset_class_allocation()
                            if not df_temp.empty:
                                dicio = {}
                                for idx, row in df_temp.iterrows():
                                    dicio[row['Classe']] = valor * row['PesoFinal']
                                    
            # 4.c. Verifica se é preciso inserir classes
            for classe in dicio.keys():
                if classe not in df.index:
                    df_temp = pd.DataFrame(index=[classe], columns=df.columns);  df_temp.index.name='Classe'
                    for col in df_temp.columns:
                        df_temp.iloc[0][col] = 0
                    df = pd.concat([df, df_temp])
            
            # 4.d. insere valores de movimentação
            for classe in dicio.keys():
                df.loc[classe, 'Movs'] += dicio[classe]
        
        # 5. Calcula o novo percentual do PL
        for idx, row in df.iterrows():
            peso_proj = (row['FinanceiroFuturo'] + row['Movs']) / novo_pl
            df.loc[idx, 'PesoProjetado'] = peso_proj
        
        # Verifica o enquadramento
        if o_sc.perfil == 'N/A':
            df.insert(len(df.columns), 'Minimo', [0] * len(df))
            df.insert(len(df.columns), 'Maximo', [0] * len(df))
            for idx, row in df.iterrows():
                if row['Movs'] != 0:
                    # df.loc[idx, 'Aprovacao'] = teste
                    df.loc[idx, 'Mensagem'] = 'Não verificado'
            seguir = 2
        elif o_sc.perfil[:9] == 'Flutuação':  # Políticas legado Reliance
            texto = o_sc.perfil
            texto = texto[texto.find('('):].replace('(', '').replace(')', '').split(' ')
            if texto[0] == 'acima':
                minimo = float(texto[2])
                maximo = minimo * 2
            else:
                minimo = float(texto[0])
                maximo = float(texto[2])
            risk = CalculosRiscoPort()
            resultado = risk.calcula_vols(df)
            resultado = pd.DataFrame(resultado).set_index('Campo').T
            resultado['Minimo'] = minimo
            resultado['Maximo'] = maximo
            df = pd.concat([df, resultado])
            seguir = 1
            idx = 'Pontos'
            row = df.loc[idx]
            df.loc[idx, 'EnqAtual'] = False
            df.loc[idx, 'EnqFuturo'] = False
            df.loc[idx, 'EnqProjetado'] = False
            
            if row['Minimo'] <= row['PesoFinal'] <= row['Maximo']:
                df.loc[idx, 'EnqAtual'] = True
            if row['Minimo'] <= row['PesoFuturo'] <= row['Maximo']:
                df.loc[idx, 'EnqFuturo'] = True
            if row['Minimo'] <= row['PesoProjetado'] <= row['Maximo']:
                df.loc[idx, 'EnqProjetado'] = True
            row = df.loc[idx]
            teste, mensagem = self.__verifica_enquadramento_decisao__(row)
            df.loc[idx, 'Aprovacao'] = teste
            df.loc[idx, 'Mensagem'] = mensagem
            if not teste:
                seguir = 0
            
        else:
            # 4. Calcula o novo percentual do PL e verifica os enquadramentos
            for idx, row in df.iterrows():
                if row['Minimo'] <= row['PesoFinal'] <= row['Maximo']:
                    df.loc[idx, 'EnqAtual'] = True
                if row['Minimo'] <= row['PesoFuturo'] <= row['Maximo']:
                    df.loc[idx, 'EnqFuturo'] = True
                if row['Minimo'] <= row['PesoProjetado'] <= row['Maximo']:
                    df.loc[idx, 'EnqProjetado'] = True
                    
            # 5 . Verifica se realocação não desenquadra ou não piora um desenquadramento
            seguir = 1
            for idx, row in df.iterrows():
                if row['Movs'] != 0:
                    teste, mensagem = self.__verifica_enquadramento_decisao__(row)
                    df.loc[idx, 'Aprovacao'] = teste
                    df.loc[idx, 'Mensagem'] = mensagem
                    if not teste:
                        seguir = 0
        
        # 6 Formata dataframe
        df = df[['PesoFinal', 'PesoFuturo','Movs', 'PesoProjetado','Minimo', 'Maximo', 'Mensagem']]
        for col in df.columns:
            df[col] = df[col].fillna(0)
        temp = df.isna()
        for idx, row in df.iterrows():
            if idx not in ['Vol', 'Pontos']:
                df.loc[idx, 'PesoFinal'] = int(row['PesoFinal'] * 10000)/100
                df.loc[idx, 'PesoFuturo'] = int(row['PesoFuturo'] * 10000)/100
                df.loc[idx, 'PesoProjetado'] = int(row['PesoProjetado'] * 10000)/100
                df.loc[idx, 'Minimo'] = int(row['Minimo'] * 10000)/100
                df.loc[idx, 'Maximo'] = int(row['Maximo'] * 10000)/100
                df.loc[idx, 'Movs'] = f'{int(row["Movs"]):,}'
                if temp.loc[idx, 'Mensagem']:
                    df.loc[idx, 'Mensagem'] = ''
                
        return seguir, o_sc.perfil, df
        
    
    def __verifica_enquadramento_decisao__(self, linha):
        if linha['EnqProjetado']:
            if linha['EnqAtual'] and linha['EnqFuturo']:  # são indiferentes se destino está enquadrado
                return True, 'Enquadramento OK'
            else:
                return True, 'Solicitação reenquadra SC'
        elif (linha['EnqAtual'] or linha['EnqFuturo']) and not linha['EnqProjetado']:
            return False, 'Ordem desenquadra SC'
        else:
            # Se chega aqui, tem muitos desenquadramentos
            desenq_atu = self.__descasamento__(linha['PesoFinal'], linha['Minimo'], linha['Maximo'])
            desenq_fut = self.__descasamento__(linha['PesoFuturo'], linha['Minimo'], linha['Maximo'])
            desenq_proj = self.__descasamento__(linha['PesoProjetado'], linha['Minimo'], linha['Maximo'])
            
            if desenq_proj <= desenq_fut:
                return True, 'Solicitação reduz desenquadramento da SC'
            else:
                return False, 'Ordem piora desenquadramento da SC'
            
    @staticmethod
    def __descasamento__(peso, minimo, maximo):
        if peso < minimo:
            return round((minimo - peso), 3)
        else:
            return round(max(peso - maximo, 0) , 3)
   

class ProcSolicitacoes:
    
    def __init__(self, homologacao=False):
        self.homologacao = homologacao
        self.sol = SolRealocacao(homologacao=homologacao)
        self.dm1 = PosicaoDm1Pickle(homologacao=homologacao)
    
    def analisar_solicitacoes_inicial(self, executar_solicitacoes_automaticas=True):
        """
        Args:
            executar_solicitacoes_automaticas (bool, optional): As solicitações de id 7 e 8 podem ser executadas automaticamente caso a solicitação seja enquadrada. Defaults to False.
        """

        hoje = self.dm1.banco.hoje() - relativedelta(days=30)
        analisar = self.sol.status_inicial(data_busca_ini=hoje)
        if analisar.empty:
            print('Nenhuma nova solicitação.')
            return
        for idx, row in analisar.iterrows():
            print('Analisando solicitação: ' + str(row['IdSol']))
            try:
                
                self.analise_inicial(id_solicitacao=row['IdSol'], executar_solicitacao_auto=executar_solicitacoes_automaticas)
            except Exception as err:
                exc_info = sys.exc_info()
                print(f"Não foi possível analisar a solicitação {row['IdSol']}. {str(err)}")
                traceback.print_exception(*exc_info)
                del exc_info
                
    def analise_inicial(self, id_solicitacao, executar_solicitacao_auto=True):        
        solicitacao = Solicit(id_solicitacao)
        solicitacao.status_gravar('Em análise')
        
        # 1. Verifica se solicitação é válida
        teste, status, mensagem = solicitacao.verifica_validade()
        print(teste, status, mensagem)
        solicitacao.status_gravar(status=status, mensagem=mensagem)
        if not teste:
            solicitacao.devolver()
            return False

        # 2. Verifica enquadramento
        teste, status, mensagem = solicitacao.verifica_enquadramento()
        solicitacao.status_gravar(status=status, mensagem=mensagem)

        # 3. Se for solicitacao tipo 7 ou 8, segue com a movimentação automática
        tipo_sol = solicitacao.tipo_solicitacao()
        # status_sol = solicitacao.status()
        
        guid_sol = solicitacao.guid_solicitacao

        if tipo_sol in [7,8,9] and executar_solicitacao_auto==True:
            if tipo_sol == 9:
                distrato = True
            else:
                distrato = False
            try:
                df_teste = pd.read_csv(f'O:\\SAO\\CICH_All\\Portfolio Management\\Foundation\\files\\log_exec_auto_sol\\{guid_sol}.txt')
                continuar = False
            except:
                df_teste = pd.DataFrame({'Guid':[guid_sol]})
                df_teste.to_csv(f'O:\\SAO\\CICH_All\\Portfolio Management\\Foundation\\files\\log_exec_auto_sol\\{guid_sol}.txt')
                continuar = True
                
            if continuar == True:
                user = (os.getlogin()).lower()
                if user in ['u1j842', 'u45767', 'u1m918', 'u44891']:
                    continuar = True
                else:
                    continuar = False

            if continuar == True:
                sol_mov = solicitacao.solmov
                if sol_mov.empty:
                    sem_mov = True
                else:
                    sem_mov = False
                if status != 'Enquadrada' and sem_mov == False:
                    df_exc = Bawm().produtos_exclusivos()
                    df_liq = Bawm().produtos_liquidez_todos()
                    lista_guids = df_exc['GuidProduto'].tolist()
                    lista_guids.extend(df_liq['GuidProduto'].tolist())
                    for guid_produto in  sol_mov['GuidProduto'].tolist():
                        if guid_produto not in lista_guids:
                            mov_simples = False
                            break
                        else:
                            mov_simples = True
                
                if (status != 'Enquadrada' and mov_simples == True) or (status == 'Enquadrada'):
                    try:
                        self.solicitacao_auto_liquidez_e_distrato(id_solicitacao, aceita_data_errada=False, distrato=distrato, mensagem='Feito (Automação análise inicial)')
                        print(f'Solicitação feita automática {id_solicitacao}')
                    except:
                        print(f'não foi possível executar automaticamente a solicitação {id_solicitacao}')

        if not teste:
            # solicitacao.devolver()
            return False
        


        # Final: Tarefas iniciais concluídas
        return True
    
    def solicitacoes_executar(self):
        analisar = self.sol.status_executar()
        for idx, row in analisar.iterrows():            
            self.solicitacao_execucao(id_solicitacao=row['IdSolicitacao'])
            
    def executar_solicitacao(id_solicitacao):
        solicitacao = Solicit(id_solicitacao)
        if not solicitacao.solmov.empty:
            pass
        
    def executar_lista_solicitacoes(self, tipo_lista=1, nome_planilha=None):
        if nome_planilha:
            wb = xw.Book(nome_planilha)
        else:
            wb = xw.Book.caller()
        aba = wb.sheets('Face')
        dias_busca = aba.range('FaceDiasBusca').value
        
        df = self.sol.solicitacoes_executar_dados(tipo_lista=tipo_lista, dias=dias_busca)
        
        aba.range('ListaSol_Ini').value = df
        
    def marcacao_solicitacao(self, id_solicitacao, concluido, quem, status=None, mensagem=None, devolvida=False):
        if concluido:
            self.sol.solicitacao_campo_aux_gravar(id_solicitacao=id_solicitacao, id_campo=14, valor=1)
        if devolvida:
            self.sol.solicitacao_campo_aux_gravar(id_solicitacao=id_solicitacao, id_campo=17, valor=1)
            self.cancelamento_ordens_sol_devol(id_solicitacao)
        self.sol.solicitacao_campo_aux_gravar(id_solicitacao=id_solicitacao, id_campo=15, valor=status)
        msg_atu = self.sol.solicitacao_campo_aux(id_solicitacao=id_solicitacao, id_campo=16)
        if msg_atu != mensagem:
            self.sol.solicitacao_campo_aux_gravar(id_solicitacao=id_solicitacao, id_campo=16, valor=mensagem)

    def cancelamento_ordens_sol_devol(self, id_solicitacao):
        bol = Boletador()
        lista_ordens = Boletador().ordens_id_sol(id_solicitacao)
        # pre_bols = bol.boletas_id_sols([id_solicitacao])
        if len(lista_ordens)>0:
            lista_rets = []
            for ordem in lista_ordens:
                ret = bol.ordem_cancelar(id_ordem=ordem)
                lista_rets.append(ret)    
            
            if True in lista_rets:
                raise Exception('Solicitação devolvida mas é necessário verificar as boletas')
        # return ret

    def solicitacao_planilha_veic(self, id_solicitacao, nome_planilha=None):
        if nome_planilha:
            wb = xw.Book(nome_planilha)
        else:
            wb = xw.Book.caller()
        aba = wb.sheets('MovVeiculos')    
        
        # Carrega solicitação
        sol = Solicit(id_solicitacao=id_solicitacao, carregar_sc=True)
        
        # Calcula exposição explodida por veículo para a supercarteira
        posexp_veic = sol.sc.posexp[['Ordem', 'Origem', 'Classe', 'FinanceiroFinal', 'FinanceiroFuturo']].groupby(['Ordem', 'Classe', 'Origem']).sum().reset_index()
        posexp_veic = posexp_veic[posexp_veic ['Classe']!='Ajuste']
        posexp_veic = pd.pivot_table(posexp_veic, columns=['Origem'], index=['Ordem', 'Classe'], values=['FinanceiroFinal', 'FinanceiroFuturo']).T.reset_index()
        veiculos = []
        for idx, row in posexp_veic.iterrows():
            teste = row['Origem'][0]
            if not teste in veiculos:
                veiculos.append(teste)
        
        colunas = []
        for col in posexp_veic.columns:
            if col[1]:
                colunas.append(col[1])
            else:
                colunas.append(col[0])
        posexp_veic.columns = colunas
        posexp_veic.rename(columns={'level_0':'Dado'}, inplace=True)
        posexp_veic.set_index(['Dado', 'Origem'], inplace=True)
        
        # Insere os veículos para movimentação e posição projetada
        
        linhas = ['MovSugerida', 'FinanceiroProjetado', 'Peso', 'PesoFut', 'PesoProj', 'MovAjustada', 'FinanceiroAjustado', 'PesoAjustado']
        indice = []
        for lin in linhas:
            for veic in veiculos:            
                indice.append([lin, veic])
        df = pd.DataFrame(indice, columns=['Dado', 'Origem'])
        for col in posexp_veic.columns:
            df.insert(len(df.columns), col, [0] * len(df))
        df.set_index(['Dado', 'Origem'], inplace=True)
        
        posexp_veic = pd.concat([posexp_veic, df], axis=0).fillna(0)
        
        # Calcula os valores        
        for veic in veiculos:
            # Movimentações
            if veic == 'CartAdm':
                filtro = 'CartAdm'
            else:
                filtro = veic
            for col in posexp_veic.columns:
                df = sol.solmov[(sol.solmov['Classe']==col) & (sol.solmov['Veiculo']==filtro)].copy()
                if not df.empty:
                    soma = df[df['TipoMov']=='C']['FinanceiroDem'].sum() - \
                            df[df['TipoMov']=='V']['FinanceiroDem'].sum()
                    posexp_veic.loc[('MovSugerida', veic)][col] = posexp_veic.loc[('MovSugerida', veic)][col] + soma
                    soma_adj = df[df['TipoMov']=='C']['FinanceiroExec'].sum() - \
                               df[df['TipoMov']=='V']['FinanceiroExec'].sum()
                    posexp_veic.loc[('MovAjustada', veic)][col] = posexp_veic.loc[('MovAjustada', veic)][col] + soma_adj
            # Projetado
            posexp_veic.loc[('FinanceiroProjetado', veic)] = posexp_veic.loc[('FinanceiroFuturo', veic)] + posexp_veic.loc[('MovSugerida', veic)]
            posexp_veic.loc[('FinanceiroAjustado', veic)] = posexp_veic.loc[('FinanceiroFuturo', veic)] + posexp_veic.loc[('MovAjustada', veic)]
            
            # Pesos
            posexp_veic.loc[('Peso', veic)] = posexp_veic.loc[('FinanceiroFinal', veic)] / sol.sc.pl
            posexp_veic.loc[('PesoFut', veic)] = posexp_veic.loc[('FinanceiroFuturo', veic)] / sol.sc.pl_fut
            posexp_veic.loc[('PesoProj', veic)] = posexp_veic.loc[('FinanceiroProjetado', veic)] / sol.pl_projetado()
            posexp_veic.loc[('PesoAjustado', veic)] = posexp_veic.loc[('FinanceiroAjustado', veic)] / sol.pl_projetado()
                
        
        # Escreve na planilha
        aba.range('Mov_Veic_SC').value = sol.sc.nome
        aba.range('Mov_IdSolicitacao').value = id_solicitacao
        
        aba.range('Mov_Veic_PLSC').value = sol.sc.pl
        aba.range('Mov_Veic_PLSCFut').value = sol.sc.pl_fut
        aba.range('Mov_Veic_PLSCProj').value = sol.pl_projetado()
        aba.range('MovVeic_Ini').value = posexp_veic 
        
    def solicitacao_movs_pedidas(self, id_solicitacao, nome_sc, nome_planilha=None):
        if nome_planilha:
            wb = xw.Book(nome_planilha)
        else:
            wb = xw.Book.caller()
        aba = wb.sheets('Temp')
        
        # Parte 1: ordens da solicitação
        df_ordens = self.sol.solicitacao_ordens(id_solicitacao=id_solicitacao).set_index('IdSolMov')        
        aba.range('A1').value = 2  # LinIni
        aba.range('B1').value = len(df_ordens) + 2
        aba.range('C1').value = len(df_ordens.columns) + 1
        aba.range('A2').value = df_ordens
        
        # Parte 2: contas movimento
        df_contas = self.dm1.sc_contas_movimento(nome_supercarteira=nome_sc)
        df_fundos = self.dm1.sc_fundos_exclusivos(nome_supercarteira=nome_sc)
        
        df_contas = df_contas[['GuidContaMovimento', 'NomeContaMovimento']]
        df_contas.insert(0, 'Tipo', ['CartAdm'] * len(df_contas))
        if not df_fundos.empty:
            df_fundos = df_fundos[['GuidContaCRM', 'NomeContaCRM']]
            df_fundos.columns = ['GuidContaMovimento', 'NomeContaMovimento']
            df_fundos.insert(0, 'Tipo', ['Fundo'] * len(df_fundos))
            df_contas = pd.concat([df_contas, df_fundos]).set_index('GuidContaMovimento')
        aba.range('D1').value = 2 + len(df_ordens) + 1  # LinIni
        aba.range('E1').value = len(df_contas) + aba.range('D1').value
        aba.range('F1').value = len(df_contas.columns) + 1
        aba.range(f'A{len(df_ordens)+3}').value = df_contas
        
    def solicitacao_alterar_movs(self, id_solicitacao, lista_comandos, lista_filtros):
        # 1. Verifica se houve erros de input
        if len(lista_comandos) != len(lista_filtros):
            raise Exception('sys_realocacao\solicitacao_alterar_movs: lista de comandos e filtros incompatível!')
        # 2. Monta o dataframe com os inputs
        df = pd.concat([pd.DataFrame(lista_filtros), pd.DataFrame(lista_comandos)], axis=1).set_index('IdSolMov')
        df = df.where(pd.notnull(df), None)
        
        # 3. Busca movimentações da solicitação
        movs = self.sol.solicitacao_ordens(id_solicitacao=id_solicitacao).set_index('IdSolMov')
        
        # 4. Compara os valores para fazer as edições
        for idx, row in df.iterrows():
            if not idx in movs.index:
                raise Exception('sys_realocacao\solicitacao_alterar_movs: foi informado o id incorreto da solicitação')
            linha = movs.loc[idx]
            for col in df.columns:
               if row[col]:
                   if row[col] == 'Nulo':  # Novo valor é nulo
                       if linha[col]:  # É preciso alterar valor para nulo
                           teste = self.sol.movimentacao_atualizar(idx, col, None)
                           if teste != 'Sucesso':
                               print(f'Falha ao atualizar registro para nulo: {idx}, campo: {col}')
                   else:
                       if linha[col] != row[col]: # alterar o valor
                           teste = self.sol.movimentacao_atualizar(idx, col, row[col])
                           if teste != 'Sucesso':
                               print(f'Falha ao atualizar registro: {idx}, campo: {col}')

    def solicitacao_email_ordens_atualizadas(self, id_solicitacao, mensagem=None):
        sol = Solicit(id_solicitacao=id_solicitacao)
        movs = sol.solmov.copy()
        movs = movs[['Veiculo', 'NomeContaMovimento', 'TipoMov', 'Classe', 'NomeProduto', 'FinanceiroDem', 'QuantidadeDem', 'FinanceiroExec', 'QuantidadeExec']]
        texto = ""
        if mensagem:
            texto = f".<br>{mensagem}<br>"
        texto = f"as movimentações sugeridas foram editadas pelo time de Portfolio Management{texto} <br><b>Nova tabela de movimentações:</b><br>{movs.to_html()}"
        sol.email_envia(assunto='Movimentações alteradas', texto_mensagem=texto)
        
    def solicitacao_boletagem(self, id_solicitacao, explosao=True, nome_planilha=None):
        from funcoes_datas import FuncoesDatas
        fd = FuncoesDatas(homologacao=self.homologacao)
        
        # 1. Carrega solicitação e pega as movimentações
        sol = Solicit(id_solicitacao=id_solicitacao, carregar_sc=True)
        # nome_sc = sol.sc.nome
        movs = sol.solmov.copy()
        
        # 2. Carrega a lista de posições por veículo
        posicoes = []
        pls = []
        if movs.empty:
            if explosao:
                veiculos = sol.sc.posexp['Origem'].unique()
            else:
                veiculos = ['CartAdm']
        else:    
            veiculos = movs['Veiculo'].unique()      
            
        for veic in veiculos:
            if veic in ['PF', 'CartAdm']:
                df = sol.sc.pos.copy()
                pl = [df.iloc[0]['PLFinal'], df.iloc[0]['PLFuturo']]
                df = df[['SC', 'GuidContaMovimento', 'NomeContaMovimento', 'Classe', 'SubClasse', 'TipoProduto', 'GuidProduto', 'NomeProduto', 'RatingGPS', 'FinanceiroFinal', 'FinanceiroFuturo']]                    
                df = df[df['Classe']!='Ajuste']
                df.rename(columns={'SC': 'Veiculo'}, inplace=True)
                # Ativos que ainda não estão na carteira
                lista_faltantes = []
                for idx, row in movs[movs['Veiculo']==veic].iterrows():
                    if row['GuidProduto']:
                        guidprod = str(row['GuidProduto']).lower()
                        if len(df[df['GuidProduto']==guidprod]) == 0:
                            dicio = {'Veiculo': sol.sc.nome, 'GuidContaMovimento': row['GuidContaMovimento'], 'NomeContaMovimento': row['NomeContaMovimento'], 'Classe': row['Classe'],
                                     'GuidProduto': guidprod, 'NomeProduto': row['NomeProduto'], 
                                     'FinanceiroFinal': 0}
                            lista_faltantes.append(dicio)
                if len(lista_faltantes) > 0:
                    df_temp = pd.DataFrame(lista_faltantes)
                    df = pd.concat([df, df_temp])
            
                posicoes.append(df)
                pls.append(pl)
            else:
                fdo = Fundo(nome_conta_crm=veic, base_dm1=self.dm1)
                df = fdo.posicao_melhor()
                df = df[['NomeContaCRM', 'Classe', 'SubClasse', 'TipoProduto', 'GuidProduto', 'NomeProduto', 'RatingGPS', 'FinanceiroFinal', 'FinanceiroFuturo']]

                df = df[df['Classe']!='Ajuste']
                df.rename(columns={'NomeContaCRM': 'Veiculo'}, inplace=True)
                df.insert(1, 'GuidContaMovimento', [None] * len(df))
                df.insert(2, 'NomeContaMovimento', [None] * len(df))
                # Ativos que ainda não estão na carteira
                lista_faltantes = []
                movs.loc[movs['GuidContaMovimento'].isnull(), 'GuidContaMovimento'] = None
                for idx, row in movs[movs['Veiculo']==veic].iterrows():
                    if row['GuidProduto']:
                        # if row['GuidContaMovimento'] == None:
                        #     #solicitacoes sem conta movimento preenchida estão bugando para fundos. testando assim
                        #     row['GuidContaMovimento'] = row['Veiculo']
                        #     row['NomeContaMovimento'] = row['Veiculo']
                        guidprod = str(row['GuidProduto']).lower()
                        if len(df[df['GuidProduto']==guidprod]) == 0:
                            dicio = {'Veiculo': row['Veiculo'], 'GuidContaMovimento': row['GuidContaMovimento'], 'NomeContaMovimento': row['NomeContaMovimento'], 'Classe': row['Classe'],
                                     'GuidProduto': guidprod, 'NomeProduto': row['NomeProduto'], 
                                     'FinanceiroFinal': 0}
                            lista_faltantes.append(dicio)
                if len(lista_faltantes) > 0:
                    df_temp = pd.DataFrame(lista_faltantes)
                    df = pd.concat([df, df_temp])
                    
                posicoes.append(df)
                pl = [fdo.pl_melhor(), fdo.pl_melhor(futuro=True)]
                pls.append(pl)
        
        posicoes = pd.concat(posicoes).sort_values('Veiculo')
        
        # Adiciona linhas de totais de classe
        veiculos = posicoes['Veiculo'].unique()
        df = posicoes.copy()
        df = df[['Veiculo', 'Classe', 'FinanceiroFinal', 'FinanceiroFuturo']]
        df = df.groupby(['Veiculo', 'Classe'], dropna=False).sum().reset_index()
        df.insert(0, 'EClasse', [0] * len(df))
        posicoes = pd.concat([posicoes, df])
        posicoes['EClasse'] = posicoes['EClasse'].fillna(1)
        posicoes.reset_index(inplace=True)
        if 'index' in posicoes.columns:
            posicoes.drop('index', axis=1, inplace=True)
        
        # 3. Carrega as movimentações
        posicoes.insert(len(posicoes.columns), 'QouF', ['F'] * len(posicoes))
        posicoes.insert(len(posicoes.columns), 'ContaMovO_D', [None] * len(posicoes))
        posicoes.insert(len(posicoes.columns), 'ContaMovGuidO_D', [None] * len(posicoes))
        posicoes.insert(len(posicoes.columns), 'MovsFazer', [0] * len(posicoes))    # Tem que ser a última a ser inserida     
        for idx, row in movs.iterrows():
            sinal = 1
            if row['TipoMov'] == 'V':
                sinal = -1
            if row['GuidProduto']:
                guidprod = str(row['GuidProduto']).lower() 
                busca = posicoes[posicoes['GuidProduto']==guidprod]
                if len(busca) == 1:
                    indice = busca.index[0]        
                    if row['Veiculo'] in ['PF', 'CartAdm']:                                
                        posicoes.loc[indice, 'MovsFazer'] = row['FinanceiroExec'] * sinal
                    else:
                        posicoes.loc[indice, 'MovsFazer'] = row['FinanceiroFundoEst'] * sinal
                else:
                    if row['Veiculo'] in ['PF', 'CartAdm']:
                        sub_busca = busca[busca['GuidContaMovimento']==row['GuidContaMovimento']]
                        indice = sub_busca.index[0]
                        posicoes.loc[indice, 'MovsFazer'] = row['FinanceiroExec']  * sinal
                    else:
                        sub_busca = busca[busca['Veiculo']==row['Veiculo']]
                        indice = busca.index[0]
                        posicoes.loc[indice, 'MovsFazer'] = row['FinanceiroFundoEst']  * sinal
            else:
                busca = posicoes[(posicoes['Classe']==row['Classe']) & (posicoes['EClasse']==0)]
                if len(busca) == 1:
                    indice = busca.index[0]
                    if row['Veiculo'] in ['PF', 'CartAdm']:
                        posicoes.loc[indice, 'MovsFazer'] = row['FinanceiroExec'] * sinal
                    else:
                        posicoes.loc[indice, 'MovsFazer'] = row['FinanceiroFundoEst'] * sinal
                elif len(busca) > 1:
                    if row['Veiculo'] in ['PF', 'CartAdm']:
                        sub_busca = busca[busca['GuidContaMovimento']==row['GuidContaMovimento']]
                        indice = busca.index[0]
                        posicoes.loc[indice, 'MovsFazer'] = row['FinanceiroExec'] * sinal
                    else:
                        sub_busca = busca[busca['Veiculo']==row['Veiculo']]
                        indice = busca.index[0]
                        posicoes.loc[indice, 'MovsFazer'] = row['FinanceiroFundoEst'] * sinal
        
        # 4. Lista de Datas
        datas = [fd.hoje()]
        for i in range(1, 42):
            datas.append(fd.workday(datas[len(datas)-1], 1))    
        
        # Ordenamento
        df_classes = self.dm1.lista_classes(indice='ClasseAt')
        posicoes.insert(0, 'Ordem', [None] * len(posicoes))
        for idx, row in posicoes.iterrows():
            posicoes.loc[idx, 'Ordem'] = df_classes.loc[row['Classe'], 'Ordem'] + row['EClasse']
        posicoes.sort_values(['Veiculo', 'Ordem', 'SubClasse', 'NomeProduto'], inplace=True)
        posicoes.drop('Ordem', axis=1, inplace=True)
        
        # 5. Monta a planilha
        if nome_planilha:
            wb = xw.Book(nome_planilha)
        else:
            wb = xw.Book.caller()
        aba = wb.sheets('Boletagem')
        # Campos informativos
        aba.range('Bol_IdSolicitacao').value = id_solicitacao
        # aba.range('BolSC').value = nome_sc
        aba.range('Bol_SolGuid').value = sol.guid_solicitacao
        aba.range('Bol_TipoSol').value = sol.tipo_solicitacao_nome()
        aba.range('Bol_PLProj').value = sol.pl_projetado()
        aba.range('Bol_Motivo').value = sol.motivo_mov()
        if sol.aporte() != 0:
            aba.range('Bol_MovCliente').value = sol.aporte()
            aba.range('Bol_DataEsperada').value = sol.data_pagamento()
            aba.range('Bol_ContaMov').value = sol.conta_movimento_escolhida_nome()
        else:
            aba.range('Bol_MovCliente').value = ''
            aba.range('Bol_DataEsperada').value = ''
            aba.range('Bol_ContaMov').value = ''
        # Escreve posições
        ini = aba.range('Bol_LinIni')        
        posicoes.set_index('EClasse', inplace=True)
        for i in range(0, len(datas)):
            posicoes.insert(len(posicoes.columns), datas[i], [0] * len(posicoes))
        ini.value = posicoes
        n_cols = len(posicoes)
        
        print('teste etetete')
        rng = aba.range('A1')  # Define a célula inicial para o loop

        # lin = 1
        # while not rng.offset(lin).value is None:
        #     if rng.offset(lin).value == 0:
        #         cont = xw.func.xl_countifs(rng.column(2), rng.offset(lin, 1).value, rng.column(5), rng.offset(lin, 4).value)
        #         rng.offset(lin, 15).resize(1, 42).formula_r1c1 = f"=SUM(R[1]C:R[{cont-1}]C)"
        #         rng.offset(lin, 15).copy()
        #     else:
        #         rng.offset(lin, 16).copy()

        #     rng.offset(lin).pastespecial(xlPasteFormats:=True)
        #     lin += 1

    def solicitacao_boletagem_supercarteira(self, nome_sc:str=None, guid_sc:str=None, lista_ativos_adicionais:list=[], nome_planilha=None):
        if not nome_sc and not guid_sc:
            raise Exception('É preciso informar uma supercarteira ou seu guid')
        from funcoes_datas import FuncoesDatas
        fd = FuncoesDatas(homologacao=self.homologacao)
        
        # 1. Carrega supercarteira e busca fundos exclusivos e outros cotistas
        if nome_sc:
            sc = Supercarteira(nome_supercarteira=nome_sc, base_dm1=self.dm1)
        else:
            sc = Supercarteira(guid_supercarteira=guid_sc, base_dm1=self.dm1)
        # Fundos
        fundos = list(sc.posexp[~sc.posexp['Origem'].isin(['PF', 'CartAdm'])]['Origem'].unique())
        veiculos_dicio = {}
        veiculos = [x for x in fundos] 
        pos = self.dm1.posicao_supercarteiras()
        for veic in fundos:
            veiculos_dicio[veic] = 1
            codigo = self.dm1.dado_cadastro_conta(campo='CodigoProduto', nome_conta=veic)
            filtro = pos[pos['IdProdutoProfitSGI']==codigo]['SC'].unique()
            for item in filtro:
                if not item in veiculos:
                    veiculos_dicio[item] = 2
                    veiculos.append(item)                    
        
        # 2. Busca dados dos ativos a adicionar        
        if len(lista_ativos_adicionais) == 0:
            ativos = pd.DataFrame()
        else:
            ativos = []
            cad_ativos = self.dm1.lista_produtos_bol()
            pos = self.dm1.posicao_supercarteiras()
            pos_f = self.dm1.posicao_fundos_all()
            for item in lista_ativos_adicionais:                
                # a. Comprado no fundo ou na PF?
                if item in pos['GuidProduto'].unique() and item in pos_f['GuidProduto'].unique():
                    pos_pf = True
                    pos_fundo = True
                elif item not in pos['GuidProduto'].unique() and item not in pos_f['GuidProduto'].unique():
                    pos_pf = True
                    pos_fundo = True
                elif item in pos['GuidProduto'].unique() and item not in pos_f['GuidProduto'].unique():
                    pos_pf = True
                    pos_fundo = False
                else:
                    pos_pf = False
                    pos_fundo = True
                
                # b. Outros dados cadastrais
                if item in cad_ativos.index:
                    linha = cad_ativos.loc[[item]].iloc[0]
                    classe = linha['Classe']
                    sub_classe = linha['SubClasse']
                    nome = linha['NomeProduto'] 
                else:
                    at = Ativo(item, base_dm1=self.dm1)
                    classe = at.classe
                    sub_classe = at.sub_classe
                    nome = at.nome
                # c. dicionario
                dicio = {'GuidProduto': item, 'NomeProduto': nome, 'Classe': classe, 'SubClasse': sub_classe,
                         'PF': pos_pf, 'Fundo': pos_fundo, 'FinanceiroFinal': 0}
                ativos.append(dicio)
            ativos =pd.DataFrame(ativos)
        
        # 3. Carrega a lista de posições por veículo
        posicoes = []
        pls = []        
            
        for veic in veiculos:
            if veiculos_dicio[veic] == 2:
                df = Supercarteira(nome_supercarteira=veic, base_dm1=self.dm1).pos.copy()
                pl = [df.iloc[0]['PLFinal'], df.iloc[0]['PLFuturo']]
                df = df[['SC', 'GuidContaMovimento', 'NomeContaMovimento', 'Classe', 'SubClasse', 'TipoProduto', 'GuidProduto', 'NomeProduto', 'RatingGPS', 'FinanceiroFinal', 'FinanceiroFuturo']]                    
                df = df[df['Classe']!='Ajuste']
                df.rename(columns={'SC': 'Veiculo'}, inplace=True)
                # Ativos que ainda não estão na carteira
                lista_faltantes = []
                if not ativos.empty:
                    for idx, row in ativos[ativos['PF']==True].iterrows():
                        guidprod = str(row['GuidProduto']).lower()
                        if len(df[df['GuidProduto']==guidprod]) == 0:
                            dicio = {'Veiculo': veic, 'GuidContaMovimento': None, 'NomeContaMovimento': None, 'Classe': row['Classe'], 'SubClasse': row['SubClasse'],
                                     'GuidProduto': guidprod, 'NomeProduto': row['NomeProduto'], 'FinanceiroFinal': 0}
                            lista_faltantes.append(dicio)
                if len(lista_faltantes) > 0:
                    df_temp = pd.DataFrame(lista_faltantes)
                    df = pd.concat([df, df_temp])
            
                posicoes.append(df)
                pls.append(pl)
            else:
                fdo = Fundo(nome_conta_crm=veic, base_dm1=self.dm1)
                df = fdo.posicao_melhor()
                df = df[['NomeContaCRM', 'Classe', 'SubClasse', 'TipoProduto', 'GuidProduto', 'NomeProduto', 'RatingGPS', 'FinanceiroFinal', 'FinanceiroFuturo']]

                df = df[df['Classe']!='Ajuste']
                df.rename(columns={'NomeContaCRM': 'Veiculo'}, inplace=True)
                df.insert(1, 'GuidContaMovimento', [None] * len(df))
                df.insert(2, 'NomeContaMovimento', [None] * len(df))
                # Ativos que ainda não estão na carteira
                lista_faltantes = []
                if not ativos.empty:
                    for idx, row in ativos[ativos['Fundo']==True].iterrows():
                        guidprod = str(row['GuidProduto']).lower()
                        if len(df[df['GuidProduto']==guidprod]) == 0:
                            dicio = {'Veiculo': veic, 'GuidContaMovimento': None, 'NomeContaMovimento': None, 'Classe': row['Classe'], 'SubClasse': row['SubClasse'],
                                     'GuidProduto': guidprod, 'NomeProduto': row['NomeProduto'], 'FinanceiroFinal': 0}
                            lista_faltantes.append(dicio)
                if len(lista_faltantes) > 0:
                    df_temp = pd.DataFrame(lista_faltantes)
                    df = pd.concat([df, df_temp])
                    
                posicoes.append(df)
                pl = [fdo.pl_melhor(), fdo.pl_melhor(futuro=True)]
                pls.append(pl)
        
        posicoes = pd.concat(posicoes).sort_values('Veiculo')
        
        # Adiciona linhas de totais de classe
        veiculos = posicoes['Veiculo'].unique()
        df = posicoes.copy()
        df = df[['Veiculo', 'Classe', 'FinanceiroFinal', 'FinanceiroFuturo']]
        df = df.groupby(['Veiculo', 'Classe'], dropna=False).sum().reset_index()
        df.insert(0, 'EClasse', [0] * len(df))
        posicoes = pd.concat([posicoes, df])
        posicoes['EClasse'] = posicoes['EClasse'].fillna(1)
        posicoes.reset_index(inplace=True)
        if 'index' in posicoes.columns:
            posicoes.drop('index', axis=1, inplace=True)
        
        # 3. Carrega as movimentações
        posicoes.insert(len(posicoes.columns), 'QouF', ['F'] * len(posicoes))
        posicoes.insert(len(posicoes.columns), 'ContaMovO_D', [None] * len(posicoes))
        posicoes.insert(len(posicoes.columns), 'ContaMovGuidO_D', [None] * len(posicoes))
        posicoes.insert(len(posicoes.columns), 'MovsFazer', [0] * len(posicoes))    # Tem que ser a última a ser inserida     
        
        # 4. Lista de Datas
        datas = [fd.hoje()]
        for i in range(1, 42):
            datas.append(fd.workday(datas[len(datas)-1], 1))    
        
        # Ordenamento
        df_classes = self.dm1.lista_classes(indice='ClasseAt')
        posicoes.insert(0, 'Ordem', [None] * len(posicoes))
        for idx, row in posicoes.iterrows():
            posicoes.loc[idx, 'Ordem'] = df_classes.loc[row['Classe'], 'Ordem'] + row['EClasse']
        posicoes.sort_values(['Veiculo', 'Ordem', 'SubClasse', 'NomeProduto'], inplace=True)
        posicoes.drop('Ordem', axis=1, inplace=True)
        
        # 5. Monta a planilha
        if nome_planilha:
            wb = xw.Book(nome_planilha)
        else:
            wb = xw.Book.caller()
        aba = wb.sheets('Boletagem')
        # Campos informativos
        aba.range('BolSC').value = sc.nome
        aba.range('Bol_IdSolicitacao').value = 0
        aba.range('Bol_SolGuid').value = 'ND'
        aba.range('Bol_TipoSol').value = 'Custom'
        aba.range('Bol_PLProj').value = 0
        aba.range('Bol_Motivo').value = "ND"
        
        aba.range('Bol_MovCliente').value = ''
        aba.range('Bol_DataEsperada').value = ''
        aba.range('Bol_ContaMov').value = ''
        # Escreve posições
        ini = aba.range('Bol_LinIni')        
        posicoes.set_index('EClasse', inplace=True)
        for i in range(0, len(datas)):
            posicoes.insert(len(posicoes.columns), datas[i], [0] * len(posicoes))
        ini.value = posicoes
        n_cols = len(posicoes)       

    def solicitacao_boletagem_montar(self, executar_pre_trade:bool=False, nome_planilha=None):
        # Carrega dados da planilha
        if nome_planilha:
            wb = xw.Book(nome_planilha)
        else:
            wb = xw.Book.caller()                            
            
        aba = wb.sheets('Boletagem')
        abaf = wb.sheets('Format')
        ini = aba.range('Bol_LinIni')
        df = ini.options(pd.DataFrame, header=1,index=False, expand='table').value
        
        # Escolha ou não da solicitação
        id_solicitacao = aba.range('Bol_IdSolicitacao').value
        
        if id_solicitacao != 0:
            solicit = Solicit(id_solicitacao=id_solicitacao)
            # Contas movimento escolhidas por quem solicitou a movimentação
            conta_mov_escolhida_guid = solicit.conta_movimento_escolhida_guid()
            conta_mov_escolhida = solicit.conta_movimento_escolhida_nome()
            motivo_escolhido = solicit.motivo_mov()
            if not motivo_escolhido:
                motivo_escolhido = 'Realocação'
        else:
            solicit = None
            conta_mov_escolhida_guid = None
            conta_mov_escolhida = None
            motivo_escolhido = 'Realocação'
        
        # Importa o CRM
        from databases import Crm; from objetos import Ativo
        crm = Crm(homologacao=self.homologacao)
        
        # Verificações
        lista = list(df.columns)
        col_ini = lista.index('MovsFazer')
        # TODO Verificar se usuário fez boletas para tudo que devia (Classes e Produtos)
        lista_contas = list(df['NomeContaMovimento'].unique())
        com_conta_enc = False
        for conta in lista_contas:
            try:
                if 'ENCERRAD' in conta:
                    com_conta_enc = True
            except:
                pass
        if com_conta_enc == True:
            # df = df.loc[df['EClasse'==1]]
            df.loc[df['GuidContaMovimento']==0, 'GuidContaMovimento'] = np.nan
            df.loc[df['NomeContaMovimento'].isnull(), 'NomeContaMovimento'] = 'NULO'
            df.loc[df['NomeContaMovimento'].str.contains('ENCERRAD',case=False), 'GuidContaMovimento'] = np.nan
            df.loc[df['NomeContaMovimento'] == 'NULO', 'NomeContaMovimento'] = np.nan
        #está gerando kgado essas linhas    
        df = df.loc[df['EClasse']==1]
        # Pega dados das contas de movimentação
        if df['GuidContaMovimento'].notnull().sum() > 0:
            contas_mov = list(df['GuidContaMovimento'].loc[df['GuidContaMovimento'].notnull()].unique())
            if None in contas_mov:
                contas_mov.remove(None)
            df_contas = pd.DataFrame(index=contas_mov, columns=['NomeContaMov', 'TitularidadeGuid', 'Titularidade', 'ContaCRM', 'ContaCRMGuid'])
            for idx, row in df_contas.iterrows():
                if idx == 0 or idx == '0' or idx==0.0 or idx=='0.0':
                    continue
                dados_tit = crm.conta_movimento_titularidade(conta_mov_guid=idx)
                linha_tit = crm.titularidade(filtro=f"new_titularidadeid='{dados_tit.iloc[0]['new_titularidadeid']}'",lista_campos=['new_contaid', 'new_contaidname']).iloc[0]                      
                # df_contas.loc[idx, 'NomeContaMov'] = ?
                df_contas.loc[idx, 'TitularidadeGuid'] = str(dados_tit.iloc[0]['new_titularidadeid']).lower()
                df_contas.loc[idx, 'Titularidade'] = dados_tit.iloc[0]['new_titularidadeidname']
                df_contas.loc[idx, 'ContaCRMGuid'] = str(linha_tit['new_contaid']).lower()
                df_contas.loc[idx, 'ContaCRM'] = linha_tit['new_contaidname']
            # df_contas.dropna(subset='TitularidadeGuid', inplace=True)
            titularidades = list(df_contas['TitularidadeGuid'].unique())
            contas_cli = []
            for item in titularidades:
                contas_cli.append(crm.contas_movimento_por_titularidade(item, controle_jbfo=False))
            contas_cli = pd.concat(contas_cli)
            abaf.range('Bol_ContasMov').value = contas_cli.set_index('accountid')
        # Pega as movimentações        
        boletas = []; ativos = {}
        for col in range(col_ini + 1, len(df.columns)):
            df_temp = df[(df[lista[col]] != 0) & (df['EClasse'] != 0)]
            if not df_temp.empty:
                df_temp['GuidProduto'] = df_temp['GuidProduto'].apply(lambda x: str(x).replace('{','').replace('}',''))                
                for idx, row in df_temp.iterrows():
                    if not row['GuidProduto'] in ativos:
                        ativos[row['GuidProduto']] = Ativo(id_ativo=row['GuidProduto'], homologacao=self.homologacao)                        
                    valor = row[lista[col]]
                    tipo_mov = 'V'
                    if valor > 0:
                        tipo_mov = 'C'
                    datas = ativos[row['GuidProduto']].ler_data(dia_mov=lista[col], tipo_mov=tipo_mov)
                    prebol = False
                    quantidade = None
                    neg_tipo = row['QouF']
                    resg_tot = False
                    if neg_tipo == 'FT':
                        neg_tipo = 'F'
                        resg_tot = True
                    financeiro = abs(valor)
                    id_sis_destino = ativos[row['GuidProduto']].sistema_destino(tipo_mov=tipo_mov)                    
                    if ativos[row['GuidProduto']].neg_tipo == 'Q' and neg_tipo == 'F':
                        prebol = True
                        preco = ativos[row['GuidProduto']].preco_ultimo()
                        neg_tipo = 'Q'
                        if preco:
                            quantidade = abs(int(valor / preco))
                    elif neg_tipo == 'Q':
                        quantidade = financeiro
                        financeiro = None
                    dicio = {'AtivoGuid': row['GuidProduto'], 'AtivoNome': row['NomeProduto'], 'AtivoCadastrado': True,
                             'TipoMov': tipo_mov, 'QouF': neg_tipo, 'PreBoleta': prebol, 'DataMov': datas[0], 'DataCot': datas[1], 'DataFin': datas[2],
                             'Financeiro': financeiro, 'Quantidade': quantidade, 'Preco': None, 'ResgTot': resg_tot, 'IdTipoOrdem': 1, 'Separar': False,
                             'Contraparte': None, 'IdSysDestino': id_sis_destino}
                    if id_solicitacao != 0:
                        dicio['IdSolMov']: id_solicitacao
                    if row['GuidContaMovimento']:# PF
                        try:
                            if math.isnan(row['GuidContaMovimento'])==True:
                                valor_nulo = True
                        except:
                            valor_nulo = False
                        try:
                            if math.isnan(row['ContaMovGuidO_D'])==True:
                                valor_nulo_od = True
                        except:
                            valor_nulo_od = False
                        if valor_nulo == False:
                            # Se for movimentação de PF, preecnche os campos necessários
                            linha = df_contas.loc[row['GuidContaMovimento']]
                            dicio.update({'GuidTitularidade': linha['TitularidadeGuid'], 'Titularidade': linha['Titularidade'], 'IdTipoPortfolio': 3, 'GuidPortfolio': linha['TitularidadeGuid'],
                                        'ContaCRM': linha['ContaCRM'], 'ContaCRMGuid': linha['ContaCRMGuid'], 'MotivoMov': 'Realocação'})
                        if valor_nulo == False and valor_nulo_od == True:  
                            #Código original
                            dicio.update({'GuidContaMovimento': row['GuidContaMovimento'], 'ContaMovimento': row['NomeContaMovimento']})
                            dicio.update({'GuidContaMovimentoOrigem': row['GuidContaMovimento'], 'ContaMovimentoOrigem': row['NomeContaMovimento']})
                            dicio.update({'GuidContaMovimentoDestino': row['GuidContaMovimento'], 'ContaMovimentoDestino': row['NomeContaMovimento']})                    
                            if conta_mov_escolhida:
                                if tipo_mov == 'C':
                                    dicio.update({'GuidContaMovimento': conta_mov_escolhida_guid, 'ContaMovimento': conta_mov_escolhida})
                                else:
                                    dicio.update({'GuidContaMovimentoDestino': conta_mov_escolhida_guid, 'ContaMovimentoDestino': conta_mov_escolhida})     
                        elif valor_nulo == False and valor_nulo_od == False:  
                            #Código Dex 13/08/2024
                            if tipo_mov == 'C':
                                dicio.update({'GuidContaMovimento': row['ContaMovGuidO_D'], 'ContaMovimento': row['ContaMovO_D']})
                                dicio.update({'GuidContaMovimentoOrigem': row['GuidContaMovimento'], 'ContaMovimentoOrigem': row['NomeContaMovimento']})
                                dicio.update({'GuidContaMovimentoDestino': row['GuidContaMovimento'], 'ContaMovimentoDestino': row['NomeContaMovimento']})                    
                            else:
                                dicio.update({'GuidContaMovimento': row['GuidContaMovimento'], 'ContaMovimento': row['NomeContaMovimento']})
                                dicio.update({'GuidContaMovimentoOrigem': row['GuidContaMovimento'], 'ContaMovimentoOrigem': row['NomeContaMovimento']})
                                dicio.update({'GuidContaMovimentoDestino': row['ContaMovGuidO_D'], 'ContaMovimentoDestino': row['ContaMovO_D']})                    
                            if conta_mov_escolhida:
                                if tipo_mov == 'C':
                                    dicio.update({'GuidContaMovimento': conta_mov_escolhida_guid, 'ContaMovimento': conta_mov_escolhida})
                                else:
                                    dicio.update({'GuidContaMovimentoDestino': conta_mov_escolhida_guid, 'ContaMovimentoDestino': conta_mov_escolhida})     
                        else:  # Fundo                        
                            dados_conta = crm.account(filtro=f"Name='{row['Veiculo']}'", lista_campos=['accountid'])
                            dicio.update({'ContaCRM': row['Veiculo'], 'ContaCRMGuid': str(dados_conta.iloc[0]['accountid']).lower(), 'IdTipoPortfolio': 1, 'GuidPortfolio': str(dados_conta.iloc[0]['accountid']).lower()})               
                            if motivo_escolhido:
                                dicio['MotivoMov'] = motivo_escolhido

                    else:  # Fundo
                        veiculo_account_id = None
                        if CW().rotina_diaria(225):
                            df_po_cad = self.dm1.po_cadastro_tbl()
                            df_po_cad = df_po_cad[(df_po_cad['Tipo']=='Fundo') & (df_po_cad['NomeContaCRM']==row['Veiculo'])]
                            if not df_po_cad.empty:
                                veiculo_account_id = df_po_cad.iloc[0]['GuidContaCRM']                                
                                
                        if not veiculo_account_id:
                            dados_conta = crm.account(filtro=f"Name='{row['Veiculo']}'", lista_campos=['accountid'])
                            veiculo_account_id = str(dados_conta.iloc[0]['accountid']).lower()
                        dicio.update({'ContaCRM': row['Veiculo'], 'ContaCRMGuid': veiculo_account_id, 'IdTipoPortfolio': 1, 'GuidPortfolio': veiculo_account_id})
                    boletas.append(dicio)
        boletas = pd.DataFrame(boletas)
        
        # Verifica preços limite de execução
        if solicit:
            movs = solicit.solmov.copy()
            movs = movs[['GuidProduto', 'PrecoLimDem']].dropna()
            if not movs.empty:
                for idx, row in movs.iterrows():
                    temp = boletas[boletas['AtivoGuid']==row['GuidProduto']]
                    if not temp.empty:
                        for indice, linha in temp.iterrows():
                            boletas.loc[indice, 'Preco'] = row['PrecoLimDem']
                            boletas.loc[indice, 'IdTipoOrdem'] = 6
        
        # Escreve os dados na planilha
        aba = wb.sheets('Temp')
        aba.range('A1').value = boletas
        
        # Executa o teste de pré-trade
        if executar_pre_trade:
            pret = PreTrade(base_dm1=self.dm1)
            dicionario = pret.simulacao(boletas)                            
            aba = wb.sheets('Temp2')
            aba.range('A1').value = dicionario['Titulo']
            aba.range('B1').value = dicionario['PreTrade_Msg']
            aba.range('A3').value  = dicionario['PreTrade_MemoriaCalc']            
            aba.range('D3').value  = dicionario['Verificacoes']   

    def solicitacao_boletagem_importar(self, nome_planilha:str=None, segregar_portfolios:bool=False):
        # Carrega dados da planilha
        if nome_planilha:
            wb = xw.Book(nome_planilha)
        else:
            wb = xw.Book.caller()
        
        aba = wb.sheets('Boletagem')
        id_solicitacao = aba.range('Bol_IdSolicitacao').value
        acao = None
        observacao = None
        if id_solicitacao == 0:
            aba = wb.sheets('BolSC')
            acao = aba.range('BolAcao').value
            observacao = aba.range('BolObservacao').value
        # 1. Importa boletas
        aba = wb.sheets('Temp')
        ini = aba.range('A1')
        df = ini.options(pd.DataFrame, header=1,index=False, expand='table').value
        if 'index' in df.columns:
            df.drop('index', axis=1, inplace=True)
        
        # 2. Inicia order managers
        tipos_port = list(df['IdTipoPortfolio'].unique())
        dm1 = PosicaoDm1Pickle()           
        om_fdo = OrderManager(id_tipo_portfolio=tipos_port, id_system_origem=61, base_dm1=dm1, carregar_dados=True)
                        
        # 3. Importa ordens por portfolio
        portfolios = list(df['GuidPortfolio'].unique())
        dicio = {}
        for portfolio in portfolios:
            df_temp = df[df['GuidPortfolio']==portfolio].copy()
            tipo_port = df_temp.iloc[0]['IdTipoPortfolio']
            if tipo_port == 1:
                port = Fundo(guid_conta_crm=portfolio, base_dm1=dm1)
                if segregar_portfolios:
                    resultado = self.__importar_ordens_por_portfolio__(port, om_fdo, df_temp, id_solicitacao, acao=acao, observacao=observacao)
                else:
                    dicio[portfolio] = port
            elif tipo_port == 3:
                port = Titularidade(guid_titularidade=portfolio, base_dm1=dm1)
                if segregar_portfolios:
                    resultado = self.__importar_ordens_por_portfolio__(port, om_fdo, df_temp, id_solicitacao, acao=acao, observacao=observacao)
                else:
                    dicio[portfolio] = port
            else:
                raise Exception('Tipo de portfolio não suportado.')
        
        # 4. Importa tudo em uma ordem só
        if not segregar_portfolios:            
            resultado = self.__importar_ordens_por_portfolio__(dicio, om_fdo, df, id_solicitacao, acao=acao, observacao=observacao)
                    
        ini.value = df
    
    @staticmethod
    def teste_valor(valor):
            try:
                if valor:
                    if not math.isnan(valor):
                        return True
            except:
                pass
    
    def __importar_ordens_por_portfolio__(self, portfolio, order_mgmt:OrderManager, df_ordens:pd.DataFrame, id_solicitacao:int, acao:str='Envio de ordem', observacao:str='Via solicitação de realocação'):
        """
        Importa as ordens por portfólio ou em um único grupo

        Parameters
        ----------
        portfolio : dict ou Objeto Portfolio
            Se for enviar todas as ordens juntas, mandar um dicionario com os objetos portfolio.
        order_mgmt : OrderManager
            Objeto carregado.
        df_ordens : pd.DataFrame
            Dataframe com as ordens.
        id_solicitacao : int
            Id da solicitação.

        Returns
        -------
        ordens : TYPE
            DESCRIPTION.

        """
        if str(type(portfolio)) == "<class 'dict'>":
            buscar_port = True
        else:
            buscar_port = False
            port = portfolio
            
        ordens = []
        for idx, row in df_ordens.iterrows():
            if buscar_port:
                port = portfolio[row['GuidPortfolio']]
            campos_adicionais = {'Data Pedido': row['DataMov'], 'Data Cotização': row['DataCot'], 'Data Financeiro': row['DataFin'],
                                 'IdTipoOrdem': row['IdTipoOrdem']}
            if id_solicitacao != 0:
                campos_adicionais['Solicitação de Realocação (Id)'] = id_solicitacao
            if row['Preco']:
                campos_adicionais['Preço Limite'] = row['Preco']
            if row['Contraparte']:
                campos_adicionais['Contraparte'] = row['Contraparte']
            if port.tipo_portfolio == 3:
                campos_adicionais.update({'Titularidade': row['Titularidade'], 'TitularidadeGuid': row['GuidTitularidade'], 
                                     'ContaMovimento': row['ContaMovimento'], 'GuidContaMovimento': row['GuidContaMovimento'],
                                     'ContaMovimentoDestino': row['ContaMovimentoDestino'], 'GuidContaMovimentoDestino': row['GuidContaMovimentoDestino'],
                                     'ContaMovimentoOrigem': row['ContaMovimentoOrigem'], 'GuidContaMovimentoOrigem': row['GuidContaMovimentoOrigem'],
                                     'MotivoAplic': row['MotivoMov'], 'MotivoResg': row['MotivoMov']})
            id_sis_destino = None
            if 'IdSysDestino' in df_ordens.columns:
                if self.teste_valor(row['IdSysDestino']):
                    id_sis_destino = row['IdSysDestino']
            dicionario = port.dicio_base_ordem(guid_produto=row['AtivoGuid'], nome_produto=row['AtivoNome'], tipo_mov=row['TipoMov'],
                                               q_ou_f=row['QouF'], financeiro=row['Financeiro'], quantidade=row['Quantidade'], resg_tot=row['ResgTot'],
                                               acao=acao, observacao=observacao, id_system_destino=id_sis_destino)
            dicionario['CamposAdicionais'] = campos_adicionais
            ordens.append(dicionario)
            
        if len(ordens) > 0:
            ordens = pd.DataFrame(ordens)
            
            for idx, row in ordens.iterrows():
                order_mgmt.inserir_ordem(id_system_destino=row['SysDestino'], guid_portfolio=row['GuidPortfolio'], ativo_guid=row['AtivoGuid'], 
                                 ativo_nome=row['AtivoNome'], tipo_mov=row['TipoMov'], resgate_total=row['ResgTot'],
                                 preboleta=False, acao=row['Acao'], observacao=row['Observacao'],
                                 q_ou_f=row['QouF'], quantidade=row['Quantidade'], financeiro=row['Financeiro'],
                                 campos_adicionais=row['CamposAdicionais'])
        
            order_mgmt.upload_orders(mesmo_grupo=True)
        return ordens            

    def solicitacao_auto_liquidez_e_distrato(self, id_sol:int, forca_produto_liq:bool=True, aceita_data_errada:bool=False, marca_concluido_auto:bool=True, responde_email:bool=True, sol=None, distrato=False, mensagem=None):
        """
        Essa função importa ordens "simples" de forma automática.
        Entende-se por ordens simples as que são de liquidez

        Args:
            id_sol (int): Id da solicitação a ser gerada
            forca_produto_liq (bool, optional): Opção para se não houver produto de liquidez, ele procurar f exclusivo ou fundo de liquidez disponivel
            aceita_data_errada (bool): Quem abre as solicitações deveria usar a data como data de liquidação. Mas ainda é frequente enviarem com data de movimentação
            marca_concluido_auto (bool, optional): Marca a solicitação na base como concluída. Defaults to True.
            responde_email (bool, optional): Responde o e-mail da solicitação. Defaults to True.

       
        """
        # essa automação faz o processo automático de boletagem para ordens do tipo de resgate / aporte em portfolio
        if sol == None:
            sol = Solicit(id_solicitacao=id_sol)
        sol_mov = sol.solmov
        guid_titularidade=sol.campo_aux(24)
        # sc = sol.sc
        titularidade = sol.campo_aux(25)
        if sol_mov.empty:
            tit = Titularidade(guid_titularidade=guid_titularidade,)
        else:
            nome_sc = sol.nome_sc() 
            sc = Supercarteira(nome_supercarteira=nome_sc, data_pos=datetime.datetime.now())
            pos_sc = sc.pos
            guid_conta_mov = sol_mov['GuidContaMovimento'].iloc[0]
            if guid_conta_mov != None and guid_conta_mov != '':
                guid_titularidade = pos_sc['TitularidadeGuid'].loc[pos_sc['GuidContaMovimento']==guid_conta_mov].iloc[0]

            tit = Titularidade(guid_titularidade=guid_titularidade)
        try:
            pos = tit.pos.copy()
        except:
            #se não encontrar a titularidade da solicitação, pega a titularidade que está na sc, caso só tenha uma

            nome_sc = sol.nome_sc() 
            sc = Supercarteira(nome_supercarteira=nome_sc, data_pos=datetime.datetime.now())
            pos_sc = sc.pos
            titularidade_sc = pos_sc['Titularidade'].unique()
            if len(titularidade_sc)==1:
                tit = Titularidade(titularidade=titularidade_sc[0])
                pos = tit.pos.copy()
            else:
                raise Exception("Não foi encontrada posição para esssa titularidade")    

        produto_liq_guid = tit.produto_liquidacao_guid
        pos['ProdLiq'] = False
        pos.loc[pos['GuidProduto'] == produto_liq_guid, 'ProdLiq'] = True
        guid_conta_escolhida = sol.conta_movimento_escolhida_guid()
        valor_mov = sol.aporte()
        data_liq = sol.data_pagamento()
        
        
        if data_liq < datetime.datetime(2024,1,1):
            data_liq = datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time())
        om_tit = OrderManager(id_tipo_portfolio=3, id_system_origem=61, carregar_dados=True)

        if sol_mov.empty:
            if len(pos.loc[pos['ProdLiq']==True]) == 0:
                if forca_produto_liq == True:
                    pos.sort_values(by=['FinanceiroInicial'], inplace=True, ascending=False)
                    #TODO buscar se o fundo é exclusivo da maneira correta
                    pos_exc = pos.loc[(pos['SubClasse']=='F Exclusivo') & (pos['FormaCondominio']=='Aberto')]
                    pos_rf_liq = pos.loc[pos['SubClasse']=='RF Pos Liquidez']

                    #verifica se tem saldo em um fundo de liquidez para fazer o resgate. Se tiver e o valor for maior que o de resgate solicitado, segue. Se não
                    # faz a mesma verificação para fundo exclusivo

                    if len(pos_rf_liq) > 0:
                        if pos_rf_liq['FinanceiroInicial'].iloc[0] > abs(valor_mov):
                            pos['ProdLiq'].loc[pos['SubClasse']=='RF Pos Liquidez'] = True
                            produto_liq_guid = pos_rf_liq['GuidProduto'].iloc[0]
                    elif len(pos_exc) > 0:
                        if pos_exc['FinanceiroInicial'].iloc[0] > abs(valor_mov):
                            pos['ProdLiq'].loc[(pos['SubClasse']=='F Exclusivo') & (pos['FormaCondominio']=='Aberto')] = True
                            produto_liq_guid = pos_exc['GuidProduto'].iloc[0]
                    else:
                        raise Exception("Não foi encontrado fundo de liquidez na carteira")
                else:
                    raise Exception("Fundo de liquidez cadastrado sem posição na carteira")

            pos_prod_liq = pos.loc[pos['ProdLiq']==True,'FinanceiroFuturo'].iloc[0]

            


            if valor_mov < 0:
                tipo_mov = 'V'
                contas = tit.conta_obj(produto_liq_guid, tipo_mov=tipo_mov, conta_destino=guid_conta_escolhida)
            else:
                tipo_mov = 'C'
                contas = tit.conta_obj(produto_liq_guid, tipo_mov=tipo_mov, conta_entrada_fin=guid_conta_escolhida)

            try:
                datas = Ativo(produto_liq_guid).forca_data_mov(tipo_mov=tipo_mov, forca_data_fin=data_liq)
            except:
                if aceita_data_errada == True:
                    datas = Ativo(produto_liq_guid).ler_data(dia_mov=data_liq, tipo_mov=tipo_mov)
                    datas = {
                        'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                        'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                        'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                    }
                else:
                    mensagem = f'Não é possível solicitar liquidez para essa titularidade com o produto de liquidez cadastrado para a data {data_liq.strftime("%d/%m/%Y")}'
                    self.responde_email_solicitacao(id_sol=id_sol, texto=mensagem)
                    return ''
                    # raise Exception(f'Não foi possível encontrar datas de movimentação para a data de liquidação {data_liq.strftime("%Y-%m-%d")} e o ativo {produto_liq_guid}')
                

            if tipo_mov == 'V' and abs(valor_mov) > pos_prod_liq:
                #TODO criar parametro e logica para caso não tenha posição em produto de liquidez, ir para outro caminho de resgatar 
                raise Exception('Resgate maior que a posição em fundo de liquidez')
            else:
                #criar ordem de resgate do ativo de liquidez no valor solicitado
                df_ordem = pos.loc[pos['GuidProduto']==produto_liq_guid]
                df_ordem = df_ordem[[
                    'GuidProduto',
                    'NomeProduto',
                    'GuidContaMovimento',
                    'NomeContaCRM',
                    'Titularidade',
                    # 'GuidTitularidade'
                ]]
                df_ordem.rename(columns={
                    'GuidProduto':'AtivoGuid',
                    'NomeProduto':'AtivoNome'
                }, inplace=True)

                df_ordem['AtivoCadastrado'] = True
                df_ordem['TipoMov'] = tipo_mov
                df_ordem['QouF'] = 'F'
                df_ordem['PreBoleta'] = False
                df_ordem['DataMov'] = datas['DataMov']
                df_ordem['DataCot'] = datas['DataCot']
                df_ordem['DataFin'] = datas['DataFin']
                df_ordem['Financeiro'] = abs(valor_mov)
                df_ordem['Quantidade'] = np.nan
                df_ordem['Preco'] = np.nan
                if valor_mov == pos_prod_liq and tipo_mov=='V':
                    resg_total = True
                else:
                    resg_total = False
                df_ordem['ResgTot'] = resg_total
                df_ordem['IdTipoOrdem'] = 1
                df_ordem['Separar'] = False
                
                df_ordem['Contraparte'] = np.nan
                df_ordem['MotivoMov'] = sol.motivo_mov()
                df_ordem['GuidTitularidade'] = guid_titularidade
                for key in contas.keys():
                    df_ordem[key] = contas[key]

                    #calcula as datas de mov, cot baseado nessa data de liq
            df_ordem['IdSolMov'] = id_sol  
            df_ordem['IdSysDestino'] = 17 
            resultado = self.__importar_ordens_por_portfolio__(tit, om_tit, df_ordem, id_sol)

        else:
            df_ordem = sol_mov[[
                'GuidContaMovimento',
                'GuidProduto',
                'NomeProduto',
                'FinanceiroDem',
                'QuantidadeDem', 
                'PrecoLimDem',
                'FinanceiroExec',
                'QuantidadeExec',
                'TipoMov',
                'ResgTotal'
            ]]

            crm = Crm()
            df_ordem['Titularidade'] = titularidade
            df_ordem['GuidTitularidade'] = guid_titularidade
            for idx, row in df_ordem.iterrows():
                conta_mov = row['GuidContaMovimento']
                titularidade_temp = crm.titularidade_conta_movimento(conta_mov)
                if len(titularidade_temp)>0:
                    df_ordem.loc[idx,'Titularidade'] = titularidade_temp['TitularidadeNome']
                    df_ordem.loc[idx,'GuidTitularidade'] = titularidade_temp['TitularidadeGuid']
            
            df_ordem['AtivoCadastrado'] = True #TODO checar o cadastro
            df_ordem.loc[df_ordem['QuantidadeDem'].notnull(), 'QouF'] = 'Q'
            df_ordem.loc[df_ordem['QuantidadeDem'].notnull(), 'PreBoleta'] = True
            df_ordem.loc[df_ordem['QuantidadeDem'].isnull(), 'QouF'] = 'F'
            df_ordem.loc[df_ordem['QuantidadeDem'].isnull(), 'PreBoleta'] = False
            
            df_ordem['DataMov'] = None
            df_ordem['DataCot'] = None
            df_ordem['DataFin'] = None
            from funcoes_datas import FuncoesDatas
            fdt = FuncoesDatas()
            for i in range(0,len(df_ordem)):
                if len(df_ordem['GuidContaMovimento'].unique().tolist())>0:
                    guid_conta_mov = df_ordem['GuidContaMovimento'].iloc[i]
                    tit = Titularidade(guid_titularidade=df_ordem['GuidTitularidade'].iloc[i])
                prod_guid_temp = df_ordem['GuidProduto'].iloc[i]
                tipo_mov_temp = df_ordem['TipoMov'].iloc[i]
                try:
                    at = Ativo(prod_guid_temp)
                    datas = at.forca_data_mov(tipo_mov=tipo_mov_temp, forca_data_fin=data_liq)
                except:
                    if aceita_data_errada == True:
                        datas = Ativo(prod_guid_temp).ler_data(dia_mov=data_liq, tipo_mov=tipo_mov_temp)
                        datas = {
                            'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                            'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                            'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                        }
                    else:
                        mensagem = f'Não foi possível solicitar as movimentações para liquidar no dia {data_liq.strftime("%d/%m/%Y")}'
                        self.responde_email_solicitacao(id_sol=id_sol, texto=mensagem)
                        return ''
                    # raise Exception(f'Não foi possível encontrar datas de movimentação para a data de liquidação {data_liq.strftime("%Y-%m-%d")} e o ativo {prod_guid_temp}')
                    

                df_ordem.loc[df_ordem['GuidProduto']==prod_guid_temp, 'DataMov'] = datas['DataMov']
                df_ordem.loc[df_ordem['GuidProduto']==prod_guid_temp, 'DataCot'] = datas['DataCot']
                df_ordem.loc[df_ordem['GuidProduto']==prod_guid_temp, 'DataFin'] = datas['DataFin']
                if tipo_mov_temp == 'C':
                    if guid_conta_mov == None or guid_conta_mov == '':
                        contas = tit.conta_obj(prod_guid_temp, tipo_mov=tipo_mov_temp, conta_entrada_fin=guid_conta_escolhida, tipo_produto='COTAS')

                    else:
                        contas = tit.conta_obj(prod_guid_temp, tipo_mov=tipo_mov_temp, conta_entrada_fin=guid_conta_escolhida, conta_origem=guid_conta_mov, tipo_produto='COTAS')

                elif tipo_mov_temp == 'V':
                    if guid_conta_mov == None and guid_conta_mov == '':
                        contas = tit.conta_obj(prod_guid_temp, tipo_mov=tipo_mov_temp, conta_destino=guid_conta_escolhida)
                    else:
                        contas = tit.conta_obj(prod_guid_temp, tipo_mov=tipo_mov_temp, conta_destino=guid_conta_escolhida, conta_origem=guid_conta_mov)

                for key in contas.keys():
                    if key not in df_ordem.columns:
                        df_ordem[key] = np.nan
                    df_ordem.loc[i, key] = contas[key]

            df_ordem.rename(columns={
                'FinanceiroDem':'Financeiro',
                'QuantidadeDem':'Quantidade',
                'PrecoLimDem':'Preco',
                'GuidProduto':'AtivoGuid',
                'NomeProduto':'AtivoNome',

            },
            inplace=True)

            df_ordem['Contraparte'] = np.nan #TODO descobrir a contraparte dependendo do ativo
            df_ordem['ResgTot'] = sol_mov['ResgTotal']
            df_ordem['IdTipoOrdem'] = 1 
            df_ordem.loc[df_ordem['Preco'].notnull()] = 7
            df_ordem['Separar'] = False #TODO entender esse parâmetro
            df_ordem['IdSolMov'] = id_sol
            df_ordem['MotivoMov'] = sol.motivo_mov()
            # df_ordem['MotivoMov'] = 'teste automacao'
            # df_ordem['Financeiro'] = 10 
            df_ordem['Financeiro'] = abs(df_ordem['Financeiro'])
            if distrato == True:
                    df_ordem['ResgTot'] = True
            if len(df_ordem.loc[(df_ordem['ResgTot']==True)]) > 0:
                df_ordem_aux = df_ordem.merge(pos[['GuidProduto','TipoProduto','QuantidadeFinal']], left_on='AtivoGuid', right_on='GuidProduto')
                df_ordem_aux = df_ordem_aux.loc[df_ordem_aux['ResgTot']==True]
                df_ordem_aux = df_ordem_aux.loc[~df_ordem_aux['TipoProduto'].isin(['COTAS','FUNDO'])]

                if len(df_ordem_aux)>0:
                    for idx,row in df_ordem_aux.iterrows():
                        guid_produto = row['AtivoGuid']
                        quantidade = row['QuantidadeFinal']
                        df_ordem.loc[df_ordem['AtivoGuid']==guid_produto, 'Quantidade'] = quantidade

            df_ordem['IdSysDestino'] = 17
            
            if aceita_data_errada == False and distrato==False:
                for data_temp in df_ordem['DataFin'].unique().tolist():
                    if sol.data_pagamento() != data_temp:
                        raise Exception('HÁ RESGATES COM DATAS DIFERENTES DA DATA DE LIQUIDAÇÃO DESEJADA. AUTOMAÇÃO ABORTADA')

            resultado = self.__importar_ordens_por_portfolio__(tit, om_tit, df_ordem, id_sol)

        if marca_concluido_auto:
            if mensagem == None:
                msg = 'Feito. (Automação)'
            else:
                msg = mensagem
            self.marcacao_solicitacao(id_solicitacao=id_sol, concluido=True, quem=None, mensagem=msg, status='Concluído')
        if responde_email:
            df_ordem = df_ordem[[
                'Titularidade',
                'AtivoNome',
                'TipoMov',
                'Financeiro',
                'Quantidade',
                'Preco',
                'ResgTotal',
                'DataMov',
                'DataCot',
                'DataFin',
                'ContaMovimentoOrigem',
                'ContaMovimentoDestino',
                'ContaMovimento'
            ]]
            if responde_email == True:
                self.responde_email_solicitacao(id_sol=id_sol, sol=sol, enviar=True)
        return resultado

    def solicitacao_auto_fundos(self, id_sol:int, marca_concluido_auto:bool=True, responde_email:bool=True, sol=None):
        if sol == None:
            sol = Solicit(id_solicitacao=id_sol)
        sol_mov = sol.solmov
        guid_titularidade=sol.campo_aux(24)
        titularidade = sol.campo_aux(25)

        #define os guids_produto que devem entrar na demanda e não são enviados diretamente ao cockpit
        self.lista_guids_credito = Bawm().guids_ativos_demanda()

        #checamos se nas movimentações há apenas vendas, apenas compras ou os dois tipos de operações
        lista_tipos_mov = sol_mov['TipoMov'].tolist()

        if 'C' in lista_tipos_mov and 'V' in lista_tipos_mov:
            tipo_sol = 'CeV'
        elif 'V' in lista_tipos_mov:
            tipo_sol = 'V'
        else:
            tipo_sol = 'C'
        

        if sol_mov.empty:
            raise Exception("Para ordens de fundos é necessários especificar as movimentações a serem feitas")
        else:
            veiculo = sol_mov['Veiculo'].iloc[0]
            
            if veiculo == 'CartAdm' or veiculo=='PF':
                nome_sc = sol.nome_sc() 
                sc = Supercarteira(nome_supercarteira=nome_sc, data_pos=datetime.datetime.now())
                pos_sc = sc.pos
                if len(pos_sc['TitularidadeGuid'].unique()) == 1:
                    guid_titularidade = pos_sc['TitularidadeGuid'].iloc[0]
                else:
                    sol_mov_aux = sol_mov.loc[sol_mov['GuidContaMovimento'].notnull()]
                    if len(sol_mov_aux) == 0:
                        raise Exception('Não foi possível encontrar o GuidTitularidade')
                    else:
                        guid_conta_mov = sol_mov_aux['GuidContaMovimento'].iloc[0]
                        if guid_conta_mov != None and guid_conta_mov != '':
                            guid_titularidade = pos_sc['TitularidadeGuid'].loc[pos_sc['GuidContaMovimento']==guid_conta_mov].iloc[0]
                        else:
                            raise Exception('Não foi possível encontrar o GuidTitularidade')
                tit = Titularidade(guid_titularidade=guid_titularidade)
                df_final = self.gera_ordens_sol_pf(tit=tit, sol_mov=sol_mov, sol=sol, tipo_sol=tipo_sol, id_sol=id_sol)
            else:
                fundo = Fundo(veiculo)
                df_final = self.gera_ordens_sol_fundo(fundo=fundo, sol_mov=sol_mov, sol=sol, tipo_sol=tipo_sol, id_sol=id_sol)
            

            
          
        if len(df_final)>0:
            if veiculo == 'CartAdm' or veiculo=='PF':
                om_tit = OrderManager(id_tipo_portfolio=3, id_system_origem=61, carregar_dados=True)
                port = tit
            else:
                om_tit = OrderManager(id_tipo_portfolio=1, id_system_origem=61, carregar_dados=True)
                port = fundo

            resultado = self.__importar_ordens_por_portfolio__(port, om_tit, df_final, id_sol)  
        else:
            raise Exception("Não foi possível criar movimentações pela automação")
        if marca_concluido_auto:
            self.marcacao_solicitacao(id_solicitacao=id_sol, concluido=True, quem=None, mensagem='Feito. (Automação)', status='Concluído')
        if responde_email:
           
            self.responde_email_solicitacao(id_sol=id_sol, sol=sol)
    
    def gera_ordens_sol_pf(self, tit, sol_mov, sol, tipo_sol, id_sol):
        fdt = FuncoesDatas()
        pos = tit.pos.copy()
        titularidade = tit.nome
        guid_titularidade = tit.guid
        produto_liq_guid = tit.produto_liquidacao_guid
        pos['ProdLiq'] = False
        pos.loc[pos['GuidProduto'] == produto_liq_guid, 'ProdLiq'] = True
        data_liq = sol.data_pagamento()

       

        df_ordem = sol_mov[[
                'GuidContaMovimento',
                'GuidProduto',
                'NomeProduto',
                'FinanceiroDem',
                'QuantidadeDem', 
                'PrecoLimDem',
                'FinanceiroExec',
                'QuantidadeExec',
                'TipoMov',
                'ResgTotal'
            ]]
        df_ordem['Titularidade'] = titularidade
        df_ordem['GuidTitularidade'] = guid_titularidade
        df_ordem['AtivoCadastrado'] = True #TODO checar o cadastro
        df_ordem.loc[df_ordem['QuantidadeDem'].notnull(), 'QouF'] = 'Q'
        df_ordem.loc[df_ordem['QuantidadeDem'].notnull(), 'PreBoleta'] = True
        df_ordem.loc[df_ordem['QuantidadeDem'].isnull(), 'QouF'] = 'F'
        df_ordem.loc[df_ordem['QuantidadeDem'].isnull(), 'PreBoleta'] = False
        
        df_ordem['DataMov'] = None
        df_ordem['DataCot'] = None
        df_ordem['DataFin'] = None

        df_ordem.rename(columns={
                'FinanceiroDem':'Financeiro',
                'QuantidadeDem':'Quantidade',
                'PrecoLimDem':'Preco',
                'GuidProduto':'AtivoGuid',
                'NomeProduto':'AtivoNome',
                'ResgTotal':'ResgTot'

            },
            inplace=True)

        df_ordem['Contraparte'] = np.nan 
        df_ordem['IdTipoOrdem'] = 1 
        # df_ordem.loc[df_ordem['Preco'].notnull()] = 7
        df_ordem['Separar'] = False 
        df_ordem['IdSolMov'] = id_sol
        df_ordem['MotivoMov'] = sol.motivo_mov()
        df_ordem['AtivoGuid'] = df_ordem['AtivoGuid'].str.lower()
        # df_ordem['MotivoMov'] = 'teste automacao'
        # df_ordem['Financeiro'] = 10 
        df_ordem['Financeiro'] = abs(df_ordem['Financeiro'])

        for col in ['DataMov','DataCot','DataFin']:
            df_ordem[col] = None

        lista_dfs_final = []

        data_mov = datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time())

        

        if tipo_sol == 'V':
            # Em casos onde há apenas vendas, é só casar as vendas com aplicações no produto de liquidez
            # primeiro passamos por cada uma das linhas do dataframe e adicionamo colunas importantes, como as de datas e de contas



            for idx, row in df_ordem.iterrows():
                prod_guid_temp = row['AtivoGuid']
                tipo_mov_temp = row['TipoMov']
                try:
                    datas = Ativo(prod_guid_temp).forca_data_mov(tipo_mov=tipo_mov_temp, forca_data_fin=data_liq)
                except:
                    datas = Ativo(prod_guid_temp).ler_data(dia_mov=data_mov, tipo_mov=tipo_mov_temp)
                    datas = {
                        'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                        'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                        'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                    }

                

                df_ordem.loc[idx, 'DataMov'] = datas['DataMov']
                df_ordem.loc[idx, 'DataCot'] = datas['DataCot']
                df_ordem.loc[idx, 'DataFin'] = datas['DataFin']

                # if guid_conta_mov == None and guid_conta_mov == '':
                contas = tit.conta_obj(prod_guid_temp, tipo_mov='V')
                # else:
                #     contas = tit.conta_obj(prod_guid_temp, tipo_mov='V', conta_destino=guid_conta_mov)
                for key in contas.keys():
                    if key not in df_ordem.columns:
                        df_ordem[key] = np.nan
                    df_ordem.loc[df_ordem['AtivoGuid']==prod_guid_temp, key] = contas[key]

            # com as ordens de vendas feitas, é necessário fazer as de compras para zerar o caixa dessas movimentações
            # o parametro usado não será o de ordens individuais e sim por data de liquidação              
            
            #separa resgates totais de parciais para evitar o problema de pgto 90 10

            df_ordem_rt = df_ordem.loc[df_ordem['ResgTot']==True]
            df_ordem = df_ordem.loc[df_ordem['ResgTot']==False]

            for data_liq_temp in df_ordem['DataFin'].unique().tolist():
                financeiro_temp = df_ordem.loc[df_ordem['DataFin']==data_liq_temp, 'Financeiro'].sum()


                #copia uma linha do df_ordem para pegar outras informações e facilitar o preenchimento do df_liquidez

                ordem_liquidez = df_ordem.copy().head(1)
                ordem_liquidez['AtivoGuid'] = produto_liq_guid
                ordem_liquidez['Financeiro'] = financeiro_temp
                ordem_liquidez['FinanceiroExec'] = financeiro_temp
                ordem_liquidez['AtivoNome'] = pos.loc[pos['ProdLiq']==True, 'NomeProduto'].iloc[0]
                ordem_liquidez['TipoMov'] = 'C'
                ordem_liquidez['ResgTot'] = False
                

                datas = Ativo(produto_liq_guid).ler_data(dia_mov=data_liq_temp, tipo_mov='C')
                
                conta_destino_guid = df_ordem.loc[df_ordem['DataFin']==data_liq_temp, 'GuidContaMovimentoDestino'].values[0]

                datas = {
                        'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                        'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                        'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                    }
                contas = tit.conta_obj(produto_liq_guid, tipo_mov='C', conta_entrada_fin=conta_destino_guid)
                for key in contas.keys():
                    ordem_liquidez[key] = contas[key]
                for key in datas.keys():
                    ordem_liquidez[key] = datas[key]

                lista_dfs_final.append(ordem_liquidez)

            for data_liq_temp in df_ordem_rt['DataFin'].unique().tolist():
                financeiro_temp = (df_ordem_rt.loc[df_ordem_rt['DataFin']==data_liq_temp, 'Financeiro'].sum())
                ordem_liquidez = df_ordem_rt.copy().head(1)
                ordem_liquidez['AtivoGuid'] = produto_liq_guid
                ordem_liquidez['Financeiro'] = financeiro_temp*0.85
                ordem_liquidez['FinanceiroExec'] = financeiro_temp*0.85
                ordem_liquidez['AtivoNome'] = pos.loc[pos['ProdLiq']==True, 'NomeProduto'].iloc[0]
                ordem_liquidez['TipoMov'] = 'C'
                ordem_liquidez['ResgTot'] = False
                

                datas = Ativo(produto_liq_guid).ler_data(dia_mov=data_liq_temp, tipo_mov='C')
                
                conta_destino_guid = df_ordem_rt.loc[df_ordem_rt['DataFin']==data_liq_temp, 'GuidContaMovimentoDestino'].values[0]

                datas = {
                        'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                        'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                        'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                    }
                contas = tit.conta_obj(produto_liq_guid, tipo_mov='C', conta_entrada_fin=conta_destino_guid)
                for key in contas.keys():
                    ordem_liquidez[key] = contas[key]
                for key in datas.keys():
                    ordem_liquidez[key] = datas[key]

                lista_dfs_final.append(ordem_liquidez)

                ordem_liquidez = df_ordem_rt.copy().head(1)
                ordem_liquidez['AtivoGuid'] = produto_liq_guid
                ordem_liquidez['Financeiro'] = financeiro_temp*0.85
                ordem_liquidez['FinanceiroExec'] = financeiro_temp*0.85
                ordem_liquidez['AtivoNome'] = pos.loc[pos['ProdLiq']==True, 'NomeProduto'].iloc[0]
                ordem_liquidez['TipoMov'] = 'C'
                ordem_liquidez['ResgTot'] = False

                d1 = data_liq_temp + datetime.timedelta(days=1)
                d1 = fdt.verificar_du(d1, True)
                datas2 = Ativo(produto_liq_guid).ler_data(dia_mov=d1, tipo_mov='C')
                datas2 = {
                        'DataMov' : datetime.datetime.combine(datas2[0], datetime.datetime.min.time()),
                        'DataCot' : datetime.datetime.combine(datas2[1], datetime.datetime.min.time()),
                        'DataFin' : datetime.datetime.combine(datas2[2], datetime.datetime.min.time())
                    }
                for key in datas2.keys():
                    ordem_liquidez[key] = datas2[key]
                
                ordem_liquidez['Financeiro'] = financeiro_temp*0.1
                ordem_liquidez['FinanceiroExec'] = financeiro_temp*0.1

                lista_dfs_final.append(ordem_liquidez)

            df_t = pd.concat(lista_dfs_final)

            if len(df_ordem)>0:
                lista_dfs_final.append(df_ordem)    
            if len(df_ordem_rt)>0:
                lista_dfs_final.append(df_ordem_rt)

        # se há apenas compras, é necessário fazer o saldo. Além disso, se houver fundos de crédito, eles entram na demanda semanal
        # e não são enviados ao cockpit

        elif tipo_sol == 'C':
            df_ordem_sem_credito = df_ordem.loc[~df_ordem['AtivoGuid'].isin(self.lista_guids_credito)]
            df_ordem_credito = df_ordem.loc[df_ordem['AtivoGuid'].isin(self.lista_guids_credito)]
            
            if len(df_ordem_sem_credito) > 0:
                financeiro_compras = df_ordem_sem_credito['Financeiro'].sum()
                # faz o resgate do financeiro a ser usado nas compras do produto de liquidez
                # primeiro checa se o produto de liquidez tem financeiro suficiente para as compras

                financeiro_liquidez = pos.loc[pos['ProdLiq'] == True, 'FinanceiroFinal'].iloc[0]

                if financeiro_compras >= financeiro_liquidez:
                    raise Exception('Não há financeiro no produto de liquidez cadastrado suficiente para executar as compras')
                
                #para fazer a ordem de resgate do ativo de liquidez, copia as informações de alguma ordem e modifica alguns parametros, como guidproduto, financeiro, etc

                ordem_resg = df_ordem.head(1)
                ordem_resg['AtivoGuid'] = produto_liq_guid
                ordem_resg['Financeiro'] = financeiro_compras
                ordem_resg['AtivoNome'] = pos.loc[pos['ProdLiq']==True, 'NomeProduto'].iloc[0]
                ordem_resg['TipoMov'] = 'V'


                try:
                    datas = Ativo(produto_liq_guid).forca_data_mov(tipo_mov=tipo_mov_temp, forca_data_fin=data_mov)
                except:
                    datas = Ativo(produto_liq_guid).ler_data(dia_mov=data_mov, tipo_mov='V')
                    data_fin = datas[2]
                    datas = {
                        'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                        'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                        'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                    }
                    contas = tit.conta_obj(produto_liq_guid, tipo_mov='V')
                    for key in contas.keys():
                        ordem_resg[key] = contas[key]
                    for key in datas.keys():
                        ordem_resg[key] = datas[key]

                    conta_mov_destino = contas['GuidContaMovimentoDestino']
                for idx, row in df_ordem_sem_credito.iterrows():
                    prod_guid_temp = row['AtivoGuid']
                    datas = Ativo(prod_guid_temp).ler_data(dia_mov=data_fin, tipo_mov='C')
                    datas = {
                            'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                            'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                            'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                        }
                    
                    for key in datas.keys():
                        df_ordem_sem_credito[key] = datas[key]

                    contas = tit.conta_obj(prod_guid_temp, tipo_mov='C', conta_entrada_fin=conta_mov_destino)
                    for key in contas.keys():
                        df_ordem_sem_credito[key] = contas[key]
                    

                lista_dfs_final.append(ordem_resg)
                lista_dfs_final.append(df_ordem_sem_credito)

                
            
            if len(df_ordem_credito) > 0:
                df_ordem_credito['SysDestino'] = 69
                for idx, row in df_ordem_credito.iterrows():
                    prod_guid_temp = row['AtivoGuid']
                    contas = tit.conta_obj(prod_guid_temp, tipo_mov='C')
                    for key in contas.keys():
                        df_ordem_credito[key] = contas[key]

                    datas = {
                            'DataMov' : datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()),
                            'DataCot' : datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()),
                            'DataFin' : datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time())
                        }
                    
                    for key in datas.keys():
                        df_ordem_credito[key] = datas[key]
                
                

                lista_dfs_final.append(df_ordem_credito)


        
        else:

            df_compras = df_ordem.loc[df_ordem['TipoMov']=='C']
            df_vendas = df_ordem.loc[df_ordem['TipoMov']=='V']
            financeiro_compra = df_compras['Financeiro'].sum()
            financeiro_venda = df_vendas['Financeiro'].sum()

            df_compras_credito = df_compras.loc[df_compras['AtivoGuid'].isin(self.lista_guids_credito)]
            df_compras_outros = df_compras.loc[~df_compras['AtivoGuid'].isin(self.lista_guids_credito)]

            if (financeiro_compra / financeiro_venda) >= 0.99 and (financeiro_compra / financeiro_venda) <= 1.01:
                #se houver apenas uma ordem de resgate, provavelmente é a ordem de resgate de onde vem a liquidez para as compras
                if len(df_vendas) == 1:
                    if len(df_compras_credito) > 0:
                        compras_credito_financeiro = df_compras_credito['Financeiro'].sum()
                        df_vendas['Financeiro'] = df_vendas['Financeiro'] - compras_credito_financeiro
                    # else:
                    #     if df_vendas['ResgateTotal'].iloc[0] == True:

                    if df_vendas['Financeiro'].values[0] > 0:
                        prod_guid_temp = df_vendas['AtivoGuid'].iloc[0]
                        datas = Ativo(prod_guid_temp).ler_data(dia_mov=datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()), tipo_mov='V')

                        datas = {
                                'DataMov' : datas[0],
                                'DataCot' : datas[1],
                                'DataFin' : datas[2]
                            }
                        

                        data_fin = datas['DataFin']

                        for key in datas.keys():
                            df_vendas[key] = datas[key]

                        contas = tit.conta_obj(prod_guid_temp, tipo_mov='V')
                        conta_mov_destino = contas['GuidContaMovimentoDestino']

                        for key in contas.keys():
                            df_vendas[key] = contas[key]
                        
                        lista_dfs_final.append(df_vendas)
                    
                    else:
                        #mesmo que não haja boletas de vendas (há apenas compras de fundos de crédito), é necessário saber a conta entrada financeira
                        prod_guid_temp = df_vendas['AtivoGuid'].iloc[0]
                        contas = tit.conta_obj(prod_guid_temp, tipo_mov='V')
                        conta_mov_destino = contas['GuidContaMovimentoDestino']

                    for idx,row in df_compras_credito.iterrows():
                        prod_guid_temp = row['AtivoGuid']
                        contas = tit.conta_obj(prod_guid_temp, tipo_mov='C', conta_entrada_fin=conta_mov_destino)
                        for key in contas.keys():
                            df_compras_credito[key] = contas[key]

                        datas = {
                                'DataMov' : datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()),
                                'DataCot' : datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()),
                                'DataFin' : datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time())
                            }
                        
                        for key in datas.keys():
                            df_compras_credito[key] = datas[key]
                    if len(df_compras_credito)>0:
                        # df_compras_credito['SysDestino'] = 69
                        lista_dfs_final.append(df_compras_credito)

                    for idx, row in df_compras_outros.iterrows():
                        prod_guid_temp = row['AtivoGuid']
                        datas = Ativo(prod_guid_temp).ler_data(dia_mov=data_fin, tipo_mov='C')
                        datas = {
                                'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                                'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                                'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                            }
                        
                        for key in datas.keys():
                            df_compras_outros.loc[idx,key] = datas[key]

                        contas = tit.conta_obj(prod_guid_temp, tipo_mov='C', conta_entrada_fin=conta_mov_destino)
                        for key in contas.keys():
                            df_compras_outros.loc[idx,key] = contas[key]


                    if len(df_compras_outros)>0:
                        lista_dfs_final.append(df_compras_outros)

                #se houver apenas umas ordem de aplicação, provavelmente será a posição a ser comprada com o saldo das vendas
                elif len(df_compras) == 1:
                    if len(df_compras_credito) == 1:
                        raise Exception("Não é possível seguir com essa solicitação por não conseguirmos agendar fundos de crédito")
                    else:
                        for idx_v, row_v in df_vendas.iterrows():
                            prod_guid_temp = row_v['AtivoGuid']
                            datas = Ativo(prod_guid_temp).ler_data(dia_mov=datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()), tipo_mov='V')

                            datas = {
                                    'DataMov' : datas[0],
                                    'DataCot' : datas[1],
                                    'DataFin' : datas[2]
                                }

                            for key in datas.keys():
                                df_vendas.loc[idx_v,key] = datas[key]

                            contas = tit.conta_obj(prod_guid_temp, tipo_mov='V')

                            for key in contas.keys():
                                df_vendas.loc[idx_v, key] = contas[key]
                        
                        lista_dfs_final.append(df_vendas)

                        for data_liq in df_vendas['DataFin'].unique().tolist():
                            df_compra_temp = df_compras.copy()
                            df_venda_temp = df_vendas.loc[df_vendas['DataFin']==data_liq]
                            financeiro_compra_temp = math.floor(df_venda_temp['Financeiro'].sum())
                            df_compra_temp['Financeiro'] = financeiro_compra_temp
                            guid_conta_dest = df_venda_temp['GuidContaMovimentoDestino'].iloc[0]
                            prod_guid_compra = df_compras['AtivoGuid'].iloc[0]
                            datas = Ativo(prod_guid_compra).ler_data(dia_mov=datetime.datetime.combine(data_liq, datetime.datetime.min.time()), tipo_mov='V')

                            datas = {
                                    'DataMov' : datas[0],
                                    'DataCot' : datas[1],
                                    'DataFin' : datas[2]
                                }

                            for key in datas.keys():
                                df_compra_temp[key] = datas[key]
                            
                            contas = tit.conta_obj(prod_guid_compra, 'C', conta_entrada_fin=guid_conta_dest)

                            for key in contas.keys():
                                df_compra_temp[key] = contas[key]
                            
                            lista_dfs_final.append(df_compra_temp)



                # se houver mais de uma venda, devemos casar da melhor maneira com as compras
                else:
                    df_vendas_aux = df_vendas.copy()
                    df_vendas_aux['enviar'] = False
                    df_compras_aux = df_compras_outros.copy()
                    df_compras_aux['enviar'] = False
                    for idx, row in df_vendas_aux.iterrows():
                        financeiro_temp_vendas = row['Financeiro']
                        compra_temp = df_compras_aux.loc[(df_compras_aux['Financeiro'] == financeiro_temp_vendas) & (df_compras_aux['enviar'] == False)]
                        if len(compra_temp)>0:
                            df_vendas_aux.loc[idx, 'enviar'] = True
                            prod_guid_temp = df_vendas['AtivoGuid']
                            contas = tit.conta_obj(guid_ativo=prod_guid_temp, tipo_mov='V')
                            conta_liq = contas['GuidContaMovimentoDestino']
                            for key in contas.keys():
                                df_vendas_aux.loc[idx, key] = contas[key]
                            
                            datas = Ativo(prod_guid_temp).ler_data(dia_mov=datetime.now(), tipo_mov=tipo_mov_temp)
                            datas = {
                                'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                                'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                                'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                                 }
                            
                            data_fin = datas['DataFin']
                            
                            for key in contas.keys():
                                df_vendas_aux[key] = contas[key]
                            
                            #pode ser que tenha mais de uma compra com esse financeiro, por enquanto pegaremos a primeira que aparece, depois pensar em uma lógica melhor
                            compra_temp = compra_temp.head(1)
                            prod_guid_temp = compra_temp['AtivoGuid'].iloc[0]

                            datas = Ativo(prod_guid_temp).ler_data(dia_mov=data_liq, tipo_mov=tipo_mov_temp)
                            datas = {
                                'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                                'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                                'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                                 }
                            
                            contas = tit.conta_obj(guid_ativo=prod_guid_temp, tipo_mov='C', conta_entrada_fin=conta_liq)
                            
                            for key in contas.keys():
                                df_compras_aux.loc[(df_compras_aux['AtivoGuid']==prod_guid_temp) & (df_compras_aux['Financeiro']==financeiro_temp_vendas), key] = contas[key]

                            for key in datas.keys():
                                df_compras_aux.loc[(df_compras_aux['AtivoGuid']==prod_guid_temp) & (df_compras_aux['Financeiro']==financeiro_temp_vendas), key] = datas[key]

                            df_compras_aux.loc[(df_compras_aux['AtivoGuid']==prod_guid_temp) & (df_compras_aux['Financeiro']==financeiro_temp_vendas), 'enviar'] = True


                    if len(df_compras_aux.loc[df_compras_aux['enviar']==False])>0:
                        for idx,row in df_vendas_aux.loc[df_vendas_aux['enviar']==False].iterrows():
                            financeiro_venda = row['Financeiro']
                            prod_guid_temp = row['AtivoGuid']
                            datas = Ativo(prod_guid_temp).ler_data(dia_mov=datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()), tipo_mov='V')
                            datas = {
                                'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                                'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                                'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                                 }
                            
                            data_fin_resg = datas['DataFin']
                            contas = tit.conta_obj(guid_ativo=prod_guid_temp, tipo_mov='V')
                            conta_liq = contas['GuidContaMovimentoDestino']
                        
                            df_compras_temp = df_compras_aux[df_compras_aux['enviar']==False]
                            # df_compras_temp['Financeiro'] = (df_compras_temp['Financeiro'] / df_compras_temp['Financeiro'].sum()) * financeiro
                            df_compras_temp['enviar'] = True
                            
                            for idx2, row2 in df_compras_temp.iterrows():
                                compra_temp = df_compras_temp.loc[idx2]
                                prod_guid_temp = compra_temp['AtivoGuid']
                                financeiro_compra = compra_temp['Financeiro']

                                financeiro_compra_final = (financeiro_compra / df_compras_temp['Financeiro'].sum()) * financeiro_venda
                                financeiro_compra_final = math.floor(financeiro_compra_final)

                                compra_temp['Financeiro'] = financeiro_compra_final

                                datas = Ativo(prod_guid_temp).ler_data(dia_mov=data_fin_resg, tipo_mov='C')
                                datas = {
                                    'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                                    'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                                    'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                                    }
                                
                                contas = tit.conta_obj(prod_guid_temp, tipo_mov='C', conta_destino=conta_liq)

                                for key in datas.keys():
                                    compra_temp[key] = datas[key]
                                for key in contas.keys():
                                    compra_temp[key] = contas[key]

                                df_compras_aux = pd.concat([df_compras_aux, compra_temp])
                                df_compras_aux.drop_duplicates(inplace=True)
                                # df_compras_aux = df_compras_aux.append(compra_temp)
                    if len(df_compras_aux.loc[df_compras_aux['enviar']==True])>0:
                        df_compras_aux = df_compras_aux.loc[df_compras_aux['enviar']==True]
                        df_vendas_aux = df_vendas_aux.loc[df_vendas_aux['enviar']==True]

                        df_compras_aux.drop(columns='enviar', inplace=True)
                        df_vendas_aux.drop(columns='enviar', inplace=True)

                        lista_dfs_final.append(df_compras_aux)
                        lista_dfs_final.append(df_vendas_aux)

                    if len(lista_dfs_final) == 0:
                        #faz a ultima tentativa de gerar as ordens - usa o financeiro das vendas e faz as compras proporcionais a cada liquidação das vendas
                        for idx_, row_ in df_vendas.iterrows():
                            prod_guid_temp = row_['AtivoGuid']
                            datas = Ativo(prod_guid_temp).ler_data(dia_mov=datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()), tipo_mov='V')

                            datas = {
                                    'DataMov' : datas[0],
                                    'DataCot' : datas[1],
                                    'DataFin' : datas[2]
                                }
                            

                            data_fin = datas['DataFin']

                            for key in datas.keys():
                                df_vendas.loc[idx_, key] = datas[key]

                            contas = tit.conta_obj(prod_guid_temp, tipo_mov='V')
                            # conta_mov_destino = contas['GuidContaMovimentoDestino']

                            for key in contas.keys():
                                df_vendas.loc[idx_,key] = contas[key]
                            
                            financeiro_compras = df_compras['Financeiro'].sum()

                        if len(df_compras_outros)>0:
                            df_compras_outros['Percent_prop'] = df_compras_outros['Financeiro'] / financeiro_compras

                            for data_liq in df_vendas['DataFin'].unique():
                                df_vendas_temp = df_vendas.loc[df_vendas['DataFin']==data_liq]
                                financeiro_vendas_temp = df_vendas_temp['Financeiro'].sum()
                                conta_movimento = df_vendas_temp['GuidContaMovimentoDestino'].iloc[0]



                                for idx_,row_ in df_compras_outros.iterrows():
                                    financeiro_compra_temp = row_['Percent_prop'] * financeiro_vendas_temp
                                    df_compras_outros.loc[idx_, 'Financeiro'] = financeiro_compra_temp
                                    guid_compra_temp = row_['AtivoGuid']
                                    datas = Ativo(guid_compra_temp).ler_data(dia_mov=datetime.datetime.combine(data_liq, datetime.datetime.min.time()), tipo_mov='V')
                                    contas = tit.conta_obj(guid_compra_temp, tipo_mov='C', conta_entrada_fin=conta_movimento)
                                    
                                    datas = {
                                        'DataMov':datas[0],
                                        'DataCot':datas[1],
                                        'DataFin':datas[2]
                                    }

                                    for key in datas.keys():
                                        df_compras_outros.loc[idx_, key] = datas[key]
                                    for key in contas.keys():
                                        df_compras_outros.loc[idx_, key] = contas[key]
                                    
                                    lista_dfs_final.append(df_compras_outros.drop(columns=['Percent_prop']))
                          
        
        if len(lista_dfs_final) > 0:
            df_final = pd.concat(lista_dfs_final)
            
            # 13 de novembro - aparentemente eu já havia feito o codigo abaixo
            # lista_compras_novas = []
            # para evitar problemas relacionados a compras do financeiro estimado de regate total ou então pagamento 90 10, vamos dividir a compra em duas
            

            # if len(df_final.loc[df_final['ResgTot']==True])>0:
            #     datas_resg_tot = df_final.loc[df_final['ResgTot']==True, 'DataFin'].unique().tolist()
                
            #     #pega o financeiro de resg total e o usa para deixar uma aplicação no dia do resg total de 85% do valor e outra no du seguinte de 10% do valor
            #     # isso só funciona se tiver apenas uma compra por datas de RT
            #     for data_liq in datas_resg_tot:
            #         df_compras_data = df_final.loc[(df_final['DataFin']==data_liq) & (df_final['TipoMov']=='C')]
            #         if len(df_compras_data)>1:
            #             continue
            #         fin_venda = df_final.loc[(df_final['ResgTot']==True) & (df_final['DataFin']==data_liq) & (df_final['TipoMov']=='V'), 'Financeiro'].sum()
            #         fin_15 = fin_venda*0.15
            #         fin_10 = round(fin_venda*0.10,0)
                    
            #         fin_compra = df_compras_data['Financeiro'].iloc[0]

            #         df_final.loc[(df_final['DataFin']==data_liq) & (df_final['TipoMov']=='C'), 'Financeiro'] = round(fin_compra - fin_15, 0)
            #         df_final.loc[(df_final['DataFin']==data_liq) & (df_final['TipoMov']=='C'), 'FinanceiroExec'] = round(fin_compra - fin_15, 0)

            #         d1 = data_liq + datetime.timedelta(days=1)
            #         d1 = fdt.verificar_du(d1, True)
            #         df_compra_d1 = df_compras_data.copy()
            #         datas2 = Ativo(produto_liq_guid).ler_data(dia_mov=d1, tipo_mov='C')
            #         datas2 = {
            #                 'DataMov' : datetime.datetime.combine(datas2[0], datetime.datetime.min.time()),
            #                 'DataCot' : datetime.datetime.combine(datas2[1], datetime.datetime.min.time()),
            #                 'DataFin' : datetime.datetime.combine(datas2[2], datetime.datetime.min.time())
            #             }
            #         for key in datas2.keys():
            #             df_compra_d1[key] = datas2[key]

            #         df_compra_d1['Financeiro'] = fin_10
            #         df_compra_d1['FinanceiroExec'] = fin_10

            #         lista_compras_novas.append(df_compra_d1)
                
            #     if len(lista_compras_novas)>0:
            #         df_compras_novas = pd.concat(lista_compras_novas)
            #         df_final = pd.concat([df_final,df_compras_novas])
            #         df_final['DataFin'] = pd.to_datetime(df_final['DataFin'])
            #         df_final.sort_values(by='DataFin', inplace=True)

            return df_final      
        
        else:
            return []
    
    def gera_ordens_sol_fundo(self, fundo, sol_mov, sol, tipo_sol, id_sol):
        pos = fundo.pos.copy()
        produto_liq_guid = fundo.produto_liquidacao_guid
        pos['ProdLiq'] = False
        pos.loc[pos['GuidProduto'] == produto_liq_guid, 'ProdLiq'] = True
        nome = fundo.nome
        guid_portfolio = fundo.guid
        data_liq = sol.data_pagamento()

        #tira produtos de zeragem
        # sol_mov = sol_mov.loc[~sol_mov['GuidProduto'].isin(['5aa8e514-0fc2-dd11-9886-000f1f6a9c1c',
        #                                                     'f4a7e952-713a-e311-87ef-000c29cb7e20',
        #                                                     '8ba7e514-0fc2-dd11-9886-000f1f6a9c1c',
        #                                                     '838559d4-3f1f-ee11-8d7a-005056b17af5',
        #                                                     '3656d172-7ac5-df11-a957-d8d385b9752e'])]

        lista_tipos_mov = sol_mov['TipoMov'].tolist() 

        if 'C' in lista_tipos_mov and 'V' in lista_tipos_mov:
            tipo_sol = 'CeV'
        elif 'V' in lista_tipos_mov:
            tipo_sol = 'V'
        else:
            tipo_sol = 'C'
        

        df_ordem = sol_mov[[
                'GuidProduto',
                'NomeProduto',
                'FinanceiroFundoEst',
                'QuantidadeDem', 
                'PrecoLimDem',
                'FinanceiroExec',
                'QuantidadeExec',
                'TipoMov',
                'ResgTotal'
            ]]
        df_ordem['Portfolio'] = nome
        df_ordem['GuidPortfolio'] = guid_portfolio
        df_ordem['AtivoCadastrado'] = True #TODO checar o cadastro
        df_ordem.loc[df_ordem['QuantidadeDem'].notnull(), 'QouF'] = 'Q'
        df_ordem.loc[df_ordem['QuantidadeDem'].notnull(), 'PreBoleta'] = True
        df_ordem.loc[df_ordem['QuantidadeDem'].isnull(), 'QouF'] = 'F'
        df_ordem.loc[df_ordem['QuantidadeDem'].isnull(), 'PreBoleta'] = False
        
        df_ordem['DataMov'] = None
        df_ordem['DataCot'] = None
        df_ordem['DataFin'] = None

        df_ordem.rename(columns={
                'FinanceiroFundoEst':'Financeiro',
                'QuantidadeDem':'Quantidade',
                'PrecoLimDem':'Preco',
                'GuidProduto':'AtivoGuid',
                'NomeProduto':'AtivoNome',
                'ResgTotal':'ResgTot'

            },
            inplace=True)

        df_ordem['Contraparte'] = np.nan 
        df_ordem['IdTipoOrdem'] = 1 
        df_ordem['Separar'] = False 
        df_ordem['IdSolMov'] = id_sol
        motivo = sol.motivo_mov()
        if motivo != None and motivo != motivo:
            df_ordem['MotivoMov'] = sol.motivo_mov()
        else:
            motivo = 'Realocação'

        df_ordem['AtivoGuid'] = df_ordem['AtivoGuid'].str.lower()
        df_ordem['Financeiro'] = abs(df_ordem['Financeiro'])

        for col in ['DataMov','DataCot','DataFin']:
            df_ordem[col] = None

        lista_dfs_final = []

        data_mov = datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time())

        

        if tipo_sol == 'V':
            # Em casos onde há apenas vendas, é só casar as vendas com aplicações no produto de liquidez
            # primeiro passamos por cada uma das linhas do dataframe e adicionamo colunas importantes, como as de datas e de contas

            for idx, row in df_ordem.iterrows():
                prod_guid_temp = row['AtivoGuid']
                tipo_mov_temp = row['TipoMov']
                try:
                    datas = Ativo(prod_guid_temp).forca_data_mov(tipo_mov=tipo_mov_temp, forca_data_fin=data_liq)
                except:
                    datas = Ativo(prod_guid_temp).ler_data(dia_mov=data_mov, tipo_mov=tipo_mov_temp)
                    datas = {
                        'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                        'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                        'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                    }     

                df_ordem.loc[idx, 'DataMov'] = datas['DataMov']
                df_ordem.loc[idx, 'DataCot'] = datas['DataCot']
                df_ordem.loc[idx, 'DataFin'] = datas['DataFin']

         

            if len(df_ordem)>0:
                lista_dfs_final.append(df_ordem)    

        # se há apenas compras, é necessário fazer o saldo. Além disso, se houver fundos de crédito, eles entram na demanda semanal
        # e não são enviados ao cockpit

        elif tipo_sol == 'C':
            df_ordem_sem_credito = df_ordem.loc[~df_ordem['AtivoGuid'].isin(self.lista_guids_credito)]
            df_ordem_credito = df_ordem.loc[df_ordem['AtivoGuid'].isin(self.lista_guids_credito)]
            
            if len(df_ordem_sem_credito) > 0:
                financeiro_compras = df_ordem_sem_credito['Financeiro'].sum()
                # faz o resgate do financeiro a ser usado nas compras do produto de liquidez
                # primeiro checa se o produto de liquidez tem financeiro suficiente para as compras

                financeiro_liquidez = pos.loc[pos['ProdLiq'] == True, 'FinanceiroFinal'].iloc[0]

                if financeiro_compras >= financeiro_liquidez:
                    raise Exception('Não há financeiro no produto de liquidez cadastrado suficiente para executar as compras')
                
                #para fazer a ordem de resgate do ativo de liquidez, copia as informações de alguma ordem e modifica alguns parametros, como guidproduto, financeiro, etc


                for idx, row in df_ordem_sem_credito.iterrows():
                    prod_guid_temp = row['AtivoGuid']
                    datas = Ativo(prod_guid_temp).ler_data(dia_mov=data_mov, tipo_mov='C')
                    datas = {
                            'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                            'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                            'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                        }
                    
                    for key in datas.keys():
                        df_ordem_sem_credito[key] = datas[key]
                    

                lista_dfs_final.append(df_ordem_sem_credito)

                
            
            if len(df_ordem_credito) > 0:
                df_ordem_credito['SysDestino'] = 69
                for idx, row in df_ordem_credito.iterrows():
                    prod_guid_temp = row['AtivoGuid']

                    datas = {
                            'DataMov' : datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()),
                            'DataCot' : datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()),
                            'DataFin' : datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time())
                        }
                    
                    for key in datas.keys():
                        df_ordem_credito[key] = datas[key]
                
                

                lista_dfs_final.append(df_ordem_credito)


        
        else:
            df_compras = df_ordem.loc[df_ordem['TipoMov']=='C']
            df_vendas = df_ordem.loc[df_ordem['TipoMov']=='V']
            financeiro_compra = df_compras['Financeiro'].sum()
            financeiro_venda = df_vendas['Financeiro'].sum()

            df_compras_credito = df_compras.loc[df_compras['AtivoGuid'].isin(self.lista_guids_credito)]
            df_compras_outros = df_compras.loc[~df_compras['AtivoGuid'].isin(self.lista_guids_credito)]

            if (financeiro_compra / financeiro_venda) >= 0.99 and (financeiro_compra / financeiro_venda) <= 1.01:
                #se houver apenas uma ordem de resgate, provavelmente é a ordem de resgate de onde vem a liquidez para as compras
                if len(df_vendas) == 1:
                    if len(df_compras_credito) > 0:
                        compras_credito_financeiro = df_compras_credito['Financeiro'].sum()
                        df_vendas['Financeiro'] = df_vendas['Financeiro'] - compras_credito_financeiro
                    # else:
                    #     if df_vendas['ResgateTotal'].iloc[0] == True:

                    if df_vendas['Financeiro'].values[0] > 0:
                        prod_guid_temp = df_vendas['AtivoGuid'].iloc[0]
                        datas = Ativo(prod_guid_temp).ler_data(dia_mov=datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()), tipo_mov='V')

                        datas = {
                                'DataMov' : datas[0],
                                'DataCot' : datas[1],
                                'DataFin' : datas[2]
                            }
                        

                        data_fin = datas['DataFin']

                        for key in datas.keys():
                            df_vendas[key] = datas[key]
                        
                        lista_dfs_final.append(df_vendas)
                    
                    else:
                        #mesmo que não haja boletas de vendas (há apenas compras de fundos de crédito), é necessário saber a conta entrada financeira
                        prod_guid_temp = df_vendas['AtivoGuid'].iloc[0]

                    for idx,row in df_compras_credito.iterrows():
                        prod_guid_temp = row['AtivoGuid']

                        datas = {
                                'DataMov' : datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()),
                                'DataCot' : datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()),
                                'DataFin' : datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time())
                            }
                        
                        for key in datas.keys():
                            df_compras_credito[key] = datas[key]
                    if len(df_compras_credito)>0:
                        # df_compras_credito['SysDestino'] = 69
                        lista_dfs_final.append(df_compras_credito)

                    for idx, row in df_compras_outros.iterrows():
                        prod_guid_temp = row['AtivoGuid']
                        datas = Ativo(prod_guid_temp).ler_data(dia_mov=data_fin, tipo_mov='C')
                        datas = {
                                'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                                'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                                'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                            }
                        
                        for key in datas.keys():
                            df_compras_outros.loc[idx,key] = datas[key]


                    if len(df_compras_outros)>0:
                        lista_dfs_final.append(df_compras_outros)

                #se houver apenas umas ordem de aplicação, provavelmente será a posição a ser comprada com o saldo das vendas
                elif len(df_compras) == 1:
                    if len(df_compras_credito) == 1:
                        raise Exception("Não é possível seguir com essa solicitação por não conseguirmos agendar fundos de crédito")
                    else:
                        for idx_v, row_v in df_vendas.iterrows():
                            prod_guid_temp = row_v['AtivoGuid']
                            datas = Ativo(prod_guid_temp).ler_data(dia_mov=datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()), tipo_mov='V')

                            datas = {
                                    'DataMov' : datas[0],
                                    'DataCot' : datas[1],
                                    'DataFin' : datas[2]
                                }

                            for key in datas.keys():
                                df_vendas.loc[idx_v,key] = datas[key]
                        
                        lista_dfs_final.append(df_vendas)

                        for data_liq in df_vendas['DataFin'].unique().tolist():
                            df_compra_temp = df_compras.copy()
                            df_venda_temp = df_vendas.loc[df_vendas['DataFin']==data_liq]
                            financeiro_compra_temp = math.floor(df_venda_temp['Financeiro'].sum())
                            df_compra_temp['Financeiro'] = financeiro_compra_temp
                            # guid_conta_dest = df_venda_temp['GuidContaMovimentoDestino'].iloc[0]
                            prod_guid_compra = df_compras['AtivoGuid'].iloc[0]
                            datas = Ativo(prod_guid_compra).ler_data(dia_mov=datetime.datetime.combine(data_liq, datetime.datetime.min.time()), tipo_mov='V')

                            datas = {
                                    'DataMov' : datas[0],
                                    'DataCot' : datas[1],
                                    'DataFin' : datas[2]
                                }

                            for key in datas.keys():
                                df_compra_temp[key] = datas[key]
                            
                            
                            lista_dfs_final.append(df_compra_temp)



                # se houver mais de uma venda, devemos casar da melhor maneira com as compras
                else:
                    df_vendas_aux = df_vendas.copy()
                    df_vendas_aux['enviar'] = False
                    df_compras_aux = df_compras_outros.copy()
                    df_compras_aux['enviar'] = False
                    for idx, row in df_vendas_aux.iterrows():
                        financeiro_temp_vendas = row['Financeiro']
                        compra_temp = df_compras_aux.loc[(df_compras_aux['Financeiro'] == financeiro_temp_vendas) & (df_compras_aux['enviar'] == False)]
                        if len(compra_temp)>0:
                            df_vendas_aux.loc[idx, 'enviar'] = True
                            prod_guid_temp = df_vendas['AtivoGuid']
                            datas = Ativo(prod_guid_temp).ler_data(dia_mov=datetime.now(), tipo_mov=tipo_mov_temp)
                            datas = {
                                'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                                'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                                'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                                 }
                            
                            data_fin = datas['DataFin']
       
                            #pode ser que tenha mais de uma compra com esse financeiro, por enquanto pegaremos a primeira que aparece, depois pensar em uma lógica melhor
                            compra_temp = compra_temp.head(1)
                            prod_guid_temp = compra_temp['AtivoGuid'].iloc[0]

                            datas = Ativo(prod_guid_temp).ler_data(dia_mov=data_liq, tipo_mov=tipo_mov_temp)
                            datas = {
                                'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                                'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                                'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                                 }

                            for key in datas.keys():
                                df_compras_aux.loc[(df_compras_aux['AtivoGuid']==prod_guid_temp) & (df_compras_aux['Financeiro']==financeiro_temp_vendas), key] = datas[key]

                            df_compras_aux.loc[(df_compras_aux['AtivoGuid']==prod_guid_temp) & (df_compras_aux['Financeiro']==financeiro_temp_vendas), 'enviar'] = True


                    if len(df_compras_aux.loc[df_compras_aux['enviar']==False])>0:
                        for idx,row in df_vendas_aux.loc[df_vendas_aux['enviar']==False].iterrows():
                            financeiro_venda = row['Financeiro']
                            prod_guid_temp = row['AtivoGuid']
                            datas = Ativo(prod_guid_temp).ler_data(dia_mov=datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()), tipo_mov='V')
                            datas = {
                                'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                                'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                                'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                                 }
                            
                            data_fin_resg = datas['DataFin']
                        
                            df_compras_temp = df_compras_aux[df_compras_aux['enviar']==False]
                            # df_compras_temp['Financeiro'] = (df_compras_temp['Financeiro'] / df_compras_temp['Financeiro'].sum()) * financeiro
                            df_compras_temp['enviar'] = True
                            
                            for idx2, row2 in df_compras_temp.iterrows():
                                compra_temp = df_compras_temp.loc[idx2]
                                prod_guid_temp = compra_temp['AtivoGuid']
                                financeiro_compra = compra_temp['Financeiro']

                                financeiro_compra_final = (financeiro_compra / df_compras_temp['Financeiro'].sum()) * financeiro_venda
                                financeiro_compra_final = math.floor(financeiro_compra_final)

                                compra_temp['Financeiro'] = financeiro_compra_final

                                datas = Ativo(prod_guid_temp).ler_data(dia_mov=data_fin_resg, tipo_mov='C')
                                datas = {
                                    'DataMov' : datetime.datetime.combine(datas[0], datetime.datetime.min.time()),
                                    'DataCot' : datetime.datetime.combine(datas[1], datetime.datetime.min.time()),
                                    'DataFin' : datetime.datetime.combine(datas[2], datetime.datetime.min.time())
                                    }
                        
                                for key in datas.keys():
                                    compra_temp[key] = datas[key]

                                df_compras_aux = pd.concat([df_compras_aux, compra_temp])
                                df_compras_aux.drop_duplicates(inplace=True)
                                # df_compras_aux = df_compras_aux.append(compra_temp)
                    if len(df_compras_aux.loc[df_compras_aux['enviar']==True])>0:
                        df_compras_aux = df_compras_aux.loc[df_compras_aux['enviar']==True]
                        df_vendas_aux = df_vendas_aux.loc[df_vendas_aux['enviar']==True]

                        df_compras_aux.drop(columns='enviar', inplace=True)
                        df_vendas_aux.drop(columns='enviar', inplace=True)

                        lista_dfs_final.append(df_compras_aux)
                        lista_dfs_final.append(df_vendas_aux)

                    if len(lista_dfs_final) == 0:
                        #faz a ultima tentativa de gerar as ordens - usa o financeiro das vendas e faz as compras proporcionais a cada liquidação das vendas
                        for idx_, row_ in df_vendas.iterrows():
                            prod_guid_temp = row_['AtivoGuid']
                            datas = Ativo(prod_guid_temp).ler_data(dia_mov=datetime.datetime.combine(datetime.datetime.now(), datetime.datetime.min.time()), tipo_mov='V')

                            datas = {
                                    'DataMov' : datas[0],
                                    'DataCot' : datas[1],
                                    'DataFin' : datas[2]
                                }
                            

                            data_fin = datas['DataFin']

                            for key in datas.keys():
                                df_vendas.loc[idx_, key] = datas[key]
                            
                            financeiro_compras = df_compras['Financeiro'].sum()

                        if len(df_compras_outros)>0:
                            df_compras_outros['Percent_prop'] = df_compras_outros['Financeiro'] / financeiro_compras

                            for data_liq in df_vendas['DataFin'].unique():
                                df_vendas_temp = df_vendas.loc[df_vendas['DataFin']==data_liq]
                                financeiro_vendas_temp = df_vendas_temp['Financeiro'].sum()


                                for idx_,row_ in df_compras_outros.iterrows():
                                    financeiro_compra_temp = row_['Percent_prop'] * financeiro_vendas_temp
                                    df_compras_outros.loc[idx_, 'Financeiro'] = financeiro_compra_temp
                                    guid_compra_temp = row_['AtivoGuid']
                                    datas = Ativo(guid_compra_temp).ler_data(dia_mov=datetime.datetime.combine(data_liq, datetime.datetime.min.time()), tipo_mov='V')
        
                                    
                                    datas = {
                                        'DataMov':datas[0],
                                        'DataCot':datas[1],
                                        'DataFin':datas[2]
                                    }

                                    for key in datas.keys():
                                        df_compras_outros.loc[idx_, key] = datas[key]

                                    
                                    lista_dfs_final.append(df_compras_outros.drop(columns=['Percent_prop']))
                          
        
        if len(lista_dfs_final) > 0:
            df_final = pd.concat(lista_dfs_final)
            return df_final      
        
        else:
            return []

    def solicitacao_auto_bolsa(self, id_sol:int, marca_concluido_auto:bool=True, responde_email:bool=True, sol=None):
        if sol == None:
            sol = Solicit(id_solicitacao=id_sol)
        sol_mov = sol.solmov
        guid_titularidade=sol.campo_aux(24)
        # sc = sol.sc
        titularidade = sol.campo_aux(25)
        if sol_mov.empty:
            raise Exception('Para esse tipo de solicitação é necessário que tenha alguma movimentação')
        else:
            nome_sc = sol.nome_sc() 
            sc = Supercarteira(nome_supercarteira=nome_sc, data_pos=datetime.datetime.now())
            pos_sc = sc.pos
            guid_conta_mov = sol_mov['GuidContaMovimento'].iloc[0]
            if guid_conta_mov != None and guid_conta_mov != '':
                guid_titularidade = pos_sc['TitularidadeGuid'].loc[pos_sc['GuidContaMovimento']==guid_conta_mov].iloc[0]

            tit = Titularidade(guid_titularidade=guid_titularidade)
        try:
            pos = tit.pos.copy()
        except:
            #se não encontrar a titularidade da solicitação, pega a titularidade que está na sc, caso só tenha uma

            nome_sc = sol.nome_sc() 
            sc = Supercarteira(nome_supercarteira=nome_sc, data_pos=datetime.datetime.now())
            pos_sc = sc.pos
            titularidade_sc = pos_sc['Titularidade'].unique()
            if len(titularidade_sc)==1:
                tit = Titularidade(titularidade=titularidade_sc[0])
                pos = tit.pos.copy()
            else:
                raise Exception("Não foi encontrada posição para esssa titularidade")    
        
    def email_solicitacao_enquadradamento(self, id_sol:int, enviar:bool=False, sol=None):
        """
        Essa função encontrará a solicitação pelo seu id, buscará o email original na caixa e responderá com o texto salvo do enquadramento
        
        Args:
            id_sol (int): id da solicitação a ser respondida
            texto (str, optional): texto de resposta
            enviar (bool, optional): se é mail.display ou mail.send
        """
        if sol == None:
            sol = Solicit(id_solicitacao=id_sol)
            sol.envio_email_enquadramento()
            
    def responde_email_solicitacao(self, id_sol:int, texto:str='Movimentações solicitadas. <br> Obrigado.', enviar:bool=False, sol=None):

        """
        Essa função encontrará a solicitação pelo seu id, buscará o email original na caixa e responderá com o texto selecionado

        Args:
            id_sol (int): id da solicitação a ser respondida
            texto (str, optional): texto de resposta
            enviar (bool, optional): se é mail.display ou mail.send
        """
        if sol == None:
            sol = Solicit(id_solicitacao=id_sol)
        
        nome_sc = sol.nome_sc()
        data_pedido = sol.data_pedido()
        days_ago = datetime.datetime.now() - data_pedido
        days_ago = math.ceil(abs(days_ago.days))
        days_ago += 1
        data_str = data_pedido.strftime('%d/%b/%Y').lower()

        mes_converter = {
            'fev':'feb',
            'abr':'apr',
            'mai':'may',
            'ago':'aug',
            'set':'sep',
            'out':'oct',
            'dez':'dec'
        }
        mes_converter_inv = {v: k for k, v in mes_converter.items()}

        conver = False

        for key in mes_converter.keys():
            if key in data_str:
                data_str2 = data_str.replace(key,mes_converter[key])
                conver = True
                break
        
        if conver == False:
            for key in mes_converter_inv.keys():
                if key in data_str:
                    data_str2 = data_str.replace(key, mes_converter_inv[key])
                    conver = True
                    break
        
        if conver == False:
            data_str2 = data_str
        guid_sol = sol.guid_solicitacao

        assunto = f'[SolRealocacao] {nome_sc} - {data_str}: Aprovação do Enquadramento'
        assunto2 = f'[SolRealocacao] {nome_sc} - {data_str2}: Aprovação do Enquadramento'

        df_ordem = Bawm().infos_ordens_sol(id_sol)

        if len(df_ordem) > 0:
            df_ordens_status = Boletador().ordens_status(data=data_pedido, apenas_pendentes=False)[['IdOrdem','StatusNome']]
            df_ordens_status.rename(columns={'StatusNome':'Status'}, inplace=True)
            df_ordem = df_ordem.merge(df_ordens_status, on='IdOrdem', how='left')
            df_ordem['Financeiro'] = df_ordem['Financeiro'].apply(lambda x: self.converte_moeda(x))
            df_ordem.loc[df_ordem['ResgTot']==True, 'TipoMov'] = 'RESGATE TOTAL'
            df_ordem.loc[df_ordem['ResgTot']==True, 'Financeiro'] = 'TOTAL'
            df_ordem_cockpit = df_ordem.loc[df_ordem['IdSysDestino'].isin([17])]
            df_ordem_demanda = df_ordem.loc[df_ordem['IdSysDestino'].isin([19, 21, 69])]
            df_ordem_td = df_ordem.loc[df_ordem['IdSysDestino'].isin([18])]

            if len(df_ordem) == 1:
                texto = 'Movimentação solicitada'
            
            else:
                texto = 'Movimentações solicitadas'

            if len(df_ordem_cockpit)>0:
                df_ordem_cockpit = df_ordem_cockpit.drop(columns=['IdSysDestino','IdOrdem'])
                df_ordem_cockpit = build_table(df_ordem_cockpit, 'blue_light', font_size=12)
                texto += f':<br> {df_ordem_cockpit}'

            if len(df_ordem_demanda)>0:
                for column in ['IdSysDestino','IdOrdem', 'DataMov', 'DataCot', 'DataFin','Data Pedido', 'Data Cotização', 'Data Financeiro']:
                    try:
                        df_ordem_demanda = df_ordem_demanda.drop(columns=column)
                    except:
                        pass
                df_ordem_demanda = build_table(df_ordem_demanda, 'blue_light', font_size=12)
                texto += f'<br> Foram inseridas as seguintes demandas: {df_ordem_demanda}'
            
            if len(df_ordem_td)>0:
                df_ordem_td = df_ordem_td.drop(columns=['IdSysDestino','IdOrdem'])
                df_ordem_td = build_table(df_ordem_td, 'blue_light', font_size=12)
                texto += f'<br> Foram enviadas as seguintes ordens para Trading Desk: {df_ordem_td}'
            
            texto+='<br><br><i>Atenção: movimentações sujeitas a aprovação de DPM, Veto, enquadramento de suitability e verificação de documentação (termo e ficha cadastral).</i>'
        emailer = EmailLer(nome_sub_pasta='SolRealocacao')

        lista_assuntos = [
            f'[SolRealocacao] {nome_sc} - {data_str}: Aprovação do Enquadramento',
            f'[SolRealocacao] {nome_sc} - {data_str2}: Aprovação do Enquadramento',
            f'[SolRealocacao]{nome_sc} - {data_str}: Aprovação do Enquadramento',
            f'[SolRealocacao]{nome_sc} - {data_str2}: Aprovação do Enquadramento',
            f'[SolRealocacao][DPM] {nome_sc} - {data_str}: Aprovação do Enquadramento',
            f'[SolRealocacao][DPM] {nome_sc} - {data_str2}: Aprovação do Enquadramento',
            f'[SolRealocacao] [DPM]{nome_sc} - {data_str}: Aprovação do Enquadramento',
            f'[SolRealocacao] [DPM]{nome_sc} - {data_str2}: Aprovação do Enquadramento',
            f'[SolRealocacao] {nome_sc} - {data_str}',
            f'[SolRealocacao] {nome_sc} - {data_str2}',
            f'[SolRealocacao]{nome_sc} - {data_str}',
            f'[SolRealocacao]{nome_sc} - {data_str2}',
            f'[SolRealocacao][DPM] {nome_sc} - {data_str}',
            f'[SolRealocacao][DPM] {nome_sc} - {data_str2}',
            f'[SolRealocacao] [DPM]{nome_sc} - {data_str}',
            f'[SolRealocacao] [DPM]{nome_sc} - {data_str2}',
        ]
        
        for assunto in lista_assuntos:
            found = emailer.reply_email(text=texto, content_key_to_reply=guid_sol, subject_to_reply=assunto, send=enviar, days_ago=days_ago)
            if found == True:
                break
        if found == False:
            raise Exception("E-mail não encontrado")

    def converte_moeda(self, float_moeda):

        try:
            locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
            valor = locale.currency(float_moeda, grouping=True, symbol=None)
        except:
            valor = float_moeda
        
        return valor

    def executa_solicitacao_auto(self, id_sol):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            sol = Solicit(id_sol)

            id_tipo_solicitacao = sol.tipo_solicitacao()

            if id_tipo_solicitacao == 5 or id_tipo_solicitacao==4:
                self.solicitacao_auto_fundos(id_sol=id_sol, marca_concluido_auto=True, responde_email=True, sol=sol)
            elif id_tipo_solicitacao == 7 or id_tipo_solicitacao==8:
                self.solicitacao_auto_liquidez_e_distrato(id_sol=id_sol, forca_produto_liq=True, aceita_data_errada=False, marca_concluido_auto=True, responde_email=True, sol=sol)
            elif id_tipo_solicitacao == 9:
                self.solicitacao_auto_liquidez_e_distrato(id_sol=id_sol, forca_produto_liq=True, aceita_data_errada=True, marca_concluido_auto=True, responde_email=True, sol=sol, distrato=True)
            else:
                raise Exception('Tipo de solicitação inválida para a automação')

    
if __name__ == '__main__':
    ps = ProcSolicitacoes()    
    # ps.executa_solicitacao_auto(15072)
    # ps.solicitacao_auto_liquidez_e_distrato(9905, mensagem='teste')
    # ps.analisar_solicitacoes_inicial()
    # ps.analise_inicial(14730)
    # ps.responde_email_solicitacao(11583)
    # ps.executa_solicitacao_auto(14867)
    ps.executa_solicitacao_auto(17317)

    # ps.responde_email_solicitacao(id_sol=7745)
    # sol = Solicit(7712)
    # sol.responde_email_analise_enquadramento(texto='teste', send=False, subject='testee')
    # ps.cancelamento_ordens_sol_devol(11127)
    # ps.marcacao_solicitacao(id_solicitacao=11127, concluido=False, status='Devolvido', quem=None, mensagem='Cliente chato', devolvida=True)
    print(datetime.datetime.now())
    #ps.executar_lista_solicitacoes(tipo_lista=4, nome_planilha='ExecucaoSolicitacoes.xlsm')
    # ps.solicitacao_boletagem(11809, nome_planilha='ExecucaoSolicitacoes.xlsm')
    # ps.solicitacao_boletagem_supercarteira('AcaciaGPS', nome_planilha='ExecucaoSolicitacoes.xlsm')
    # ps.solicitacao_boletagem_supercarteira(guid_sc='b55ca166-d173-e811-8a09-005056912b96',lista_ativos_adicionais=['410837a5-5125-e311-ba3b-000c29cb7e20','5dbcce90-0ec2-dd11-9886-000f1f6a9c1c'], nome_planilha='ExecucaoSolicitacoes.xlsm')
    # ps.solicitacao_boletagem_montar(nome_planilha='ExecucaoSolicitacoes.xlsm',executar_pre_trade=True)
    print(datetime.datetime.now())
    # ps.solicitacao_planilha_veic(id_solicitacao=7427, nome_planilha='ExecucaoSolicitacoes.xlsm')
    # ps.solicitacao_movs_pedidas(id_solicitacao=975, nome_sc='AcaciaGPS', nome_planilha='ExecucaoSolicitacoes.xlsm')
    # ps.solicitacao_alterar_movs(105, [{'Veiculo': '_Amazônia Verde FIM CP IE','GuidContaMovimento': 'Nulo','NomeContaMovimento': 'Nulo','FinanceiroExec': 1450000}],[{'IdSolMov': 396}])
    # ps.solicitacao_email_ordens_atualizadas(105, 'Foi preciso fazer algumas movs na física')    
    # ps.solicitacao_boletagem(id_solicitacao=9843, nome_planilha='ExecucaoSolicitacoes.xlsm')
    
    # ps.solicitacao_boletagem_importar(nome_planilha='ExecucaoSolicitacoes.xlsm')
  
    print('ok')