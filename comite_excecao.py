# -*- coding: utf-8 -*-
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

class email:

    def identifica_email(self,data_base):
        outlook = win32.Dispatch('outlook.application')
        mapi = outlook.GetNamespace("MAPI")
        inbox = mapi.GetDefaultFolder(6).Folders['Implementacao']
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
            if 'Análise de ativos | Sírio' in assunto:
                print(data_base, message_dt)
                if data_base == message_dt:
                    print('entrei')
                    attachments = message.Attachments
                    for att in attachments:
                        att_name = str(att).lower()
                        print(att_name)
                        if '.zip' in att_name:
                                att.SaveASFile(f'O:/SAO/CICH_All/Investment Solutions/11. Comitê Aprovação Ativos/Análises Iniciais - Ativos/{assunto}-{data_base}.xlsx')
            
                        else:
                            print('não entrei')
                        return achei_arquivo
                        
                        
#Rodar a função acima
today = datetime.today()
tem_arq = email().identifica_email(today)
