import win32com.client as client
import time
from PIL import ImageGrab
from datetime import date, datetime, timedelta
import pandas as pd
import re

outlook = client.Dispatch("Outlook.Application")


class Email:
    def __init__(self, to, subject, text, cc=None, bcc=None, send=False, attachments=None, hiperlink = None, list_path_figures=None, send_from=None, xlwings_range_to_pic=None, xlwings_sheet_imagem=None, retry=100, interval_seconds=20):
        """
        :param text: has to be HTML
        :param send: if `False` the code will display the email at the end so that the user can send
        :param attachments: either string or list of strings
        """
        try:
            outlook = client.Dispatch("Outlook.Application")
        except:
            pass
        if xlwings_range_to_pic and xlwings_sheet_imagem:
            xlwings_range_to_pic.api.CopyPicture(Appearance=2)
            xlwings_sheet_imagem.api.Paste()
            pic=xlwings_sheet_imagem.pictures[0]
            pic.api.Copy()
            img = ImageGrab.grabclipboard()
            img.save('C:/Temp/Foto.png')
            pic.delete()
            list_path_figures = 'C:/Temp/Foto.png'

        mail = self.create_email(outlook, to, subject, text, cc, bcc, send, attachments, list_path_figures, send_from,hiperlink)

        if send:
            for i in range(0, retry):
                while True:
                    try:
                        print("[Emailer]: Sending mail \"{subject}\" to {email}".format(subject=subject, email=to))
                        mail.Display()
                        mail.Send()
                        break
                    except Exception as e:
                        print(e)
                        time.sleep(interval_seconds)
                        outlook = client.Dispatch("Outlook.Application")
                        mail = self.create_email(outlook, to, subject, text, cc, bcc, send, attachments, )
                        continue
                break
        else:
            mail.Display()

    def create_email(self, outlook, to, subject, text, cc, bcc, send, attachments, list_path_figures, send_from, hiperlink):
        mail = outlook.CreateItem(0)

        mail.To = self.join_if_list(to)

        mail.Subject = subject
        if cc is not None:
            mail.CC = self.join_if_list(cc)
        if bcc is not None:
            mail.BCC = self.join_if_list(bcc)
            

        if attachments is not None:
            if not isinstance(attachments, list):
                attachments = [attachments]

            for attachment in attachments:
                mail.Attachments.Add(attachment)
                
        if list_path_figures is not None:
            if type(list_path_figures) == str:
                list_path_figures = [list_path_figures]
                
            all_fig_html = '<hr /><>br />'
            for i,pfig in enumerate(list_path_figures):
                attachment = mail.Attachments.Add(pfig)
                attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", f'Fig{i}')
                all_fig_html = all_fig_html + f'<img src="cid:Fig{i}"> <br /><br />'
                
            text = text + all_fig_html
            
        if hiperlink is not None:
            text = text + f'<a href={hiperlink}>{hiperlink}<a/>'  
            
        if not send_from is None:
            mail.SentOnBehalfOfName = send_from

        mail.GetInspector
        index = mail.HTMLbody.find('>', mail.HTMLbody.find('<body'))
        mail.HTMLbody = mail.HTMLbody[:index + 1] + text + mail.HTMLbody[index + 1:]
        return mail

    def join_if_list(self, obj, join_by=';'):
        if isinstance(obj, list):
            return join_by.join(obj)
        else:
            return obj
   

class EmailLer:
    
    def __init__(self, nome_sub_pasta, numero_pasta_outlook=6):
        self.numero_pasta_outlook = numero_pasta_outlook
        self.nome_sub_pasta = nome_sub_pasta
    
    def busca_anexo(self, assunto_busca, data_base, pasta_destino, nome_destino, extensao_arquivo='.txt', nome_anexo= None, case_sensitive=False):
        outlook = client.Dispatch('outlook.application')
        mapi = outlook.GetNamespace("MAPI")
        try:
            inbox = mapi.GetDefaultFolder(self.numero_pasta_outlook).Folders[self.nome_sub_pasta]
        except:
            raise Exception(f'EmailLer #01: Pasta {self.nome_sub_pasta} não encontrada como subpasta do Inbox.')
        messages = inbox.Items
        print('Processando ', len(messages), 'emails')           

        achei_arquivo = False
        lista_salvos = []
        
        busca = assunto_busca
        if not case_sensitive:
            busca = busca.lower()
        
        for message in messages:
            message_dt = message.senton.date()
            print('Data do email:', message_dt, type(message_dt))
            assunto = message.subject   # [0:12]
            if not case_sensitive:
                assunto = assunto.lower()
            if busca in assunto:
                print(data_base, message_dt)
                if pd.to_datetime(data_base) == pd.to_datetime(message_dt):
                    print('entrei')
                    attachments = message.Attachments
                    encontrados = 0
                    for att in attachments:
                        if not case_sensitive:
                            att_name = str(att).lower()
                        else:
                            att_name = str(att)
                        if extensao_arquivo in att_name:
                            print(att_name)
                            if nome_anexo:
                                if re.search(nome_anexo, att_name):
                                    teste = True
                                else:
                                    teste = False                                
                            else:
                                teste = True
                            if teste:
                                encontrados += 1
                                txt = ''
                                if encontrados > 1:
                                    txt = f'_{str(encontrados)}'
                                nome = f'{pasta_destino}{nome_destino}{txt}{extensao_arquivo}'
                                att.SaveASFile(nome)
                                lista_salvos.append(nome)
                                achei_arquivo = True
                                print(attachments)
                            
        return lista_salvos
        
    def busca_anexo_xlsx(self, assunto_busca, data_base, pasta_destino, nome_destino,nome_anexo= None, case_sensitive=False):
        return self.busca_anexo(assunto_busca=assunto_busca, data_base=data_base, pasta_destino=pasta_destino, 
                                nome_destino=nome_destino, extensao_arquivo='.xlsx', case_sensitive=case_sensitive,nome_anexo=nome_anexo)
    
    def busca_anexo_zip(self, assunto_busca, data_base, pasta_destino, nome_destino,nome_anexo= None, case_sensitive=False):
        return self.busca_anexo(assunto_busca=assunto_busca, data_base=data_base, pasta_destino=pasta_destino, 
                                nome_destino=nome_destino, extensao_arquivo='.zip', case_sensitive=case_sensitive,nome_anexo=nome_anexo)
    
    def reply_email(self,  subject_to_reply=None, content_key_to_reply=None, text=None,  attachments=None, send_from=None, send:bool=False, days_ago:int=4, assunto_resp:str=None):
        """
        Essa função acha o email na pasta declarada na classe pelo assunto, pelo conteúdo do e-mail ou por ambos e responde com o texto selecionado

        Args:
            subject_to_reply (_type_, optional): Assunto do e-mail a ser respondido. Defaults to None.
            content_key_to_reply (_type_, optional): Conteúdo a ser encontrado no e-mail a ser respondido. Defaults to None.
            text (_type_, optional): Texto da resposta. Defaults to None.
            attachments (_type_, optional): Anexos a serem inseridos na resposta. Defaults to None.
            send_from (_type_, optional): E-mail selecionado para a resposta. Defaults to None.
            send (bool, optional): Se verdadeiro, mail.Send() se não, mail.Display(). Defaults to False.

        Raises:
            Exception: _description_
        """
        outlook = client.Dispatch("Outlook.Application").GetNameSpace("MAPI")
        if subject_to_reply == None and content_key_to_reply == None:
            raise Exception('É necessário especificar ou o assunto, o conteúdo do e-mail ou ambos para responder')

        if self.nome_sub_pasta != None:
            inbox_itens = outlook.GetDefaultFolder(self.numero_pasta_outlook).Folders[self.nome_sub_pasta].Items.Restrict(
                f"[ReceivedTime]>= '{(datetime.today() - timedelta(days=days_ago)).strftime('%d/%m/%Y %H:%M%p')}'"
            )
            # inbox_itens = outlook.GetDefaultFolder(self.numero_pasta_outlook).Folders[self.nome_sub_pasta]
        found = False
        for message in inbox_itens:
            try:
                assunto_temp = message.Subject
                conteudo = message.Body
            except:
                continue
            if message.Class == 43:
                if subject_to_reply != None:
                    if assunto_temp.lower() != subject_to_reply.lower():
                        continue
                if content_key_to_reply != None:
                    if content_key_to_reply not in conteudo:
                        continue
                found = True
                mail = message.ReplyAll()
                mail.GetInspector 
                if attachments is not None:
                    if not isinstance(attachments, list):
                        attachments = [attachments]

                    for attachment in attachments:
                        mail.Attachments.Add(attachment)

                if not send_from is None:
                    mail.SentOnBehalfOfName = send_from
                rgb_code_color = (68, 84, 106)
                html_color = "#{:02x}{:02x}{:02x}".format(*rgb_code_color)
                text = f'<p style="color: {html_color}; font-family:Arial, sans-serif; font-size:10pt;"> {text} </p>'
                index = mail.HTMLbody.find('>', mail.HTMLbody.find('<body'))
                mail.HTMLbody = mail.HTMLbody[:index + 1] + text + mail.HTMLbody[index + 1:]

                if assunto_resp == None:
                    assunto = f'RE: {assunto_temp}'
                else:
                    assunto = assunto_resp
                
                mail.Subject = assunto

                if send == False:
                    mail.Display()
                else:
                    mail.Display()
                    mail.Send()

        return found
   
if __name__ == '__main__':
    #Email(to='teste@teste.com; ', subject='Teste conta', text='oi')  # send_from='teste@teste.com'
    EmailLer('Ordem JBFO').reply_email('Teste','#Teste1243','teste', send=True)
    # ler = EmailLer(nome_sub_pasta='BMF')        
    # ler.reply_email(content_key_to_reply='Giovanni',text='Teste <br> teste', days_ago=1000, assunto_resp='TEstinhoooo')
    # lista = ler.busca_anexo('ZERAGEM GPS', date.today(), 'C:/Temp/Zeragem/', 'zeragem',nome_anexo='zeragem.caixa.jbfo', extensao_arquivo ='.xlsx')
