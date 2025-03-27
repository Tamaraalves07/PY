import pandas as pd
import xlwings as xw
from databases import Bawm, BDS


class Fundos:
    
    def __init__(self, homologacao=False):
        self.bawm = Bawm(homologacao=homologacao)
        self.bds = BDS(homologacao=homologacao)
            
    def busca_cadastro(self, texto_busca, codfam_fonte=599, nome_planilha=None):
        if nome_planilha:
            wb = xw.Book(nome_planilha)
        else:
            wb = xw.Book.caller()
        
        df = self.bds.fundos_busca_serie(texto_busca=texto_busca, codfam=codfam_fonte).set_index('idser')
        aba = wb.sheets('Fundos')                
        aba.range('Fd_LinIni').value = df
    