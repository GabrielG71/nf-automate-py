import pandas as pd
import PyPDF2
import re
import os
from typing import Dict

class ExtratorNFePDF:
    def __init__(self):
        self.dados_extraidos = []
    
    def extrair_dados_pdf(self, nome_arquivo: str) -> Dict[str, any]:
        try:
            with open(nome_arquivo, 'rb') as arquivo:
                leitor_pdf = PyPDF2.PdfReader(arquivo)
                texto_completo = "".join(pagina.extract_text() + "\n" for pagina in leitor_pdf.pages)
            
            return {
                'razao_social_emitente': self._extrair_razao_social_emitente(texto_completo),
                'cnpj_emitente': self._extrair_cnpj_emitente(texto_completo),
                'razao_social_destinatario': self._extrair_razao_social_destinatario(texto_completo),
                'cnpj_destinatario': self._extrair_cnpj_destinatario(texto_completo),
                'numero_nf': self._extrair_numero_nf(texto_completo),
                'data_nf': self._extrair_data_nf(texto_completo),
                'valor_total': self._extrair_valor_total(texto_completo),
                'arquivo': nome_arquivo
            }
            
        except Exception as e:
            return {
                'razao_social_emitente': '', 'cnpj_emitente': '', 'razao_social_destinatario': '',
                'cnpj_destinatario': '', 'numero_nf': '', 'data_nf': '', 'valor_total': 0.0,
                'arquivo': nome_arquivo, 'erro': str(e)
            }
    
    def _extrair_razao_social_emitente(self, texto: str) -> str:
        padrao_danfe = r'(?:DANFE|IDENTIFICAÇAO DO EMITENTE)\s*\n\s*(.*?)(?:\n|\d{2}[.\s]?\d{3}[.\s]?\d{3})'
        match_danfe = re.search(padrao_danfe, texto, re.DOTALL | re.IGNORECASE)
        if match_danfe:
            candidato = self._limpar_nome_empresa(match_danfe.group(1))
            if self._validar_nome_empresa(candidato):
                return candidato
        
        # Tentar extrair a razão social do emitente usando o padrão de CNPJ
        linhas = texto.split('\n')
        for i, linha in enumerate(linhas):
            if re.search(r'\d{2}[.\s]?\d{3}[.\s]?\d{3}[/\s]?\d{4}[-\s]?\d{2}', linha):
                # Encontrou um CNPJ, tentar pegar a linha anterior como razão social
                if i > 0:
                    candidato = self._limpar_nome_empresa(linhas[i-1])
                    if self._validar_nome_empresa(candidato):
                        return candidato
        
        # Padrões adicionais para empresas
        padroes_empresa = [
            r'([A-ZÁÊÀÎÔÇ][A-ZÁÊÀÎÔÇ\s]+(?:LTDA|S\.?A\.?|ME|EPP|EIRELI))',
            r'([A-ZÁÊÀÎÔÇ][A-ZÁÊÀÎÔÇ\s]+(?:COMERCIO|INDUSTRIA|SERVICOS))',
            r'([A-ZÁÊÀÎÔÇ\s]{15,}?)(?=\s*\d{2}[.\s]?\d{3})',
        ]
        
        for padrao in padroes_empresa:
            for match in re.findall(padrao, texto, re.IGNORECASE):
                candidato = self._limpar_nome_empresa(match)
                if self._validar_nome_empresa(candidato):
                    return candidato
        return ""    
    def _limpar_nome_empresa(self, texto: str) -> str:
        if not texto:
            return ""
        nome = re.sub(r'[^\w\s]', ' ', texto)
        nome = re.sub(r'\b\d+\b', ' ', nome)
        nome = re.sub(r'\s+', ' ', nome)
        return nome.strip().upper()
    
    def _validar_nome_empresa(self, nome: str) -> bool:
        if not nome or len(nome) < 10:
            return False
        palavras = nome.split()
        if len(palavras) < 2:
            return False
        numeros = re.findall(r'\d', nome)
        if len(numeros) > len(nome) * 0.3:
            return False
        palavras_validas = [p for p in palavras if len(p) >= 3]
        return len(palavras_validas) >= 2
    
    def _extrair_cnpj_emitente(self, texto: str) -> str:
        matches = re.findall(r'(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2})', texto)
        if matches:
            cnpj_limpo = re.sub(r'[^\d]', '', matches[0])
            if len(cnpj_limpo) == 14:
                return f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:14]}"
        return ""
    
    def _extrair_razao_social_destinatario(self, texto: str) -> str:
        cnpjs = re.findall(r'\d{2}[.\s]?\d{3}[.\s]?\d{3}[/\s]?\d{4}[-\s]?\d{2}', texto)
        if len(cnpjs) > 1:
            segundo_cnpj = cnpjs[1]
            for padrao in [rf'([A-ZÁÊÀÎÔÇ][^\n\d]{{10,}}?)\s*{re.escape(segundo_cnpj)}',
                          rf'{re.escape(segundo_cnpj)}\s*([A-ZÁÊÀÎÔÇ][^\n\d]{{10,}})']:
                match = re.search(padrao, texto, re.IGNORECASE)
                if match:
                    candidato = self._limpar_nome_empresa(match.group(1))
                    if self._validar_nome_empresa(candidato):
                        return candidato
        
        for padrao in [r'DESTINAT[AÁ]RIO[^A-Z]*([A-ZÁÊÀÎÔÇ][^\n\d]{10,})',
                      r'DEST[^A-Z]*([A-ZÁÊÀÎÔÇ][^\n\d]{10,})']:
            match = re.search(padrao, texto, re.IGNORECASE)
            if match:
                candidato = self._limpar_nome_empresa(match.group(1))
                if self._validar_nome_empresa(candidato):
                    return candidato
        
        empresas_encontradas = []
        for padrao in [r'([A-ZÁÊÀÎÔÇ][A-ZÁÊÀÎÔÇ\s]+(?:LTDA|S\.?A\.?|ME|EPP|EIRELI))',
                      r'([A-ZÁÊÀÎÔÇ][A-ZÁÊÀÎÔÇ\s]+(?:COMERCIO|INDUSTRIA|SERVICOS))']:
            for match in re.findall(padrao, texto, re.IGNORECASE):
                candidato = self._limpar_nome_empresa(match)
                if self._validar_nome_empresa(candidato) and candidato not in empresas_encontradas:
                    empresas_encontradas.append(candidato)
        
        return empresas_encontradas[1] if len(empresas_encontradas) > 1 else ""
    
    def _extrair_cnpj_destinatario(self, texto: str) -> str:
        matches = re.findall(r'(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2})', texto)
        if len(matches) > 1:
            cnpj_limpo = re.sub(r'[^\d]', '', matches[1])
            if len(cnpj_limpo) == 14:
                return f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:14]}"
        return ""
    
    def _extrair_numero_nf(self, texto: str) -> str:
        for padrao in [r'N\s+(\d{3}\.\d{3}\.\d{3})', r'NF-e\s*N[ºo°]?\s*(\d+)',
                      r'NÚMERO\s*:?\s*(\d+)', r'N[ºo°]\s*(\d+)']:
            match = re.search(padrao, texto, re.IGNORECASE)
            if match:
                numero = match.group(1)
                return numero.replace('.', '') if '.' in numero else numero
        return ""
    
    def _extrair_data_nf(self, texto: str) -> str:
        for padrao in [r'(\d{2}/\d{2}/\d{4})\s+\d{2}:\d{2}',
                      r'(?:EMISS[AÃ]O|DATA)[^0-9]*(\d{2}/\d{2}/\d{4})',
                      r'(\d{2}/\d{2}/\d{4})', r'(\d{4}-\d{2}-\d{2})', r'(\d{2}-\d{2}-\d{4})']:
            for match in re.findall(padrao, texto, re.IGNORECASE):
                data_formatada = self._formatar_data(match)
                if data_formatada and self._validar_data(data_formatada):
                    return data_formatada
        
        for match in re.findall(r'(\d{8})', texto):
            if len(match) == 8:
                data_formatada = f"{match[:2]}/{match[2:4]}/{match[4:]}"
                if self._validar_data(data_formatada):
                    return data_formatada
        return ""
    
    def _formatar_data(self, data: str) -> str:
        if '-' in data:
            partes = data.split('-')
            if len(partes) == 3:
                return f"{partes[2]}/{partes[1]}/{partes[0]}" if len(partes[0]) == 4 else f"{partes[0]}/{partes[1]}/{partes[2]}"
        return data
    
    def _validar_data(self, data: str) -> bool:
        try:
            if '/' not in data:
                return False
            partes = data.split('/')
            if len(partes) != 3:
                return False
            dia, mes, ano = int(partes[0]), int(partes[1]), int(partes[2])
            if not (2000 <= ano <= 2030) or not (1 <= mes <= 12) or not (1 <= dia <= 31):
                return False
            if mes in [4, 6, 9, 11] and dia > 30:
                return False
            if mes == 2 and dia > 29:
                return False
            return True
        except:
            return False
    
    def _extrair_valor_total(self, texto: str) -> float:
        valores_encontrados = []
        for padrao in [r'TOTAL.*?(\d{1,3}(?:\.\d{3})*,\d{2})',
                      r'VALOR.*?TOTAL.*?R?\$?\s*(\d{1,3}(?:\.\d{3})*,\d{2})',
                      r'vNF.*?(\d{1,3}(?:\.\d{3})*,\d{2})',
                      r'(\d{1,3}(?:\.\d{3})*,\d{2})(?=\s*$)']:
            for match in re.findall(padrao, texto, re.IGNORECASE | re.MULTILINE):
                try:
                    valor = float(match.replace('.', '').replace(',', '.'))
                    if valor > 0:
                        valores_encontrados.append(valor)
                except:
                    continue
        return max(valores_encontrados) if valores_encontrados else 0.0
    
    def processar_pdfs_pasta_atual(self):
        arquivos_pdf = [f for f in os.listdir(os.getcwd()) if f.lower().endswith('.pdf')]
        if not arquivos_pdf:
            return
        
        for arquivo_pdf in arquivos_pdf:
            dados = self.extrair_dados_pdf(arquivo_pdf)
            self.dados_extraidos.append(dados)
    
    def gerar_dataframe(self) -> pd.DataFrame:
        if not self.dados_extraidos:
            return pd.DataFrame()
        
        colunas_ordenadas = ['razao_social_emitente', 'cnpj_emitente', 'razao_social_destinatario',
                           'cnpj_destinatario', 'numero_nf', 'data_nf', 'valor_total', 'arquivo']
        return pd.DataFrame(self.dados_extraidos, columns=colunas_ordenadas)
    
    def salvar_excel(self, nome_arquivo: str = "dados_nfe_extraidos.xlsx"):
        df = self.gerar_dataframe()
        if not df.empty:
            if 'erro' in df.columns:
                df = df.drop('erro', axis=1)
            df.to_excel(nome_arquivo, index=False, engine='openpyxl')
            print(f"✅ Arquivo Excel gerado: {nome_arquivo} ({len(df)} registros)")
            return True
        return False

if __name__ == "__main__":
    extrator = ExtratorNFePDF()
    extrator.processar_pdfs_pasta_atual()
    extrator.salvar_excel("dados_nfe_extraidos.xlsx")