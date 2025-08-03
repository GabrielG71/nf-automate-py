import pandas as pd
import PyPDF2
import re
from datetime import datetime
import os
from typing import Dict, List

class ExtratorPDFNotaFiscal:
    def __init__(self):
        self.dados_extraidos = []
    
    def extrair_dados_pdf(self, nome_arquivo: str) -> Dict[str, any]:
        """
        Extrai dados de uma nota fiscal em PDF
        """
        try:
            # Abre o PDF
            with open(nome_arquivo, 'rb') as arquivo:
                leitor_pdf = PyPDF2.PdfReader(arquivo)
                texto_completo = ""
                
                # Extrai texto de todas as páginas
                for pagina in leitor_pdf.pages:
                    texto_completo += pagina.extract_text() + "\n"
            
            print(f"Processando: {nome_arquivo}")
            
            # Padrões para extrair informações das NFs
            dados = {
                'arquivo': nome_arquivo,
                'numero_nf': self._extrair_numero_nf(texto_completo),
                'serie': self._extrair_serie(texto_completo),
                'data_emissao': self._extrair_data_emissao(texto_completo),
                'cnpj_emitente': self._extrair_cnpj_emitente(texto_completo),
                'nome_emitente': self._extrair_nome_emitente(texto_completo),
                'cnpj_destinatario': self._extrair_cnpj_destinatario(texto_completo),
                'nome_destinatario': self._extrair_nome_destinatario(texto_completo),
                'valor_total': self._extrair_valor_total(texto_completo),
                'valor_produtos': self._extrair_valor_produtos(texto_completo),
                'valor_icms': self._extrair_valor_icms(texto_completo),
                'chave_acesso': self._extrair_chave_acesso(texto_completo)
            }
            
            return dados
            
        except Exception as e:
            print(f"Erro ao processar {nome_arquivo}: {str(e)}")
            return {'arquivo': nome_arquivo, 'erro': str(e)}
    
    def _extrair_numero_nf(self, texto: str) -> str:
        """Extrai o número da nota fiscal"""
        padroes = [
            r'N[ºo°]\s*(\d+)',
            r'Número\s*:?\s*(\d+)',
            r'NF-e\s*N[ºo°]\s*(\d+)',
            r'NOTA FISCAL.*?(\d{6,})',
        ]
        
        for padrao in padroes:
            match = re.search(padrao, texto, re.IGNORECASE)
            if match:
                return match.group(1)
        return ""
    
    def _extrair_serie(self, texto: str) -> str:
        """Extrai a série da nota fiscal"""
        padroes = [
            r'Série\s*:?\s*(\d+)',
            r'SÉRIE\s*:?\s*(\d+)',
            r'Serie\s*:?\s*(\d+)',
        ]
        
        for padrao in padroes:
            match = re.search(padrao, texto, re.IGNORECASE)
            if match:
                return match.group(1)
        return ""
    
    def _extrair_data_emissao(self, texto: str) -> str:
        """Extrai a data de emissão"""
        padroes = [
            r'Data.*?Emissão.*?(\d{2}/\d{2}/\d{4})',
            r'Emissão.*?(\d{2}/\d{2}/\d{4})',
            r'(\d{2}/\d{2}/\d{4})',
            r'(\d{2}-\d{2}-\d{4})',
        ]
        
        for padrao in padroes:
            match = re.search(padrao, texto, re.IGNORECASE)
            if match:
                return match.group(1)
        return ""
    
    def _extrair_cnpj_emitente(self, texto: str) -> str:
        """Extrai CNPJ do emitente"""
        # Procura pelo primeiro CNPJ que aparece (geralmente é do emitente)
        match = re.search(r'(\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})', texto)
        if match:
            return match.group(1)
        return ""
    
    def _extrair_nome_emitente(self, texto: str) -> str:
        """Extrai nome do emitente"""
        padroes = [
            r'EMITENTE.*?\n(.*?)\n',
            r'Razão Social.*?:?\s*(.*?)\n',
            r'DADOS.*?EMITENTE.*?\n(.*?)\n',
        ]
        
        for padrao in padroes:
            match = re.search(padrao, texto, re.IGNORECASE | re.DOTALL)
            if match:
                nome = match.group(1).strip()
                if len(nome) > 5:  # Filtro básico
                    return nome
        return ""
    
    def _extrair_cnpj_destinatario(self, texto: str) -> str:
        """Extrai CNPJ do destinatário"""
        # Procura por CNPJs após encontrar o primeiro (emitente)
        cnpjs = re.findall(r'(\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})', texto)
        if len(cnpjs) > 1:
            return cnpjs[1]  # Segundo CNPJ encontrado
        return ""
    
    def _extrair_nome_destinatario(self, texto: str) -> str:
        """Extrai nome do destinatário"""
        padroes = [
            r'DESTINAT[AÁ]RIO.*?\n(.*?)\n',
            r'DADOS.*?DESTINAT[AÁ]RIO.*?\n(.*?)\n',
        ]
        
        for padrao in padroes:
            match = re.search(padrao, texto, re.IGNORECASE | re.DOTALL)
            if match:
                nome = match.group(1).strip()
                if len(nome) > 5:
                    return nome
        return ""
    
    def _extrair_valor_total(self, texto: str) -> float:
        """Extrai valor total da nota"""
        padroes = [
            r'TOTAL.*?NOTA.*?R\$\s*([\d.,]+)',
            r'Valor.*?Total.*?R\$\s*([\d.,]+)',
            r'TOTAL.*?R\$\s*([\d.,]+)',
            r'Total.*?Geral.*?R\$\s*([\d.,]+)',
        ]
        
        for padrao in padroes:
            match = re.search(padrao, texto, re.IGNORECASE)
            if match:
                valor_str = match.group(1)
                try:
                    # Converte string para float (trata vírgula decimal brasileira)
                    valor = float(valor_str.replace('.', '').replace(',', '.'))
                    return valor
                except:
                    continue
        return 0.0
    
    def _extrair_valor_produtos(self, texto: str) -> float:
        """Extrai valor dos produtos"""
        padroes = [
            r'Produtos.*?R\$\s*([\d.,]+)',
            r'PRODUTOS.*?R\$\s*([\d.,]+)',
            r'Valor.*?Produtos.*?R\$\s*([\d.,]+)',
        ]
        
        for padrao in padroes:
            match = re.search(padrao, texto, re.IGNORECASE)
            if match:
                valor_str = match.group(1)
                try:
                    valor = float(valor_str.replace('.', '').replace(',', '.'))
                    return valor
                except:
                    continue
        return 0.0
    
    def _extrair_valor_icms(self, texto: str) -> float:
        """Extrai valor do ICMS"""
        padroes = [
            r'ICMS.*?R\$\s*([\d.,]+)',
            r'Valor.*?ICMS.*?R\$\s*([\d.,]+)',
        ]
        
        for padrao in padroes:
            match = re.search(padrao, texto, re.IGNORECASE)
            if match:
                valor_str = match.group(1)
                try:
                    valor = float(valor_str.replace('.', '').replace(',', '.'))
                    return valor
                except:
                    continue
        return 0.0
    
    def _extrair_chave_acesso(self, texto: str) -> str:
        """Extrai chave de acesso da NFe"""
        match = re.search(r'(\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4})', texto)
        if match:
            return match.group(1).replace(' ', '')
        
        # Tenta sem espaços
        match = re.search(r'(\d{44})', texto)
        if match:
            return match.group(1)
        
        return ""
    
    def processar_pdfs_pasta_atual(self):
        """
        Processa todos os PDFs da pasta atual
        """
        pasta_atual = os.getcwd()
        arquivos_pdf = [f for f in os.listdir(pasta_atual) if f.lower().endswith('.pdf')]
        
        if not arquivos_pdf:
            print("Nenhum arquivo PDF encontrado na pasta atual.")
            return
        
        print(f"Encontrados {len(arquivos_pdf)} arquivo(s) PDF:")
        for arquivo in arquivos_pdf:
            print(f"  - {arquivo}")
        
        print("\nProcessando arquivos...")
        
        for arquivo_pdf in arquivos_pdf:
            dados = self.extrair_dados_pdf(arquivo_pdf)
            self.dados_extraidos.append(dados)
        
        print(f"\nProcessamento concluído! {len(self.dados_extraidos)} arquivo(s) processado(s).")
    
    def gerar_dataframe(self) -> pd.DataFrame:
        """
        Converte os dados extraídos em um DataFrame pandas
        """
        if not self.dados_extraidos:
            return pd.DataFrame()
        
        df = pd.DataFrame(self.dados_extraidos)
        return df
    
    def salvar_excel(self, nome_arquivo: str = "dados_notas_fiscais.xlsx"):
        """
        Salva os dados em arquivo Excel
        """
        df = self.gerar_dataframe()
        if not df.empty:
            df.to_excel(nome_arquivo, index=False)
            print(f"\nDados salvos em: {nome_arquivo}")
        else:
            print("Nenhum dado para salvar.")
    
    def exibir_relatorio(self):
        """
        Exibe um relatório dos dados extraídos
        """
        if not self.dados_extraidos:
            print("Nenhum dado extraído.")
            return
        
        df = self.gerar_dataframe()
        
        print("\n" + "="*50)
        print("RELATÓRIO DOS DADOS EXTRAÍDOS")
        print("="*50)
        
        print(f"\nTotal de arquivos processados: {len(df)}")
        
        # Relatório de valores
        if 'valor_total' in df.columns:
            valores_validos = df['valor_total'][df['valor_total'] > 0]
            if not valores_validos.empty:
                print(f"Valor total de todas as notas: R$ {valores_validos.sum():,.2f}")
                print(f"Valor médio por nota: R$ {valores_validos.mean():,.2f}")
                print(f"Maior valor: R$ {valores_validos.max():,.2f}")
                print(f"Menor valor: R$ {valores_validos.min():,.2f}")
        
        print(f"\nPrimeiras linhas dos dados:")
        print(df.head())
        
        # Verifica campos vazios
        print(f"\nCampos com dados extraídos:")
        for coluna in df.columns:
            if coluna != 'arquivo':
                dados_preenchidos = df[coluna].notna().sum()
                if coluna in ['valor_total', 'valor_produtos', 'valor_icms']:
                    dados_preenchidos = (df[coluna] > 0).sum()
                print(f"  {coluna}: {dados_preenchidos}/{len(df)} arquivo(s)")


# Exemplo de uso
if __name__ == "__main__":
    print("EXTRATOR DE DADOS DE NOTA FISCAL - PDF")
    print("="*40)
    
    # Cria o extrator
    extrator = ExtratorPDFNotaFiscal()
    
    # Processa todos os PDFs da pasta atual
    extrator.processar_pdfs_pasta_atual()
    
    # Exibe relatório
    extrator.exibir_relatorio()
    
    # Salva em Excel
    extrator.salvar_excel()
    
    print("\n" + "="*40)
    print("PROCESSAMENTO FINALIZADO!")
    print("Verifique o arquivo 'dados_notas_fiscais.xlsx' gerado.")