import pandas as pd
import PyPDF2
import re
import os
import requests
import time
from typing import Dict, Optional, List, Tuple
from pdf2image import convert_from_path
import pytesseract

class ExtratorNFePDF:
    def __init__(self):
        self.dados_extraidos: List[Dict] = []
        self.cache_cnpj: Dict[str, Optional[Dict]] = {}
        
    def consultar_cnpj_api(self, cnpj: str) -> Optional[Dict]:
        """Consulta dados do CNPJ usando a BrasilAPI."""
        cnpj_limpo = re.sub(r'[^\d]', '', cnpj)
        
        if not self._validar_cnpj(cnpj_limpo) or cnpj_limpo in self.cache_cnpj:
            return self.cache_cnpj.get(cnpj_limpo)
            
        try:
            time.sleep(0.5)  # Rate limit
            url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_limpo}"
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                dados = response.json()
                resultado = {
                    'razao_social': dados.get('razao_social', '').strip().upper(),
                    'uf': dados.get('uf', ''),
                    'municipio': dados.get('municipio', ''),
                    'situacao': dados.get('descricao_situacao_cadastral', '')
                }
                self.cache_cnpj[cnpj_limpo] = resultado
                return resultado
            else:
                self.cache_cnpj[cnpj_limpo] = None
                return None
                
        except Exception:
            self.cache_cnpj[cnpj_limpo] = None
            return None

    def _validar_cnpj(self, cnpj: str) -> bool:
        """Valida CNPJ usando dígitos verificadores."""
        if len(cnpj) != 14 or not cnpj.isdigit() or len(set(cnpj)) == 1:
            return False
            
        def calcular_digito(cnpj: str, pesos: List[int]) -> int:
            soma = sum(int(cnpj[i]) * pesos[i] for i in range(len(pesos)))
            resto = soma % 11
            return 0 if resto < 2 else 11 - resto
            
        pesos_d1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        pesos_d2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        
        return (int(cnpj[12]) == calcular_digito(cnpj, pesos_d1) and 
                int(cnpj[13]) == calcular_digito(cnpj, pesos_d2))

    def _extrair_cnpjs_estruturados(self, texto: str) -> Tuple[str, str]:
        """Extrai CNPJs considerando as seções específicas do DANFE."""
        cnpj_emitente = ""
        cnpj_destinatario = ""
        
        # Padrões para encontrar CNPJs
        padrao_cnpj = r'(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2})'
        
        # Procura seção do emitente
        secao_emitente = re.search(
            r'(?:IDENTIFICAÇÃO DO EMITENTE|IDENTIFICACAO DO EMITENTE|EMITENTE).*?(?=DESTINATÁRIO|DESTINATARIO|REMETENTE|DADOS DO PRODUTO)',
            texto, re.DOTALL | re.IGNORECASE
        )
        
        if secao_emitente:
            cnpjs_emitente = re.findall(padrao_cnpj, secao_emitente.group(0))
            for cnpj in cnpjs_emitente:
                cnpj_limpo = re.sub(r'[^\d]', '', cnpj)
                if len(cnpj_limpo) == 14 and self._validar_cnpj(cnpj_limpo):
                    cnpj_emitente = f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:14]}"
                    break
        
        # Procura seção do destinatário
        secao_destinatario = re.search(
            r'(?:DESTINATÁRIO|DESTINATARIO).*?(?=DUPLICATAS|CÁLCULO DO IMPOSTO|CALCULO DO IMPOSTO|TRANSPORTADOR|DADOS DO PRODUTO)',
            texto, re.DOTALL | re.IGNORECASE
        )
        
        if secao_destinatario:
            cnpjs_destinatario = re.findall(padrao_cnpj, secao_destinatario.group(0))
            for cnpj in cnpjs_destinatario:
                cnpj_limpo = re.sub(r'[^\d]', '', cnpj)
                if len(cnpj_limpo) == 14 and self._validar_cnpj(cnpj_limpo) and cnpj != cnpj_emitente:
                    cnpj_destinatario = f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:14]}"
                    break
        
        return cnpj_emitente, cnpj_destinatario

    def _extrair_cnpjs_fallback(self, texto: str) -> Tuple[str, str]:
        """Método fallback para documentos sem estrutura clara (como digitalizados)."""
        padroes = [r'(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2})', r'(\d{14})']
        cnpjs_validos = []
        
        for padrao in padroes:
            matches = re.findall(padrao, texto)
            for match in matches:
                cnpj_limpo = re.sub(r'[^\d]', '', match)
                if len(cnpj_limpo) == 14 and self._validar_cnpj(cnpj_limpo):
                    cnpj_formatado = f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:14]}"
                    if cnpj_formatado not in cnpjs_validos:
                        cnpjs_validos.append(cnpj_formatado)
        
        return (cnpjs_validos[0] if len(cnpjs_validos) > 0 else "", 
                cnpjs_validos[1] if len(cnpjs_validos) > 1 else "")

    def _extrair_razao_social_estruturada(self, texto: str, emitente: bool = True) -> str:
        """Extrai razão social das seções específicas do DANFE."""
        if emitente:
            # Procura na seção do emitente
            match = re.search(
                r'(?:IDENTIFICAÇÃO DO EMITENTE|IDENTIFICACAO DO EMITENTE)[\s\n]*([^\n]+?)(?=\n.*?(?:ENDEREÇO|ENDERECO|CNPJ|\d{2}\.\d{3}\.\d{3}))',
                texto, re.DOTALL | re.IGNORECASE
            )
        else:
            # Procura na seção do destinatário
            match = re.search(
                r'(?:DESTINATÁRIO|DESTINATARIO).*?NOME.*?RAZÃO SOCIAL[\s\n]*([^\n]+?)(?=\n.*?(?:ENDEREÇO|ENDERECO|CNPJ|\d{2}\.\d{3}\.\d{3}))',
                texto, re.DOTALL | re.IGNORECASE
            )
            
            # Se não encontrar, tenta um padrão mais simples
            if not match:
                match = re.search(
                    r'(?:DESTINATÁRIO|DESTINATARIO)[\s\n]*([^\n]+?)(?=\n.*?(?:ENDEREÇO|ENDERECO|RUA|CNPJ|\d{2}\.\d{3}\.\d{3}))',
                    texto, re.DOTALL | re.IGNORECASE
                )
        
        if match:
            nome = re.sub(r'[^\w\sÀ-ÿ&\-]', ' ', match.group(1))
            nome = re.sub(r'\s+', ' ', nome).strip().upper()
            # Remove palavras comuns que não fazem parte do nome
            palavras_remover = ['NOME', 'RAZÃO', 'SOCIAL', 'REMETENTE', 'DESTINATÁRIO', 'DESTINATARIO']
            palavras = nome.split()
            palavras_filtradas = [p for p in palavras if p not in palavras_remover]
            nome_final = ' '.join(palavras_filtradas).strip()
            
            if len(nome_final) > 5 and len(nome_final.split()) >= 2:
                return nome_final
        return ""

    def _extrair_razao_social_fallback(self, texto: str, emitente: bool = True) -> str:
        """Método fallback para extrair razão social de documentos sem estrutura clara."""
        if emitente:
            padroes = [
                r'(?:EMITENTE)[\s\n]*(.*?)(?=\n.*?CNPJ|\n.*?\d{2}\.\d{3}\.\d{3})',
                r'DANFE[\s\n]*.*?[\s\n]*(.*?)(?=\n.*?CNPJ|\n.*?\d{2}\.\d{3}\.\d{3})'
            ]
        else:
            padroes = [
                r'(?:DESTINATÁRIO|DESTINATARIO)[\s\n]*(.*?)(?=\n.*?CNPJ|\n.*?\d{2}\.\d{3}\.\d{3})'
            ]
        
        for padrao in padroes:
            match = re.search(padrao, texto, re.DOTALL | re.IGNORECASE)
            if match:
                nome = re.sub(r'[^\w\sÀ-ÿ&\-]', ' ', match.group(1))
                nome = re.sub(r'\s+', ' ', nome).strip().upper()
                if len(nome) > 5 and len(nome.split()) >= 2:
                    return nome
        return ""

    def _detectar_tipo_documento(self, texto: str) -> str:
        """Detecta se o documento é um DANFE estruturado ou digitalizado."""
        indicadores_danfe = [
            'IDENTIFICAÇÃO DO EMITENTE',
            'IDENTIFICACAO DO EMITENTE', 
            'DESTINATÁRIO / REMETENTE',
            'DESTINATARIO / REMETENTE',
            'DOCUMENTO AUXILIAR DA'
        ]
        
        for indicador in indicadores_danfe:
            if indicador in texto.upper():
                return 'danfe_estruturado'
        
        return 'documento_digitalizado'

    def _extrair_numero_nf(self, texto: str) -> str:
        """Extrai número da NFe."""
        padroes = [
            r'NF-e\s*[nN]º?\s*(\d{9}|\d{3}\.\d{3}\.\d{3})',
            r'NÚMERO\s*[:\-]?\s*(\d{9}|\d{3}\.\d{3}\.\d{3})',
            r'Nº\s*(\d{9}|\d{3}\.\d{3}\.\d{3})'
        ]
        
        for padrao in padroes:
            match = re.search(padrao, texto, re.IGNORECASE)
            if match:
                numero = match.group(1).replace('.', '')
                if len(numero) >= 6 and numero.isdigit():
                    return numero
        return ""

    def _extrair_data_nf(self, texto: str) -> str:
        """Extrai data da NFe."""
        padroes = [
            r'DATA DE EMISSÃO\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})',
            r'EMISSÃO\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})',
            r'(\d{2}/\d{2}/\d{4})'
        ]
        
        for padrao in padroes:
            match = re.search(padrao, texto, re.IGNORECASE)
            if match:
                data = match.group(1)
                try:
                    partes = data.split('/')
                    if len(partes) == 3 and 2000 <= int(partes[2]) <= 2030:
                        return data
                except:
                    continue
        return ""

    def _extrair_valor_total(self, texto: str) -> float:
        """Extrai valor total da NFe."""
        padroes = [
            r'VALOR TOTAL DA NOTA\s*[:\-]?\s*([\d\.,]+)',
            r'vNF\s*[:\-]?\s*([\d\.,]+)',
            r'TOTAL GERAL\s*[:\-]?\s*([\d\.,]+)',
            r'VALOR TOTAL:\s*R\$\s*([\d\.,]+)'
        ]
        
        for padrao in padroes:
            match = re.search(padrao, texto, re.IGNORECASE)
            if match:
                try:
                    valor_str = match.group(1).replace('.', '').replace(',', '.')
                    valor = float(valor_str)
                    if 0 < valor < 999999999:
                        return valor
                except:
                    continue
        return 0.0

    def extrair_dados_pdf(self, nome_arquivo: str) -> Dict:
        """Extrai dados de um PDF."""
        try:
            # Tenta PyPDF2 primeiro
            with open(nome_arquivo, 'rb') as arquivo:
                leitor_pdf = PyPDF2.PdfReader(arquivo)
                texto = "".join(pagina.extract_text() for pagina in leitor_pdf.pages)
            
            # Se texto insuficiente, usa OCR
            if len(texto.strip()) < 200:
                images = convert_from_path(nome_arquivo, dpi=300)
                texto = "\n".join(pytesseract.image_to_string(img, lang='por') for img in images)
            
            if not texto.strip():
                return {'arquivo': nome_arquivo, 'erro': 'Falha na extração'}

            # Detecta tipo de documento e extrai dados apropriadamente
            tipo_documento = self._detectar_tipo_documento(texto)
            
            if tipo_documento == 'danfe_estruturado':
                cnpj_emitente, cnpj_destinatario = self._extrair_cnpjs_estruturados(texto)
                razao_emitente_extraida = self._extrair_razao_social_estruturada(texto, True)
                razao_destinatario_extraida = self._extrair_razao_social_estruturada(texto, False)
            else:
                cnpj_emitente, cnpj_destinatario = self._extrair_cnpjs_fallback(texto)
                razao_emitente_extraida = self._extrair_razao_social_fallback(texto, True)
                razao_destinatario_extraida = self._extrair_razao_social_fallback(texto, False)
            
            # Consulta APIs para dados complementares
            dados_emitente = self.consultar_cnpj_api(cnpj_emitente) if cnpj_emitente else None
            dados_destinatario = self.consultar_cnpj_api(cnpj_destinatario) if cnpj_destinatario else None
            
            return {
                'arquivo': nome_arquivo,
                'cnpj_emitente': cnpj_emitente,
                'cnpj_destinatario': cnpj_destinatario,
                'razao_social_emitente': (dados_emitente['razao_social'] if dados_emitente 
                                        else razao_emitente_extraida),
                'razao_social_destinatario': (dados_destinatario['razao_social'] if dados_destinatario 
                                            else razao_destinatario_extraida),
                'numero_nf': self._extrair_numero_nf(texto),
                'data_nf': self._extrair_data_nf(texto),
                'valor_total': self._extrair_valor_total(texto),
                'uf_emitente': dados_emitente.get('uf', '') if dados_emitente else '',
                'uf_destinatario': dados_destinatario.get('uf', '') if dados_destinatario else '',
                'tipo_documento': tipo_documento
            }
            
        except Exception as e:
            return {'arquivo': nome_arquivo, 'erro': str(e)}

    def processar_pdfs(self):
        """Processa todos os PDFs da pasta atual."""
        arquivos_pdf = [f for f in os.listdir('.') if f.lower().endswith('.pdf')]
        
        if not arquivos_pdf:
            print("❌ Nenhum arquivo PDF encontrado")
            return
        
        print(f"🔍 Processando {len(arquivos_pdf)} arquivo(s) PDF...")
        
        for arquivo in arquivos_pdf:
            print(f"📄 Processando: {arquivo}")
            dados = self.extrair_dados_pdf(arquivo)
            self.dados_extraidos.append(dados)
            
            # Mostra informações do processamento
            if 'erro' not in dados:
                print(f"  ✅ Emitente: {dados['razao_social_emitente'][:50]}...")
                print(f"  ✅ Destinatário: {dados['razao_social_destinatario'][:50]}...")
                print(f"  ✅ Tipo: {dados['tipo_documento']}")
            else:
                print(f"  ❌ Erro: {dados['erro']}")
        
        # Salva Excel
        if self.dados_extraidos:
            df = pd.DataFrame(self.dados_extraidos)
            # Remove colunas de erro se existirem
            if 'erro' in df.columns:
                df_limpo = df[df['erro'].isna()].drop('erro', axis=1)
                if not df_limpo.empty:
                    df_limpo.to_excel('dados_nfe_extraidos.xlsx', index=False)
                    print(f"✅ Excel gerado com sucesso! {len(df_limpo)} registro(s) processado(s)")
                else:
                    print("❌ Nenhum dado válido para salvar")
            else:
                df.to_excel('dados_nfe_extraidos.xlsx', index=False)
                print(f"✅ Excel gerado com sucesso! {len(self.dados_extraidos)} registro(s) processado(s)")
        else:
            print("❌ Nenhum dado extraído")

if __name__ == "__main__":
    print("🚀 Iniciando extração de dados NFe...")
    extrator = ExtratorNFePDF()
    extrator.processar_pdfs()