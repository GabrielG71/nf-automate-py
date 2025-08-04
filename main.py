import pandas as pd
import PyPDF2
import re
import os
import requests
import time
from typing import Dict, Optional, List, Tuple
from pdf2image import convert_from_path
import pytesseract
from datetime import datetime
import logging

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extrator_nfe.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ExtratorNFePDF:
    def __init__(self):
        self.dados_extraidos: List[Dict] = []
        self.cache_cnpj: Dict[str, Optional[Dict]] = {}
        self.delay_api = 0.5  # Delay entre chamadas da API para evitar rate limit
        
    def consultar_cnpj_api(self, cnpj: str) -> Optional[Dict]:
        """Consulta dados do CNPJ usando a BrasilAPI com melhor tratamento de erros."""
        cnpj_limpo = re.sub(r'[^\d]', '', cnpj)
        
        if not self.validar_cnpj(cnpj_limpo):
            logger.warning(f"CNPJ inválido: {cnpj_limpo}")
            return None
            
        # Verifica cache
        if cnpj_limpo in self.cache_cnpj:
            logger.debug(f"CNPJ {cnpj_limpo} encontrado no cache")
            return self.cache_cnpj[cnpj_limpo]
            
        try:
            # Delay para evitar rate limit
            time.sleep(self.delay_api)
            
            url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_limpo}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8'
            }
            
            logger.info(f"Consultando CNPJ na API: {cnpj_limpo}")
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                dados = response.json()
                resultado = {
                    'razao_social': dados.get('razao_social', '').strip().upper(),
                    'nome_fantasia': dados.get('nome_fantasia', '').strip().upper(),
                    'cnpj': self._formatar_cnpj(dados.get('cnpj', cnpj_limpo)),
                    'situacao': dados.get('descricao_situacao_cadastral', ''),
                    'uf': dados.get('uf', ''),
                    'municipio': dados.get('municipio', ''),
                    'logradouro': dados.get('logradouro', ''),
                    'cep': dados.get('cep', ''),
                    'atividade_principal': dados.get('cnae_fiscal_descricao', '')
                }
                self.cache_cnpj[cnpj_limpo] = resultado
                logger.info(f"✅ CNPJ {cnpj_limpo} consultado com sucesso: {resultado['razao_social']}")
                return resultado
                
            elif response.status_code == 404:
                logger.warning(f"⚠️ CNPJ {cnpj_limpo} não encontrado na base da Receita")
                self.cache_cnpj[cnpj_limpo] = None
                return None
                
            elif response.status_code == 429:
                logger.warning(f"⚠️ Rate limit atingido, aumentando delay e tentando novamente...")
                self.delay_api = min(self.delay_api * 2, 5.0)  # Aumenta delay até 5s
                time.sleep(self.delay_api)
                return self.consultar_cnpj_api(cnpj)  # Tenta novamente
                
            else:
                logger.error(f"❌ Erro na API (status {response.status_code}) para CNPJ {cnpj_limpo}")
                if response.text:
                    logger.error(f"Resposta da API: {response.text[:200]}")
                self.cache_cnpj[cnpj_limpo] = None
                return None
                
        except requests.exceptions.Timeout:
            logger.error(f"❌ Timeout na consulta do CNPJ {cnpj_limpo}")
            self.cache_cnpj[cnpj_limpo] = None
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro na consulta do CNPJ {cnpj_limpo}: {str(e)}")
            self.cache_cnpj[cnpj_limpo] = None
            return None
            
        except Exception as e:
            logger.error(f"❌ Erro inesperado na consulta do CNPJ {cnpj_limpo}: {str(e)}")
            self.cache_cnpj[cnpj_limpo] = None
            return None

    def _formatar_cnpj(self, cnpj: str) -> str:
        """Formata o CNPJ no padrão XX.XXX.XXX/XXXX-XX."""
        cnpj_limpo = re.sub(r'[^\d]', '', cnpj)
        if len(cnpj_limpo) == 14:
            return f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:14]}"
        return cnpj_limpo

    def validar_cnpj(self, cnpj: str) -> bool:
        """Valida o CNPJ usando os dígitos verificadores."""
        if len(cnpj) != 14 or not cnpj.isdigit():
            return False
            
        # Verifica se todos os dígitos são iguais (inválido)
        if len(set(cnpj)) == 1:
            return False
            
        def calcular_digito(cnpj: str, pesos: List[int]) -> int:
            soma = sum(int(cnpj[i]) * pesos[i] for i in range(len(pesos)))
            resto = soma % 11
            return 0 if resto < 2 else 11 - resto
            
        pesos_d1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        pesos_d2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        
        return (int(cnpj[12]) == calcular_digito(cnpj, pesos_d1) and 
                int(cnpj[13]) == calcular_digito(cnpj, pesos_d2))

    def extrair_texto_com_ocr(self, nome_arquivo: str) -> str:
        """Extrai texto de PDFs usando OCR."""
        try:
            logger.info(f"Iniciando OCR para {nome_arquivo}")
            images = convert_from_path(nome_arquivo, dpi=300)  # Maior DPI para melhor qualidade
            
            texto_completo = []
            for i, img in enumerate(images):
                # Configura o OCR com parâmetros otimizados para NFe
                config = '--oem 1 --psm 6 -l por'
                texto_pagina = pytesseract.image_to_string(img, lang='por', config=config)
                texto_completo.append(texto_pagina)
                logger.debug(f"OCR página {i+1}/{len(images)} concluída")
            
            texto = "\n".join(texto_completo)
            logger.info(f"OCR concluído para {nome_arquivo} - {len(texto)} caracteres extraídos")
            return texto
            
        except Exception as e:
            logger.error(f"Erro no OCR do arquivo {nome_arquivo}: {str(e)}")
            return ""

    def _extrair_cnpjs_melhorado(self, texto: str) -> Tuple[str, str]:
        """Extrai CNPJs do emitente e destinatário com melhor precisão."""
        # Padrões mais específicos para NFe
        padroes_cnpj = [
            r'(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2})',  # Formato completo
            r'(\d{14})'  # Apenas números
        ]
        
        cnpjs_encontrados = []
        
        # Busca por todos os CNPJs no texto
        for padrao in padroes_cnpj:
            matches = re.findall(padrao, texto)
            for match in matches:
                cnpj_limpo = re.sub(r'[^\d]', '', match)
                if len(cnpj_limpo) == 14 and self.validar_cnpj(cnpj_limpo):
                    cnpj_formatado = self._formatar_cnpj(cnpj_limpo)
                    if cnpj_formatado not in cnpjs_encontrados:
                        cnpjs_encontrados.append(cnpj_formatado)
        
        logger.info(f"CNPJs válidos encontrados: {cnpjs_encontrados}")
        
        # Tenta identificar emitente e destinatário por contexto
        cnpj_emitente = ""
        cnpj_destinatario = ""
        
        if len(cnpjs_encontrados) >= 2:
            # Busca por contexto específico
            for cnpj in cnpjs_encontrados:
                cnpj_busca = cnpj.replace('.', r'\.').replace('/', r'\/').replace('-', r'\-')
                
                # Verifica contexto do emitente
                if re.search(rf'(?:EMITENTE|IDENTIFICAÇÃO DO EMITENTE|REMETENTE).*?{cnpj_busca}', 
                           texto, re.IGNORECASE | re.DOTALL):
                    cnpj_emitente = cnpj
                    logger.info(f"CNPJ emitente identificado por contexto: {cnpj}")
                
                # Verifica contexto do destinatário
                elif re.search(rf'(?:DESTINATÁRIO|DESTINATARIO|DEST\.|CLIENTE).*?{cnpj_busca}', 
                             texto, re.IGNORECASE | re.DOTALL):
                    cnpj_destinatario = cnpj
                    logger.info(f"CNPJ destinatário identificado por contexto: {cnpj}")
            
            # Se não identificou por contexto, assume ordem de aparição
            if not cnpj_emitente and not cnpj_destinatario:
                cnpj_emitente = cnpjs_encontrados[0]
                cnpj_destinatario = cnpjs_encontrados[1] if len(cnpjs_encontrados) > 1 else ""
                logger.info(f"CNPJs atribuídos por ordem: Emitente={cnpj_emitente}, Destinatário={cnpj_destinatario}")
                
        elif len(cnpjs_encontrados) == 1:
            # Tenta determinar se é emitente ou destinatário pelo contexto
            cnpj = cnpjs_encontrados[0]
            if re.search(r'(?:EMITENTE|IDENTIFICAÇÃO DO EMITENTE)', texto, re.IGNORECASE):
                cnpj_emitente = cnpj
            else:
                cnpj_destinatario = cnpj
        
        return cnpj_emitente, cnpj_destinatario

    def extrair_dados_pdf(self, nome_arquivo: str) -> Dict[str, any]:
        """Extrai dados de um PDF usando PyPDF2 e OCR como fallback."""
        try:
            logger.info(f"🔍 Processando arquivo: {nome_arquivo}")
            
            # Tenta extrair texto com PyPDF2
            with open(nome_arquivo, 'rb') as arquivo:
                leitor_pdf = PyPDF2.PdfReader(arquivo)
                texto = ""
                for i, pagina in enumerate(leitor_pdf.pages):
                    texto += pagina.extract_text() + "\n"
                    
            logger.info(f"PyPDF2 extraiu {len(texto)} caracteres de {nome_arquivo}")
            
            # Se o texto for muito curto ou não contiver dados essenciais, tenta OCR
            if len(texto.strip()) < 200 or not re.search(r'\d{2}\.?\d{3}\.?\d{3}', texto):
                logger.warning(f"Texto insuficiente extraído de {nome_arquivo}, tentando OCR")
                texto_ocr = self.extrair_texto_com_ocr(nome_arquivo)
                if len(texto_ocr) > len(texto):
                    texto = texto_ocr
                    logger.info("OCR produziu melhor resultado")
            
            if not texto.strip():
                logger.error(f"Não foi possível extrair texto de {nome_arquivo}")
                return self._criar_registro_erro(nome_arquivo, "Falha na extração de texto")

            # Debug: salva texto extraído para análise
            debug_file = f"debug_{os.path.splitext(nome_arquivo)[0]}.txt"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(texto)
            logger.debug(f"Texto extraído salvo em {debug_file}")

            # Extrai CNPJs com método melhorado
            cnpj_emitente, cnpj_destinatario = self._extrair_cnpjs_melhorado(texto)
            
            # Consulta API para CNPJs válidos
            dados_emitente = None
            dados_destinatario = None
            
            if cnpj_emitente:
                logger.info(f"Consultando dados do emitente: {cnpj_emitente}")
                dados_emitente = self.consultar_cnpj_api(cnpj_emitente)
                
            if cnpj_destinatario:
                logger.info(f"Consultando dados do destinatário: {cnpj_destinatario}")
                dados_destinatario = self.consultar_cnpj_api(cnpj_destinatario)
            
            # Monta resultado
            resultado = {
                'arquivo': nome_arquivo,
                'cnpj_emitente': cnpj_emitente,
                'cnpj_destinatario': cnpj_destinatario,
                'razao_social_emitente': dados_emitente['razao_social'] if dados_emitente else self._extrair_razao_social(texto, emitente=True),
                'razao_social_destinatario': dados_destinatario['razao_social'] if dados_destinatario else self._extrair_razao_social(texto, emitente=False),
                'numero_nf': self._extrair_numero_nf(texto),
                'data_nf': self._extrair_data_nf(texto),
                'valor_total': self._extrair_valor_total(texto)
            }
            
            # Adiciona informações extras da API
            if dados_emitente:
                resultado.update({
                    'situacao_emitente': dados_emitente.get('situacao', ''),
                    'uf_emitente': dados_emitente.get('uf', ''),
                    'municipio_emitente': dados_emitente.get('municipio', ''),
                    'atividade_emitente': dados_emitente.get('atividade_principal', '')
                })
            
            if dados_destinatario:
                resultado.update({
                    'situacao_destinatario': dados_destinatario.get('situacao', ''),
                    'uf_destinatario': dados_destinatario.get('uf', ''),
                    'municipio_destinatario': dados_destinatario.get('municipio', ''),
                    'atividade_destinatario': dados_destinatario.get('atividade_principal', '')
                })
            
            # Log de validação
            sucesso_emitente = bool(resultado['razao_social_emitente'] and resultado['cnpj_emitente'])
            sucesso_destinatario = bool(resultado['razao_social_destinatario'] and resultado['cnpj_destinatario'])
            
            if sucesso_emitente and sucesso_destinatario:
                logger.info(f"✅ Extração completa para {nome_arquivo}")
            elif sucesso_emitente or sucesso_destinatario:
                logger.warning(f"⚠️ Extração parcial para {nome_arquivo}")
            else:
                logger.error(f"❌ Falha na extração para {nome_arquivo}")
                
            return resultado
            
        except Exception as e:
            logger.error(f"❌ Erro ao processar {nome_arquivo}: {str(e)}")
            return self._criar_registro_erro(nome_arquivo, str(e))

    def _criar_registro_erro(self, nome_arquivo: str, erro: str) -> Dict:
        """Cria um registro com valores padrão para erros."""
        return {
            'arquivo': nome_arquivo,
            'erro': erro,
            'razao_social_emitente': '',
            'cnpj_emitente': '',
            'razao_social_destinatario': '',
            'cnpj_destinatario': '',
            'numero_nf': '',
            'data_nf': '',
            'valor_total': 0.0
        }

    def _extrair_razao_social(self, texto: str, emitente: bool = True) -> str:
        """Extrai razão social do emitente ou destinatário com padrões melhorados."""
        if emitente:
            padroes = [
                r'(?:IDENTIFICAÇÃO DO EMITENTE|EMITENTE)[\s\n]*(.*?)(?=\n.*?CNPJ|\n.*?\d{2}\.\d{3}\.\d{3})',
                r'DANFE[\s\n]*.*?[\s\n]*(.*?)(?=\n.*?CNPJ|\n.*?\d{2}\.\d{3}\.\d{3})',
                r'RAZÃO SOCIAL[\s\n]*(.*?)(?=\n|CNPJ)',
            ]
        else:
            padroes = [
                r'(?:DESTINATÁRIO|DESTINATARIO)[\s\n]*(.*?)(?=\n.*?CNPJ|\n.*?\d{2}\.\d{3}\.\d{3})',
                r'NOME\/RAZÃO SOCIAL[\s\n]*(.*?)(?=\n|CNPJ)',
            ]
        
        for padrao in padroes:
            match = re.search(padrao, texto, re.DOTALL | re.IGNORECASE)
            if match:
                candidato = self._limpar_nome_empresa(match.group(1))
                if self._validar_nome_empresa(candidato):
                    return candidato
        
        return ""

    def _limpar_nome_empresa(self, texto: str) -> str:
        """Limpa o texto da razão social."""
        if not texto:
            return ""
        
        # Remove caracteres especiais mas mantém acentos
        nome = re.sub(r'[^\w\sÀ-ÿ]', ' ', texto)
        # Remove números isolados
        nome = re.sub(r'\b\d+\b', ' ', nome)
        # Remove espaços extras
        nome = re.sub(r'\s+', ' ', nome)
        # Remove palavras muito curtas no início
        palavras = nome.strip().split()
        palavras_filtradas = [p for p in palavras if len(p) >= 2 or p.upper() in ['SA', 'ME']]
        
        return ' '.join(palavras_filtradas).strip().upper()

    def _validar_nome_empresa(self, nome: str) -> bool:
        """Valida se o texto é uma razão social válida."""
        if not nome or len(nome) < 5:
            return False
            
        palavras = nome.split()
        if len(palavras) < 2:
            return False
            
        # Verifica se tem pelo menos 2 palavras significativas (>= 3 caracteres)
        palavras_significativas = [p for p in palavras if len(p) >= 3]
        if len(palavras_significativas) < 2:
            return False
            
        # Verifica se não tem muitos números
        if len(re.findall(r'\d', nome)) > len(nome) * 0.3:
            return False
            
        # Verifica padrões comuns de empresas
        padroes_empresa = r'(?:LTDA|S\.?A\.?|ME|EPP|EIRELI|COMERCIO|INDUSTRIA|SERVICOS|CIA)'
        if re.search(padroes_empresa, nome, re.IGNORECASE):
            return True
            
        # Se tem pelo menos 3 palavras e mais de 10 caracteres, provavelmente é válido
        return len(palavras) >= 3 and len(nome) >= 10

    def _extrair_numero_nf(self, texto: str) -> str:
        """Extrai o número da NF-e com padrões melhorados."""
        padroes = [
            r'NF-e\s*[nN]º?\s*(\d{3}\.\d{3}\.\d{3})',  # Formato com pontos
            r'NF-e\s*[nN]º?\s*(\d{9})',  # Formato sem pontos
            r'NÚMERO\s*[:\-]?\s*(\d{3}\.\d{3}\.\d{3})',
            r'NÚMERO\s*[:\-]?\s*(\d{9})',
            r'Nº?\s*(\d{3}\.\d{3}\.\d{3})',
            r'N\s+(\d{3}\.\d{3}\.\d{3})'
        ]
        
        for padrao in padroes:
            match = re.search(padrao, texto, re.IGNORECASE)
            if match:
                numero = match.group(1).replace('.', '')
                if len(numero) == 9 and numero.isdigit():
                    return numero
        return ""

    def _extrair_data_nf(self, texto: str) -> str:
        """Extrai a data da NF-e com validação melhorada."""
        padroes = [
            r'DATA DE EMISSÃO\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})',
            r'EMISSÃO\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})',
            r'(\d{2}/\d{2}/\d{4})',
            r'(\d{4}-\d{2}-\d{2})'
        ]
        
        datas_encontradas = []
        for padrao in padroes:
            matches = re.findall(padrao, texto, re.IGNORECASE)
            for match in matches:
                data_formatada = self._formatar_data(match)
                if self._validar_data(data_formatada):
                    datas_encontradas.append(data_formatada)
        
        # Retorna a primeira data válida encontrada
        return datas_encontradas[0] if datas_encontradas else ""

    def _formatar_data(self, data: str) -> str:
        """Formata a data para DD/MM/YYYY."""
        if '-' in data:
            partes = data.split('-')
            if len(partes[0]) == 4:
                return f"{partes[2]}/{partes[1]}/{partes[0]}"
        return data

    def _validar_data(self, data: str) -> bool:
        """Valida se a data é válida e está em um range aceitável."""
        try:
            if '/' not in data:
                return False
            partes = data.split('/')
            if len(partes) != 3:
                return False
            dia, mes, ano = map(int, partes)
            
            # Validações básicas
            if not (2000 <= ano <= 2030):
                return False
            if not (1 <= mes <= 12):
                return False
            if not (1 <= dia <= 31):
                return False
            
            # Validações específicas por mês
            if mes in [4, 6, 9, 11] and dia > 30:
                return False
            if mes == 2:
                is_leap = (ano % 4 == 0 and ano % 100 != 0) or (ano % 400 == 0)
                if dia > (29 if is_leap else 28):
                    return False
            
            return True
        except (ValueError, IndexError):
            return False

    def _extrair_valor_total(self, texto: str) -> float:
        """Extrai o valor total da NF-e com melhor precisão."""
        padroes = [
            r'VALOR TOTAL DA NOTA\s*[:\-]?\s*([\d\.,]+)',
            r'vNF\s*[:\-]?\s*([\d\.,]+)',
            r'TOTAL GERAL\s*[:\-]?\s*([\d\.,]+)',
            r'VALOR TOTAL\s*[:\-]?\s*([\d\.,]+)',
            r'TOTAL\s*[:\-]?\s*([\d\.,]+)'
        ]
        
        valores_encontrados = []
        for padrao in padroes:
            matches = re.findall(padrao, texto, re.IGNORECASE)
            for match in matches:
                try:
                    # Limpa e converte o valor
                    valor_str = match.replace('.', '').replace(',', '.')
                    valor = float(valor_str)
                    if 0 < valor < 999999999:  # Range razoável para NFe
                        valores_encontrados.append(valor)
                except (ValueError, AttributeError):
                    continue
        
        # Retorna o maior valor encontrado (provavelmente o total)
        return max(valores_encontrados) if valores_encontrados else 0.0

    def processar_pdfs_pasta_atual(self):
        """Processa todos os PDFs na pasta atual."""
        arquivos_pdf = [f for f in os.listdir(os.getcwd()) 
                       if f.lower().endswith('.pdf') and not f.startswith('debug_')]
        
        if not arquivos_pdf:
            logger.warning("❌ Nenhum arquivo PDF encontrado na pasta atual")
            return
            
        logger.info(f"📁 Encontrados {len(arquivos_pdf)} arquivo(s) PDF para processar")
        
        for i, arquivo_pdf in enumerate(arquivos_pdf, 1):
            logger.info(f"\n📄 Processando {i}/{len(arquivos_pdf)}: {arquivo_pdf}")
            dados = self.extrair_dados_pdf(arquivo_pdf)
            self.dados_extraidos.append(dados)
        
        logger.info(f"\n🎉 Processamento concluído! {len(self.dados_extraidos)} arquivo(s) processado(s)")

    def gerar_dataframe(self) -> pd.DataFrame:
        """Gera um DataFrame com os dados extraídos."""
        if not self.dados_extraidos:
            return pd.DataFrame()
            
        # Define todas as colunas possíveis
        colunas_base = [
            'arquivo', 'cnpj_emitente', 'razao_social_emitente', 
            'cnpj_destinatario', 'razao_social_destinatario',
            'numero_nf', 'data_nf', 'valor_total'
        ]
        
        colunas_api = [
            'situacao_emitente', 'uf_emitente', 'municipio_emitente', 'atividade_emitente',
            'situacao_destinatario', 'uf_destinatario', 'municipio_destinatario', 'atividade_destinatario'
        ]
        
        # Verifica quais colunas existem nos dados
        colunas_existentes = set()
        for registro in self.dados_extraidos:
            colunas_existentes.update(registro.keys())
        
        colunas_finais = [col for col in colunas_base + colunas_api if col in colunas_existentes]
        
        df = pd.DataFrame(self.dados_extraidos)
        
        # Garante que as colunas principais existam
        for col in colunas_base:
            if col not in df.columns:
                df[col] = ''
        
        # Converte tipos
        df['valor_total'] = pd.to_numeric(df['valor_total'], errors='coerce').fillna(0.0)
        
        # Converte data
        if 'data_nf' in df.columns:
            df['data_nf'] = pd.to_datetime(df['data_nf'], format='%d/%m/%Y', errors='coerce')
        
        # Preenche valores nulos
        df = df.fillna('')
        
        return df[colunas_finais]

    def salvar_excel(self, nome_arquivo: str = "dados_nfe_extraidos.xlsx"):
        """Salva os dados extraídos em um arquivo Excel."""
        df = self.gerar_dataframe()
        if not df.empty:
            # Remove coluna de erro se existir
            if 'erro' in df.columns:
                df_limpo = df.drop('erro', axis=1)
            else:
                df_limpo = df
            
            df_limpo.to_excel(nome_arquivo, index=False, engine='openpyxl')
            logger.info(f"📊 Arquivo Excel gerado: {nome_arquivo} ({len(df)} registros)")
            return True
        else:
            logger.warning("⚠️ Nenhum dado foi extraído para salvar")
            return False

    def exibir_estatisticas(self):
        """Exibe estatísticas detalhadas do processamento."""
        if not self.dados_extraidos:
            logger.warning("⚠️ Nenhum dado processado")
            return
            
        total = len(self.dados_extraidos)
        com_emitente = sum(1 for d in self.dados_extraidos if d.get('razao_social_emitente'))
        com_destinatario = sum(1 for d in self.dados_extraidos if d.get('razao_social_destinatario'))
        com_cnpj_emitente = sum(1 for d in self.dados_extraidos if d.get('cnpj_emitente'))
        com_cnpj_destinatario = sum(1 for d in self.dados_extraidos if d.get('cnpj_destinatario'))
        com_numero_nf = sum(1 for d in self.dados_extraidos if d.get('numero_nf'))
        com_data_nf = sum(1 for d in self.dados_extraidos if d.get('data_nf'))
        com_valor_total = sum(1 for d in self.dados_extraidos if d.get('valor_total', 0) > 0)
        
        # Estatísticas da API
        cnpjs_consultados = len(self.cache_cnpj)
        cnpjs_encontrados = sum(1 for v in self.cache_cnpj.values() if v is not None)
        
        logger.info("\n" + "="*60)
        logger.info("📊 ESTATÍSTICAS DO PROCESSAMENTO")
        logger.info("="*60)
        logger.info(f"📁 Total de arquivos processados: {total}")
        logger.info(f"🏢 Razão social emitente extraída: {com_emitente}/{total} ({com_emitente/total*100:.1f}%)")
        logger.info(f"🏪 Razão social destinatário extraída: {com_destinatario}/{total} ({com_destinatario/total*100:.1f}%)")
        logger.info(f"🆔 CNPJ emitente extraído: {com_cnpj_emitente}/{total} ({com_cnpj_emitente/total*100:.1f}%)")
        logger.info(f"🆔 CNPJ destinatário extraído: {com_cnpj_destinatario}/{total} ({com_cnpj_destinatario/total*100:.1f}%)")
        logger.info(f"🔢 Número da NFe extraído: {com_numero_nf}/{total} ({com_numero_nf/total*100:.1f}%)")
        logger.info(f"📅 Data da NFe extraída: {com_data_nf}/{total} ({com_data_nf/total*100:.1f}%)")
        logger.info(f"💰 Valor total extraído: {com_valor_total}/{total} ({com_valor_total/total*100:.1f}%)")
        logger.info(f"🌐 CNPJs consultados na API: {cnpjs_consultados}")
        percentual_api = (cnpjs_encontrados/cnpjs_consultados*100) if cnpjs_consultados > 0 else 0
        logger.info(f"✅ CNPJs encontrados na API: {cnpjs_encontrados}/{cnpjs_consultados} ({percentual_api:.1f}%)")
        
        # Exibe dados com problemas
        problemas = [d for d in self.dados_extraidos 
                    if not d.get('cnpj_emitente') or not d.get('cnpj_destinatario')]
        
        if problemas:
            logger.info(f"\n⚠️ ARQUIVOS COM PROBLEMAS DE EXTRAÇÃO ({len(problemas)}):")
            for p in problemas:
                logger.info(f"   📄 {p['arquivo']}: "
                          f"Emitente={'✓' if p.get('cnpj_emitente') else '✗'} "
                          f"Destinatário={'✓' if p.get('cnpj_destinatario') else '✗'}")
        
        logger.info("="*60)

    def gerar_relatorio_detalhado(self, nome_arquivo: str = "relatorio_extracao_nfe.txt"):
        """Gera um relatório detalhado do processamento."""
        with open(nome_arquivo, 'w', encoding='utf-8') as f:
            f.write("RELATÓRIO DETALHADO - EXTRAÇÃO DE DADOS NFe\n")
            f.write("="*60 + "\n")
            f.write(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f.write(f"Total de arquivos processados: {len(self.dados_extraidos)}\n\n")
            
            for i, dados in enumerate(self.dados_extraidos, 1):
                f.write(f"\n--- ARQUIVO {i}: {dados['arquivo']} ---\n")
                f.write(f"CNPJ Emitente: {dados.get('cnpj_emitente', 'NÃO ENCONTRADO')}\n")
                f.write(f"Razão Social Emitente: {dados.get('razao_social_emitente', 'NÃO ENCONTRADO')}\n")
                f.write(f"CNPJ Destinatário: {dados.get('cnpj_destinatario', 'NÃO ENCONTRADO')}\n")
                f.write(f"Razão Social Destinatário: {dados.get('razao_social_destinatario', 'NÃO ENCONTRADO')}\n")
                f.write(f"Número NFe: {dados.get('numero_nf', 'NÃO ENCONTRADO')}\n")
                f.write(f"Data NFe: {dados.get('data_nf', 'NÃO ENCONTRADO')}\n")
                f.write(f"Valor Total: R$ {dados.get('valor_total', 0):,.2f}\n")
                
                if dados.get('erro'):
                    f.write(f"ERRO: {dados['erro']}\n")
            
            f.write(f"\n\n--- CACHE DE CNPJ (API) ---\n")
            for cnpj, dados_api in self.cache_cnpj.items():
                f.write(f"\nCNPJ: {cnpj}\n")
                if dados_api:
                    f.write(f"  Razão Social: {dados_api.get('razao_social', 'N/A')}\n")
                    f.write(f"  Situação: {dados_api.get('situacao', 'N/A')}\n")
                    f.write(f"  UF: {dados_api.get('uf', 'N/A')}\n")
                    f.write(f"  Município: {dados_api.get('municipio', 'N/A')}\n")
                else:
                    f.write("  Status: NÃO ENCONTRADO NA API\n")
        
        logger.info(f"📋 Relatório detalhado salvo em: {nome_arquivo}")

if __name__ == "__main__":
    logger.info("🚀 Iniciando extração melhorada de dados de NFe...")
    
    try:
        extrator = ExtratorNFePDF()
        extrator.processar_pdfs_pasta_atual()
        
        if extrator.dados_extraidos:
            extrator.exibir_estatisticas()
            
            # Salva arquivos de saída
            if extrator.salvar_excel():
                logger.info("✅ Arquivo Excel salvo com sucesso")
            
            extrator.gerar_relatorio_detalhado()
            
            # Limpa arquivos de debug se desejado
            debug_files = [f for f in os.listdir(os.getcwd()) if f.startswith('debug_') and f.endswith('.txt')]
            if debug_files:
                resposta = input(f"\n🧹 Encontrados {len(debug_files)} arquivos de debug. Deseja removê-los? (s/n): ")
                if resposta.lower() == 's':
                    for debug_file in debug_files:
                        try:
                            os.remove(debug_file)
                            logger.info(f"🗑️ Arquivo {debug_file} removido")
                        except Exception as e:
                            logger.warning(f"⚠️ Não foi possível remover {debug_file}: {e}")
        else:
            logger.error("❌ Nenhum dado foi extraído. Verifique os arquivos PDF e tente novamente.")
            
    except KeyboardInterrupt:
        logger.info("\n⏹️ Processamento interrompido pelo usuário")
    except Exception as e:
        logger.error(f"❌ Erro inesperado: {str(e)}")
    finally:
        logger.info("🏁 Processamento finalizado!")