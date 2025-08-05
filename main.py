# -*- coding: utf-8 -*-
"""
NFe PDF para Excel - Processador Otimizado com API CNPJ
======================================================

Processa PDFs de NFe e extrai dados para planilha Excel.
Inclui consulta automática de CNPJ e classificação de materiais.

Autor: Adaptado para uso pessoal
Data: 2025
"""

import re
import pathlib
import logging
import datetime as dt
import time
import requests
from typing import List, Dict, Optional, Tuple
import traceback

# Importações principais
import fitz  # PyMuPDF
import pdfplumber
import pandas as pd

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/nfe_processor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NFeProcessor:
    """Classe principal para processamento de NFes com API CNPJ"""
    
    def __init__(self, config_dir: str = "config"):
        self.base_dir = pathlib.Path(__file__).parent
        self.config_dir = self.base_dir / config_dir
        
        # Diretórios de trabalho
        self.input_dir = self.base_dir / "input"
        self.output_dir = self.base_dir / "output" 
        self.processed_dir = self.base_dir / "processed"
        self.logs_dir = self.base_dir / "logs"
        
        # Criar diretórios se não existirem
        for dir_path in [self.input_dir, self.output_dir, self.processed_dir, self.logs_dir]:
            dir_path.mkdir(exist_ok=True)
        
        # Cache para consultas CNPJ
        self.cache_cnpj: Dict[str, Optional[Dict]] = {}
        
        # Configurar expressões regulares
        self._setup_patterns()
        
        # Configurar mapeamento de colunas
        self._setup_column_mapping()
    
    def _setup_patterns(self):
        """Configura padrões regex para extração"""
        self.patterns = {
            'decimal_trans': str.maketrans({".": "", ",": "."}),
            'cnpj': re.compile(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}"),
            'cnpj_limpo': re.compile(r"\d{14}"),
            'cpf': re.compile(r"\d{3}\.\d{3}\.\d{3}-\d{2}"),
            'data_emissao': re.compile(r"EMISS[ÃA]O[:\s]*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.I),
            'numero_nfe': re.compile(r"NF-e\s+N[ºº°]\s*(\d{1,9})\s+S[ÉE]RIE\s*(\d{1,3})", re.I),
            'chave_acesso': re.compile(r"CHAVE\s+DE\s+ACESSO[:\s\n]*((?:\d[\s\n]*){44})", re.I),
            'valor_total': re.compile(r"VALOR\s+TOTAL[:\sR\$]*([0-9\.\,]+)", re.I),
            'numero_simples': re.compile(r"N[ºº°]\s*(\d+)", re.I),
            'serie_simples': re.compile(r"S[ÉE]RIE\s*(\d+)", re.I),
        }
    
    def _setup_column_mapping(self):
        """Configura mapeamento de colunas da tabela de itens"""
        self.column_mapping = {
            0: "codigo_item", 1: "descricao", 2: "ncm", 3: "cst", 4: "cfop", 
            5: "unid", 6: "quantidade", 7: "valor_unit", 8: "valor_total", 
            9: "desconto", 10: "base_calculo_icms", 11: "valor_icms", 
            12: "valor_ipi", 13: "aliquota_icms", 14: "aliquota_ipi"
        }
        
        self.numeric_columns = {
            "quantidade", "valor_unit", "valor_total", "desconto",
            "base_calculo_icms", "valor_icms", "valor_ipi",
            "aliquota_icms", "aliquota_ipi"
        }
    
    def _validar_cnpj(self, cnpj: str) -> bool:
        """Valida se o CNPJ é válido usando algoritmo oficial"""
        if len(cnpj) != 14 or not cnpj.isdigit():
            return False
            
        def calc_digito(cnpj: str, pesos: List[int]) -> int:
            soma = sum(int(cnpj[i]) * pesos[i] for i in range(len(pesos)))
            resto = soma % 11
            return 0 if resto < 2 else 11 - resto
            
        pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        
        return (int(cnpj[12]) == calc_digito(cnpj, pesos1) and 
                int(cnpj[13]) == calc_digito(cnpj, pesos2))
    
    def consultar_cnpj_api(self, cnpj: str) -> Optional[Dict]:
        """Consulta dados do CNPJ na API do Brasil API"""
        if not cnpj:
            return None
            
        cnpj_limpo = re.sub(r'[^\d]', '', cnpj)
        
        if not self._validar_cnpj(cnpj_limpo):
            logger.warning(f"CNPJ inválido: {cnpj}")
            return None
            
        if cnpj_limpo in self.cache_cnpj:
            logger.info(f"CNPJ {cnpj_limpo} encontrado no cache")
            return self.cache_cnpj[cnpj_limpo]
            
        try:
            time.sleep(0.3)  # Rate limiting para não sobrecarregar a API
            logger.info(f"Consultando CNPJ na API: {cnpj_limpo}")
            
            response = requests.get(
                f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_limpo}", 
                timeout=20
            )
            
            if response.status_code == 200:
                dados = response.json()
                resultado = {
                    'razao_social': dados.get('razao_social', '').strip().upper(),
                    'nome_fantasia': dados.get('nome_fantasia', '').strip().upper(),
                    'porte': dados.get('porte', ''),
                    'atividade_principal': dados.get('atividade_principal', {}).get('text', ''),
                    'natureza_juridica': dados.get('natureza_juridica', ''),
                    'situacao': dados.get('situacao', ''),
                    'uf': dados.get('uf', ''),
                    'municipio': dados.get('municipio', ''),
                    'bairro': dados.get('bairro', ''),
                    'logradouro': dados.get('logradouro', ''),
                    'numero': dados.get('numero', ''),
                    'cep': dados.get('cep', ''),
                    'telefone': dados.get('telefone', ''),
                    'email': dados.get('email', '')
                }
                self.cache_cnpj[cnpj_limpo] = resultado
                logger.info(f"✅ CNPJ consultado: {resultado['razao_social']}")
                return resultado
            else:
                logger.warning(f"API retornou status {response.status_code} para CNPJ {cnpj_limpo}")
                self.cache_cnpj[cnpj_limpo] = None
                
        except Exception as e:
            logger.error(f"Erro ao consultar CNPJ {cnpj_limpo}: {e}")
            self.cache_cnpj[cnpj_limpo] = None
            
        return None
    
    def identificar_tipo_material(self, descricao: str) -> str:
        """Classifica o tipo de material baseado na descrição"""
        if not descricao:
            return "Outros"
            
        desc_lower = descricao.lower()
        
        # Classificação hierárquica de materiais recicláveis
        if any(palavra in desc_lower for palavra in [
            'metal', 'ferro', 'aco', 'aço', 'ferroso', 'inox', 'inoxidavel', 
            'aluminio', 'alumínio', 'cobre', 'bronze', 'latao', 'latão', 
            'zinco', 'chumbo', 'sucata metalica', 'sucata metálica'
        ]):
            return 'Metal'
        
        elif any(palavra in desc_lower for palavra in [
            'papel', 'papelao', 'papelão', 'cartao', 'cartão', 'jornal', 
            'revista', 'livro', 'caderno', 'arquivo', 'branco', ' iv', 
            'ondulado', 'kraft', 'sulfite'
        ]):
            return 'Papel'
        
        elif any(palavra in desc_lower for palavra in [
            'plastico', 'plástico', 'pet', 'pvc', 'polietileno', 'polipropileno',
            'poliestireno', 'sacolinha', 'sacola', 'mista', 'misto', 'pet branca',
            'pet cristal', 'pet verde', 'pead', 'pebd', 'pp', 'ps'
        ]):
            return 'Plastico'
        
        elif any(palavra in desc_lower for palavra in [
            'vidro', 'cristal', 'garrafa vidro', 'vidro branco', 'vidro verde',
            'vidro ambar', 'vidro âmbar', 'vidro marrom'
        ]):
            return 'Vidro'
        
        elif any(palavra in desc_lower for palavra in [
            'oleo', 'óleo', 'lubrificante', 'combustivel', 'combustível',
            'graxa', 'fluido', 'oleo usado', 'óleo usado'
        ]):
            return 'Oleo'
        
        elif any(palavra in desc_lower for palavra in [
            'bateria', 'pilha', 'eletronico', 'eletrônico', 'computador',
            'celular', 'televisor', 'monitor', 'placa', 'cabo', 'fio'
        ]):
            return 'Eletronico'
        
        elif any(palavra in desc_lower for palavra in [
            'textil', 'tecido', 'roupa', 'algodao', 'algodão', 'la', 'lã',
            'poliester', 'nylon', 'fibra'
        ]):
            return 'Textil'
        
        elif any(palavra in desc_lower for palavra in [
            'madeira', 'compensado', 'mdf', 'aglomerado', 'pinus', 'eucalipto',
            'tora', 'tábua', 'prancha'
        ]):
            return 'Madeira'
        
        elif any(palavra in desc_lower for palavra in [
            'pneu', 'borracha', 'latex', 'látex', 'mangueira', 'vedacao', 'vedação'
        ]):
            return 'Borracha'
        
        return "Outros"
    
    def to_float(self, text: str) -> Optional[float]:
        """Converte texto brasileiro para float (1.234,56 -> 1234.56)"""
        if not text or not isinstance(text, str):
            return None
        
        try:
            # Remove espaços e caracteres especiais
            clean_text = re.sub(r'[^\d,.]', '', text.strip())
            if not clean_text:
                return None
            
            # Converte formato brasileiro para float
            return float(clean_text.translate(self.patterns['decimal_trans']))
        except (ValueError, TypeError):
            return None
    
    def extract_pdf_text(self, pdf_path: pathlib.Path) -> str:
        """Extrai texto do PDF usando PyMuPDF"""
        try:
            with fitz.open(pdf_path) as doc:
                return "\n".join(page.get_text("text") for page in doc)
        except Exception as e:
            logger.error(f"Erro ao extrair texto do PDF {pdf_path}: {e}")
            return ""
    
    def extract_cnpjs(self, text: str) -> Tuple[str, str]:
        """Extrai CNPJs do emitente e destinatário de forma mais robusta"""
        cnpjs = []
        
        # Padrões mais específicos para CNPJs em NFes
        patterns = [
            r'CNPJ\s*/\s*CPF[:\s]*(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2})',
            r'(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2})',
            r'(\d{14})'  # CNPJ sem formatação
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                cnpj_limpo = re.sub(r'[^\d]', '', match)
                if len(cnpj_limpo) == 14 and self._validar_cnpj(cnpj_limpo):
                    cnpj_fmt = f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:14]}"
                    if cnpj_fmt not in cnpjs:
                        cnpjs.append(cnpj_fmt)
        
        return (cnpjs[0] if cnpjs else "", cnpjs[1] if len(cnpjs) > 1 else "")
    
    def extract_metadata(self, text: str) -> Dict[str, any]:
        """Extrai metadados da NFe (cabeçalho, emitente, destinatário)"""
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        
        metadata = {
            "numero_nfe": "",
            "serie_nfe": "",
            "data_emissao": None,
            "chave_acesso": "",
            "valor_total_nf": None,
            "emit_nome": "",
            "emit_cnpj": "",
            "emit_ie": "",
            "emit_razao_social": "",
            "emit_uf": "",
            "emit_municipio": "",
            "dest_nome": "",
            "dest_cnpj": "",
            "dest_ie": "",
            "dest_razao_social": "",
            "dest_uf": "",
            "dest_municipio": ""
        }
        
        # Extrair número e série da NFe
        nfe_match = self.patterns['numero_nfe'].search(text)
        if nfe_match:
            metadata["numero_nfe"], metadata["serie_nfe"] = nfe_match.groups()
        else:
            # Tentar padrões alternativos
            num_match = self.patterns['numero_simples'].search(text)
            serie_match = self.patterns['serie_simples'].search(text)
            if num_match:
                metadata["numero_nfe"] = num_match.group(1)
            if serie_match:
                metadata["serie_nfe"] = serie_match.group(1)
        
        # Extrair data de emissão
        data_match = self.patterns['data_emissao'].search(text)
        if data_match:
            try:
                metadata["data_emissao"] = dt.datetime.strptime(
                    data_match.group(1), "%d/%m/%Y"
                ).date()
            except ValueError:
                pass
        
        # Extrair chave de acesso
        chave_match = self.patterns['chave_acesso'].search(text)
        if chave_match:
            metadata["chave_acesso"] = re.sub(r"\s+", "", chave_match.group(1))
        
        # Extrair valor total
        valor_match = self.patterns['valor_total'].search(text)
        if valor_match:
            metadata["valor_total_nf"] = self.to_float(valor_match.group(1))
        
        # Extrair CNPJs
        cnpj_emit, cnpj_dest = self.extract_cnpjs(text)
        metadata["emit_cnpj"] = cnpj_emit
        metadata["dest_cnpj"] = cnpj_dest
        
        # Consultar dados dos CNPJs na API
        if cnpj_emit:
            dados_emit = self.consultar_cnpj_api(cnpj_emit)
            if dados_emit:
                metadata["emit_razao_social"] = dados_emit.get('razao_social', '')
                metadata["emit_uf"] = dados_emit.get('uf', '')
                metadata["emit_municipio"] = dados_emit.get('municipio', '')
                metadata["emit_nome"] = dados_emit.get('nome_fantasia', '') or dados_emit.get('razao_social', '')
        
        if cnpj_dest:
            dados_dest = self.consultar_cnpj_api(cnpj_dest)
            if dados_dest:
                metadata["dest_razao_social"] = dados_dest.get('razao_social', '')
                metadata["dest_uf"] = dados_dest.get('uf', '')
                metadata["dest_municipio"] = dados_dest.get('municipio', '')
                metadata["dest_nome"] = dados_dest.get('nome_fantasia', '') or dados_dest.get('razao_social', '')
        
        return metadata
    
    def extract_items_pdfplumber(self, pdf_path: pathlib.Path) -> List[Dict]:
        """Extrai itens usando pdfplumber com classificação de materiais"""
        items = []
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    
                    for table in tables:
                        if not table:
                            continue
                        
                        header_found = False
                        for row in table:
                            if not row:
                                continue
                            
                            # Procurar cabeçalho
                            if not header_found and row[0] and "PROD" in str(row[0]).upper():
                                header_found = True
                                continue
                            
                            if not header_found:
                                continue
                            
                            # Verificar se é linha de item válida (NCM com 8 dígitos)
                            if (len(row) >= 9 and row[2] and 
                                re.fullmatch(r"\d{8}", str(row[2]).strip())):
                                
                                item = {}
                                for i in range(min(len(row), 15)):
                                    col_name = self.column_mapping.get(i, f"col_{i}")
                                    value = str(row[i] or "").strip()
                                    
                                    if col_name in self.numeric_columns:
                                        item[col_name] = self.to_float(value)
                                    else:
                                        item[col_name] = value
                                
                                # Adicionar classificação de material
                                if 'descricao' in item:
                                    item['tipo_material'] = self.identificar_tipo_material(item['descricao'])
                                
                                items.append(item)
        
        except Exception as e:
            logger.error(f"Erro ao extrair itens com pdfplumber: {e}")
        
        return items
    
    def extract_items_regex(self, text: str) -> List[Dict]:
        """Extrai itens usando regex como fallback com classificação de materiais"""
        items = []
        
        # Padrão regex para linha de item
        item_pattern = re.compile(
            r"(?P<codigo_item>\d{3})\s+"
            r"(?P<descricao>.+?)\s+"
            r"(?P<ncm>\d{8})\s+"
            r"(?P<cst>\d{3})\s+"
            r"(?P<cfop>\d{4})\s+"
            r"(?P<unid>[A-Z]{2,4})\s+"
            r"(?P<quantidade>[0-9\.\,]+)\s+"
            r"(?P<valor_unit>[0-9\.\,]+)\s+"
            r"(?P<valor_total>[0-9\.\,]+)", re.S
        )
        
        for match in item_pattern.finditer(text):
            item = match.groupdict()
            
            # Converter campos numéricos
            for col in ("quantidade", "valor_unit", "valor_total"):
                item[col] = self.to_float(item[col])
            
            # Adicionar classificação de material
            item['tipo_material'] = self.identificar_tipo_material(item.get('descricao', ''))
            
            items.append(item)
        
        return items
    
    def process_pdf(self, pdf_path: pathlib.Path) -> List[Dict]:
        """Processa um único PDF e retorna lista de itens"""
        logger.info(f"Processando: {pdf_path.name}")
        
        try:
            # Extrair texto
            text = self.extract_pdf_text(pdf_path)
            if not text:
                logger.warning(f"Não foi possível extrair texto de {pdf_path.name}")
                return []
            
            # Extrair metadados (inclui consulta API CNPJ)
            metadata = self.extract_metadata(text)
            
            # Extrair itens (tentar pdfplumber primeiro, depois regex)
            items = self.extract_items_pdfplumber(pdf_path)
            if not items:
                items = self.extract_items_regex(text)
            
            # Adicionar metadados e nome do arquivo a cada item
            for item in items:
                item.update(metadata)
                item["arquivo_origem"] = pdf_path.name
            
            if items:
                logger.info(f"✓ {pdf_path.name} - {len(items)} itens extraídos")
                # Log dos tipos de materiais encontrados
                tipos = set(item.get('tipo_material', 'Outros') for item in items)
                logger.info(f"  Materiais: {', '.join(tipos)}")
            else:
                logger.warning(f"⚠ {pdf_path.name} - Nenhum item encontrado")
            
            return items
            
        except Exception as e:
            logger.error(f"❌ Erro ao processar {pdf_path.name}: {e}")
            logger.error(traceback.format_exc())
            return []
    
    def process_all_pdfs(self) -> Tuple[List[Dict], List[str]]:
        """Processa todos os PDFs da pasta input"""
        all_items = []
        failed_files = []
        
        pdf_files = sorted(self.input_dir.glob("*.pdf"))
        
        if not pdf_files:
            logger.warning("Nenhum arquivo PDF encontrado na pasta 'input'")
            return all_items, failed_files
        
        logger.info(f"Encontrados {len(pdf_files)} arquivos PDF para processar")
        
        for pdf_path in pdf_files:
            try:
                items = self.process_pdf(pdf_path)
                if items:
                    all_items.extend(items)
                    # Mover arquivo processado
                    processed_path = self.processed_dir / pdf_path.name
                    pdf_path.rename(processed_path)
                    logger.info(f"Arquivo movido para: {processed_path}")
                else:
                    failed_files.append(pdf_path.name)
            
            except Exception as e:
                failed_files.append(pdf_path.name)
                logger.error(f"Falha geral ao processar {pdf_path.name}: {e}")
        
        return all_items, failed_files
    
    def save_to_excel(self, items: List[Dict], filename: str = None) -> pathlib.Path:
        """Salva dados em planilha Excel com estatísticas"""
        if not filename:
            timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"nfes_processadas_{timestamp}.xlsx"
        
        output_path = self.output_dir / filename
        
        try:
            df = pd.DataFrame(items)
            
            # Reordenar colunas para melhor visualização
            priority_columns = [
                "arquivo_origem", "numero_nfe", "serie_nfe", "data_emissao",
                "emit_razao_social", "emit_cnpj", "emit_uf", "emit_municipio",
                "dest_razao_social", "dest_cnpj", "dest_uf", "dest_municipio",
                "valor_total_nf", "codigo_item", "descricao", "tipo_material",
                "quantidade", "valor_unit", "valor_total", "ncm", "cfop"
            ]
            
            # Organizar colunas
            existing_cols = [col for col in priority_columns if col in df.columns]
            other_cols = [col for col in df.columns if col not in priority_columns]
            final_columns = existing_cols + other_cols
            
            df = df[final_columns]
            
            # Salvar com múltiplas abas
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Aba principal com todos os dados
                df.to_excel(writer, sheet_name='Todos_os_Dados', index=False)
                
                # Aba de resumo por tipo de material
                if 'tipo_material' in df.columns:
                    resumo_material = df.groupby('tipo_material').agg({
                        'quantidade': 'sum',
                        'valor_total': 'sum',
                        'arquivo_origem': 'count'
                    }).rename(columns={'arquivo_origem': 'qtd_registros'})
                    resumo_material.to_excel(writer, sheet_name='Resumo_Materiais')
                
                # Aba de resumo por empresa emitente
                if 'emit_razao_social' in df.columns:
                    resumo_emit = df.groupby(['emit_razao_social', 'emit_uf']).agg({
                        'valor_total_nf': 'sum',
                        'numero_nfe': 'count'
                    }).rename(columns={'numero_nfe': 'qtd_nfes'})
                    resumo_emit.to_excel(writer, sheet_name='Resumo_Emitentes')
                
                # Ajustar largura das colunas na aba principal
                worksheet = writer.sheets['Todos_os_Dados']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            logger.info(f"✔ Planilha salva: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Erro ao salvar planilha: {e}")
            raise
    
    def run(self):
        """Executa o processamento completo"""
        logger.info("="*50)
        logger.info("INICIANDO PROCESSAMENTO DE NFes COM API CNPJ")
        logger.info("="*50)
        
        # Processar todos os PDFs
        all_items, failed_files = self.process_all_pdfs()
        
        # Relatório final
        logger.info("\n" + "="*50)
        logger.info("RELATÓRIO FINAL")
        logger.info("="*50)
        
        if all_items:
            # Salvar planilha
            output_file = self.save_to_excel(all_items)
            
            logger.info(f"✅ SUCESSO!")
            logger.info(f"   - {len(all_items)} itens processados")
            logger.info(f"   - Arquivo gerado: {output_file}")
            
            # Estatísticas detalhadas
            df = pd.DataFrame(all_items)
            nfes_processadas = df['numero_nfe'].nunique()
            valor_total = df['valor_total'].sum() if 'valor_total' in df.columns else 0
            
            logger.info(f"   - {nfes_processadas} NFes únicas processadas")
            logger.info(f"   - Valor total: R$ {valor_total:,.2f}")
            
            # Estatísticas por tipo de material
            if 'tipo_material' in df.columns:
                materiais = df['tipo_material'].value_counts()
                logger.info("\n📊 DISTRIBUIÇÃO POR TIPO DE MATERIAL:")
                for material, qtd in materiais.items():
                    logger.info(f"   - {material}: {qtd} itens")
            
            # Consultas CNPJ realizadas
            logger.info(f"\n🏢 CNPJs consultados: {len(self.cache_cnpj)}")
        
        else:
            logger.warning("❌ Nenhum dado foi extraído dos PDFs")
        
        if failed_files:
            logger.warning(f"\n⚠ Arquivos não processados ({len(failed_files)}):")
            for filename in failed_files:
                logger.warning(f"   - {filename}")
        
        logger.info("\nProcessamento concluído!")


def main():
    """Função principal"""
    try:
        processor = NFeProcessor()
        processor.run()
    except KeyboardInterrupt:
        logger.info("\nProcessamento interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()