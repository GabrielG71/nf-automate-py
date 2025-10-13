import re
import pathlib
import logging
import datetime as dt
import time
import requests
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional, Tuple
import tkinter as tk
from tkinter import ttk, scrolledtext
import threading
import fitz
import pdfplumber
import pandas as pd
from decimal import Decimal, InvalidOperation

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('logs/nfe_processor.log', encoding='utf-8')]
)
logger = logging.getLogger(__name__)


class TextHandler(logging.Handler):
    """Handler customizado para exibir logs na interface gráfica"""
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)
        self.text_widget.configure(state='normal')
        self.text_widget.insert(tk.END, msg + '\n')
        self.text_widget.see(tk.END)
        self.text_widget.configure(state='disabled')


class NFeProcessor:
    """Processador de Notas Fiscais Eletrônicas (PDF e XML)"""
    
    # Namespace padrão do XML da NFe
    NFE_NAMESPACE = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
    
    def __init__(self, log_widget=None):
        self.base_dir = pathlib.Path(__file__).parent
        self.input_dir = self.base_dir / "input"
        self.output_dir = self.base_dir / "output"
        self.processed_dir = self.base_dir / "processed"
        self.logs_dir = self.base_dir / "logs"
        
        for dir_path in [self.input_dir, self.output_dir, self.processed_dir, self.logs_dir]:
            dir_path.mkdir(exist_ok=True)
        
        self.cache_cnpj: Dict[str, Optional[Dict]] = {}
        self._setup_patterns()
        
        if log_widget:
            text_handler = TextHandler(log_widget)
            text_handler.setFormatter(
                logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            )
            logger.addHandler(text_handler)
    
    def _setup_patterns(self):
        """Configura padrões regex para extração de dados"""
        self.patterns = {
            'decimal_trans': str.maketrans({".": "", ",": "."}),
            'cnpj': re.compile(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}"),
            'data_emissao': re.compile(r"EMISS[ÃA]O[:\s]*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.I),
            'numero_nfe': re.compile(r"NF-e\s+N[ºº°]\s*(\d{1,9})", re.I),
            'numero_simples': re.compile(r"N[ºº°]\s*(\d+)", re.I),
        }
    
    def _validar_cnpj(self, cnpj: str) -> bool:
        """Valida CNPJ com cálculo de dígitos verificadores"""
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
        """Consulta dados do CNPJ via API com cache"""
        if not cnpj:
            return None
        
        cnpj_limpo = re.sub(r'[^\d]', '', cnpj)
        
        if not self._validar_cnpj(cnpj_limpo):
            return None
        
        if cnpj_limpo in self.cache_cnpj:
            return self.cache_cnpj[cnpj_limpo]
        
        try:
            time.sleep(0.3)
            response = requests.get(
                f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_limpo}",
                timeout=20
            )
            
            if response.status_code == 200:
                dados = response.json()
                resultado = {'razao_social': dados.get('razao_social', '').strip().upper()}
                self.cache_cnpj[cnpj_limpo] = resultado
                return resultado
            else:
                self.cache_cnpj[cnpj_limpo] = None
        
        except Exception as e:
            logger.error(f"Erro ao consultar CNPJ {cnpj_limpo}: {e}")
            self.cache_cnpj[cnpj_limpo] = None
        
        return None
    
    def identificar_tipo_material(self, descricao: str) -> Optional[str]:
        """Identifica tipo de material reciclável pela descrição"""
        if not descricao:
            return None
        
        desc_lower = descricao.lower()
        
        materiais = {
            'PLASTICO': ['plastico', 'plástico', 'pet', 'pvc', 'pead', 'pebd', 'pp', 'ps',
                        'polietileno', 'polipropileno', 'poliestireno'],
            'METAL': ['metal', 'ferro', 'aco', 'aço', 'ferroso', 'inox', 'inoxidavel',
                     'aluminio', 'alumínio', 'cobre', 'bronze', 'latao', 'latão',
                     'zinco', 'chumbo', 'sucata metalica', 'sucata metálica'],
            'VIDRO': ['vidro', 'cristal', 'garrafa vidro'],
            'PAPEL': ['papel', 'papelao', 'papelão', 'cartao', 'cartão']
        }
        
        for tipo, palavras_chave in materiais.items():
            if any(palavra in desc_lower for palavra in palavras_chave):
                return tipo
        
        return None
    
    def to_float(self, text: str) -> Optional[float]:
        """Converte string para float tratando formato brasileiro"""
        if not text or not isinstance(text, str):
            return None
        
        try:
            clean_text = re.sub(r'[^\d,.]', '', text.strip())
            if not clean_text:
                return None
            return float(clean_text.translate(self.patterns['decimal_trans']))
        except (ValueError, TypeError):
            return None
    
    # ==================== PROCESSAMENTO XML ====================
    
    def process_xml(self, xml_path: pathlib.Path) -> List[Dict]:
        """Processa arquivo XML da NFe"""
        logger.info(f"Processando XML: {xml_path.name}")
        
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            # Busca todas as notas no XML (pode haver mais de uma)
            notas = root.findall('.//nfe:NFe', self.NFE_NAMESPACE)
            
            all_items = []
            for nota in notas:
                items = self._extract_items_from_xml_note(nota)
                all_items.extend(items)
            
            if all_items:
                tipos = set(item.get('tipo_material', '') for item in all_items)
                logger.info(f"✓ {xml_path.name} - {len(all_items)} itens ({', '.join(tipos)})")
            
            return all_items
        
        except Exception as e:
            logger.error(f"❌ Erro ao processar XML {xml_path.name}: {e}")
            return []
    
    def _extract_items_from_xml_note(self, nota_element) -> List[Dict]:
        """Extrai itens de uma nota específica do XML"""
        items = []
        
        try:
            # Extrai metadados da nota
            metadata = self._extract_metadata_from_xml(nota_element)
            
            # Extrai produtos
            produtos = nota_element.findall('.//nfe:det', self.NFE_NAMESPACE)
            
            for produto in produtos:
                prod = produto.find('.//nfe:prod', self.NFE_NAMESPACE)
                if prod is None:
                    continue
                
                descricao = self._get_xml_text(prod, 'nfe:xProd')
                tipo_material = self.identificar_tipo_material(descricao)
                
                if tipo_material:
                    item = {
                        'descricao': descricao,
                        'quantidade': self._get_xml_float(prod, 'nfe:qCom'),
                        'valor': self._get_xml_float(prod, 'nfe:vProd'),
                        'tipo_material': tipo_material
                    }
                    item.update(metadata)
                    items.append(item)
        
        except Exception as e:
            logger.error(f"Erro ao extrair itens do XML: {e}")
        
        return items
    
    def _extract_metadata_from_xml(self, nota_element) -> Dict:
        """Extrai metadados da nota fiscal do XML"""
        metadata = {
            "numero_nfe": "",
            "data_emissao": None,
            "emit_razao_social": "",
            "emit_cnpj": "",
            "dest_razao_social": "",
            "dest_cnpj": ""
        }
        
        try:
            # Informações da nota
            ide = nota_element.find('.//nfe:ide', self.NFE_NAMESPACE)
            if ide is not None:
                metadata["numero_nfe"] = self._get_xml_text(ide, 'nfe:nNF')
                
                data_emissao = self._get_xml_text(ide, 'nfe:dhEmi') or self._get_xml_text(ide, 'nfe:dEmi')
                if data_emissao:
                    try:
                        if 'T' in data_emissao:
                            metadata["data_emissao"] = dt.datetime.fromisoformat(
                                data_emissao.split('T')[0]
                            ).date()
                        else:
                            metadata["data_emissao"] = dt.datetime.strptime(
                                data_emissao, "%Y-%m-%d"
                            ).date()
                    except ValueError:
                        pass
            
            # Emitente
            emit = nota_element.find('.//nfe:emit', self.NFE_NAMESPACE)
            if emit is not None:
                metadata["emit_cnpj"] = self._format_cnpj(self._get_xml_text(emit, 'nfe:CNPJ'))
                metadata["emit_razao_social"] = self._get_xml_text(emit, 'nfe:xNome')
            
            # Destinatário
            dest = nota_element.find('.//nfe:dest', self.NFE_NAMESPACE)
            if dest is not None:
                cnpj_dest = self._get_xml_text(dest, 'nfe:CNPJ')
                if not cnpj_dest:
                    cnpj_dest = self._get_xml_text(dest, 'nfe:CPF')
                metadata["dest_cnpj"] = self._format_cnpj(cnpj_dest)
                metadata["dest_razao_social"] = self._get_xml_text(dest, 'nfe:xNome')
        
        except Exception as e:
            logger.error(f"Erro ao extrair metadados do XML: {e}")
        
        return metadata
    
    def _get_xml_text(self, element, tag: str) -> str:
        """Obtém texto de um elemento XML"""
        child = element.find(tag, self.NFE_NAMESPACE)
        return child.text.strip().upper() if child is not None and child.text else ""
    
    def _get_xml_float(self, element, tag: str) -> Optional[float]:
        """Obtém valor float de um elemento XML"""
        text = self._get_xml_text(element, tag)
        return self.to_float(text) if text else None
    
    def _format_cnpj(self, cnpj: str) -> str:
        """Formata CNPJ no padrão XX.XXX.XXX/XXXX-XX"""
        if not cnpj:
            return ""
        cnpj_limpo = re.sub(r'[^\d]', '', cnpj)
        if len(cnpj_limpo) == 14:
            return f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:14]}"
        elif len(cnpj_limpo) == 11:  # CPF
            return f"{cnpj_limpo[:3]}.{cnpj_limpo[3:6]}.{cnpj_limpo[6:9]}-{cnpj_limpo[9:11]}"
        return cnpj
    
    # ==================== PROCESSAMENTO PDF ====================
    
    def extract_pdf_text(self, pdf_path: pathlib.Path) -> str:
        """Extrai texto completo do PDF"""
        try:
            with fitz.open(pdf_path) as doc:
                return "\n".join(page.get_text("text") for page in doc)
        except Exception as e:
            logger.error(f"Erro ao extrair texto do PDF {pdf_path}: {e}")
            return ""
    
    def process_pdf(self, pdf_path: pathlib.Path) -> List[Dict]:
        """Processa arquivo PDF (pode conter múltiplas NF-e)"""
        logger.info(f"Processando PDF: {pdf_path.name}")
        
        try:
            all_items = []
            
            # Verifica se há múltiplas notas fiscais no PDF
            with fitz.open(pdf_path) as doc:
                num_pages = len(doc)
                
                # Agrupa páginas por nota fiscal
                nota_groups = self._group_pages_by_nfe(doc)
                
                logger.info(f"Detectadas {len(nota_groups)} nota(s) fiscal(is) em {num_pages} página(s)")
                
                for i, pages in enumerate(nota_groups, 1):
                    items = self._process_nfe_pages(doc, pages)
                    if items:
                        logger.info(f"  NF-e {i}: {len(items)} itens extraídos")
                        all_items.extend(items)
            
            if all_items:
                tipos = set(item.get('tipo_material', '') for item in all_items)
                logger.info(f"✓ {pdf_path.name} - {len(all_items)} itens ({', '.join(tipos)})")
            
            return all_items
        
        except Exception as e:
            logger.error(f"❌ Erro ao processar PDF {pdf_path.name}: {e}")
            return []
    
    def _group_pages_by_nfe(self, doc) -> List[List[int]]:
        """Agrupa páginas do PDF por nota fiscal"""
        groups = []
        current_group = []
        
        for page_num in range(len(doc)):
            page = doc[page_num]
            text = page.get_text("text")
            
            # Detecta início de nova nota fiscal
            if re.search(r"NF-e\s+N[ºº°]|DANFE|NOTA\s+FISCAL\s+ELETR[OÔ]NICA", text, re.I):
                if current_group:
                    groups.append(current_group)
                current_group = [page_num]
            elif current_group:
                current_group.append(page_num)
        
        if current_group:
            groups.append(current_group)
        
        # Se não detectou nenhuma NF-e, trata todo o PDF como uma nota
        if not groups:
            groups = [[i for i in range(len(doc))]]
        
        return groups
    
    def _process_nfe_pages(self, doc, page_numbers: List[int]) -> List[Dict]:
        """Processa um grupo de páginas que formam uma NF-e"""
        # Extrai texto de todas as páginas da nota
        text = "\n".join(doc[page_num].get_text("text") for page_num in page_numbers)
        
        # Extrai metadados
        metadata = self.extract_metadata(text)
        
        # Tenta extrair itens com pdfplumber primeiro
        items = []
        for page_num in page_numbers:
            items.extend(self._extract_items_from_page(doc.name, page_num))
        
        # Se não encontrou itens, tenta com regex
        if not items:
            items = self.extract_items_regex(text)
        
        # Adiciona metadados a todos os itens
        for item in items:
            item.update(metadata)
        
        return items
    
    def _extract_items_from_page(self, pdf_path: str, page_num: int) -> List[Dict]:
        """Extrai itens de uma página específica do PDF"""
        items = []
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                if page_num >= len(pdf.pages):
                    return items
                
                page = pdf.pages[page_num]
                tables = page.extract_tables()
                
                for table in tables:
                    if not table:
                        continue
                    
                    header_found = False
                    for row in table:
                        if not row:
                            continue
                        
                        # Identifica cabeçalho da tabela
                        if not header_found:
                            row_text = ' '.join(str(cell or '') for cell in row).upper()
                            if any(keyword in row_text for keyword in ['PROD', 'DESCRI', 'ITEM']):
                                header_found = True
                            continue
                        
                        # Processa linha de produto
                        if len(row) >= 9 and row[2] and re.fullmatch(r"\d{8}", str(row[2]).strip()):
                            descricao = str(row[1] or "").strip()
                            quantidade = self.to_float(str(row[6] or "").strip())
                            valor_total = self.to_float(str(row[8] or "").strip())
                            
                            tipo_material = self.identificar_tipo_material(descricao)
                            
                            if tipo_material:
                                items.append({
                                    'descricao': descricao,
                                    'quantidade': quantidade,
                                    'valor': valor_total,
                                    'tipo_material': tipo_material
                                })
        
        except Exception as e:
            logger.error(f"Erro ao extrair itens da página {page_num}: {e}")
        
        return items
    
    def extract_cnpjs(self, text: str) -> Tuple[str, str]:
        """Extrai CNPJs do texto"""
        cnpjs = []
        patterns = [
            r'CNPJ\s*/\s*CPF[:\s]*(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2})',
            r'(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2})',
            r'(\d{14})'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                cnpj_limpo = re.sub(r'[^\d]', '', match)
                if len(cnpj_limpo) == 14 and self._validar_cnpj(cnpj_limpo):
                    cnpj_fmt = self._format_cnpj(cnpj_limpo)
                    if cnpj_fmt not in cnpjs:
                        cnpjs.append(cnpj_fmt)
        
        return (cnpjs[0] if cnpjs else "", cnpjs[1] if len(cnpjs) > 1 else "")
    
    def extract_metadata(self, text: str) -> Dict:
        """Extrai metadados da nota fiscal do texto"""
        metadata = {
            "numero_nfe": "",
            "data_emissao": None,
            "emit_razao_social": "",
            "emit_cnpj": "",
            "dest_razao_social": "",
            "dest_cnpj": ""
        }
        
        # Número da NF-e
        nfe_match = self.patterns['numero_nfe'].search(text)
        if nfe_match:
            metadata["numero_nfe"] = nfe_match.group(1)
        else:
            num_match = self.patterns['numero_simples'].search(text)
            if num_match:
                metadata["numero_nfe"] = num_match.group(1)
        
        # Data de emissão
        data_match = self.patterns['data_emissao'].search(text)
        if data_match:
            try:
                metadata["data_emissao"] = dt.datetime.strptime(
                    data_match.group(1), "%d/%m/%Y"
                ).date()
            except ValueError:
                pass
        
        # CNPJs
        cnpj_emit, cnpj_dest = self.extract_cnpjs(text)
        metadata["emit_cnpj"] = cnpj_emit
        metadata["dest_cnpj"] = cnpj_dest
        
        # Consulta razão social via API
        if cnpj_emit:
            dados_emit = self.consultar_cnpj_api(cnpj_emit)
            if dados_emit:
                metadata["emit_razao_social"] = dados_emit.get('razao_social', '')
        
        if cnpj_dest:
            dados_dest = self.consultar_cnpj_api(cnpj_dest)
            if dados_dest:
                metadata["dest_razao_social"] = dados_dest.get('razao_social', '')
        
        return metadata
    
    def extract_items_regex(self, text: str) -> List[Dict]:
        """Extrai itens usando expressões regulares (fallback)"""
        items = []
        
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
            data = match.groupdict()
            tipo_material = self.identificar_tipo_material(data.get('descricao', ''))
            
            if tipo_material:
                items.append({
                    'descricao': data['descricao'],
                    'quantidade': self.to_float(data['quantidade']),
                    'valor': self.to_float(data['valor_total']),
                    'tipo_material': tipo_material
                })
        
        return items
    
    # ==================== PROCESSAMENTO GERAL ====================
    
    def process_file(self, file_path: pathlib.Path) -> List[Dict]:
        """Processa arquivo (PDF ou XML)"""
        if file_path.suffix.lower() == '.xml':
            return self.process_xml(file_path)
        elif file_path.suffix.lower() == '.pdf':
            return self.process_pdf(file_path)
        else:
            logger.warning(f"Tipo de arquivo não suportado: {file_path.name}")
            return []
    
    def process_all_files(self) -> Tuple[List[Dict], List[str]]:
        """Processa todos os arquivos PDF e XML da pasta input"""
        all_items = []
        failed_files = []
        
        files = sorted(list(self.input_dir.glob("*.pdf")) + list(self.input_dir.glob("*.xml")))
        
        if not files:
            logger.warning("Nenhum arquivo PDF ou XML encontrado na pasta 'input'")
            return all_items, failed_files
        
        logger.info(f"Processando {len(files)} arquivo(s)")
        
        for file_path in files:
            try:
                items = self.process_file(file_path)
                if items:
                    all_items.extend(items)
                    processed_path = self.processed_dir / file_path.name
                    file_path.rename(processed_path)
                    logger.info(f"Movido para 'processed': {file_path.name}")
                else:
                    failed_files.append(file_path.name)
            except Exception as e:
                failed_files.append(file_path.name)
                logger.error(f"Falha ao processar {file_path.name}: {e}")
        
        return all_items, failed_files
    
    def save_to_excel(self, items: List[Dict], filename: str = None) -> pathlib.Path:
        """Salva itens processados em planilha Excel"""
        if not filename:
            timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"materiais_reciclaveis_{timestamp}.xlsx"
        
        output_path = self.output_dir / filename
        
        try:
            df = pd.DataFrame(items)
            
            columns_order = [
                "emit_razao_social", "emit_cnpj", "dest_razao_social", "dest_cnpj",
                "numero_nfe", "data_emissao", "quantidade", "valor", "tipo_material", "descricao"
            ]
            
            existing_cols = [col for col in columns_order if col in df.columns]
            df = df[existing_cols]
            
            df = df.rename(columns={
                'emit_razao_social': 'Razão Social Emitente',
                'emit_cnpj': 'CNPJ Emitente',
                'dest_razao_social': 'Razão Social Destinatário',
                'dest_cnpj': 'CNPJ Destinatário',
                'numero_nfe': 'Número NFe',
                'data_emissao': 'Data Emissão',
                'quantidade': 'Quantidade',
                'valor': 'Valor Total',
                'tipo_material': 'Tipo Material',
                'descricao': 'Descrição'
            })
            
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Materiais_Reciclaveis', index=False)
                
                # Aba de resumo
                resumo = df.groupby('Tipo Material').agg({
                    'Quantidade': 'sum',
                    'Valor Total': 'sum',
                    'Número NFe': 'count'
                }).rename(columns={'Número NFe': 'Qtd Registros'})
                resumo.to_excel(writer, sheet_name='Resumo_por_Material')
                
                # Ajusta largura das colunas
                worksheet = writer.sheets['Materiais_Reciclaveis']
                for column in worksheet.columns:
                    max_length = max(len(str(cell.value or '')) for cell in column)
                    worksheet.column_dimensions[column[0].column_letter].width = min(max_length + 2, 50)
            
            logger.info(f"✔ Planilha salva: {output_path}")
            return output_path
        
        except Exception as e:
            logger.error(f"Erro ao salvar planilha: {e}")
            raise
    
    def run(self):
        """Executa o processamento completo"""
        logger.info("=" * 60)
        logger.info("INICIANDO PROCESSAMENTO - MATERIAIS RECICLÁVEIS")
        logger.info("=" * 60)
        
        all_items, failed_files = self.process_all_files()
        
        if all_items:
            output_file = self.save_to_excel(all_items)
            
            df = pd.DataFrame(all_items)
            materiais = df['tipo_material'].value_counts()
            valor_total = df['valor'].sum() if 'valor' in df.columns else 0
            
            logger.info("=" * 60)
            logger.info(f"✅ PROCESSAMENTO CONCLUÍDO")
            logger.info(f"Total de itens: {len(all_items)}")
            logger.info(f"Valor total: R$ {valor_total:,.2f}")
            logger.info("-" * 60)
            for material, qtd in materiais.items():
                logger.info(f"  {material}: {qtd} itens")
            logger.info("=" * 60)
        else:
            logger.warning("❌ Nenhum material reciclável encontrado nos arquivos")
        
        if failed_files:
            logger.warning(f"Arquivos sem materiais: {', '.join(failed_files)}")


class NFeProcessorGUI:
    """Interface gráfica para o processador de NFe"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("NFe Processor - Materiais Recicláveis v2.0")
        self.root.geometry("700x500")
        
        self.main_frame = ttk.Frame(self.root, padding="10")
        self.main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Título
        title_label = ttk.Label(
            self.main_frame,
            text="Processador de Notas Fiscais (NF-e)",
            font=("Helvetica", 14, "bold")
        )
        title_label.grid(row=0, column=0, columnspan=2, pady=10)
        
        # Subtítulo
        subtitle_label = ttk.Label(
            self.main_frame,
            text="Suporta arquivos PDF e XML | Múltiplas NF-e por arquivo",
            font=("Helvetica", 9)
        )
        subtitle_label.grid(row=1, column=0, columnspan=2, pady=5)
        
        # Frame de informações
        info_frame = ttk.LabelFrame(self.main_frame, text="Instruções", padding="10")
        info_frame.grid(row=2, column=0, columnspan=2, pady=10, sticky=(tk.W, tk.E))
        
        instructions = (
            "1. Coloque os arquivos PDF ou XML na pasta 'input'\n"
            "2. Clique em 'Processar Arquivos'\n"
            "3. Aguarde o processamento\n"
            "4. Os resultados serão salvos na pasta 'output'\n"
            "5. Arquivos processados vão para 'processed'"
        )
        ttk.Label(info_frame, text=instructions, justify=tk.LEFT).pack()
        
        # Área de log
        log_frame = ttk.LabelFrame(self.main_frame, text="Log de Processamento", padding="5")
        log_frame.grid(row=3, column=0, columnspan=2, pady=10, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        self.log_text = scrolledtext.ScrolledText(
            log_frame,
            height=15,
            width=80,
            state='disabled',
            wrap=tk.WORD
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # Frame de botões
        button_frame = ttk.Frame(self.main_frame)
        button_frame.grid(row=4, column=0, columnspan=2, pady=10)
        
        self.process_button = ttk.Button(
            button_frame,
            text="Processar Arquivos",
            command=self.start_processing,
            width=20
        )
        self.process_button.pack(side=tk.LEFT, padx=5)
        
        self.clear_button = ttk.Button(
            button_frame,
            text="Limpar Log",
            command=self.clear_log,
            width=15
        )
        self.clear_button.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            button_frame,
            text="Sair",
            command=self.root.quit,
            width=15
        ).pack(side=tk.LEFT, padx=5)
        
        # Barra de progresso
        self.progress = ttk.Progressbar(
            self.main_frame,
            mode='indeterminate',
            length=300
        )
        self.progress.grid(row=5, column=0, columnspan=2, pady=5)
        
        # Configurações de grid
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        self.main_frame.columnconfigure(0, weight=1)
        self.main_frame.rowconfigure(3, weight=1)
        
        self.is_processing = False
    
    def clear_log(self):
        """Limpa a área de log"""
        self.log_text.configure(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state='disabled')
    
    def start_processing(self):
        """Inicia o processamento em thread separada"""
        if self.is_processing:
            return
        
        self.is_processing = True
        self.process_button.configure(state='disabled')
        self.clear_button.configure(state='disabled')
        self.progress.start(10)
        
        self.clear_log()
        
        threading.Thread(target=self.run_processing, daemon=True).start()
    
    def run_processing(self):
        """Executa o processamento"""
        try:
            processor = NFeProcessor(self.log_text)
            processor.run()
        except Exception as e:
            logger.error(f"Erro fatal: {e}")
        finally:
            self.is_processing = False
            self.process_button.configure(state='normal')
            self.clear_button.configure(state='normal')
            self.progress.stop()


def main():
    """Função principal"""
    root = tk.Tk()
    app = NFeProcessorGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()