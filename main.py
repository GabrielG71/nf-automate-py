# -*- coding: utf-8 -*-
"""
NFe PDF Simplificado - Processador Otimizado com Interface Gráfica
===============================================================

Processa PDFs de NFe e extrai dados essenciais para planilha Excel.
Foca apenas em materiais recicláveis específicos.

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
import tkinter as tk
from tkinter import ttk, scrolledtext
import threading

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
    ]
)
logger = logging.getLogger(__name__)

# Handler para exibir logs na interface gráfica
class TextHandler(logging.Handler):
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)
        self.text_widget.configure(state='normal')
        self.text_widget.insert(tk.END, msg + '\n')
        self.text_widget.see(tk.END)
        self.text_widget.configure(state='disabled')

class NFeProcessorSimplified:
    """Classe simplificada para processamento de NFes focada em materiais recicláveis"""
    
    def __init__(self, log_widget=None):
        self.base_dir = pathlib.Path(__file__).parent
        
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
        
        # Adicionar handler de log para interface gráfica, se fornecido
        if log_widget:
            text_handler = TextHandler(log_widget)
            text_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            logger.addHandler(text_handler)
    
    def _setup_patterns(self):
        """Configura padrões regex para extração"""
        self.patterns = {
            'decimal_trans': str.maketrans({".": "", ",": "."}),
            'cnpj': re.compile(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}"),
            'data_emissao': re.compile(r"EMISS[ÃA]O[:\s]*([0-9]{2}/[0-9]{2}/[0-9]{4})", re.I),
            'numero_nfe': re.compile(r"NF-e\s+N[ºº°]\s*(\d{1,9})", re.I),
            'numero_simples': re.compile(r"N[ºº°]\s*(\d+)", re.I),
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
            time.sleep(0.3)  # Rate limiting
            logger.info(f"Consultando CNPJ na API: {cnpj_limpo}")
            
            response = requests.get(
                f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_limpo}", 
                timeout=20
            )
            
            if response.status_code == 200:
                dados = response.json()
                resultado = {
                    'razao_social': dados.get('razao_social', '').strip().upper(),
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
    
    def identificar_tipo_material(self, descricao: str) -> Optional[str]:
        """Classifica o tipo de material - APENAS os 4 tipos específicos solicitados"""
        if not descricao:
            return None
            
        desc_lower = descricao.lower()
        
        # PLÁSTICO (PEAD, PET, PS, PP, PEBD, PVC)
        if any(palavra in desc_lower for palavra in [
            'plastico', 'plástico', 'pet', 'pvc', 'pead', 'pebd', 'pp', 'ps',
            'polietileno', 'polipropileno', 'poliestireno'
        ]):
            return 'PLASTICO'
        
        # METAL
        elif any(palavra in desc_lower for palavra in [
            'metal', 'ferro', 'aco', 'aço', 'ferroso', 'inox', 'inoxidavel', 
            'aluminio', 'alumínio', 'cobre', 'bronze', 'latao', 'latão', 
            'zinco', 'chumbo', 'sucata metalica', 'sucata metálica'
        ]):
            return 'METAL'
        
        # VIDRO
        elif any(palavra in desc_lower for palavra in [
            'vidro', 'cristal', 'garrafa vidro'
        ]):
            return 'VIDRO'
        
        # PAPEL (Papelão)
        elif any(palavra in desc_lower for palavra in [
            'papel', 'papelao', 'papelão', 'cartao', 'cartão'
        ]):
            return 'PAPEL'
        
        # Se não for nenhum dos tipos específicos, retorna None (será filtrado)
        return None
    
    def to_float(self, text: str) -> Optional[float]:
        """Converte texto brasileiro para float (1.234,56 -> 1234.56)"""
        if not text or not isinstance(text, str):
            return None
        
        try:
            clean_text = re.sub(r'[^\d,.]', '', text.strip())
            if not clean_text:
                return None
            
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
        """Extrai CNPJs do emitente e destinatário"""
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
                    cnpj_fmt = f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:14]}"
                    if cnpj_fmt not in cnpjs:
                        cnpjs.append(cnpj_fmt)
        
        return (cnpjs[0] if cnpjs else "", cnpjs[1] if len(cnpjs) > 1 else "")
    
    def extract_metadata(self, text: str) -> Dict[str, any]:
        """Extrai apenas os metadados essenciais da NFe"""
        metadata = {
            "numero_nfe": "",
            "data_emissao": None,
            "emit_razao_social": "",
            "emit_cnpj": "",
            "dest_razao_social": "",
            "dest_cnpj": ""
        }
        
        # Extrair número da NFe
        nfe_match = self.patterns['numero_nfe'].search(text)
        if nfe_match:
            metadata["numero_nfe"] = nfe_match.group(1)
        else:
            num_match = self.patterns['numero_simples'].search(text)
            if num_match:
                metadata["numero_nfe"] = num_match.group(1)
        
        # Extrair data de emissão
        data_match = self.patterns['data_emissao'].search(text)
        if data_match:
            try:
                metadata["data_emissao"] = dt.datetime.strptime(
                    data_match.group(1), "%d/%m/%Y"
                ).date()
            except ValueError:
                pass
        
        # Extrair CNPJs
        cnpj_emit, cnpj_dest = self.extract_cnpjs(text)
        metadata["emit_cnpj"] = cnpj_emit
        metadata["dest_cnpj"] = cnpj_dest
        
        # Consultar razão social dos CNPJs
        if cnpj_emit:
            dados_emit = self.consultar_cnpj_api(cnpj_emit)
            if dados_emit:
                metadata["emit_razao_social"] = dados_emit.get('razao_social', '')
        
        if cnpj_dest:
            dados_dest = self.consultar_cnpj_api(cnpj_dest)
            if dados_dest:
                metadata["dest_razao_social"] = dados_dest.get('razao_social', '')
        
        return metadata
    
    def extract_items_pdfplumber(self, pdf_path: pathlib.Path) -> List[Dict]:
        """Extrai apenas itens dos materiais específicos usando pdfplumber"""
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
                                
                                # Extrair apenas os campos necessários
                                descricao = str(row[1] or "").strip()
                                quantidade = self.to_float(str(row[6] or "").strip())
                                valor_total = self.to_float(str(row[8] or "").strip())
                                
                                # Classificar material
                                tipo_material = self.identificar_tipo_material(descricao)
                                
                                # Só incluir se for um dos materiais específicos
                                if tipo_material:
                                    item = {
                                        'descricao': descricao,
                                        'quantidade': quantidade,
                                        'valor': valor_total,
                                        'tipo_material': tipo_material
                                    }
                                    items.append(item)
        
        except Exception as e:
            logger.error(f"Erro ao extrair itens com pdfplumber: {e}")
        
        return items
    
    def extract_items_regex(self, text: str) -> List[Dict]:
        """Extrai itens usando regex como fallback - apenas materiais específicos"""
        items = []
        
        # Padrão regex simplificado para linha de item
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
            
            # Classificar material
            tipo_material = self.identificar_tipo_material(data.get('descricao', ''))
            
            # Só incluir se for um dos materiais específicos
            if tipo_material:
                item = {
                    'descricao': data['descricao'],
                    'quantidade': self.to_float(data['quantidade']),
                    'valor': self.to_float(data['valor_total']),
                    'tipo_material': tipo_material
                }
                items.append(item)
        
        return items
    
    def process_pdf(self, pdf_path: pathlib.Path) -> List[Dict]:
        """Processa um único PDF e retorna lista de itens filtrados"""
        logger.info(f"Processando: {pdf_path.name}")
        
        try:
            # Extrair texto
            text = self.extract_pdf_text(pdf_path)
            if not text:
                logger.warning(f"Não foi possível extrair texto de {pdf_path.name}")
                return []
            
            # Extrair metadados essenciais
            metadata = self.extract_metadata(text)
            
            # Extrair itens filtrados
            items = self.extract_items_pdfplumber(pdf_path)
            if not items:
                items = self.extract_items_regex(text)
            
            # Adicionar metadados a cada item
            for item in items:
                item.update(metadata)
            
            if items:
                logger.info(f"✓ {pdf_path.name} - {len(items)} itens de materiais específicos extraídos")
                tipos = set(item.get('tipo_material', '') for item in items)
                logger.info(f"  Materiais encontrados: {', '.join(tipos)}")
            else:
                logger.warning(f"⚠ {pdf_path.name} - Nenhum material específico encontrado")
            
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
        """Salva dados simplificados em planilha Excel"""
        if not filename:
            timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"materiais_reciclaveis_{timestamp}.xlsx"
        
        output_path = self.output_dir / filename
        
        try:
            df = pd.DataFrame(items)
            
            # Definir ordem das colunas conforme solicitado
            columns_order = [
                "emit_razao_social",      # Razão social do emitente
                "emit_cnpj",              # CNPJ do emitente  
                "dest_razao_social",      # Razão social do destinatário
                "dest_cnpj",              # CNPJ do destinatário
                "numero_nfe",             # Número da nota
                "data_emissao",           # Data
                "quantidade",             # Quantidade
                "valor",                  # Valor
                "tipo_material",          # Tipo do material
                "descricao"               # Descrição (adicional para contexto)
            ]
            
            # Reorganizar colunas
            existing_cols = [col for col in columns_order if col in df.columns]
            df = df[existing_cols]
            
            # Renomear colunas para nomes mais limpos
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
            
            # Salvar planilha
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Materiais_Reciclaveis', index=False)
                
                # Aba de resumo por tipo de material
                resumo = df.groupby('Tipo Material').agg({
                    'Quantidade': 'sum',
                    'Valor Total': 'sum',
                    'Número NFe': 'count'
                }).rename(columns={'Número NFe': 'Qtd Registros'})
                resumo.to_excel(writer, sheet_name='Resumo_por_Material')
                
                # Ajustar largura das colunas
                worksheet = writer.sheets['Materiais_Reciclaveis']
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
        """Executa o processamento completo focado em materiais específicos"""
        logger.info("="*50)
        logger.info("PROCESSAMENTO SIMPLIFICADO - MATERIAIS RECICLÁVEIS")
        logger.info("Materiais alvo: PLÁSTICO, METAL, VIDRO, PAPEL")
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
            logger.info(f"   - {len(all_items)} itens de materiais específicos processados")
            logger.info(f"   - Arquivo gerado: {output_file}")
            
            # Estatísticas por tipo de material
            df = pd.DataFrame(all_items)
            materiais = df['tipo_material'].value_counts()
            valor_total = df['valor'].sum() if 'valor' in df.columns else 0
            
            logger.info(f"   - Valor total: R$ {valor_total:,.2f}")
            logger.info("\n📊 MATERIAIS ENCONTRADOS:")
            for material, qtd in materiais.items():
                logger.info(f"   - {material}: {qtd} itens")
            
            logger.info(f"\n🏢 CNPJs consultados: {len(self.cache_cnpj)}")
        
        else:
            logger.warning("❌ Nenhum material específico foi encontrado nos PDFs")
        
        if failed_files:
            logger.warning(f"\n⚠ Arquivos sem materiais específicos ({len(failed_files)}):")
            for filename in failed_files:
                logger.warning(f"   - {filename}")
        
        logger.info("\nProcessamento concluído!")

class NFeProcessorGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("NFe Processor - Materiais Recicláveis")
        self.root.geometry("600x400")
        
        # Frame principal
        self.main_frame = ttk.Frame(self.root, padding="10")
        self.main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Label de título
        ttk.Label(self.main_frame, text="Processador de Notas Fiscais (NF-e)", font=("Helvetica", 12, "bold")).grid(row=0, column=0, columnspan=2, pady=5)
        
        # Área de log
        self.log_text = scrolledtext.ScrolledText(self.main_frame, height=15, width=70, state='disabled')
        self.log_text.grid(row=1, column=0, columnspan=2, pady=10)
        
        # Botão de processar
        self.process_button = ttk.Button(self.main_frame, text="Processar PDFs", command=self.start_processing)
        self.process_button.grid(row=2, column=0, pady=5, sticky=tk.W)
        
        # Botão de sair
        ttk.Button(self.main_frame, text="Sair", command=self.root.quit).grid(row=2, column=1, pady=5, sticky=tk.E)
        
        # Configurar redimensionamento
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        self.main_frame.columnconfigure(0, weight=1)
        self.main_frame.rowconfigure(1, weight=1)
        
        # Estado do processamento
        self.is_processing = False
    
    def start_processing(self):
        if self.is_processing:
            return
        
        self.is_processing = True
        self.process_button.configure(state='disabled')
        
        # Limpar área de log
        self.log_text.configure(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state='disabled')
        
        # Iniciar processamento em uma thread separada
        threading.Thread(target=self.run_processing, daemon=True).start()
    
    def run_processing(self):
        try:
            processor = NFeProcessorSimplified(self.log_text)
            processor.run()
        except Exception as e:
            logger.error(f"Erro fatal: {e}")
            logger.error(traceback.format_exc())
        finally:
            self.is_processing = False
            self.process_button.configure(state='normal')

def main():
    """Função principal com interface gráfica"""
    root = tk.Tk()
    app = NFeProcessorGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()