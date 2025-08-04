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
        """Consulta dados do CNPJ na API do Brasil API"""
        cnpj_limpo = re.sub(r'[^\d]', '', cnpj)
        
        if not self._validar_cnpj(cnpj_limpo):
            return None
            
        if cnpj_limpo in self.cache_cnpj:
            return self.cache_cnpj[cnpj_limpo]
            
        try:
            time.sleep(0.3)  # Rate limiting
            response = requests.get(
                f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_limpo}", 
                timeout=20
            )
            
            if response.status_code == 200:
                dados = response.json()
                resultado = {
                    'razao_social': dados.get('razao_social', '').strip().upper(),
                    'uf': dados.get('uf', '')
                }
                self.cache_cnpj[cnpj_limpo] = resultado
                return resultado
            else:
                self.cache_cnpj[cnpj_limpo] = None
                
        except Exception as e:
            print(f"   ⚠️ Erro ao consultar CNPJ {cnpj_limpo}: {e}")
            self.cache_cnpj[cnpj_limpo] = None
            
        return None

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

    def _extrair_cnpjs(self, texto: str) -> Tuple[str, str]:
        """Extrai CNPJs do emitente e destinatário"""
        cnpjs = []
        
        # Padrões mais específicos para CNPJs em NFes
        patterns = [
            r'CNPJ\s*/\s*CPF[:\s]*(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2})',
            r'(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2})',
            r'(\d{14})'  # CNPJ sem formatação
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, texto, re.IGNORECASE)
            for match in matches:
                cnpj_limpo = re.sub(r'[^\d]', '', match)
                if len(cnpj_limpo) == 14 and self._validar_cnpj(cnpj_limpo):
                    cnpj_fmt = f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:14]}"
                    if cnpj_fmt not in cnpjs:
                        cnpjs.append(cnpj_fmt)
        
        return (cnpjs[0] if cnpjs else "", cnpjs[1] if len(cnpjs) > 1 else "")

    def _extrair_dados_basicos(self, texto: str) -> Dict:
        """Extrai número, data e valor total da NFe"""
        
        # Número NFe - padrões mais robustos
        numero = ""
        patterns_numero = [
            r'NF-e\s*[nN]º?\s*(\d{6,9})',
            r'Nº\s*(\d{6,9})',
            r'(\d{6,9})\s*SÉRIE',
            r'SÉRIE[:\s]*\d+[^0-9]*(\d{6,9})'
        ]
        
        for pattern in patterns_numero:
            match = re.search(pattern, texto, re.IGNORECASE)
            if match:
                numero = match.group(1).zfill(9)  # Pad com zeros à esquerda
                break
        
        # Data de emissão
        data = ""
        patterns_data = [
            r'EMISSÃO[:\s]*(\d{2}/\d{2}/\d{4})',
            r'DATA\s+DA\s+EMISSÃO[:\s]*(\d{2}/\d{2}/\d{4})',
            r'DATA[:\s]*(\d{2}/\d{2}/\d{4})'
        ]
        
        for pattern in patterns_data:
            match = re.search(pattern, texto, re.IGNORECASE)
            if match:
                data = match.group(1)
                break
        
        # Valor total - melhor tratamento de números
        valor = 0.0
        patterns_valor = [
            r'VALOR\s+TOTAL\s+DA\s+NOTA[:\s]*R?\$?\s*([\d\.,]+)',
            r'VALOR\s+TOTAL[:\s]*R?\$?\s*([\d\.,]+)',
            r'TOTAL[:\s]*R?\$?\s*([\d\.,]+)'
        ]
        
        for pattern in patterns_valor:
            match = re.search(pattern, texto, re.IGNORECASE)
            if match:
                try:
                    valor_str = match.group(1)
                    # Trata formato brasileiro: 1.234,56
                    if ',' in valor_str and '.' in valor_str:
                        valor_str = valor_str.replace('.', '').replace(',', '.')
                    elif ',' in valor_str:
                        valor_str = valor_str.replace(',', '.')
                    
                    valor = float(valor_str)
                    break
                except ValueError:
                    continue
        
        return {
            'numero_nf': numero,
            'data_nf': data,
            'valor_total_nf': valor
        }

    def _identificar_tipo_produto(self, descricao: str) -> str:
        """Classifica o tipo de produto baseado na descrição"""
        desc_lower = descricao.lower()
        
        # Classificação hierárquica mais precisa
        if any(palavra in desc_lower for palavra in ['metal', 'ferro', 'aco', 'aço', 'ferroso', 'inox', 'aluminio', 'alumínio']):
            return 'Metal'
        elif any(palavra in desc_lower for palavra in ['papel', 'papelao', 'papelão', 'branco', ' iv']):
            return 'Papel'
        elif any(palavra in desc_lower for palavra in ['plastico', 'plástico', 'pet', 'pvc', 'sacolinha', 'mista', 'misto', 'polietileno']):
            return 'Plastico'
        elif any(palavra in desc_lower for palavra in ['vidro', 'cristal']):
            return 'Vidro'
        elif any(palavra in desc_lower for palavra in ['oleo', 'óleo', 'lubrificante']):
            return 'Oleo'
        elif any(palavra in desc_lower for palavra in ['bateria', 'pilha', 'eletronic']):
            return 'Eletronico'
        
        return "Outros"

    def _extrair_produtos_melhorado(self, texto: str) -> List[Dict]:
        """Extração melhorada de produtos das NFes"""
        produtos = []
        
        # Limpa o texto e separa em linhas
        linhas = [linha.strip() for linha in texto.split('\n') if linha.strip()]
        
        # Encontra a seção de produtos
        inicio_produtos = -1
        for i, linha in enumerate(linhas):
            if 'DADOS DO PRODUTO' in linha.upper() or 'DESCRIÇÃO DO PRODUTO' in linha.upper():
                inicio_produtos = i
                break
        
        if inicio_produtos == -1:
            # Fallback: procura por linhas com produtos diretamente
            inicio_produtos = 0
        
        # Padrões para diferentes estruturas de NFe
        patterns_produto = [
            # Padrão 1: Descrição completa em uma linha com todos os dados
            r'^([A-ZÁÀÂÃÉÊÍÓÔÕÚÇ\s]+?)\s+(\d{3})\s+(5102)\s+(KG)\s+([\d,\.]+)\s+([\d,\.]+)\s+([\d,\.]+)',
            
            # Padrão 2: Linha com código no início
            r'^(\d{3})\s+([A-ZÁÀÂÃÉÊÍÓÔÕÚÇ\s]+?)\s+(5102)\s+(KG)\s+([\d,\.]+)\s+([\d,\.]+)\s+([\d,\.]+)',
            
            # Padrão 3: Descrição seguida de valores separados
            r'^([A-ZÁÀÂÃÉÊÍÓÔÕÚÇ\s]+?)\s+([\d,\.]+)\s+([\d,\.]+)\s+([\d,\.]+)\s+0,00\s+0,00\s+0,00'
        ]
        
        produtos_encontrados = set()
        
        for i in range(inicio_produtos, len(linhas)):
            linha = linhas[i].strip()
            
            # Pula linhas irrelevantes
            if (len(linha) < 20 or 
                any(skip in linha.upper() for skip in ['DADOS ADICIONAIS', 'INFORMAÇÕES', 'CÁLCULO', 'TRANSPORTADOR', 'BASE'])):
                continue
            
            # Verifica se é linha de produto
            if not any(keyword in linha.upper() for keyword in ['APARAS', 'SUCATA', 'RESIDUO', 'RESÍDUO']):
                continue
            
            linha_normalizada = re.sub(r'\s+', ' ', linha)
            produto_extraido = None
            
            # Tenta cada padrão
            for pattern in patterns_produto:
                match = re.match(pattern, linha_normalizada.upper())
                if match:
                    groups = match.groups()
                    
                    if len(groups) >= 6:
                        # Identifica a ordem dos campos baseado no padrão
                        if pattern == patterns_produto[0]:  # Descrição primeiro
                            descricao, codigo, cfop, unidade, qtd, val_unit, val_total = groups[:7]
                        elif pattern == patterns_produto[1]:  # Código primeiro
                            codigo, descricao, cfop, unidade, qtd, val_unit, val_total = groups[:7]
                        else:  # Padrão simples
                            descricao = groups[0]
                            codigo = ""
                            cfop = "5102"
                            unidade = "KG"
                            qtd, val_unit, val_total = groups[1:4]
                        
                        try:
                            produto_extraido = {
                                'codigo_produto': codigo,
                                'descricao_produto': descricao.strip(),
                                'tipo_produto': self._identificar_tipo_produto(descricao),
                                'cfop': cfop,
                                'unidade': unidade,
                                'quantidade': float(qtd.replace(',', '.')),
                                'valor_unitario': float(val_unit.replace(',', '.')),
                                'valor_total_produto': float(val_total.replace(',', '.'))
                            }
                            break
                        except ValueError:
                            continue
            
            # Se não conseguiu com regex, tenta extração manual
            if not produto_extraido:
                produto_extraido = self._extrair_produto_manual(linha_normalizada)
            
            # Adiciona se válido e não duplicado
            if (produto_extraido and 
                produto_extraido['descricao_produto'] and
                produto_extraido['descricao_produto'] not in produtos_encontrados):
                
                produtos_encontrados.add(produto_extraido['descricao_produto'])
                produtos.append(produto_extraido)
        
        return produtos

    def _extrair_produto_manual(self, linha: str) -> Optional[Dict]:
        """Extração manual quando regex falha"""
        linha_upper = linha.upper()
        
        # Verifica se é realmente uma linha de produto
        if not any(keyword in linha_upper for keyword in ['APARAS', 'SUCATA']):
            return None
        
        # Separa palavras e números
        palavras = linha.split()
        numeros = []
        descricao_words = []
        codigo = ""
        
        for palavra in palavras:
            if re.match(r'^[\d,\.]+$', palavra):
                numeros.append(palavra)
            elif re.match(r'^\d{3}$', palavra):  # Código de 3 dígitos
                codigo = palavra
            elif palavra != '5102' and palavra != 'KG':  # Exclui CFOP e unidade
                descricao_words.append(palavra)
        
        if not descricao_words:
            return None
        
        descricao = ' '.join(descricao_words)
        
        # Tenta extrair valores dos números encontrados
        quantidade = valor_unitario = valor_total = 0.0
        
        if len(numeros) >= 3:
            try:
                # Assume ordem: quantidade, valor unitário, valor total
                quantidade = float(numeros[0].replace(',', '.'))
                valor_unitario = float(numeros[1].replace(',', '.'))
                valor_total = float(numeros[2].replace(',', '.'))
            except (ValueError, IndexError):
                pass
        
        return {
            'codigo_produto': codigo,
            'descricao_produto': descricao,
            'tipo_produto': self._identificar_tipo_produto(descricao),
            'cfop': '5102',
            'unidade': 'KG',
            'quantidade': quantidade,
            'valor_unitario': valor_unitario,
            'valor_total_produto': valor_total
        }

    def _extrair_texto_pdf(self, nome_arquivo: str) -> str:
        """Extrai texto do PDF usando PyPDF2 e OCR como fallback"""
        texto = ""
        
        # Primeira tentativa: PyPDF2
        try:
            with open(nome_arquivo, 'rb') as arquivo:
                leitor = PyPDF2.PdfReader(arquivo)
                for pagina in leitor.pages:
                    texto += pagina.extract_text() + "\n"
        except Exception as e:
            print(f"   ⚠️ Erro PyPDF2: {e}")
        
        # Se o texto está muito pequeno, tenta OCR
        if len(texto.strip()) < 300:
            print(f"   🔍 Texto insuficiente ({len(texto)} chars), tentando OCR...")
            try:
                # Converte PDF para imagens
                images = convert_from_path(nome_arquivo, dpi=300, first_page=1, last_page=3)
                
                texto_ocr = ""
                for i, img in enumerate(images):
                    # Configuração otimizada para documentos fiscais
                    config = r'--oem 3 --psm 6 -l por'
                    pagina_texto = pytesseract.image_to_string(img, config=config)
                    texto_ocr += pagina_texto + "\n"
                
                if len(texto_ocr.strip()) > len(texto.strip()):
                    texto = texto_ocr
                    print(f"   ✅ OCR bem-sucedido: {len(texto)} caracteres")
                    
            except Exception as e:
                print(f"   ❌ OCR falhou: {e}")
        
        return texto

    def extrair_dados_pdf(self, nome_arquivo: str) -> List[Dict]:
        """Extrai todos os dados de um PDF de NFe"""
        try:
            print(f"📄 Processando: {nome_arquivo}")
            
            # Extrai texto
            texto = self._extrair_texto_pdf(nome_arquivo)
            
            if not texto.strip():
                return [{'arquivo': nome_arquivo, 'erro': 'Não foi possível extrair texto'}]

            # Dados básicos
            dados_basicos = self._extrair_dados_basicos(texto)
            print(f"   📋 NFe: {dados_basicos['numero_nf']} - {dados_basicos['data_nf']} - R$ {dados_basicos['valor_total_nf']:.2f}")
            
            # CNPJs
            cnpj_emit, cnpj_dest = self._extrair_cnpjs(texto)
            print(f"   🏢 Emitente: {cnpj_emit}")
            print(f"   🏬 Destinatário: {cnpj_dest}")
            
            # Consulta dados dos CNPJs
            dados_emit = self.consultar_cnpj_api(cnpj_emit) if cnpj_emit else None
            dados_dest = self.consultar_cnpj_api(cnpj_dest) if cnpj_dest else None
            
            # Monta dados gerais
            dados_gerais = {
                'arquivo': nome_arquivo,
                'numero_nf': dados_basicos['numero_nf'],
                'data_nf': dados_basicos['data_nf'],
                'valor_total_nf': dados_basicos['valor_total_nf'],
                'cnpj_emitente': cnpj_emit,
                'cnpj_destinatario': cnpj_dest,
                'razao_social_emitente': dados_emit.get('razao_social', '') if dados_emit else '',
                'razao_social_destinatario': dados_dest.get('razao_social', '') if dados_dest else '',
                'uf_emitente': dados_emit.get('uf', '') if dados_emit else '',
                'uf_destinatario': dados_dest.get('uf', '') if dados_dest else ''
            }
            
            # Extrai produtos
            produtos = self._extrair_produtos_melhorado(texto)
            print(f"   📦 {len(produtos)} produto(s) encontrado(s)")
            
            # Mostra resumo dos produtos
            for produto in produtos:
                print(f"      • {produto['descricao_produto'][:50]} ({produto['tipo_produto']}) - {produto['quantidade']}kg - R$ {produto['valor_total_produto']:.2f}")
            
            if not produtos:
                print("   ⚠️ Nenhum produto encontrado, retornando dados gerais")
                return [dados_gerais]
            
            # Combina dados gerais com cada produto
            resultado = []
            for produto in produtos:
                linha = {**dados_gerais, **produto}
                resultado.append(linha)
            
            return resultado
            
        except Exception as e:
            print(f"   ❌ Erro geral: {str(e)}")
            return [{'arquivo': nome_arquivo, 'erro': str(e)}]

    def processar_pdfs(self):
        """Processa todos os PDFs no diretório atual"""
        arquivos_pdf = [f for f in os.listdir('.') if f.lower().endswith('.pdf')]
        
        if not arquivos_pdf:
            print("❌ Nenhum arquivo PDF encontrado no diretório atual")
            return
        
        print(f"🚀 Iniciando processamento de {len(arquivos_pdf)} arquivo(s) PDF...")
        print("=" * 60)
        
        total_produtos = 0
        total_valor = 0.0
        
        for arquivo in arquivos_pdf:
            try:
                linhas = self.extrair_dados_pdf(arquivo)
                self.dados_extraidos.extend(linhas)
                
                # Conta produtos válidos
                produtos_validos = [l for l in linhas if 'erro' not in l and l.get('descricao_produto')]
                total_produtos += len(produtos_validos)
                
                # Soma valores
                for linha in produtos_validos:
                    total_valor += linha.get('valor_total_produto', 0)
                
            except Exception as e:
                print(f"   ❌ Erro crítico em {arquivo}: {e}")
                self.dados_extraidos.append({'arquivo': arquivo, 'erro': f'Erro crítico: {e}'})
            
            print("-" * 40)
        
        # Gera relatório final
        self._gerar_relatorio_final(total_produtos, total_valor)

    def _gerar_relatorio_final(self, total_produtos: int, total_valor: float):
        """Gera o arquivo Excel e relatório final"""
        if not self.dados_extraidos:
            print("❌ Nenhum dado foi extraído")
            return
        
        print("📊 GERANDO RELATÓRIO FINAL...")
        
        # Cria DataFrame
        df = pd.DataFrame(self.dados_extraidos)
        
        # Remove linhas com erro (mas mantém log)
        df_erros = df[df.get('erro').notna()] if 'erro' in df.columns else pd.DataFrame()
        df_ok = df[df.get('erro').isna()] if 'erro' in df.columns else df
        
        if 'erro' in df_ok.columns:
            df_ok = df_ok.drop('erro', axis=1)
        
        if df_ok.empty:
            print("❌ Nenhum dado válido extraído")
            if not df_erros.empty:
                print("Erros encontrados:")
                for _, erro in df_erros.iterrows():
                    print(f"   • {erro['arquivo']}: {erro.get('erro', 'Erro desconhecido')}")
            return
        
        # Organiza colunas
        colunas_ordenadas = [
            'arquivo', 'numero_nf', 'data_nf', 'valor_total_nf',
            'cnpj_emitente', 'razao_social_emitente', 'uf_emitente',
            'cnpj_destinatario', 'razao_social_destinatario', 'uf_destinatario',
            'codigo_produto', 'descricao_produto', 'tipo_produto',
            'quantidade', 'unidade', 'valor_unitario', 'valor_total_produto', 'cfop'
        ]
        
        # Reordena colunas (apenas as que existem)
        colunas_existentes = [col for col in colunas_ordenadas if col in df_ok.columns]
        df_final = df_ok[colunas_existentes]
        
        # Salva Excel
        nome_arquivo = 'dados_nfe_extraidos_completo.xlsx'
        df_final.to_excel(nome_arquivo, index=False)
        
        # Relatório de estatísticas
        print("=" * 60)
        print("✅ PROCESSAMENTO CONCLUÍDO!")
        print("=" * 60)
        print(f"📁 Arquivo gerado: {nome_arquivo}")
        print(f"📊 Total de linhas: {len(df_final)}")
        print(f"📦 Total de produtos: {total_produtos}")
        print(f"💰 Valor total processado: R$ {total_valor:.2f}")
        
        if not df_final.empty:
            print(f"🏢 Arquivos processados: {df_final['arquivo'].nunique()}")
            print(f"📋 NFes únicas: {df_final['numero_nf'].nunique()}")
            
            # Estatísticas por tipo de produto
            print("\n📈 DISTRIBUIÇÃO POR TIPO DE PRODUTO:")
            if 'tipo_produto' in df_final.columns:
                tipos = df_final['tipo_produto'].value_counts()
                for tipo, qtd in tipos.items():
                    print(f"   • {tipo}: {qtd} produto(s)")
            
            # Estatísticas por arquivo
            print("\n📄 RESUMO POR ARQUIVO:")
            for arquivo in df_final['arquivo'].unique():
                df_arq = df_final[df_final['arquivo'] == arquivo]
                valor_arquivo = df_arq['valor_total_produto'].sum()
                print(f"   • {arquivo}")
                print(f"     - Produtos: {len(df_arq)}")
                print(f"     - Valor: R$ {valor_arquivo:.2f}")
        
        if not df_erros.empty:
            print(f"\n⚠️ ARQUIVOS COM ERRO: {len(df_erros)}")
            df_erros.to_excel('erros_processamento.xlsx', index=False)
            print("   Detalhes salvos em: erros_processamento.xlsx")

if __name__ == "__main__":
    print("🔧 EXTRATOR DE DADOS NFE - VERSÃO MELHORADA")
    print("=" * 60)
    
    extrator = ExtratorNFePDF()
    extrator.processar_pdfs()
    
    print("\n✨ Processamento finalizado!")
    input("Pressione Enter para sair...")