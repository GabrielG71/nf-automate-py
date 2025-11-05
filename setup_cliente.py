"""
Script para criar distribuição completa para o cliente
Execute: python setup_cliente.py
"""

import os
import shutil
import zipfile
from pathlib import Path

def criar_build_spec():
    """Cria arquivo build.spec otimizado"""
    build_spec = """# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(
    ['nfe_processor.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[
        'pdfplumber',
        'PIL',
        'PIL._tkinter_finder',
        'pandas',
        'openpyxl',
        'requests',
        'fitz'
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='Processador_NFe',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
"""
    
    with open('build.spec', 'w', encoding='utf-8') as f:
        f.write(build_spec)
    print("✓ Arquivo build.spec criado")


def criar_estrutura_distribuicao():
    """Cria estrutura de pastas para o cliente"""
    
    dist_name = "Processador_NFe"
    dist_path = Path(dist_name)
    
    # Remove pasta antiga se existir
    if dist_path.exists():
        shutil.rmtree(dist_path)
    
    # Cria estrutura
    pastas = [
        dist_path,
        dist_path / "1-ENTRADA",
        dist_path / "2-SAIDA",
        dist_path / "3-PROCESSADOS"
    ]
    
    for pasta in pastas:
        pasta.mkdir(exist_ok=True)
        print(f"✓ Criada pasta: {pasta}")
    
    # Copia executável
    exe_source = Path("dist/Processador_NFe.exe")
    if exe_source.exists():
        shutil.copy2(exe_source, dist_path / "Processador_NFe.exe")
        print(f"✓ Copiado executável")
    else:
        print(f"❌ Executável não encontrado em {exe_source}")
        print("   Execute primeiro: pyinstaller build.spec")
        return None
    
    # Cria README
    readme_content = """
╔═══════════════════════════════════════════════════════════════╗
║    PROCESSADOR DE NOTAS FISCAIS - MATERIAIS RECICLÁVEIS       ║
╚═══════════════════════════════════════════════════════════════╝

📋 COMO USAR:

1️⃣ ADICIONAR ARQUIVOS
   • Abra a pasta "1-ENTRADA"
   • Copie seus arquivos PDF ou XML de Notas Fiscais para lá
   • Pode adicionar vários arquivos de uma vez
   • Cada PDF pode conter várias NF-e

2️⃣ PROCESSAR
   • Dê um duplo clique no arquivo "Processador_NFe.exe"
   • Clique no botão "PROCESSAR ARQUIVOS"
   • Aguarde o processamento (acompanhe na tela)

3️⃣ VER RESULTADOS
   • Abra a pasta "2-SAIDA"
   • Você encontrará uma planilha Excel com todos os dados
   • Os arquivos processados estarão em "3-PROCESSADOS"


📂 ESTRUTURA DE PASTAS:

Processador_NFe.exe      ← Execute este arquivo
│
├── 1-ENTRADA/           ← Coloque os PDFs e XMLs aqui
│
├── 2-SAIDA/             ← Planilhas geradas aqui
│
└── 3-PROCESSADOS/       ← Arquivos já processados


🔍 O QUE O PROGRAMA FAZ:

✓ Lê arquivos PDF e XML de Notas Fiscais Eletrônicas
✓ Identifica materiais recicláveis automaticamente:
  • Plástico (PET, PVC, PEAD, PP, etc.)
  • Metal (Ferro, Aço, Alumínio, Cobre, etc.)
  • Vidro
  • Papel e Papelão

✓ Extrai informações:
  • Dados do emitente e destinatário (com consulta automática de CNPJ)
  • Número da NF-e
  • Data de emissão
  • Descrição dos produtos
  • Quantidades
  • Valores

✓ Gera planilha Excel completa com:
  • Todos os itens encontrados
  • Resumo por tipo de material
  • Colunas organizadas e formatadas


⚠️ REQUISITOS:

• Windows 7 ou superior
• Conexão com internet (para consulta automática de CNPJ)
• Os arquivos devem ser Notas Fiscais Eletrônicas válidas


❓ DÚVIDAS COMUNS:

P: O programa não encontrou materiais na minha nota?
R: Verifique se a NF-e contém produtos com palavras-chave como 
   "plástico", "metal", "vidro", "papel", etc. na descrição.

P: Posso processar várias notas de uma vez?
R: Sim! Coloque todos os arquivos na pasta "1-ENTRADA" e processe.

P: O programa funciona offline?
R: Funciona, mas não conseguirá consultar os nomes das empresas
   por CNPJ. As demais funções continuarão operando normalmente.

P: Onde ficam os arquivos depois de processados?
R: São movidos automaticamente para a pasta "3-PROCESSADOS".


📞 SUPORTE:

Em caso de problemas:
1. Verifique se seguiu todos os passos corretamente
2. Certifique-se que os arquivos são NF-e válidas
3. Entre em contato com o suporte técnico


═══════════════════════════════════════════════════════════════

Versão 1.0 - © 2025
Todos os direitos reservados
"""
    
    with open(dist_path / "LEIA-ME.txt", "w", encoding="utf-8") as f:
        f.write(readme_content)
    print("✓ Criado LEIA-ME.txt")
    
    # Cria arquivo na pasta ENTRADA
    entrada_txt = """╔════════════════════════════════════════════════════╗
║              PASTA DE ENTRADA                      ║
╚════════════════════════════════════════════════════╝

👉 Coloque aqui seus arquivos de Notas Fiscais:
   • Arquivos PDF
   • Arquivos XML

📌 IMPORTANTE:
   • Você pode excluir este arquivo depois de ler
   • Pode adicionar vários arquivos de uma vez
   • Os arquivos serão processados automaticamente

🚀 Depois de adicionar os arquivos:
   1. Volte para a pasta principal
   2. Execute o "Processador_NFe.exe"
   3. Clique em "PROCESSAR ARQUIVOS"
"""
    
    with open(dist_path / "1-ENTRADA" / "_Coloque_seus_arquivos_aqui.txt", "w", encoding="utf-8") as f:
        f.write(entrada_txt)
    
    # Cria arquivo na pasta SAIDA
    saida_txt = """╔════════════════════════════════════════════════════╗
║              PASTA DE SAÍDA                        ║
╚════════════════════════════════════════════════════╝

📊 As planilhas Excel geradas aparecerão aqui!

Cada planilha conterá:
✓ Aba "Materiais" - Todos os itens processados
✓ Aba "Resumo" - Totais por tipo de material

Você pode excluir este arquivo.
"""
    
    with open(dist_path / "2-SAIDA" / "_Planilhas_aparecem_aqui.txt", "w", encoding="utf-8") as f:
        f.write(saida_txt)
    
    print("✓ Criados arquivos de orientação")
    
    return dist_path


def criar_zip(dist_path):
    """Cria arquivo ZIP da distribuição"""
    if not dist_path or not dist_path.exists():
        print("❌ Pasta de distribuição não encontrada")
        return
    
    zip_name = f"{dist_path.name}.zip"
    
    print(f"\n📦 Criando arquivo ZIP: {zip_name}")
    
    with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file in dist_path.rglob('*'):
            if file.is_file():
                arcname = file.relative_to(dist_path.parent)
                zipf.write(file, arcname)
                print(f"   + {arcname}")
    
    file_size = Path(zip_name).stat().st_size / 1024 / 1024
    print(f"\n✅ ZIP criado com sucesso!")
    print(f"📁 Arquivo: {zip_name}")
    print(f"💾 Tamanho: {file_size:.2f} MB")
    
    return zip_name


def main():
    """Função principal"""
    print("\n" + "="*60)
    print("  SETUP - PROCESSADOR DE NOTAS FISCAIS")
    print("="*60 + "\n")
    
    # 1. Cria build.spec
    print("1️⃣ Criando arquivo de build...")
    criar_build_spec()
    
    # 2. Verifica se executável existe
    print("\n2️⃣ Verificando executável...")
    exe_path = Path("dist/Processador_NFe.exe")
    
    if not exe_path.exists():
        print("\n⚠️  Executável não encontrado!")
        print("\n📋 Execute os seguintes comandos:")
        print("   1. pip install pyinstaller")
        print("   2. pyinstaller build.spec")
        print("   3. python setup_cliente.py")
        print("\n" + "="*60)
        return
    
    print("✓ Executável encontrado")
    
    # 3. Cria estrutura
    print("\n3️⃣ Criando estrutura de distribuição...")
    dist_path = criar_estrutura_distribuicao()
    
    if not dist_path:
        return
    
    # 4. Cria ZIP
    print("\n4️⃣ Criando arquivo ZIP...")
    zip_file = criar_zip(dist_path)
    
    # 5. Resumo final
    print("\n" + "="*60)
    print("✅ SETUP CONCLUÍDO COM SUCESSO!")
    print("="*60)
    print(f"\n📦 Arquivo pronto para envio: {zip_file}")
    print(f"📁 Pasta de distribuição: {dist_path}/")
    print("\n💡 O cliente precisa apenas:")
    print("   1. Extrair o arquivo ZIP")
    print("   2. Executar Processador_NFe.exe")
    print("\n🎉 Tudo pronto para uso!")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()