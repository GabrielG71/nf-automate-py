# 📄 NF-AUTOMATE-PY

**Automatização de extração de dados de Notas Fiscais (NFe) em PDF utilizando Python.**

Este projeto foi desenvolvido para **uma associação de catadores de Minas Gerais**, que anteriormente digitava manualmente os dados das notas fiscais em planilhas.  
O script automatiza a leitura de PDFs de NF-es e gera automaticamente um arquivo Excel pronto, contendo:

- ✅ Razão Social  
- ✅ CNPJ  
- ✅ Número da Nota  
- ✅ Data de Emissão  
- ✅ Valor da Nota  

Com isso, o processo que antes levava horas se torna muito mais rápido, eficiente e sem erros humanos.

---

## 🚀 Tecnologias utilizadas

<p align="left">
  <img alt="Python" src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
  <img alt="pandas" src="https://img.shields.io/badge/pandas-150458?style=for-the-badge&logo=pandas&logoColor=white"/>
  <img alt="PyPDF2" src="https://img.shields.io/badge/PyPDF2-003B57?style=for-the-badge"/>
  <img alt="Excel" src="https://img.shields.io/badge/Excel-217346?style=for-the-badge&logo=microsoft-excel&logoColor=white"/>
</p>

---

## 📌 Funcionalidades

- Leitura automática de **arquivos PDF de NF-e**  
- Extração de **CNPJ, Razão Social, Número, Data e Valor**  
- Geração de **planilha Excel (XLSX)** organizada e pronta para uso  
- Facilidade para processar **vários PDFs de uma vez**  

---

## ⚙️ Como rodar o projeto localmente
### 🔧 Pré-requisitos
- [Python 3.10+](https://www.python.org/downloads/)
- Bibliotecas: `pandas`, `PyPDF2`, `openpyxl`

### 🛠️ Passos

1. Clone este repositório:
   ```bash
   git clone https://github.com/GabrielG71/NF-AUTOMATE-PY.git
   cd NF-AUTOMATE-PY
2. Crie um ambiente virtual:
   ```bash
   python -m venv venv
3. Ative o ambiente virtual:
   ```bash
   venv\Scripts\Activate
4. Instale as dependências:
   ```bash
   pip install -r requirements.txt
5. Execute a aplicação:
   ```bash
   python main.py

---

## 📂 Estrutura do Projeto 
```bash
nf-automate-py/
│── main.py            # Arquivo principal da aplicação
│── requirements.txt   # Dependências do projeto
│── README.md          # Documentação
```
---

Feito com 💙 por Gabriel Gonçalves
