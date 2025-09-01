# buscador_de_dados.py
__version__ = "2.5.0" # Altere esta versão para a versão atual do seu script

# ... (EXISTING IMPORTS) ...
import json
import sys
import subprocess
import re
import warnings
from tkinter import Tk, filedialog
import csv
from collections import defaultdict, OrderedDict
import time
import os
import datetime
import unicodedata
import shutil
import requests
from packaging.version import parse as parse_version


# Aumentar o limite do campo CSV para evitar OverflowError em campos malformados.
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**20 * 100) # 100MB
#print(f"DEBUG: CSV field size limit set to: {csv.field_size_limit()}")
print(f"Versão " + __version__ + " - Criado por: Jefferson Oliveira (t_jefferson.oliveira)")
#print(f"Ajustando a implementação - 4 Importar Carga")
print(f"Iniciando aplicação....aguarde!")

try:
  import pandas as pd
  from fpdf import FPDF
  from fpdf.enums import XPos, YPos
  from tqdm import tqdm
except ImportError:
  print("ERRO: Bibliotecas cruciais não estão instaladas (pandas, fpdf2, tqdm, openpyxl, xlrd).")
  print("Por favor, execute no seu terminal: pip install pandas fpdf2 tqdm openpyxl xlrd")
  input("Pressione Enter para sair.")
  exit()
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message="Could not infer format, so each element will be parsed individually", category=UserWarning)

# --- CLASSE E FUNÇÕES GLOBAIS / UTILS ---
# ... (EXISTING ReportPDF CLASS AND UTILS FUNCTIONS like normalizar_texto, sanitizar_para_excel, _exibir_linha_como_tabela, etc.) ...
class ReportPDF(FPDF):
  def __init__(self, orientation='P', unit='mm', format='A4', title='Relatório'):
    super().__init__(orientation, unit, format)
    self.set_title(title)
    
    # Adiciona a fonte DejaVuSans para suporte a Unicode
    # CORREÇÃO AQUI: Usar sys._MEIPASS para encontrar os arquivos de fonte empacotados
    base_path = ''
    if hasattr(sys, '_MEIPASS'): # Verifica se o script está rodando como um executável PyInstaller
        base_path = sys._MEIPASS
    else:
        # Se não estiver em um executável PyInstaller, usa o diretório atual do script
        base_path = os.path.abspath(os.path.dirname(__file__))

    try:
        # CONCATENAR o base_path com o nome do arquivo da fonte
        self.add_font('DejaVuSans', '', os.path.join(base_path, 'DejaVuSans.ttf'))
        self.add_font('DejaVuSans', 'B', os.path.join(base_path, 'DejaVuSans-Bold.ttf'))
    except RuntimeError:
        print("AVISO: Fonte DejaVuSans.ttf ou DejaVuSans-Bold.ttf não encontrada ou erro ao carregar. Usando Helvetica como fallback. Caracteres especiais podem não ser exibidos corretamente.")


  def header(self):
    self.set_font('Helvetica', 'B', 14)
    self.cell(0, 10, self.title, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C')
    self.set_font('Helvetica', 'I', 8)
    self.cell(0, 5, f'Gerado em: {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}', new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C')
    self.ln(5)

  def footer(self):
    self.set_y(-15); self.set_font('Helvetica', 'I', 8); self.cell(0, 10, f'Página {self.page_no()}/{{nb}}', align='C')

# NOVO: Função para detectar propriedades do CSV (delimitador, quoting, cabeçalho)
def _detect_csv_properties(file_path):
    """
    Tenta detectar o delimitador e o dialeto de um arquivo CSV.
    Retorna um dicionário com 'delimiter', 'quoting' e 'has_header'.
    Assume que o arquivo pode ser grande, então lê apenas uma amostra.
    """
    initial_properties = {'delimiter': ';', 'quoting': csv.QUOTE_NONE, 'has_header': True} # Defaults
    sample_size = 1024 * 10  # 10KB sample

    # Tenta ler uma amostra com diferentes encodings
    sample = ""
    for encoding in ENCODINGS_TO_TRY:
        try:
            with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
                sample = f.read(sample_size)
            break # Sucesso na leitura da amostra
        except Exception:
            continue

    if not sample: # Se não conseguiu ler nem a amostra
        return initial_properties

    try:
        # Tenta inferir com ponto e vírgula como delimitador
        dialect = csv.Sniffer().sniff(sample, delimiters=';')
        initial_properties['delimiter'] = ';'
        initial_properties['quoting'] = dialect.quoting
        initial_properties['has_header'] = csv.Sniffer().has_header(sample)

        # Uma verificação extra para quoting se QUOTE_NONE foi inferido, mas aspas existem
        # Isso é para casos onde Sniffer pode não detectar QUOTE_MINIMAL se houver poucos campos quotados
        if initial_properties['quoting'] == csv.QUOTE_NONE and '"' in sample:
            # Se QUOTE_NONE foi inferido mas há aspas na amostra, assume QUOTE_MINIMAL
            # e que essas aspas são para serem removidas.
            initial_properties['quoting'] = csv.QUOTE_MINIMAL

    except csv.Error:
        # Se sniffing falhar, tenta com vírgula (delimitador comum)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=',')
            initial_properties['delimiter'] = ','
            initial_properties['quoting'] = dialect.quoting
            initial_properties['has_header'] = csv.Sniffer().has_header(sample)

            if initial_properties['quoting'] == csv.QUOTE_NONE and '"' in sample:
                initial_properties['quoting'] = csv.QUOTE_MINIMAL
        except csv.Error:
            # Fallback se Sniffer não conseguir inferir nada, mantém os defaults
            pass

    # Garante que o header é tratado como True, já que nossos CSVs devem ter.
    initial_properties['has_header'] = True 

    # Ajuste para garantir que csv.QUOTE_NONE seja sempre um int
    if isinstance(initial_properties['quoting'], type(csv.QUOTE_NONE)):
        pass # Já é o tipo correto
    elif initial_properties['quoting'] == 0: # Pode vir como 0 de algum dialeto, garantimos o tipo
         initial_properties['quoting'] = csv.QUOTE_NONE
    elif initial_properties['quoting'] == 1: # Pode vir como 1 de algum dialeto, garantimos o tipo
         initial_properties['quoting'] = csv.QUOTE_ALL # Ou QUOTE_NONNUMERIC, dependendo do caso

    return initial_properties
    
# NOVO: Função auxiliar para exibir o resumo dos arquivos após a execução do DDL
def _exibir_resumo_pos_ddl(processed_files_data_after_ddl):
    if not processed_files_data_after_ddl:
        print("\n[AVISO] Nenhuma informação de DDL para exibir no resumo pós-DDL.")
        return

    print("\n" + "="*50)
    print("--- Resumo da Execução de DDL ---")

    for i, file_info in enumerate(processed_files_data_after_ddl):
        original_name = os.path.basename(file_info.get('original_path', 'N/A'))
        table_name = file_info.get('table_name', 'N/A')

        # Agora, o status DDL é verdadeiro
        status_ddl = "✅ OK" if file_info.get('ddl_success', False) else "❌ FALHA"

        print(f"  {i+1}. Arquivo: '{original_name}'")
        print(f"     Tabela Alvo: '{table_name}'")
        print(f"     Status DDL: {status_ddl}")
        print("-" * 40)

    print("="*50)
    #input("\nPressione Enter para continuar com a importação (SQL*Loader)...")
    
# NOVO: Função auxiliar para exibir o resumo dos arquivos após o processamento inicial
def _exibir_resumo_importacao_inicial(processed_files_info):
    if not processed_files_info:
        print("\n[AVISO] Nenhum arquivo processado com sucesso para resumo.")
        return

    print("\n" + "="*50)
    print("--- Resumo dos Arquivos Processados para Importação ---")
    total_arquivos = len(processed_files_info)
    total_registros_geral = 0

    for i, file_info in enumerate(processed_files_info):
        original_name = os.path.basename(file_info.get('original_path', 'N/A'))
        table_name = file_info.get('table_name', 'N/A')
        record_count = file_info.get('record_count', 0)
        status_ddl = "OK" if file_info.get('ddl_success', False) else "Falha DDL"

        print(f"  {i+1}. Arquivo: '{original_name}'")
        print(f"     Tabela Alvo: '{table_name}'")
        print(f"     Linhas Detectadas: {record_count:,}".replace(",", ".")) # Formata número com ponto para milhares
        #print(f"     Status DDL: {status_ddl}")
        print("-" * 40)
        total_registros_geral += record_count

    print(f"\nTotal de arquivos prontos: {total_arquivos}")
    print(f"Total geral de registros: {total_registros_geral:,}".replace(",", "."))
    print("="*50)
    #input("\nPressione Enter para continuar com a verificação de tabela no banco...")
    
# --- NOVO MÓDULO: ATUALIZAÇÃO ---
def _checar_e_atualizar_versao():
    print("\n" + "="*50)
    print("--- Verificação e Atualização de Versão ---")
    print("Versão atual do script: v" + __version__)

    # URL base onde o latest_version.json e os executáveis serão armazenados
    # VOCÊ PRECISARÁ ALTERAR ESTA URL PARA SEU PRÓPRIO LINK PÚBLICO!
    BASE_UPDATE_URL = "https://hapvida-my.sharepoint.com/:u:/g/personal/t_jefferson_oliveira_hapvida_com_br/EfXsAYdGVTpNvvf8FzweNUcBUZp4LWm9ZbS-6lgT_scIXg?e=TnI980" # Ex: https://onedrive.live.com/download?cid=SEU_CID&resid=SEU_RESID&authkey=!SEU_AUTHKEY&encodeWeb=1&AP=1

    try:
        # 1. Baixar o arquivo de metadados da versão mais recente
        print(f"Buscando informações da versão mais recente em: {BASE_UPDATE_URL}latest_version.json")

        # Usar requests para baixar o JSON
        import requests # Certifique-se de ter 'import requests' no topo do seu script

        response = requests.get(f"{BASE_UPDATE_URL}latest_version.json")
        response.raise_for_status() # Lança um erro para status HTTP 4xx/5xx

        latest_info = response.json()

        remote_version_str = latest_info.get("version")
        remote_filename = latest_info.get("filename")
        remote_checksum = latest_info.get("checksum_sha256")

        if not all([remote_version_str, remote_filename, remote_checksum]):
            print("ERRO: Informações incompletas no arquivo latest_version.json remoto. Atualização abortada.")
            return False

        print(f"Versão remota disponível: v{remote_version_str}")

        # 2. Comparar versões
        from packaging.version import parse as parse_version
        # Certifique-se de ter 'from packaging.version import parse' no topo do seu script
        # Você pode precisar instalar: pip install packaging

        local_version = parse_version(__version__)
        remote_version = parse_version(remote_version_str)

        if remote_version > local_version:
            print(f"✅ Nova versão disponível (v{remote_version_str}). Sua versão é v{__version__}.")
            if _confirmar_atualizacao(remote_version_str, "Nova Versão"):
                # Aqui virá a lógica de download e substituição
                print("Iniciando download e substituição da nova versão...")
                #_realizar_download_e_substituicao(remote_filename, remote_checksum, BASE_UPDATE_URL)
                return True # Indica que a atualização foi iniciada (não necessariamente concluída)
            else:
                print("Atualização cancelada pelo usuário.")
                return False
        elif remote_version < local_version:
            print(f"⚠️ Versão remota (v{remote_version_str}) é anterior à sua (v{__version__}). Isso seria um downgrade.")
            if _confirmar_atualizacao(remote_version_str, "Downgrade"):
                # Aqui virá a lógica de download e substituição
                print("Iniciando download e substituição para versão anterior...")
                #_realizar_download_e_substituicao(remote_filename, remote_checksum, BASE_UPDATE_URL)
                return True # Indica que o downgrade foi iniciado
            else:
                print("Downgrade cancelado pelo usuário.")
                return False
        else:
            print("Sua versão já é a mais recente. Nenhuma atualização necessária.")
            return False # Nenhuma atualização/downgrade necessário

    except requests.exceptions.RequestException as e:
        print(f"ERRO DE CONEXÃO: Não foi possível acessar o servidor de atualização. Verifique sua conexão com a internet ou a URL. Erro: {e}")
        return False
    except ValueError as e: # Erro ao processar JSON
        print(f"ERRO: Formato inválido do arquivo de informações de atualização. Erro: {e}")
        return False
    except Exception as e:
        print(f"ERRO INESPERADO durante a checagem de atualização: {e}")
        return False
    finally:
        input("\nPressione Enter para continuar.")
        os.system('cls' if os.name == 'nt' else 'clear')


# Função auxiliar para confirmar a atualização/downgrade com o usuário
def _confirmar_atualizacao(versao_alvo, tipo_operacao):
    while True:
        confirm = input(f"Deseja {tipo_operacao.lower()} para v{versao_alvo}? (s/n): ").strip().lower()
        if confirm == 's':
            return True
        elif confirm == 'n':
            return False
        else:
            print("Opção inválida. Por favor, digite 's' para sim ou 'n' para não.")
            
def normalizar_texto(texto):
    """Remove acentos, converte para minúsculas e remove espaços das pontas.
    Lida com NaN e None retornando string vazia."""
    if pd.isna(texto) or texto is None or (isinstance(texto, str) and texto.lower().strip() == 'nan'):
        return ""
    if not isinstance(texto, str):
        texto = str(texto)
    return ''.join(c for c in unicodedata.normalize('NFD', texto)
                   if unicodedata.category(c) != 'Mn').lower().strip()

def _exibir_linha_como_tabela(linha_dict, coluna_destaque=None, valor_display_limit=80):
    """
    Exibe um dicionário de linha como uma tabela vertical no console,
    tratando 'nan' e limitando o tamanho dos valores.
    """
    if not linha_dict:
        print("    [Linha vazia ou sem dados para exibir]")
        return

    clean_line_dict = {
        k: "" if pd.isna(v) or (isinstance(v, str) and v.lower() == 'nan') else str(v)
        for k, v in linha_dict.items()
    }

    max_col_name_len = max(len(str(col)) for col in clean_line_dict.keys()) if clean_line_dict else 0
    if max_col_name_len < len("CAMPO"):
        max_col_name_len = len("CAMPO")

    max_val_len = max(min(valor_display_limit, len(v)) for v in clean_line_dict.values()) if clean_line_dict else 0
    if max_val_len < len("VALOR"):
        max_val_len = len("VALOR")

    col1_width = max_col_name_len + 2
    col2_width = max_val_len + 2

    header_line = f"| {'CAMPO'.ljust(col1_width)} | {'VALOR'.ljust(col2_width)} |"
    separator_line = f"+-{'-'*col1_width}-+-{'-'*col2_width}-+"

    print(f"  {separator_line}")
    print(f"  {header_line}")
    print(f"  {separator_line}")

    for col, val in clean_line_dict.items():
        display_col = str(col).ljust(col1_width)
        display_val = str(val)
        if len(display_val) > valor_display_limit:
            display_val = display_val[:valor_display_limit-3] + "..."

        if col == coluna_destaque:
            display_col = f"-> {str(col).ljust(col1_width - 3)}"

        print(f"  | {display_col} | {display_val.ljust(col2_width)} |")

    print(f"  {separator_line}")

def sanitizar_para_excel(texto):
  if not isinstance(texto, str): return texto
  return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', texto)

def _carregar_config_colunas():
  config_padrao = { "date_keywords": ["data", "dt_", "_data", "_dt", r"\bdate\b"] }
  try:
    with open("config_colunas.json", 'r', encoding='utf-8') as f:
      config = json.load(f)
      if "date_keywords" not in config: config["date_keywords"] = config_padrao["date_keywords"]
      return config
  except (FileNotFoundError, json.JSONDecodeError):
    return config_padrao

def _salvar_config_colunas(config):
  with open("config_colunas.json", 'w', encoding='utf-8') as f:
    json.dump(config, f, indent=4, ensure_ascii=False)

# A constante para tentar codificações, centralizada
ENCODINGS_TO_TRY = ['utf-8-sig', 'cp1252', 'latin-1', 'utf-8']

def _get_file_columns(file_path):
    """
    Tenta ler as colunas de um único arquivo, mantendo a ordem original.
    Retorna um OrderedDict com colunas por sheet (para Excel) ou 'Sheet1' (para CSV),
    ou um OrderedDict vazio em caso de falha.
    """
    column_map = OrderedDict()
    file_name = os.path.basename(file_path)

    if file_path.lower().endswith(('.xls', '.xlsx')):
        try:
            excel_file = pd.ExcelFile(file_path)
            for sheet_name in excel_file.sheet_names:
                df_temp = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=0, dtype=str)
                column_map[sheet_name] = df_temp.columns.astype(str).str.strip().tolist()
            return column_map
        except Exception as e:
            print(f"    [ERRO] Não foi possível ler as colunas do Excel '{file_name}': {e}")
            return OrderedDict()
    elif file_path.lower().endswith('.csv'):
        # Necessita da função _detect_csv_properties
        csv_props = _detect_csv_properties(file_path)
        detected_delimiter = csv_props['delimiter']
        detected_quoting = csv_props['quoting']

        for encoding in ENCODINGS_TO_TRY:
            try:
                df_temp = pd.read_csv(file_path, 
                                      sep=detected_delimiter, 
                                      nrows=0, 
                                      dtype=str, 
                                      encoding=encoding,
                                      on_bad_lines='skip', 
                                      engine='python', 
                                      header=0, 
                                      quoting=detected_quoting)
                column_map['Sheet1'] = df_temp.columns.astype(str).str.strip().tolist()
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                continue
        
        if 'Sheet1' not in column_map: 
            print(f"    [ERRO] Não foi possível ler as colunas do CSV '{file_name}' com nenhuma codificação tentada.")
            return OrderedDict()

    else:
        print(f"    [AVISO] Formato de arquivo não suportado para '{file_name}'.")
        return OrderedDict()

    return column_map


def _get_all_file_columns_map(file_paths):
    """
    Retorna um dicionário mapeando nomes de arquivo para suas colunas (lista ordenada para cada sheet),
    e também o conjunto de colunas comuns a todos os arquivos (da primeira/única sheet de cada).
    """
    all_file_columns_raw = {} # Mantém o OrderedDict de sheet_name -> [cols]
    common_columns_set = set()
    first_file = True

    print("\n[PASSO 1/X] Identificando colunas em cada arquivo...")
    for path in tqdm(file_paths, desc="Extraindo colunas"):
        file_name = os.path.basename(path)
        current_file_cols_by_sheet = _get_file_columns(path) # Isso retorna um OrderedDict

        if current_file_cols_by_sheet: # Se a leitura de colunas foi bem-sucedida
            all_file_columns_raw[path] = current_file_cols_by_sheet # Guarda o OrderedDict completo

            # Pega as colunas da primeira (ou única) sheet para a lógica de colunas comuns
            # Para CSV, será 'Sheet1'. Para Excel, será a primeira aba.
            first_sheet_name = list(current_file_cols_by_sheet.keys())[0]
            cols_of_first_sheet = current_file_cols_by_sheet[first_sheet_name]

            if first_file:
                common_columns_set = set(cols_of_first_sheet)
                first_file = False
            else:
                common_columns_set.intersection_update(set(cols_of_first_sheet))
        else:
            print(f"  [AVISO] '{file_name}' será ignorado devido a problemas na leitura de suas colunas.")

    return all_file_columns_raw, sorted(list(common_columns_set))

def _prompt_for_column(columns_list, prompt_title, purpose_description, selected_so_far=None, is_single_selection=True):
    """
    Exibe uma lista de colunas e solicita ao usuário que selecione uma ou mais.
    Retorna a coluna(s) selecionada(s) ou None/[] se pular/não selecionar.
    The columns_list is assumed to be already ordered as desired for display.
    """
    selected_so_far = selected_so_far if selected_so_far is not None else set()

    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        print("\n" + "="*50)
        print(f"--- {prompt_title} ---")
        print(f"Finalidade: {purpose_description}")
        print("-" * 50)

        if not columns_list:
            print("[AVISO] Não há colunas disponíveis para seleção aqui.")
            if is_single_selection: return None
            return []

        print("Colunas disponíveis (na ordem do arquivo):")
        for i, col in enumerate(columns_list):
            marcador = "[x]" if col in selected_so_far else "[ ]"
            print(f" {marcador} {i+1}. {col}")

        if selected_so_far:
            currently_selected_ordered = [col for col in columns_list if col in selected_so_far]
            print(f"\nColunas selecionadas atualmente: {', '.join(currently_selected_ordered)}")

        if is_single_selection:
            input_prompt = "\nDigite o NÚMERO da coluna desejada (ou 'p' para pular e não selecionar): "
        else:
            input_prompt = "\nDigite os NÚMEROS das colunas separados por vírgula para MARCAR/DESMARCAR.\nOu 'd' para CONCLUIR seleção.\nOu 'p' para PULAR e não selecionar NENHUMA coluna.\nSua escolha: "

        choice_str = input(input_prompt).strip().lower()

        if choice_str == 'p':
            print("[INFO] Seleção pulada.")
            if is_single_selection: return None
            return []

        if not is_single_selection and choice_str == 'd':
            return [col for col in columns_list if col in selected_so_far]

        try:
            if is_single_selection:
                idx = int(choice_str) - 1
                if 0 <= idx < len(columns_list):
                    return columns_list[idx]
                else:
                    print("[ERRO] Número inválido. Tente novamente.")
                    time.sleep(1.5)
            else:
                for num_str in choice_str.split(','):
                    idx = int(num_str.strip()) - 1
                    if 0 <= idx < len(columns_list):
                        col = columns_list[idx]
                        if col in selected_so_far:
                            selected_so_far.remove(col)
                        else:
                            selected_so_far.add(col)
                    else:
                        print(f"[ERRO] Número '{num_str}' inválido. Ignored.")
                        time.sleep(1)

        except ValueError:
            print("[ERRO] Entrada inválida. Por favor, digite um número ou use as opções 'd'/'p'.")
            time.sleep(1.5)


# --- MÓDULO 1: BUSCA DE DADOS FUNCS ---
# Na função _processar_arquivo_para_busca()

def _processar_arquivo_para_busca(caminho_completo, termo_busca, tipo_arquivo):
  resultados_neste_arquivo, nome_arquivo = [], os.path.basename(caminho_completo)
  tamanho_chunk = 100000 
  timestamp_mod = os.path.getmtime(caminho_completo) 
  data_mod = datetime.datetime.fromtimestamp(timestamp_mod).strftime('%d/%m/%Y %H:%M:%S')
  
  try:
    if tipo_arquivo == 'csv':
      sucesso_leitura = False
      df_iterator = None

      # --- TENTATIVA 1: Usar as configurações padrão do Pandas para CSV (engine='c' por inferência) ---
      # Removidas as mensagens detalhadas de tentativa.
      try:
          # print(f"    Tentando ler '{nome_arquivo}' com configurações padrão do Pandas (engine='c')...")
          leitor = pd.read_csv(caminho_completo, sep=';', chunksize=tamanho_chunk, dtype=str, 
                               encoding='utf-8-sig', on_bad_lines='warn', skipinitialspace=True)
          first_chunk = next(leitor) 
          df_iterator = [first_chunk] + list(leitor) 
          sucesso_leitura = True
          # print(f"    Leitura bem-sucedida com configurações padrão!") # Mensagem mais limpa ou suprimida.
      except (UnicodeDecodeError, pd.errors.ParserError) as e:
          # print(f"    Falha na leitura com configurações padrão (Decodificação/ParserError): {e}") # Silenciado.
          pass 
      except Exception as e:
          # print(f"    Erro inesperado na leitura com configurações padrão: {e}") # Silenciado.
          pass 
      
      if not sucesso_leitura: 
          # AVISO: AVISO ÚNICO SE FOR REQUERER MÚLTIPLAS TENTATIVAS
          print(f"    Tentando codificações alternativas para '{nome_arquivo}'...")
          
          for current_encoding in ENCODINGS_TO_TRY:
            try:
              # print(f"    Tentando ler '{nome_arquivo}' com encoding '{current_encoding}' e engine='python', quoting=csv.QUOTE_NONE...") # Silenciado.
              leitor = pd.read_csv(caminho_completo, sep=';', chunksize=tamanho_chunk, dtype=str, 
                                   encoding=current_encoding, on_bad_lines='skip', 
                                   skipinitialspace=True,
                                   engine='python',
                                   quoting=csv.QUOTE_NONE) 
              first_chunk = next(leitor) 
              df_iterator = [first_chunk] + list(leitor) 
              sucesso_leitura = True
              print(f"    ✅ '{nome_arquivo}' lido com sucesso (codificação: '{current_encoding}').") # Mensagem de sucesso concisa.
              break
            except (UnicodeDecodeError, pd.errors.ParserError) as e:
              # print(f"    Falha na leitura com '{current_encoding}' e engine='python', quoting=csv.QUOTE_NONE (Decodificação/ParserError): {e}") # Silenciado.
              continue 
            except Exception as e:
                # print(f"    Erro inesperado ao tentar ler CSV '{nome_arquivo}' com '{current_encoding}' e engine='python', quoting=csv.QUOTE_NONE: {e}") # Silenciado.
                continue 
      
      if not sucesso_leitura:
          print(f"    ❌ ERRO: Não foi possível ler o arquivo CSV '{nome_arquivo}' após várias tentativas de codificação. Pulando este arquivo.")
          return [] 

      for chunk_idx_in_iterator, chunk in enumerate(df_iterator):
        chunk_processed = chunk.astype(str).fillna('')
        
        for coluna in chunk_processed.columns:
          if chunk_processed.get(coluna) is None: 
              continue
          
          normalized_column_data = chunk_processed[coluna].apply(normalizar_texto)
          
          mask = normalized_column_data.str.contains(termo_busca, na=False)
          
          if mask.any():
            for original_df_index, linha_encontrada_data in chunk_processed[mask].iterrows():
              num_linha_final = original_df_index + (chunk_idx_in_iterator * tamanho_chunk) + 2 
              
              if num_linha_final < 1:
                  print(f"AVISO: Número de linha inesperado ({num_linha_final}) encontrado para termo '{termo_busca}' no arquivo '{nome_arquivo}', coluna '{coluna}'. Forçando linha 1.")
                  num_linha_final = 1 

              clean_line_dict = {k: "" if pd.isna(v) or (isinstance(v, str) and v.lower() == 'nan') else v
                                 for k, v in linha_encontrada_data.to_dict().items()}

              resultados_neste_arquivo.append({"nome_arquivo": nome_arquivo, "data_mod": data_mod, 
                                                "num_linha": num_linha_final, "nome_coluna": coluna, 
                                                "linha_dict": clean_line_dict}) 
              
    elif tipo_arquivo == 'excel':
      abas_do_excel = pd.read_excel(caminho_completo, sheet_name=None, dtype=str)
      for nome_aba, df_aba in abas_do_excel.items():
        if df_aba.empty: continue
        df_aba.columns = df_aba.columns.astype(str).str.strip()

        df_aba_processed = df_aba.astype(str).fillna('')

        for coluna in df_aba_processed.columns:
          if df_aba_processed.get(coluna) is None: continue

          normalized_column_data = df_aba_processed[coluna].apply(normalizar_texto)

          mask = normalized_column_data.str.contains(termo_busca, na=False)

          if mask.any():
            for indice, linha_encontrada in df_aba_processed[mask].iterrows():
              num_linha_final = indice + 2
              if num_linha_final < 1:
                  print(f"AVISO: Número de linha inesperado ({num_linha_final}) encontrado para termo '{termo_busca}' no arquivo Excel '{nome_arquivo}', aba '{nome_aba}', coluna '{coluna}'. Forçando linha 1.")
                  num_linha_final = 1

              clean_line_dict = {k: "" if pd.isna(v) or (isinstance(v, str) and v.lower() == 'nan') else v
                                 for k, v in linha_encontrada.to_dict().items()}

              resultados_neste_arquivo.append({"nome_arquivo": nome_arquivo, "nome_aba": nome_aba, "data_mod": data_mod, 
                                                "num_linha": num_linha_final, "nome_coluna": coluna, 
                                                "linha_dict": clean_line_dict}) 
  except pd.errors.EmptyDataError:
      print(f"    [AVISO] O arquivo '{nome_arquivo}' está vazio ou contém apenas cabeçalho.")
      return []
  except Exception as e:
    print(f"    [ERRO] Erro inesperado ao processar '{nome_arquivo}': {e}")
    return []
  return resultados_neste_arquivo

def gerar_pdf_busca(todos_os_resultados, pasta_saida="Extracao_Busca"):
  print(" -> Gerando relatório PDF de Busca..."); os.makedirs(pasta_saida, exist_ok=True)
  pdf = ReportPDF(title='Relatório de Busca de Dados'); pdf.alias_nb_pages(); pdf.add_page()
  
  resultados_por_arquivo_pdf = defaultdict(list)
  for res in todos_os_resultados:
      resultados_por_arquivo_pdf[res['nome_arquivo']].append(res)

  if len(resultados_por_arquivo_pdf) > 1:
      nome_arquivo_final = "Extracao_Busca_Consolidada.pdf"
  else:
      nome_base = os.path.splitext(list(resultados_por_arquivo_pdf.keys())[0])[0]
      nome_arquivo_final = f"{nome_base}_busca.pdf"
  caminho_completo_saida = os.path.join(pasta_saida, nome_arquivo_final)
  
  try: 
    for nome_arquivo_atual, resultados_do_arquivo in resultados_por_arquivo_pdf.items():
      if pdf.get_y() > (pdf.h - pdf.b_margin - 30) or pdf.page_no() == 0: 
          if pdf.page_no() > 0: pdf.add_page() 
      else:
          pdf.ln(10) 

      pdf.set_font('Helvetica', 'B', 12)
      pdf.set_fill_color(60, 60, 60); pdf.set_text_color(255, 255, 255)
      pdf.cell(0, 8, f"Arquivo: {nome_arquivo_atual}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C', border=1, fill=True)
      pdf.set_text_color(0, 0, 0) 
      pdf.ln(5)

      for res in resultados_do_arquivo:
        if pdf.get_y() > (pdf.h - pdf.b_margin - 30): 
            pdf.add_page()
            pdf.set_font('Helvetica', 'B', 10)
            pdf.cell(0, 7, f"Continuação do arquivo: {nome_arquivo_atual} (Pág. {pdf.page_no()})", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C')
            pdf.ln(5)

        pdf.set_font('Helvetica', 'B', 10)
        if 'nome_aba' in res: pdf.cell(0, 6, f" - Aba/Planilha: {res['nome_aba']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.cell(0, 6, f" - Linha: {res['num_linha']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.cell(0, 6, f" - Coluna Encontrada: {res['nome_coluna']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(3) 

        pdf.set_font('Helvetica', 'B', 10)
        pdf.cell(0, 7, 'Conteúdo Completo da Linha:', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        
        max_col_name_len = max(len(str(col)) for col in res['linha_dict'].keys()) if res['linha_dict'] else 0
        col1_width = min(pdf.w * 0.3, max_col_name_len * 2 + 5) 
        col2_width = pdf.w - pdf.l_margin - pdf.r_margin - col1_width 

        pdf.set_font('Helvetica', 'B', 9)
        pdf.cell(col1_width, 7, 'Campo', border=1, align='C')
        pdf.cell(col2_width, 7, 'Valor', new_x=XPos.LMARGIN, new_y=YPos.NEXT, border=1, align='C')
        
        pdf.set_font('Helvetica', '', 8) 
        for col, val in res['linha_dict'].items():
          val_display = "" if pd.isna(val) or (isinstance(val, str) and val.lower() == 'nan') else str(val)

          estimated_line_height = pdf.font_size * 1.2
          text_width = pdf.get_string_width(val_display) 
          effective_cell_width_for_text = col2_width - (2 * pdf.c_margin) 
          
          num_lines_for_value = 1
          if effective_cell_width_for_text > 0 and text_width > effective_cell_width_for_text: # Corrigido: 'effective_text_width_in_cell' para 'effective_cell_width_for_text'
              num_lines_for_value = int(text_width / effective_cell_width_for_text) + (1 if text_width % effective_cell_width_for_text > 0 else 0)
          
          required_height_for_value_cell = num_lines_for_value * estimated_line_height
          required_height_for_value_cell = max(estimated_line_height, required_height_for_value_cell)

          if pdf.get_y() + required_height_for_value_cell > (pdf.h - pdf.b_margin - 10): 
              pdf.add_page()
              pdf.set_font('Helvetica', 'B', 9) 
              pdf.cell(col1_width, 7, 'Campo', border=1, align='C')
              pdf.cell(col2_width, 7, 'Valor', new_x=XPos.LMARGIN, new_y=YPos.NEXT, border=1, align='C')
              pdf.set_font('Helvetica', '', 8) 
              pdf.cell(0, 5, f"Continuação da linha do arquivo: {nome_arquivo_atual} (Pág. {pdf.page_no()})", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C')
              pdf.ln(2)

          fill = (col == res['nome_coluna']) 
          if fill: pdf.set_fill_color(230, 230, 230)
          
          x_start_cell = pdf.get_x()
          y_start_cell = pdf.get_y()

          pdf.cell(col1_width, required_height_for_value_cell, str(col), border='L', fill=fill, align='L')
          
          pdf.set_xy(x_start_cell + col1_width, y_start_cell)
          pdf.multi_cell(col2_width, estimated_line_height, val_display, border='R', new_x=XPos.LMARGIN, new_y=YPos.NEXT, fill=fill, align='L') 

          if fill: pdf.set_fill_color(255, 255, 255) 

        pdf.ln(5) 

    pdf.output(caminho_completo_saida); print(f"✅ Relatório PDF '{caminho_completo_saida}' gerado com sucesso!")
  except Exception as e:
    print(f"ERRO ao gerar o relatório PDF '{caminho_completo_saida}': {e}")


def gerar_csv_busca(todos_os_resultados, pasta_saida="Extracao_Busca"):
  print(" -> Gerando arquivos CSV de Busca..."); os.makedirs(pasta_saida, exist_ok=True)
  
  resultados_por_arquivo = defaultdict(list)
  for res in todos_os_resultados:
      resultados_por_arquivo[res['nome_arquivo']].append(res['linha_dict']) 

  for nome_arquivo, linhas_para_csv in resultados_por_arquivo.items():
    try:
      nome_base, _ = os.path.splitext(nome_arquivo)
      nome_arquivo_final = f"{nome_base}_busca.csv"
      caminho_completo_saida = os.path.join(pasta_saida, nome_arquivo_final)
      
      all_unique_columns_ordered = OrderedDict()
      for linha_dict in linhas_para_csv:
          for col_name in linha_dict.keys():
              if col_name not in all_unique_columns_ordered:
                  all_unique_columns_ordered[col_name] = None 

      cabecalho_final = list(all_unique_columns_ordered.keys())

      with open(caminho_completo_saida, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=cabecalho_final, delimiter=';', extrasaction='ignore') 
        writer.writeheader()
        for linha_dict in linhas_para_csv:
            clean_row_for_csv = {k: "" if pd.isna(v) or (isinstance(v, str) and v.lower() == 'nan') else v
                                 for k, v in linha_dict.items()}
            writer.writerow(clean_row_for_csv)
      print(f"✅ Arquivo CSV '{caminho_completo_saida}' gerado com sucesso!")
    except Exception as e:
      print(f"ERRO ao gerar o arquivo CSV para '{nome_arquivo}': {e}")

def realizar_uma_busca():
  root = Tk(); root.withdraw(); root.attributes('-topmost', True)
  print("\n" + "="*50 + "\n--- Módulo de Busca de Dados ---")

  arquivos_para_analise_paths_set = set()
  tipo_de_selecao_inicial = None

  while True:
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n--- Seleção de Arquivos para Busca ---")
    print("Como você gostaria de selecionar os arquivos?")
    print(" 1. Selecionar uma Pasta (buscará todos os arquivos suportados na pasta)")
    print(" 2. Selecionar Arquivos Específicos (seleção múltipla permitida)")
    print("\n 0. Cancelar e Voltar ao Menu Principal")

    escolha_selecao = input("\nDigite sua opção: ").strip()

    if escolha_selecao == '1':
      tipo_de_selecao_inicial = 'pasta'
      pasta_selecionada = filedialog.askdirectory(title="Selecione a Pasta para Busca")
      if not pasta_selecionada:
        print("\nNenhuma pasta foi selecionada. Por favor, tente novamente ou cancele.")
        time.sleep(1.5)
        continue

      try:
        arquivos_na_pasta = [os.path.join(pasta_selecionada, f) for f in os.listdir(pasta_selecionada) if f.lower().endswith(('.csv', '.xlsx', '.xls'))]
        if not arquivos_na_pasta:
          print(f"\nNenhum arquivo CSV ou Excel encontrado na pasta '{pasta_selecionada}'.")
          resposta = input("Deseja tentar outra seleção (s/n)? ").lower()
          if resposta == 's':
              continue
          else:
              print("\nOperação de busca cancelada.")
              return

        arquivos_para_analise_paths_set.update(arquivos_na_pasta)
        break

      except FileNotFoundError:
        print(f"ERRO: A pasta '{pasta_selecionada}' não foi encontrada. Tente novamente.")
        time.sleep(1.5)
        continue

    elif escolha_selecao == '2':
      tipo_de_selecao_inicial = 'arquivos'
      caminhos_arquivos_selecionados = filedialog.askopenfilenames(
          title="Selecione um ou mais Arquivos para Busca",
          filetypes=[("Arquivos Suportados", "*.csv *.xlsx *.xls")]
      )
      if not caminhos_arquivos_selecionados:
        print("\nNenhum arquivo foi selecionado. Por favor, tente novamente ou cancele.")
        time.sleep(1.5)
        continue

      arquivos_para_analise_paths_set.update(caminhos_arquivos_selecionados)
      break

    elif escolha_selecao == '0':
      print("\nOperação de busca cancelada.")
      return

    else:
      print("Opção inválida. Por favor, digite 1, 2 ou 0.")
      time.sleep(1.5)

  # --- Loop de Confirmação/Adição/Refazer ---
  while True:
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "="*50)
    print("--- Arquivos Selecionados Atualmente ---")

    arquivos_atuais = sorted(list(arquivos_para_analise_paths_set))

    if not arquivos_atuais:
      print("\nNenhum arquivo válido selecionado para análise.")
      resposta = input("Deseja iniciar uma nova seleção (s/n)? ").lower()
      if resposta == 's':
          return realizar_uma_busca()
      else:
          print("\nOperação cancelada.")
          return

    print(f"\nTotal de arquivos selecionados: {len(arquivos_atuais)}")
    print("Lista de arquivos:"); [print(f" {i + 1}. {os.path.basename(full_path)}") for i, full_path in enumerate(arquivos_atuais)]; print("-" * 50)

    print("\nOpções:"); print(" [c] Confirmar e iniciar a busca"); print(" [a] Adicionar mais arquivos/pastas"); print(" [r] Refazer toda a seleção"); print(" [n] Cancelar e voltar ao menu principal")

    confirm_option = input("\nEscolha uma opção (c/a/r/n): ").lower().strip()

    if confirm_option == 'c':
      break

    elif confirm_option == 'a':
      if tipo_de_selecao_inicial == 'pasta':
        nova_pasta = filedialog.askdirectory(title="Adicionar arquivos de outra Pasta")
        if nova_pasta:
          novos_arquivos = [os.path.join(nova_pasta, f) for f in os.listdir(nova_pasta) if f.lower().endswith(('.csv', '.xlsx', '.xls'))]
          if novos_arquivos:
              arquivos_para_analise_paths_set.update(novos_arquivos)
              print(f"Adicionados {len(novos_arquivos)} arquivos da pasta '{os.path.basename(nova_pasta)}'.")
          else:
              print(f"Nenhum arquivo válido encontrado na pasta '{os.path.basename(nova_pasta)}' para adicionar.")
        else:
          print("Nenhuma pasta adicional selecionada.")
      else:
        novos_caminhos_arquivos = filedialog.askopenfilenames(
            title="Adicionar mais Arquivos",
            filetypes=[("Arquivos Suportados", "*.csv *.xlsx *.xls")]
        )
        if novos_caminhos_arquivos:
            arquivos_para_analise_paths_set.update(novos_caminhos_arquivos)
            print(f"Adicionados {len(novos_caminhos_arquivos)} arquivos.")
        else:
            print("Nenhum arquivo adicional selecionado.")
      time.sleep(1.5)
      continue

    elif confirm_option == 'r':
      print("\nRefazendo a seleção..."); return realizar_uma_busca()

    elif confirm_option == 'n':
      print("\nOperação de busca cancelada."); return

    else:
      print("Opção inválida."); time.sleep(1)

  arquivos_para_analise_com_caminho = list(arquivos_para_analise_paths_set)

  if not arquivos_para_analise_com_caminho:
      print("\nNenhum arquivo para análise após a seleção. Operação cancelada."); return

  termo_busca = normalizar_texto(input("\nDigite a informação que deseja procurar: "))
  if not termo_busca: print("Nenhum termo de busca foi inserido."); return
  print(f"\nProcurando por '{termo_busca}'...")

  todos_os_resultados = []

  for caminho_completo in tqdm(arquivos_para_analise_com_caminho, desc="Analisando arquivos"):
    resultados = _processar_arquivo_para_busca(caminho_completo, termo_busca, 'csv' if caminho_completo.lower().endswith('.csv') else 'excel')
    if resultados: todos_os_resultados.extend(resultados)

  if not todos_os_resultados:
      print("\nNenhum resultado encontrado."); input("\nPressione Enter para continuar."); return

  os.system('cls' if os.name == 'nt' else 'clear')
  print("\n" + "="*70); print("--- REGISTROS ENCONTRADOS NA BUSCA ---".center(70)); print("="*70 + "\n")

  todos_os_resultados_ordenados = sorted(todos_os_resultados, key=lambda x: (x['nome_arquivo'], x['num_linha']))

  for i, res in enumerate(todos_os_resultados_ordenados):
    print(f"[{i+1}/{len(todos_os_resultados_ordenados)}] Resultado encontrado:"); print(f"  Arquivo: {res['nome_arquivo']}")
    if 'nome_aba' in res: print(f"  Aba/Planilha: {res['nome_aba']}")
    print(f"  Linha: {res['num_linha']}"); print(f"  Coluna Encontrada: {res['nome_coluna']}")

    print("\n  Conteúdo Completo da Linha:")
    _exibir_linha_como_tabela(res['linha_dict'], res['nome_coluna']) # Passa o dicionário da linha e a coluna encontrada para destaque

    print("\n" + "-"*70 + "\n")

  contagem_por_arquivo = defaultdict(int)
  for res in todos_os_resultados: contagem_por_arquivo[res['nome_arquivo']] += 1
  total_arquivos_com_achados = len(contagem_por_arquivo)

  print("\n" + "="*70); print("--- RESUMO FINAL DA BUSCA ---".center(70)); print("="*70)
  print(f"Busca finalizada: {len(todos_os_resultados)} registro(s) encontrado(s) em {total_arquivos_com_achados} arquivo(s) distinto(s).")
  print("\nDetalhes por arquivo:"); [print(f" - {arquivo}: {contagem} registro(s) encontrado(s)") for arquivo, contagem in contagem_por_arquivo.items()]; print("="*70)

  while True:
    exportar = input("\nDeseja exportar estes resultados? (1-PDF, 2-CSV, 3-Ambos, n-Não): ").lower()
    if exportar in ['1', '2', '3', 'n']: break
    else: print("Opção inválida.")

  if exportar in ['1', '3']: gerar_pdf_busca(todos_os_resultados)
  if exportar in ['2', '3']: gerar_csv_busca(todos_os_resultados)

# --- MÓDULO 2: CONVERSÃO DE ARQUIVOS FUNCS ---
# ... (EXISTING CONVERSION MODULE FUNCTIONS) ...
def _converter_excel_para_csv(caminho_arquivo):
  nome_arquivo_base = os.path.basename(caminho_arquivo); print(f" -> Processando Excel: {nome_arquivo_base}...")
  try:
    excel_file = pd.ExcelFile(caminho_arquivo); nomes_das_abas = excel_file.sheet_names
    print(f" (Encontradas {len(nomes_das_abas)} abas para converter)"); nome_base = os.path.splitext(caminho_arquivo)[0]
    for nome_aba in tqdm(nomes_das_abas, desc=f"Convertendo abas de '{nome_arquivo_base}'"):
      df_aba = pd.read_excel(excel_file, sheet_name=nome_aba, dtype=str).fillna('')
      nome_saida_csv = f"{nome_base}_{nome_aba}.csv"
      df_aba.to_csv(nome_saida_csv, sep=';', index=False, encoding='utf-8-sig')
    print(f"✅ Arquivo Excel convertido com sucesso em {len(nomes_das_abas)} arquivo(s) CSV.")
  except Exception as e: print(f"ERRO ao converter o arquivo Excel '{nome_arquivo_base}': {e}")

def _converter_csv_para_excel(caminho_arquivo):
  nome_arquivo_base = os.path.basename(caminho_arquivo); print(f" -> Processando CSV: {nome_arquivo_base}...")
  LIMITE_LINHAS_EXCEL, LIMITE_CARACTERES_CELULA = 1048575, 32767
  encodings_para_tentar = ENCODINGS_TO_TRY
  sucesso_conversao, dados_foram_truncados = False, False
  nome_base = os.path.splitext(caminho_arquivo)[0] # Definir nome_base aqui

  for encoding in encodings_para_tentar:
    try:
      with open(caminho_arquivo, 'r', encoding=encoding) as f: total_linhas = sum(1 for l in f) - 1
      print(f" (Arquivo contém {total_linhas} linhas, lendo com codificação '{encoding}')")
      tamanho_chunk = 50000; total_chunks_estimado = (total_linhas // tamanho_chunk) + 1 if total_linhas > 0 else 1
      # Aqui também garantimos que o CSV é lido de forma robusta para conversão
      leitor_csv = pd.read_csv(caminho_arquivo, sep=';', dtype=str, chunksize=tamanho_chunk, # Correção: Use 'caminho_arquivo' ao invés de 'caminho_completo'
                               skipinitialspace=True, encoding=encoding, on_bad_lines='skip',
                               engine='python', quoting=csv.QUOTE_NONE)
      num_partes = (total_linhas // LIMITE_LINHAS_EXCEL) + 1 if total_linhas > LIMITE_LINHAS_EXCEL else 1
      if num_partes > 1: print(f" Aviso: Arquivo muito grande. Será dividido em {num_partes} arquivos Excel.")
      parte_atual, linha_inicio = 1, 0
      nome_saida_excel = f"{nome_base}_parte_{parte_atual}.xlsx" if num_partes > 1 else f"{nome_base}.xlsx"
      writer = pd.ExcelWriter(nome_saida_excel, engine='openpyxl')
      for chunk in tqdm(leitor_csv, total=total_chunks_estimado, desc=f"Convertendo '{nome_arquivo_base}'"):
        for coluna in chunk.columns:
          # AQUI está a linha que foi adicionada para tratamento de 'nan' string
          chunk[coluna] = chunk[coluna].astype(str).apply(lambda x: "" if (isinstance(x, str) and x.lower() == 'nan') else x)
          chunk[coluna] = chunk[coluna].apply(sanitizar_para_excel)
          if chunk[coluna].str.len().max() > LIMITE_CARACTERES_CELULA:
            dados_foram_truncados = True; chunk[coluna] = chunk[coluna].str.slice(0, LIMITE_CARACTERES_CELULA)
        chunk.to_excel(writer, sheet_name='Dados', index=False, header=(linha_inicio==0), startrow=linha_inicio)
        linha_inicio += len(chunk)
        if linha_inicio >= LIMITE_LINHAS_EXCEL:
          writer.close(); print(f"\n✅ Parte {parte_atual} salva em '{nome_saida_excel}'.")
          parte_atual += 1; linha_inicio = 0
          if parte_atual <= num_partes: nome_saida_excel = f"{nome_base}_parte_{parte_atual}.xlsx"; writer = pd.ExcelWriter(nome_saida_excel, engine='openpyxl')
      writer.close(); print(f"\n✅ Conversão finalizada com sucesso! Arquivo salvo como '{nome_saida_excel}'.")
      if dados_foram_truncados: print(" AVISO IMPORTANTE: Células com mais de 32.767 caracteres foram truncadas.")
      sucesso_conversao = True; break
    except (UnicodeDecodeError, pd.errors.ParserError):
        continue
    except Exception as e:
        print(f"ERRO ao converter o arquivo CSV '{nome_arquivo_base}': {e}");
        break
  if not sucesso_conversao: print(f"ERRO FINAL: Não foi possível processar o arquivo '{nome_arquivo_base}'.")

def executar_conversao_arquivos():
  root = Tk(); root.withdraw(); root.attributes('-topmost', True)
  print("\n" + "="*50 + "\n--- Módulo de Conversão de Arquivos ---")

  arquivos_para_converter_paths_set = set()
  tipo_de_selecao_inicial = None

  while True:
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n--- Seleção de Arquivos para Conversão ---")
    print("Como você gostaria de selecionar os arquivos?")
    print(" 1. Selecionar uma Pasta (buscará todos os arquivos suportados na pasta)")
    print(" 2. Selecionar Arquivos Específicos (seleção múltipla permitida)")
    print("\n 0. Cancelar e Voltar ao Menu Principal")

    escolha_selecao = input("\nDigite sua opção: ").strip()

    if escolha_selecao == '1':
      tipo_de_selecao_inicial = 'pasta'
      pasta_selecionada = filedialog.askdirectory(title="Selecione a Pasta para Conversão")
      if not pasta_selecionada:
        print("\nNenhuma pasta foi selecionada. Por favor, tente novamente ou cancele.")
        time.sleep(1.5)
        continue

      try:
        arquivos_na_pasta = [os.path.join(pasta_selecionada, f) for f in os.listdir(pasta_selecionada) if f.lower().endswith(('.csv', '.xlsx', '.xls'))]
        if not arquivos_na_pasta:
          print(f"\nNenhum arquivo CSV ou Excel encontrado na pasta '{pasta_selecionada}'.")
          resposta = input("Deseja tentar outra seleção (s/n)? ").lower()
          if resposta == 's':
              continue
          else:
              print("\nOperação de conversão cancelada.")
              return

        arquivos_para_converter_paths_set.update(arquivos_na_pasta)
        break

      except FileNotFoundError:
        print(f"ERRO: A pasta '{pasta_selecionada}' não foi encontrada. Tente novamente.")
        time.sleep(1.5)
        continue

    elif escolha_selecao == '2':
      tipo_de_selecao_inicial = 'arquivos'
      caminhos_arquivos_selecionados = filedialog.askopenfilenames(
          title="Selecione um ou mais Arquivos para Conversão",
          filetypes=[("Arquivos Suportados", "*.csv *.xlsx *.xls")]
      )
      if not caminhos_arquivos_selecionados:
        print("\nNenhum arquivo foi selecionado. Por favor, tente novamente ou cancele.")
        time.sleep(1.5)
        continue

      arquivos_para_converter_paths_set.update(caminhos_arquivos_selecionados)
      break

    elif escolha_selecao == '0':
      print("\nOperação de conversão cancelada.")
      return

    else:
      print("Opção inválida. Por favor, digite 1, 2 ou 0.")
      time.sleep(1.5)

  # --- Loop de Confirmação/Adição/Refazer para Conversão ---
  while True:
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "="*50)
    print("--- Arquivos Selecionados para Conversão ---")

    arquivos_atuais = sorted(list(arquivos_para_converter_paths_set))

    if not arquivos_atuais:
      print("\nNenhum arquivo válido selecionado para conversão.")
      resposta = input("Deseja iniciar uma nova seleção (s/n)? ").lower()
      if resposta == 's':
          return executar_conversao_arquivos()
      else:
          print("\nOperação cancelada.")
          return

    print(f"\nTotal de arquivos selecionados: {len(arquivos_atuais)}")
    print("Lista de arquivos:"); [print(f" {i + 1}. {os.path.basename(full_path)}") for i, full_path in enumerate(arquivos_atuais)]; print("-" * 50)

    print("\nOpções:"); print(" [c] Confirmar e iniciar a conversão"); print(" [a] Adicionar mais arquivos/pastas"); print(" [r] Refazer toda a seleção"); print(" [n] Cancelar e voltar ao menu principal")

    confirm_option = input("\nEscolha uma opção (c/a/r/n): ").lower().strip()

    if confirm_option == 'c':
      break

    elif confirm_option == 'a':
      if tipo_de_selecao_inicial == 'pasta':
        nova_pasta = filedialog.askdirectory(title="Adicionar arquivos de outra Pasta para Conversão")
        if nova_pasta:
          novos_arquivos = [os.path.join(nova_pasta, f) for f in os.listdir(nova_pasta) if f.lower().endswith(('.csv', '.xlsx', '.xls'))]
          if novos_arquivos:
              arquivos_para_converter_paths_set.update(novos_arquivos)
              print(f"Adicionados {len(novos_arquivos)} arquivos da pasta '{os.path.basename(nova_pasta)}'.")
          else:
              print(f"Nenhum arquivo válido encontrado na pasta '{os.path.basename(nova_pasta)}' para adicionar.")
        else:
          print("Nenhuma pasta adicional selecionada.")
      else: # tipo_de_selecao_inicial == 'arquivos'
        novos_caminhos_arquivos = filedialog.askopenfilenames(
            title="Adicionar mais Arquivos para Conversão",
            filetypes=[("Arquivos Suportados", "*.csv *.xlsx *.xls")]
        )
        if novos_caminhos_arquivos:
            arquivos_para_converter_paths_set.update(novos_caminhos_arquivos)
            print(f"Adicionados {len(novos_caminhos_arquivos)} arquivos.")
        else:
            print("Nenhum arquivo adicional selecionado.")
      time.sleep(1.5)
      continue

    elif confirm_option == 'r':
      print("\nRefazendo a seleção..."); return executar_conversao_arquivos()

    elif confirm_option == 'n':
      print("\nOperação de conversão cancelada."); return

    else:
      print("Opção inválida."); time.sleep(1)

  arquivos_finais_para_converter = list(arquivos_para_converter_paths_set)

  if not arquivos_finais_para_converter:
      print("\nNenhum arquivo para conversão após a seleção. Operação cancelada."); return

  for caminho in tqdm(arquivos_finais_para_converter, desc="Processando conversões"):
    nome_arquivo = os.path.basename(caminho)
    if nome_arquivo.lower().endswith(('.xlsx', '.xls')):
      _converter_excel_para_csv(caminho)
    elif nome_arquivo.lower().endswith('.csv'):
      _converter_csv_para_excel(caminho)
  print("\nConversões concluídas.")
  input("\nPressione Enter para voltar ao menu principal.")

# --- MÓDULO 3: VALIDADOR DE DADOS FUNCS ---
# ... (EXISTING VALIDATION MODULE FUNCTIONS) ...

# 3.1 Funções de Validação de Datas
def _validar_datas_dataframe(df, colunas_de_data):
  """Valida as colunas de data fornecidas em um DataFrame e agrupa os erros por linha."""
  erros_agrupados_por_linha = []
  if not colunas_de_data: return erros_agrupados_por_linha

  df_validacao = df.copy()
  colunas_existentes = [col for col in colunas_de_data if col in df_validacao.columns]
  if not colunas_existentes: return []

  for coluna in colunas_existentes:
    df_validacao[f"{coluna}_valida"] = pd.to_datetime(df_validacao[coluna].astype(str).str.strip(), errors='coerce', dayfirst=True)

  ano_limite = datetime.datetime.now().year + 20 # Aumenta a tolerância para datas futuras
  for coluna in colunas_existentes:
    coluna_valida = f"{coluna}_valida"
    if pd.api.types.is_datetime64_any_dtype(df_validacao[coluna_valida]):
      df_validacao.loc[df_validacao[coluna_valida].dt.year > ano_limite, coluna_valida] = pd.NaT

  # Ajuste para garantir que df.index seja usado corretamente para num_linha
  for indice_df, linha in df_validacao.iterrows(): # indice_df é o índice original do DataFrame
    erros_na_linha = []
    for coluna in colunas_existentes:
      valor_original = linha[coluna]
      tem_conteudo = isinstance(valor_original, str) and valor_original.strip() != ""
      if tem_conteudo and pd.isna(linha[f"{coluna}_valida"]):
        erros_na_linha.append({"nome_coluna": coluna, "valor_invalido": valor_original})

    if erros_na_linha:
      # O +2 aqui é para converter o índice base-0 do DataFrame para a numeração de linha usual (começando em 1 e +1 para cabeçalho)
      erros_agrupados_por_linha.append({"num_linha": indice_df + 2, "erros": erros_na_linha, "linha_completa": df.loc[indice_df]})

  return erros_agrupados_por_linha

def _exibir_tabela_de_erros(erros_do_arquivo):
  """Prepara e exibe uma tabela formatada no console com os erros de um arquivo."""
  if not erros_do_arquivo: return

  colunas_com_erro = sorted(list(set(detalhe['nome_coluna'] for erro_linha in erros_do_arquivo for detalhe in erro_linha['erros'])))
  headers = ['LINHA'] + colunas_com_erro

  dados_para_tabela = []
  for erro_linha in erros_do_arquivo:
    linha_tabela = {'LINHA': erro_linha['num_linha']} # num_linha já vem ajustado com +2
    for col in colunas_com_erro: linha_tabela[col] = '-' # Começa com um placeholder
    for detalhe in erro_linha['erros']:
      # Tratamento de 'nan' para exibição no console da validação
      valor_display = "" if pd.isna(detalhe['valor_invalido']) or \
                              (isinstance(detalhe['valor_invalido'], str) and \
                               detalhe['valor_invalido'].lower() == 'nan') \
                        else str(detalhe['valor_invalido'])
      linha_tabela[detalhe['nome_coluna']] = valor_display
    dados_para_tabela.append(linha_tabela)

  try:
    widths = {h: len(h) for h in headers}
    for linha in dados_para_tabela:
      for h in headers:
        widths[h] = max(widths[h], len(str(linha.get(h, ''))))

    header_str = " | ".join(h.ljust(widths[h]) for h in headers)
    separator_str = "-+-".join('-' * widths[h] for h in headers)
    print(f"+-{separator_str}-+"); print(f"| {header_str} |"); print(f"+-{separator_str}-+")

    LIMITE_EXIBICAO = 25
    for i, linha in enumerate(dados_para_tabela):
      if i >= LIMITE_EXIBICAO:
        print(f"| ... e mais {len(dados_para_tabela) - LIMITE_EXIBICAO} linha(s) com erro. O relatório completo pode ser exportado.".ljust(len(header_str) + 2) + "|"); break
      row_str = " | ".join(str(linha.get(h, '')).ljust(widths[h]) for h in headers)
      print(f"| {row_str} |")
    print(f"+-{separator_str}-+")
  except Exception as e:
    print(f"Ocorreu um erro ao tentar exibir a tabela de erros: {e}")

def gerar_pdf_validacao(todos_os_erros, pasta_saida="Extracao_Validacao"):
  print(" -> Gerando relatório PDF de validação..."); os.makedirs(pasta_saida, exist_ok=True)
  pdf = ReportPDF(title='Relatório de Validação de Datas'); pdf.alias_nb_pages(); pdf.add_page()
  arquivos_origem = set(erro['nome_arquivo'] for erro in todos_os_erros)
  if len(arquivos_origem) > 1: nome_arquivo_final = "Relatorio_Validacao_Datas_Consolidado.pdf"
  else: nome_base = os.path.splitext(list(arquivos_origem)[0])[0]; nome_arquivo_final = f"{nome_base}_validacao_datas.pdf"
  caminho_completo_saida = os.path.join(pasta_saida, nome_arquivo_final)
  ultimo_arquivo = ""
  for i, erro_linha in enumerate(todos_os_erros):
    if pdf.get_y() > 250: pdf.add_page()
    if erro_linha['nome_arquivo'] != ultimo_arquivo:
      pdf.ln(5 if i > 0 else 0); pdf.set_font('Helvetica', 'B', 12)
      pdf.set_fill_color(60, 60, 60); pdf.set_text_color(255, 255, 255)
      pdf.cell(0, 8, f"Arquivo: {erro_linha['nome_arquivo']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, border=1, fill=True)
      ultimo_arquivo = erro_linha['nome_arquivo']
    pdf.set_font('Helvetica', 'B', 10); pdf.set_text_color(0, 0, 0)
    aba_info = f" (Aba: {erro_linha.get('nome_aba')})" if erro_linha.get('nome_aba') else ""
    # erro_linha['num_linha'] já está ajustado, então não precisa do +2 aqui
    pdf.cell(0, 6, f" Problemas encontrados na Linha {erro_linha['num_linha']}{aba_info}:", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font('Helvetica', '', 10)
    for detalhe_erro in erro_linha.get('erros', []):
      # Tratamento de 'nan' para PDF: ajustar valor_invalido
      valor_display = "" if pd.isna(detalhe_erro['valor_invalido']) or \
                              (isinstance(detalhe_erro['valor_invalido'], str) and \
                               detalhe_erro['valor_invalido'].lower() == 'nan') \
                        else str(detalhe_erro['valor_invalido'])
      pdf.multi_cell(0, 5, f"  - Coluna '{detalhe_erro['nome_coluna']}' contém valor inválido: '{valor_display}'", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(4)
  pdf.output(caminho_completo_saida); print(f"✅ Relatório de Validação '{caminho_completo_saida}' gerado com sucesso!")

def gerar_csv_validacao(todos_os_erros, pasta_base, pasta_saida="Extracao_Validacao"):
  print(" -> Gerando arquivos CSV com linhas problemáticas..."); os.makedirs(pasta_saida, exist_ok=True)
  erros_por_arquivo = defaultdict(list)
  for erro in todos_os_erros: erros_por_arquivo[erro['nome_arquivo']].append(erro['linha_completa'])
  for nome_arquivo, linhas_de_erro in erros_por_arquivo.items():
    try:
      df_erros = pd.DataFrame([linha.to_dict() for linha in linhas_de_erro])
      # Limpar NaNs no DataFrame antes de salvar no CSV de validação
      df_erros.fillna('', inplace=True)
      for col in df_erros.columns:
          df_erros[col] = df_erros[col].apply(lambda x: "" if (isinstance(x, str) and str(x).lower() == 'nan') else x)

      nome_base = os.path.splitext(nome_arquivo)[0]; nome_arquivo_final = f"{nome_base}_erros_data.csv"
      caminho_completo_saida = os.path.join(pasta_saida, nome_arquivo_final)
      df_erros.to_csv(caminho_completo_saida, sep=';', index=False, encoding='utf-8-sig')
      print(f"✅ Arquivo com erros '{caminho_completo_saida}' gerado com sucesso!")
    except Exception as e: print(f"ERRO ao gerar o arquivo CSV de erros para '{nome_arquivo}': {e}")

def _selecionar_colunas_manualmente(df, colunas_ja_selecionadas, config):
  while True:
    os.system('cls' if os.name == 'nt' else 'clear'); print("--- Seleção Manual de Colunas de Data ---"); print("As colunas marcadas com [x] serão validadas.")
    for i, col in enumerate(df.columns):
      marcador = "[x]" if col in colunas_ja_selecionadas else "[ ]"
      print(f" {marcador} {i+1}. {col}")
    escolha_numeros = input("\nDigite números para MARCAR/DESMARCAR (ex: 1, 5).\nPressione Enter quando terminar: ").strip()
    if escolha_numeros == "":
      novas_palavras = set()
      for col in colunas_ja_selecionadas:
        palavras = re.findall(r'[a-zA-Z]+', str(col).lower())
        for p in palavras:
          if p and len(p) > 3 and p not in config["date_keywords"]: novas_palavras.add(p)
      if novas_palavras:
        print(f"Aprendendo novas palavras-chave: {list(novas_palavras)}"); config["date_keywords"].extend(list(novas_palavras))
        _salvar_config_colunas(config); print("Conhecimento atualizado!")
      return list(colunas_ja_selecionadas)
    try:
      for num_str in escolha_numeros.split(','):
        indice = int(num_str.strip()) - 1
        if 0 <= indice < len(df.columns):
          coluna_escolhida = df.columns[indice]
          if coluna_escolhida in colunas_ja_selecionadas: colunas_ja_selecionadas.remove(coluna_escolhida)
          else: colunas_ja_selecionadas.add(coluna_escolhida)
    except (ValueError, IndexError): print("Seleção inválida. Tente novamente."); time.sleep(1)

def _rodar_validacao_de_datas():
  root = Tk(); root.withdraw(); root.attributes('-topmost', True)
  print("\n" + "="*50 + "\n--- Validador de Qualidade de Dados: Datas ---")

  arquivos_para_validar_paths_set = set()
  tipo_de_selecao_inicial = None

  while True:
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n--- Seleção de Arquivos para Validação de Datas ---")
    print("Como você gostaria de selecionar os arquivos?")
    print(" 1. Selecionar uma Pasta (buscará todos os arquivos suportados na pasta)")
    print(" 2. Selecionar Arquivos Específicos (seleção múltipla permitida)")
    print("\n 0. Cancelar e Voltar ao Menu Principal")

    escolha_selecao = input("\nDigite sua opção: ").strip()

    if escolha_selecao == '1':
      tipo_de_selecao_inicial = 'pasta'
      pasta_selecionada = filedialog.askdirectory(title="Selecione a Pasta para Validação de Datas")
      if not pasta_selecionada:
        print("\nNenhuma pasta foi selecionada. Por favor, tente novamente ou cancele.")
        time.sleep(1.5)
        continue

      try:
        arquivos_na_pasta = [os.path.join(pasta_selecionada, f) for f in os.listdir(pasta_selecionada) if f.lower().endswith(('.csv', '.xlsx', '.xls'))]
        if not arquivos_na_pasta:
          print(f"\nNenhum arquivo CSV ou Excel encontrado na pasta '{pasta_selecionada}'.")
          resposta = input("Deseja tentar outra seleção (s/n)? ").lower()
          if resposta == 's':
              continue
          else:
              print("\nOperação de validação de datas cancelada.")
              return

        arquivos_para_validar_paths_set.update(arquivos_na_pasta)
        break

      except FileNotFoundError:
        print(f"ERRO: A pasta '{pasta_selecionada}' não foi encontrada. Tente novamente.")
        time.sleep(1.5)
        continue

    elif escolha_selecao == '2':
      tipo_de_selecao_inicial = 'arquivos'
      caminhos_arquivos_selecionados = filedialog.askopenfilenames(
          title="Selecione um ou mais Arquivos para Validação de Datas",
          filetypes=[("Arquivos Suportados", "*.csv *.xlsx *.xls")]
      )
      if not caminhos_arquivos_selecionados:
        print("\nNenhum arquivo foi selecionado. Por favor, tente novamente ou cancele.")
        time.sleep(1.5)
        continue

      arquivos_para_validar_paths_set.update(caminhos_arquivos_selecionados)
      break

    elif escolha_selecao == '0':
      print("\nOperação de validação de datas cancelada.")
      return

    else:
      print("Opção inválida. Por favor, digite 1, 2 ou 0.")
      time.sleep(1.5)

  # --- Loop de Confirmação/Adição/Refazer para Validação de Datas ---
  while True:
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "="*50)
    print("--- Arquivos Selecionados para Validação de Datas ---")

    arquivos_atuais = sorted(list(arquivos_para_validar_paths_set))

    if not arquivos_atuais:
      print("\nNenhum arquivo válido selecionado para validação de datas.")
      resposta = input("Deseja iniciar uma nova seleção (s/n)? ").lower()
      if resposta == 's':
          return _rodar_validacao_de_datas()
      else:
          print("\nOperação cancelada.")
          return

    print(f"\nTotal de arquivos selecionados: {len(arquivos_atuais)}")
    print("Lista de arquivos:"); [print(f" {i + 1}. {os.path.basename(full_path)}") for i, full_path in enumerate(arquivos_atuais)]; print("-" * 50)

    print("\nOpções:"); print(" [c] Confirmar e iniciar a validação"); print(" [a] Adicionar mais arquivos/pastas"); print(" [r] Refazer toda a seleção"); print(" [n] Cancelar e voltar ao menu principal")

    confirm_option = input("\nEscolha uma opção (c/a/r/n): ").lower().strip()

    if confirm_option == 'c':
      break
    elif confirm_option == 'a':
      if tipo_de_selecao_inicial == 'pasta':
        nova_pasta = filedialog.askdirectory(title="Adicionar arquivos de outra Pasta para Validação")
        if nova_pasta:
          novos_arquivos = [os.path.join(nova_pasta, f) for f in os.listdir(nova_pasta) if f.lower().endswith(('.csv', '.xlsx', '.xls'))]
          if novos_arquivos:
              arquivos_para_validar_paths_set.update(novos_arquivos)
              print(f"Adicionados {len(novos_arquivos)} arquivos da pasta '{os.path.basename(nova_pasta)}'.")
          else:
              print(f"Nenhum arquivo válido encontrado na pasta '{os.path.basename(nova_pasta)}' para adicionar.")
        else:
          print("Nenhuma pasta adicional selecionada.")
      else: # tipo_de_selecao_inicial == 'arquivos'
        novos_caminhos_arquivos = filedialog.askopenfilenames(
            title="Adicionar mais Arquivos para Validação",
            filetypes=[("Arquivos Suportados", "*.csv *.xlsx *.xls")]
        )
        if novos_caminhos_arquivos:
            arquivos_para_validar_paths_set.update(novos_caminhos_arquivos)
            print(f"Adicionados {len(novos_caminhos_arquivos)} arquivos.")
        else:
            print("Nenhum arquivo adicional selecionado.")
      time.sleep(1.5)
      continue
    elif confirm_option == 'r':
      print("\nRefazendo a seleção..."); return _rodar_validacao_de_datas()
    elif confirm_option == 'n':
      print("\nOperação de validação de datas cancelada."); return
    else:
      print("Opção inválida."); time.sleep(1)

  arquivos_para_analise_com_caminho = list(arquivos_para_validar_paths_set)

  if not arquivos_para_analise_com_caminho:
      print("\nNenhum arquivo para análise de datas após a seleção. Operação cancelada."); return


  print(f"\nIniciando validação de datas em {len(arquivos_para_analise_com_caminho)} arquivo(s)...")
  todos_os_erros_geral = []
  config = _carregar_config_colunas()
  padrão_data = '|'.join(config.get("date_keywords", []))

  for caminho_completo in tqdm(arquivos_para_analise_com_caminho, desc="Analisando arquivos"):
    nome_arquivo = os.path.basename(caminho_completo)
    try:
      df_loaded = None # Usar um nome diferente para evitar conflito com 'df' global ou de escopo
      if caminho_completo.lower().endswith(('.xlsx', '.xls')):
          df_loaded = pd.read_excel(caminho_completo, sheet_name=None, dtype=str)
      elif caminho_completo.lower().endswith('.csv'):
          sucesso_leitura_csv = False
          for encoding_csv in ENCODINGS_TO_TRY:
              try:
                  df_loaded = pd.read_csv(caminho_completo, sep=';', dtype=str, skipinitialspace=True,
                                   encoding=encoding_csv, on_bad_lines='skip',
                                   engine='python', quoting=csv.QUOTE_NONE)
                  sucesso_leitura_csv = True
                  break
              except (UnicodeDecodeError, Exception) as e:
                  # print(f"    DEBUG: Erro ao ler CSV '{nome_arquivo}' com '{encoding_csv}' para validação: {e}") # Desativado para logs mais limpos
                  continue
          if not sucesso_leitura_csv:
              print(f" Aviso: Não foi possível ler o arquivo CSV '{nome_arquivo}' com nenhuma codificação tentada. Pulando.")
              continue
      else:
          print(f" Aviso: Formato de arquivo não suportado para '{nome_arquivo}'. Pulando.")
          continue

      if df_loaded is None: continue

      df_a_processar = df_loaded if isinstance(df_loaded, dict) else {nome_arquivo: df_loaded}

      for nome_aba, df_aba in df_a_processar.items():
        colunas_encontradas = [col for col in df_aba.columns if re.search(padrão_data, str(col), re.IGNORECASE)] if padrão_data else []
        print(f"\n--- Verificação de Colunas de Data em '{nome_arquivo}' (Aba: {nome_aba if isinstance(df_loaded, dict) else 'N/A'}) ---")
        colunas_para_validar = []
        if not colunas_encontradas:
          print("Não encontrei colunas de data com base nos meus padrões."); print("\nO que você deseja fazer?\n [1] Informar manualmente quais colunas são de data\n [2] Pular validação para este item")
          escolha_menu = input("Digite sua opção: ").strip()
          if escolha_menu == '1': colunas_para_validar = _selecionar_colunas_manualmente(df_aba, set(), config)
          else: print(f"Pulando validação de datas para '{nome_arquivo}' (Aba: {nome_aba})."); continue
        else:
          print("Encontrei as seguintes colunas que parecem conter datas:"); [print(f" - {col}") for col in colunas_encontradas]
          print("\nO que você deseja fazer?\n [1] Continuar com esta seleção\n [2] Adicionar ou Remover colunas\n [3] Pular validação")
          escolha_menu = input("Digite sua opção: ").strip()
          if escolha_menu == '1': colunas_para_validar = colunas_encontradas
          elif escolha_menu == '2': colunas_para_validar = _selecionar_colunas_manualmente(df_aba, set(colunas_encontradas), config)
          else: print(f"Pulando validação de datas para '{nome_arquivo}' (Aba: {nome_aba})."); continue
        if colunas_para_validar:
          print(f"\nOk, validando as colunas: {', '.join(colunas_para_validar)}..."); time.sleep(1)
          erros = _validar_datas_dataframe(df_aba, colunas_para_validar)
          if erros:
            for e in erros: e.update({"nome_arquivo": nome_arquivo, "nome_aba": nome_aba if isinstance(df_loaded, dict) else None})
            todos_os_erros_geral.extend(erros)
    except Exception as e: print(f" Aviso: Não foi possível ler ou processar o arquivo '{nome_arquivo}'. Erro: {e}")
  os.system('cls' if os.name == 'nt' else 'clear')
  if not todos_os_erros_geral:
    print("\nAnálise finalizada. Nenhuma data inválida foi encontrada!")
  else:
    erros_por_fonte = defaultdict(list)
    for erro in todos_os_erros_geral: erros_por_fonte[(erro['nome_arquivo'], erro.get('nome_aba'))].append(erro)
    for (nome_arquivo, nome_aba), erros_do_item in erros_por_fonte.items():
      print(f"\n--- Erros no arquivo: {nome_arquivo} (Aba: {nome_aba if nome_aba else 'N/A'}) ---")
      _exibir_tabela_de_erros(erros_do_item)
    print("\n" + "="*70); print("--- RESUMO GERAL DA VALIDAÇÃO ---")
    contagem_por_arquivo = defaultdict(int)
    for erro in todos_os_erros_geral: contagem_por_arquivo[erro['nome_arquivo']] += 1
    total_arquivos_com_erros = len(contagem_por_arquivo)
    print(f"Total Geral: {len(todos_os_erros_geral)} linha(s) com erro encontradas em {total_arquivos_com_erros} arquivo(s) distinto(s).")
    print("\nDetalhes por arquivo:"); [print(f" - {arquivo}: {contagem} linha(s) com erro.") for arquivo, contagem in contagem_por_arquivo.items()]; print("="*70)
    while True:
      exportar = input("\nDeseja exportar estes resultados? (1-PDF, 2-CSV com erros, 3-Ambos, n-Não): ").lower()
      if exportar in ['1', '2', '3', 'n']: break
      else: print("Opção inválida.")
    if exportar in ['1', '3']: gerar_pdf_validacao(todos_os_erros_geral)
    if exportar in ['2', '3']: gerar_csv_validacao(todos_os_erros_geral, None)
  input("\nPressione Enter para voltar ao menu de validação.")


def _verificar_duplicidade_registros():
  root = Tk(); root.withdraw(); root.attributes('-topmost', True)
  os.system('cls' if os.name == 'nt' else 'clear')
  print("\n" + "="*50 + "\n--- Módulo de Verificação de Duplicidade de Registros ---")

  arquivos_selecionados_paths_set = set()
  tipo_de_selecao_inicial = None

  while True:
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n--- Seleção de Arquivos para Verificação de Duplicidade ---")
    print("Como você gostaria de selecionar os arquivos?")
    print(" 1. Selecionar uma Pasta (buscará todos os arquivos suportados na pasta)")
    print(" 2. Selecionar Arquivos Específicos (seleção múltipla permitida)")
    print("\n 0. Cancelar e Voltar ao Menu Principal")

    escolha_selecao = input("\nDigite sua opção: ").strip()

    if escolha_selecao == '1':
      tipo_de_selecao_inicial = 'pasta'
      pasta_selecionada = filedialog.askdirectory(title="Selecione a Pasta para Verificação de Duplicidade")
      if not pasta_selecionada:
        print("\nNenhuma pasta foi selecionada. Por favor, tente novamente ou cancele.")
        time.sleep(1.5)
        continue

      try:
        arquivos_na_pasta = [os.path.join(pasta_selecionada, f) for f in os.listdir(pasta_selecionada) if f.lower().endswith(('.csv', '.xlsx', '.xls'))]
        if not arquivos_na_pasta:
          print(f"\nNenhum arquivo CSV ou Excel encontrado na pasta '{pasta_selecionada}'.")
          resposta = input("Deseja tentar outra seleção (s/n)? ").lower()
          if resposta == 's':
              continue
          else:
              print("\nOperação de verificação de duplicidade cancelada.")
              return

        arquivos_selecionados_paths_set.update(arquivos_na_pasta)
        break

      except FileNotFoundError:
        print(f"ERRO: A pasta '{pasta_selecionada}' não foi encontrada. Tente novamente.")
        time.sleep(1.5) # Corrigido: time.time() para time.sleep(1.5)
        continue

    elif escolha_selecao == '2':
      tipo_de_selecao_inicial = 'arquivos'
      caminhos_arquivos_selecionados = filedialog.askopenfilenames(
          title="Selecione um ou mais Arquivos para Verificação de Duplicidade",
          filetypes=[("Arquivos Suportados", "*.csv *.xlsx *.xls")]
      )
      if not caminhos_arquivos_selecionados:
        print("\nNenhum arquivo foi selecionado. Por favor, tente novamente ou cancele.")
        time.sleep(1.5)
        continue

      arquivos_selecionados_paths_set.update(caminhos_arquivos_selecionados)
      break

    elif escolha_selecao == '0':
      print("\nOperação de verificação de duplicidade cancelada.")
      return

    else:
      print("Opção inválida. Por favor, digite 1, 2 ou 0.")
      time.sleep(1.5)

  # --- Loop de Confirmação/Adição/Refazer para Duplicidade ---
  while True:
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "="*50)
    print("--- Arquivos Selecionados para Verificação de Duplicidade ---")

    arquivos_atuais = sorted(list(arquivos_selecionados_paths_set))

    if not arquivos_atuais:
      print("\nNenhum arquivo válido selecionado para verificação de duplicidade.")
      resposta = input("Deseja iniciar uma nova seleção (s/n)? ").lower()
      if resposta == 's':
          return _verificar_duplicidade_registros()
      else:
          print("\nOperação cancelada.")
          return

    print(f"\nTotal de arquivos selecionados: {len(arquivos_atuais)}")
    print("Lista de arquivos:"); [print(f" {i + 1}. {os.path.basename(full_path)}") for i, full_path in enumerate(arquivos_atuais)]; print("-" * 50)

    print("\nOpções:"); print(" [c] Confirmar e iniciar a verificação"); print(" [a] Adicionar mais arquivos/pastas"); print(" [r] Refazer toda a seleção"); print(" [n] Cancelar e voltar ao menu principal")

    confirm_option = input("\nEscolha uma opção (c/a/r/n): ").lower().strip()

    if confirm_option == 'c':
      break
    elif confirm_option == 'a':
      if tipo_de_selecao_inicial == 'pasta':
        nova_pasta = filedialog.askdirectory(title="Adicionar arquivos de outra Pasta para Duplicidade")
        if nova_pasta:
          novos_arquivos = [os.path.join(nova_pasta, f) for f in os.listdir(nova_pasta) if f.lower().endswith(('.csv', '.xlsx', '.xls'))]
          if novos_arquivos:
              arquivos_selecionados_paths_set.update(novos_arquivos)
              print(f"Adicionados {len(novos_arquivos)} arquivos da pasta '{os.path.basename(nova_pasta)}'.")
          else:
              print(f"Nenhum arquivo válido encontrado na pasta '{os.path.basename(nova_pasta)}' para adicionar.")
        else:
          print("Nenhuma pasta adicional selecionada.")
      else: # tipo_de_selecao_inicial == 'arquivos'
        novos_caminhos_arquivos = filedialog.askopenfilenames(
            title="Adicionar mais Arquivos para Duplicidade",
            filetypes=[("Arquivos Suportados", "*.csv *.xlsx *.xls")]
        )
        if novos_caminhos_arquivos:
            arquivos_selecionados_paths_set.update(novos_caminhos_arquivos)
            print(f"Adicionados {len(novos_caminhos_arquivos)} arquivos.")
        else:
            print("Nenhum arquivo adicional selecionado.")
      time.sleep(1.5)
      continue
    elif confirm_option == 'r':
      print("\nRefazendo a seleção..."); return _verificar_duplicidade_registros()
    elif confirm_option == 'n':
      print("\nOperação de verificação de duplicidade cancelada."); return
    else:
      print("Opção inválida."); time.sleep(1)

  arquivos_selecionados_paths = list(arquivos_selecionados_paths_set)

  if not arquivos_selecionados_paths:
      print("\nNenhum arquivo para verificação de duplicidade após a seleção. Operação cancelada."); return


  print(f"\n[OK] {len(arquivos_selecionados_paths)} arquivo(s) selecionado(s):")
  for i, path in enumerate(arquivos_selecionados_paths):
      print(f" {i+1}. {os.path.basename(path)}")
  print("-" * 50)

  all_file_columns_map, common_columns = _get_all_file_columns_map(arquivos_selecionados_paths)

  if not all_file_columns_map:
      print("\n[ERRO] Nenhum arquivo pôde ser lido para extração de colunas. Verifique os arquivos selecionados."); input("Pressione Enter para continuar."); return

  final_colunas_localizar = {}
  final_colunas_comparacao = {}

  first_file_path = arquivos_selecionados_paths[0]
  first_file_name = os.path.basename(first_file_path)
  # Modifique esta linha:
  # first_file_cols_ordered = all_file_columns_map.get(first_file_path, [])
  # Para:
  first_file_cols_by_sheet = all_file_columns_map.get(first_file_path, OrderedDict())
  if not first_file_cols_by_sheet:
      print(f"\n[ERRO] Não foi possível ler as colunas do primeiro arquivo '{first_file_name}'. Não é possível prosseguir com a seleção."); input("Pressione Enter para continuar."); return
  
  # Pegue as colunas da primeira sheet para usar no prompt
  first_sheet_name_for_prompt = list(first_file_cols_by_sheet.keys())[0]
  first_file_cols_ordered_for_prompt = first_file_cols_by_sheet[first_sheet_name_for_prompt]


  os.system('cls' if os.name == 'nt' else 'clear')
  print("\n" + "="*50)
  print(f"[PASSO 2/X] SELEÇÃO PARA O ARQUIVO: '{first_file_name}'")
  # Se for Excel, também exiba o nome da aba para clareza
  if first_sheet_name_for_prompt != 'Sheet1': # 'Sheet1' é o nome padrão para CSVs
      print(f"  (Aba: '{first_sheet_name_for_prompt}')")


  cols_loc_first_file = _prompt_for_column(
      # Mude aqui para usar a variável que contém a lista de colunas real
      first_file_cols_ordered_for_prompt,
      f"Selecione as colunas para LOCALIZAR DUPLICIDADE em '{first_file_name}'",
      "Escolha uma ou mais colunas que, em conjunto, definirão um registro duplicado.", is_single_selection=False
  )
  
  if not cols_loc_first_file:
      print(f"\n[AVISO] Nenhuma coluna para Localizar Duplicidade foi selecionada para '{first_file_name}'. Abortando análise."); input("Pressione Enter para continuar."); return

  available_for_comp_first_file = [col for col in first_file_cols_ordered if col not in cols_loc_first_file]
  cols_comp_first_file = _prompt_for_column(
      available_for_comp_first_file,
      f"Selecione as colunas para COMPARAÇÃO em '{first_file_name}'",
      "Escolha colunas que devem ser verificadas quanto a DIFERENÇAS dentro dos grupos de duplicados (identificados pelas colunas de localização).", is_single_selection=False
  )
  final_colunas_localizar[first_file_path] = cols_loc_first_file
  final_colunas_comparacao[first_file_path] = cols_comp_first_file

  print("\n" + "="*50); print("[PASSO 3/X] VALIDANDO E AJUSTANDO SELEÇÕES PARA OUTROS ARQUIVOS...")

  # Lista para coletar arquivos que serão de fato processados após todas as interações e decisões de pular/ignorar.
  # Começa com o primeiro arquivo, que já teve suas colunas configuradas.
  files_to_process_after_user_choices = [arquivos_selecionados_paths[0]] 
  files_to_adjust = [] # Lista para arquivos que precisarão de ajuste manual de colunas

  for path in arquivos_selecionados_paths[1:]: # Itera a partir do segundo arquivo
      file_name = os.path.basename(path)
      current_file_cols_ordered = all_file_columns_map.get(path, [])

      # --- NOVO BLOCO: Opções para processar, pular ou cancelar ---
      os.system('cls' if os.name == 'nt' else 'clear')
      print("\n" + "="*50)
      print(f"--- Processar Arquivo: '{file_name}' para Duplicidade? ---")
      print("Selecione sua opção:")
      print("   (Qualquer outra tecla) - Processar este arquivo")
      print("   [p] - Pular este arquivo (ir para o próximo)")
      print("   [c] - Cancelar a operação de duplicidade (abortar tudo)")
      
      choice_action = input(f"\nSua escolha para '{file_name}': ").strip().lower()
      
      if choice_action == 'p':
          print(f"  [INFO] Arquivo '{file_name}' pulado. Prosseguindo com o próximo.")
          continue # Pula para a próxima iteração do loop (próximo arquivo)
      elif choice_action == 'c':
          print(f"  [INFO] Operação de verificação de duplicidade cancelada pelo usuário.")
          return # Sai da função _verificar_duplicidade_registros completamente

      # Se o usuário escolheu processar, continua a lógica de validação/ajuste de colunas para este arquivo
      if not current_file_cols_ordered:
          print(f"  [AVISO] '{file_name}' será ignorado por não ter colunas válidas."); continue # Este continue permanece, pois o arquivo não tem colunas.
                                                                                               # e já passou pelo skip/cancel
      current_file_cols_set = set(current_file_cols_ordered)
      missing_loc_cols = [col for col in cols_loc_first_file if col not in current_file_cols_set]
      missing_comp_cols = [col for col in cols_comp_first_file if col not in current_file_cols_set]

      if missing_loc_cols or missing_comp_cols:
          # Se o arquivo precisa de ajuste de coluna
          files_to_adjust.append(path) # Adiciona à lista de arquivos que precisarão de ajuste individual
          if missing_loc_cols: print(f"  [AVISO] Colunas de LOCALIZAÇÃO '{', '.join(missing_loc_cols)}' não encontradas em '{file_name}'.")
          if missing_comp_cols: print(f"  [AVISO] Colunas de COMPARAÇÃO '{', '.join(missing_comp_cols)}' não encontradas em '{file_name}'.")
          print(f"  -> Será necessário selecionar colunas para '{file_name}' individualmente.")
          # Preenche com listas vazias para indicar que precisarão de seleção individual
          final_colunas_localizar[path] = [] 
          final_colunas_comparacao[path] = []
      else:
          # Se o arquivo não precisa de ajuste e não foi pulado/cancelado, ele usa as configurações do primeiro arquivo
          final_colunas_localizar[path] = cols_loc_first_file
          final_colunas_comparacao[path] = cols_comp_first_file
          print(f"  [OK] Colunas do primeiro arquivo aplicáveis a '{file_name}'.")
      
      files_to_process_after_user_choices.append(path) # Adiciona o arquivo à lista final de processamento.

  # Após o loop de decisões de pular/cancelar, a lista `final_files_to_process` é atualizada
  final_files_to_process = files_to_process_after_user_choices
  # Remove arquivos para os quais nenhuma coluna de localização foi definida (mesmo que não tenham sido pulados explicitamente)
  final_files_to_process = [f for f in final_files_to_process if final_colunas_localizar.get(f)]

  # Agora, processa a lista `files_to_adjust` (que são os arquivos que precisam de seleção de coluna individual)
  for path in files_to_adjust:
      # Verifica se o arquivo ainda está na lista de processamento (não foi pulado ou cancelado)
      if path not in final_files_to_process:
          continue # Se o arquivo já foi pulado ou não tem colunas de localização, não tenta ajustar


      file_name = os.path.basename(path)
      current_file_cols_ordered = all_file_columns_map.get(path, [])

      os.system('cls' if os.name == 'nt' else 'clear')
      print("\n" + "="*50); print(f"[AJUSTE PARA ARQUIVO: '{file_name}']")

      adjusted_loc_cols = _prompt_for_column(
          current_file_cols_ordered,
          f"Selecione as colunas para LOCALIZAR DUPLICIDADE em '{file_name}'",
          "Escolha uma ou mais colunas que, em conjunto, definirão um registro duplicado para este arquivo.", is_single_selection=False
      )
      if not adjusted_loc_cols:
          print(f"  [AVISO] Nenhuma coluna de Localizar Duplicidade foi selecionada para '{file_name}'. Este arquivo será ignorado.")
          if path in final_colunas_localizar: del final_colunas_localizar[path]
          if path in final_colunas_comparacao: del final_colunas_comparacao[path]
          continue # Pula para o próximo arquivo na lista files_to_adjust

      final_colunas_localizar[path] = adjusted_loc_cols

      available_for_comp_this_file = [col for col in current_file_cols_ordered if col not in adjusted_loc_cols]
      cols_comp_to_use_for_prompt = available_for_comp_this_file

      if not available_for_comp_this_file and common_columns:
          cols_comp_to_use_for_prompt = [c for c in common_columns if c not in adjusted_loc_cols]

      adjusted_comp_cols = _prompt_for_column(
          cols_comp_to_use_for_prompt,
          f"Selecione as colunas para COMPARAÇÃO em '{file_name}'",
          "Escolha colunas que devem ser verificadas quanto a DIFERENÇAS dentro dos grupos de duplicados (identificados pelas colunas de localização).", is_single_selection=False
      )
      final_colunas_comparacao[path] = adjusted_comp_cols

      print(f"  [OK] Seleção para '{file_name}' concluída.")

  # Revalida a lista final de arquivos a serem processados, no caso de algum ajuste individual ter resultado em nenhuma coluna de localização.
  final_files_to_process = [f for f in files_to_process_after_user_choices if final_colunas_localizar.get(f)]

  os.system('cls' if os.name == 'nt' else 'clear')
  print("\n" + "="*60); print("--- RESUMO FINAL DA CONFIGURAÇÃO DE COLUNAS ---")
  if not final_files_to_process:
      print("\n[ERRO] Nenhuma configuração válida de coluna foi feita para nenhum arquivo. Abortando análise."); input("Pressione Enter para continuar."); return

  for path in final_files_to_process:
      loc_cols = final_colunas_localizar.get(path)
      comp_cols = final_colunas_comparacao.get(path, [])
      file_name = os.path.basename(path)
      print(f"  - Arquivo: '{file_name}'"); print(f"    - Colunas para LOCALIZAR Duplicidade: {', '.join(loc_cols)}"); print(f"    - Colunas para COMPARAÇÃO: {', '.join(comp_cols) if comp_cols else '[Nenhuma]'}")

  input("\nPressione Enter para iniciar o processamento da duplicidade com as configurações acima...")


  # --- INÍCIO DO BLOCO DE FILTRO EXATO ---
  coluna_filtro_exato = None
  valor_filtro_exato = None
  
  # A pergunta sobre o filtro agora é feita independentemente do número de arquivos.
  # Coleta todas as colunas de localização de TODOS os arquivos que SERÃO PROCESSADOS
  all_loc_cols_possible_for_filter = set()
  for path in final_files_to_process: # Usa final_files_to_process para garantir que as colunas existam
      all_loc_cols_possible_for_filter.update(final_colunas_localizar[path])
  
  cols_for_filter_prompt = sorted(list(all_loc_cols_possible_for_filter))

  if cols_for_filter_prompt: # Só pergunta se há colunas de localização para filtrar
      os.system('cls' if os.name == 'nt' else 'clear')
      print("\n" + "="*50)
      print("--- FILTRAR DUPLICIDADE POR VALOR EXATO? ---")
      print("Deseja aplicar um filtro por valor exato antes de buscar a duplicidade?")
      print("Isso limitará a busca a registros que contenham um valor específico em uma das colunas de localização.")
      filter_choice = input("Digite 's' para SIM ou 'n' para NÃO: ").strip().lower()

      if filter_choice == 's':
          os.system('cls' if os.name == 'nt' else 'clear')
          print("\n" + "="*50)
          print("--- SELEÇÃO DA COLUNA PARA FILTRO EXATO ---")
          print("Selecione UMA das colunas de localização de duplicidade para aplicar o filtro exato.")
          print("\nColunas de Localização de Duplicidade disponíveis para filtro:")
          
          selected_col_for_filter = _prompt_for_column(
              cols_for_filter_prompt, # Usar a lista combinada de colunas de localização
              "Selecione a coluna para o filtro exato",
              "Escolha a coluna onde o valor exato será procurado.",
              is_single_selection=True
          )

          if selected_col_for_filter:
              coluna_filtro_exato = selected_col_for_filter
              valor_filtro_exato = input(f"\nDigite o VALOR EXATO a ser procurado na coluna '{coluna_filtro_exato}': ").strip()
              if not valor_filtro_exato:
                  print("Nenhum valor informado para o filtro. O filtro não será aplicado.")
                  coluna_filtro_exato = None 
          else:
              print("Nenhuma coluna selecionada para o filtro. O filtro não será aplicado.")
  # --- FIM DO BLOCO DE FILTRO EXATO ---


  all_dataframes_for_processing = []

  print("\n[PASSO 4/X] Lendo e preparando arquivos para verificação de duplicidade...")

  all_unique_original_cols = set()
  for path in final_files_to_process:
      all_unique_original_cols.update(all_file_columns_map.get(path, []))

  internal_cols_to_keep = ["__nome_arquivo__", "__num_linha_original__",
                           "__colunas_localizar_usadas__", "__colunas_comparacao_usadas__",
                           "__houve_divergencia_comparacao__"]
  for col in internal_cols_to_keep:
      all_unique_original_cols.add(col)


  for caminho_completo in final_files_to_process:
      nome_arquivo = os.path.basename(caminho_completo)
      colunas_localizar = final_colunas_localizar[caminho_completo]
      colunas_comparacao = final_colunas_comparacao.get(caminho_completo, [])

      print(f"  📖 Lendo '{nome_arquivo}' (Localizar: {colunas_localizar}, Comparar: {colunas_comparacao})...")
      df_loaded_duplicity = None 
      try:
          if caminho_completo.lower().endswith(('.xls', '.xlsx')):
              df_loaded_duplicity = pd.read_excel(caminho_completo, dtype=str)
          elif caminho_completo.lower().endswith('.csv'):
              sucesso_leitura_csv = False
              for encoding in ENCODINGS_TO_TRY:
                  try:
                      df_loaded_duplicity = pd.read_csv(caminho_completo, sep=';', dtype=str, encoding=encoding, on_bad_lines='skip',
                                       engine='python', quoting=csv.QUOTE_NONE)
                      sucesso_leitura_csv = True
                      break
                  except (UnicodeDecodeError, Exception) as e:
                      continue
              if not sucesso_leitura_csv:
                  print(f"    [ERRO] Não foi possível decodificar o CSV '{nome_arquivo}' com nenhuma codificação tentada. Pulando este arquivo.")
                  continue
          else:
              print(f"    [AVISO] Formato de arquivo não suportado para '{nome_arquivo}'. Pulando.")
              continue

          if df_loaded_duplicity is not None:
              df_loaded_duplicity.columns = df_loaded_duplicity.columns.astype(str).str.strip()

              # --- Aplicar o filtro exato AQUI, após o DataFrame ser carregado ---
              if coluna_filtro_exato and valor_filtro_exato:
                  if coluna_filtro_exato in df_loaded_duplicity.columns:
                      original_rows = len(df_loaded_duplicity)
                      # O filtro é de correspondência EXATA após normalização
                      df_loaded_duplicity = df_loaded_duplicity[
                          df_loaded_duplicity[coluna_filtro_exato].astype(str).apply(normalizar_texto) == normalizar_texto(valor_filtro_exato)
                      ].copy() 
                      print(f"    [INFO] Filtro exato '{valor_filtro_exato}' na coluna '{coluna_filtro_exato}' aplicado a '{nome_arquivo}'. Registros: {original_rows} -> {len(df_loaded_duplicity)}")
                      if df_loaded_duplicity.empty:
                          print(f"    [AVISO] Nenhum registro encontrado em '{nome_arquivo}' após o filtro. Pulando.")
                          continue 
                  else:
                      print(f"    [AVISO] Coluna de filtro '{coluna_filtro_exato}' não encontrada em '{nome_arquivo}'. Filtro ignorado para este arquivo.")
              # --- FIM DA APLICAÇÃO DO FILTRO ---

              original_cols_for_this_file = all_file_columns_map.get(caminho_completo, [])
              for col in original_cols_for_this_file:
                  if col not in df_loaded_duplicity.columns:
                      df_loaded_duplicity[col] = ''

              for col in all_unique_original_cols:
                  if col not in df_loaded_duplicity.columns:
                      df_loaded_duplicity[col] = ''

              df_loaded_duplicity["__nome_arquivo__"] = nome_arquivo
              df_loaded_duplicity["__num_linha_original__"] = df_loaded_duplicity.index + 2
              df_loaded_duplicity["__colunas_localizar_usadas__"] = json.dumps(colunas_localizar)
              df_loaded_duplicity["__colunas_comparacao_usadas__"] = json.dumps(colunas_comparacao)
              df_loaded_duplicity["__houve_divergencia_comparacao__"] = False

              all_dataframes_for_processing.append(df_loaded_duplicity)
              print(f"  ✅ Lido e preparado '{nome_arquivo}' com {len(df_loaded_duplicity)} linhas.")

      except Exception as e:
          print(f"  [ERRO] Erro inesperado ao processar '{nome_arquivo}': {e}. Pulando este arquivo."); continue

  filter_summary_message = "" # Definido antes do if/else
  if not all_dataframes_for_processing:
      print(f"\n[AVISO] Nenhum arquivo pôde ser lido ou preparado corretamente para processamento{filter_summary_message}. Retornando ao menu principal."); input("Pressione Enter para continuar."); return

  print("\n[INFO] Concatenando dados de todos os arquivos... Este passo pode levar tempo para grandes volumes de dados.")

  unified_columns_for_concat = sorted(list(all_unique_original_cols))

  normalized_dfs_for_concat = []
  for df_item in all_dataframes_for_processing:
      temp_df = pd.DataFrame(index=df_item.index, columns=unified_columns_for_concat)
      for col in df_item.columns:
          if col in unified_columns_for_concat:
              temp_df[col] = df_item[col]
      temp_df.fillna('', inplace=True)
      normalized_dfs_for_concat.append(temp_df)

  df_total = pd.concat(normalized_dfs_for_concat, ignore_index=True)
  print(f"[OK] Total de {len(df_total)} registros concatenados para análise de duplicidade.")

  filter_summary_message = ""
  if coluna_filtro_exato and valor_filtro_exato:
      filter_summary_message = f" (com filtro exato '{valor_filtro_exato}' na coluna '{coluna_filtro_exato}')"

  print("\n[INFO] Realizando a verificação de duplicidade e divergência...")

  def generate_localization_key(row):
      loc_cols_str = row["__colunas_localizar_usadas__"]
      try:
          loc_cols = json.loads(loc_cols_str)
      except json.JSONDecodeError:
          loc_cols = []

      key_parts = []
      for col in loc_cols:
          key_parts.append(normalizar_texto(row.get(col, '')))
      return tuple(key_parts)

  df_total["__chave_localizacao__"] = df_total.apply(generate_localization_key, axis=1)

  def calculate_group_divergence(group_slice):
      try:
          comp_cols = json.loads(group_slice.iloc[0]["__colunas_comparacao_usadas__"])
      except (json.JSONDecodeError, IndexError):
          return False

      if not comp_cols:
          return False

      comparison_keys_in_group = []
      for _, row_in_group_slice in group_slice.iterrows():
          current_row_comp_key_parts = []
          for col_name in comp_cols:
              current_row_comp_key_parts.append(normalizar_texto(row_in_group_slice.get(col_name, ''))) # Corrigido: row para row_in_group_slice
          comparison_keys_in_group.append(tuple(current_row_comp_key_parts))

      if len(set(comparison_keys_in_group)) > 1:
          return True
      return False


  num_unique_localization_groups = df_total["__chave_localizacao__"].nunique()

  divergence_status_results = []

  grouped = df_total.groupby("__chave_localizacao__")

  with tqdm(total=num_unique_localization_groups, desc="Verificando divergência por grupo") as pbar_groups:
      for name, group_slice in grouped:
          divergence_found = calculate_group_divergence(group_slice)
          divergence_status_results.append((name, divergence_found))
          pbar_groups.update(1)

  divergence_map = dict(divergence_status_results)

  df_total['__houve_divergencia_comparacao__'] = df_total['__chave_localizacao__'].map(divergence_map)


  duplicados = df_total[df_total["__chave_localizacao__"].duplicated(keep=False)].copy()

  cols_to_drop_after_process = [
      "__chave_localizacao__",
  ]
  duplicados.drop(columns=cols_to_drop_after_process, errors='ignore', inplace=True)


  if duplicados.empty:
      print(f"\n🎉 Nenhuma duplicidade encontrada com os critérios definidos{filter_summary_message}!"); input("Pressione Enter para voltar ao menu principal."); return

  total_registros_duplicados = len(duplicados)

  total_registros_com_divergencia = duplicados['__houve_divergencia_comparacao__'].sum()

  print("\n" + "="*60); print("--- RESULTADOS DA VERIFICAÇÃO DE DUPLICIDADE ---"); print(f"Total de registros duplicados (pelas colunas de localização): {total_registros_duplicados}{filter_summary_message}"); print(f"Total de registros duplicados COM DIVERGÊNCIA (nas colunas de comparação): {total_registros_com_divergencia}{filter_summary_message}"); print("="*60)

  print("\nDetalhes por arquivo de origem:"); contagem_por_arquivo = duplicados["__nome_arquivo__"].value_counts()
  for arq, qtde in contagem_por_arquivo.items():
      divergence_count = duplicados[
          (duplicados["__nome_arquivo__"] == arq) &
          (duplicados["__houve_divergencia_comparacao__"] == True)
      ].shape[0]
      print(f" - {arq}: {qtde} registro(s) duplicado(s) ({divergence_count} com divergência)")
  print("-" * 60)

  duplicados_info_for_report = []
  localization_key_to_filenames = defaultdict(set)

  for index, row in duplicados.iterrows():
      loc_cols_str = row["__colunas_localizar_usadas__"]
      try:
          loc_cols = json.loads(loc_cols_str)
      except json.JSONDecodeError:
          loc_cols = []

      loc_key = tuple(normalizar_texto(row.get(col, '')) for col in loc_cols)

      localization_key_to_filenames[loc_key].add(row["__nome_arquivo__"])


  for index, row in tqdm(duplicados.iterrows(), total=len(duplicados), desc="Preparando dados para Relatórios"):
      nome_arquivo = row["__nome_arquivo__"]
      num_linha = row["__num_linha_original__"]

      col_loc_usadas = json.loads(row["__colunas_localizar_usadas__"])
      cols_comp_usadas = json.loads(row["__colunas_comparacao_usadas__"])
      houve_divergencia = row["__houve_divergencia_comparacao__"]

      original_path = next((p for p in arquivos_selecionados_paths if os.path.basename(p) == nome_arquivo), None)

      clean_row_dict = OrderedDict()
      if original_path and original_path in all_file_columns_map:
          ordered_original_cols = all_file_columns_map[original_path]
          for col in ordered_original_cols:
              if col in row.index and not col.startswith('__'):
                  clean_row_dict[col] = "" if pd.isna(row[col]) or (isinstance(row[col], str) and str(row[col]).lower() == 'nan') else row[col]
      else:
          for k, v in row.to_dict().items():
              if not k.startswith('__'):
                  clean_row_dict[k] = "" if pd.isna(v) or (isinstance(v, str) and str(v).lower() == 'nan') else v

      loc_key_for_this_row = tuple(normalizar_texto(row.get(col, '')) for col in col_loc_usadas)
      related_files_for_this_item = sorted(list(localization_key_to_filenames.get(loc_key_for_this_row, set())))

      duplicados_info_for_report.append({
          "nome_arquivo": nome_arquivo,
          "num_linha": num_linha,
          "linha_completa": clean_row_dict,
          "colunas_localizar_usadas": col_loc_usadas,
          "colunas_comparacao_usadas": cols_comp_usadas,
          "houve_divergencia_comparacao": houve_divergencia,
          "arquivos_relacionados": related_files_for_this_item
      })

  pasta_saida = "Duplicidade_Results"
  os.makedirs(pasta_saida, exist_ok=True)

  print("\n[INFO] Gerando arquivos CSV para cada arquivo com registros divergentes (se houver)...")
  duplicados_com_divergencia_para_csv = duplicados[duplicados["__houve_divergencia_comparacao__"] == True].copy()

  if duplicados_com_divergencia_para_csv.empty:
      print("  [INFO] Nenhuma divergência encontrada nas colunas de comparação. Nenhum arquivo CSV de divergência será gerado.")
  else:
      for arq_name in duplicados_com_divergencia_para_csv["__nome_arquivo__"].unique():
          df_arq = duplicados_com_divergencia_para_csv[duplicados_com_divergencia_para_csv["__nome_arquivo__"] == arq_name].copy()

          original_path_for_arq = next((p for p in arquivos_selecionados_paths if os.path.basename(p) == arq_name), None)

          if original_path_for_arq and original_path_for_arq in all_file_columns_map:
              cols_to_keep_ordered = all_file_columns_map[original_path_for_arq]
          else:
              cols_to_keep_ordered = sorted([c for c in df_arq.columns if not c.startswith('__')])

          final_cols_for_csv_output = [col for col in cols_to_keep_ordered if col in df_arq.columns and not col.startswith('__')]

          df_arq_final_csv = df_arq[final_cols_for_csv_output].copy()

          df_arq_final_csv.fillna('', inplace=True)
          for col in df_arq_final_csv.columns:
              df_arq_final_csv.loc[:, col] = df_arq_final_csv[col].apply(lambda x: "" if (isinstance(x, str) and str(x).lower() == 'nan') else x)


          loc_cols_for_this_file = None
          for path, loc_cols in final_colunas_localizar.items():
              if os.path.basename(path) == arq_name:
                  loc_cols_for_this_file = loc_cols
                  break

          if loc_cols_for_this_file:
              valid_sort_cols = [col for col in loc_cols_for_this_file if col in df_arq_final_csv.columns]
              if valid_sort_cols:
                  df_arq_final_csv = df_arq_final_csv.sort_values(by=valid_sort_cols, ascending=True)
              else:
                  print(f"  [AVISO] Colunas de ordenação '{loc_cols_for_this_file}' não encontradas no CSV de saída para '{arq_name}'. Não será ordenada.")


          nome_saida = os.path.join(pasta_saida, f"{os.path.splitext(arq_name)[0]}_divergencia_comparacao.csv")
          try:
              df_arq_final_csv.to_csv(nome_saida, sep=';', index=False, encoding='utf-8-sig')
              print(f"  ✅ CSV gerado: '{nome_saida}' ({len(df_arq_final_csv)} registros)")
          except Exception as e:
              print(f"  ❌ Erro ao gerar CSV para '{arq_name}': {e}")

  print("\n[INFO] Iniciando Geração do Relatório PDF Consolidado de Duplicidade... Este processo pode levar tempo para muitos registros.")

  pdf_info_filtered_for_divergence = [item for item in duplicados_info_for_report if item['houve_divergencia_comparacao']]

  if not pdf_info_filtered_for_divergence:
      print("  [INFO] Nenhuma divergência encontrada nas colunas de comparação. Nenhum relatório PDF de divergência será gerado.")
      input("\nPressione Enter para voltar ao menu principal.")
      return

  pdf = ReportPDF(title='Relatório de Divergência de Registros Duplicados'); pdf.alias_nb_pages(); pdf.add_page()
  caminho_completo_saida_pdf = os.path.join(pasta_saida, "Relatorio_Divergencia_Comparacao_Consolidado.pdf")

  pdf.set_y(pdf.get_y() + 10)

  pdf.set_font('Helvetica', 'B', 16)
  pdf.cell(0, 12, f"Resumo da Verificação de Duplicidade e Divergência{filter_summary_message}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C') # Mensagem de filtro aqui
  pdf.ln(8)

  pdf.set_font('Helvetica', '', 12)
  pdf.set_text_color(50, 50, 50)

  total_page_width_for_content = pdf.w - pdf.l_margin - pdf.r_margin

  label_max_print_width = 150
  value_numeric_width = 30

  start_x_for_totals = (total_page_width_for_content - (label_max_print_width + value_numeric_width)) / 2 + pdf.l_margin

  label1 = "Total de registros duplicados (pelas colunas de localização):"
  value1 = f"{total_registros_duplicados}"

  pdf.set_x(start_x_for_totals)
  pdf.cell(label_max_print_width, 8, label1, align='L')

  pdf.set_xy(pdf.get_x(), pdf.get_y())
  pdf.set_font('Helvetica', 'B', 12)
  pdf.cell(value_numeric_width, 8, value1, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')

  label2 = "Total de registros duplicados COM DIVERGÊNCIA (nas colunas de comparação):"
  value2 = f"{total_registros_com_divergencia}"

  pdf.set_font('Helvetica', '', 12)
  pdf.set_x(start_x_for_totals) # Corrigido: start_x_table para start_x_for_totals
  pdf.cell(label_max_print_width, 8, label2, align='L')

  pdf.set_xy(pdf.get_x(), pdf.get_y())
  pdf.set_font('Helvetica', 'B', 12)
  pdf.cell(value_numeric_width, 8, value2, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R')

  pdf.set_text_color(0, 0, 0)
  pdf.ln(15)

  pdf.set_font('Helvetica', 'B', 14)
  pdf.cell(0, 10, "Detalhes por Arquivo de Origem", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C')
  pdf.ln(5)

  file_name_col_width = pdf.w * 0.45
  stats_col_width = pdf.w * 0.45

  pdf.set_font('Helvetica', 'B', 10)
  pdf.set_fill_color(220, 220, 220)

  total_cols_width = file_name_col_width + stats_col_width
  start_x_table = (pdf.w - total_cols_width) / 2

  pdf.set_x(start_x_table)
  pdf.cell(file_name_col_width, 8, "Arquivo", border=1, align='C', fill=True)
  pdf.cell(stats_col_width, 8, "Registros Duplicados (com divergência)", new_x=XPos.LMARGIN, new_y=YPos.NEXT, border=1, align='C', fill=True)

  pdf.set_font('Helvetica', '', 9)
  pdf.set_fill_color(255, 255, 255)

  for arq, qtde in contagem_por_arquivo.items():
      divergence_count = duplicados[
          (duplicados["__nome_arquivo__"] == arq) &
          (duplicados["__houve_divergencia_comparacao__"] == True)
      ].shape[0]

      pdf.set_x(start_x_table)
      pdf.cell(file_name_col_width, 7, f"{arq}", border='LR', align='L')
      pdf.cell(stats_col_width, 7, f"{qtde} registro(s) duplicado(s) ({divergence_count} com divergência)", new_x=XPos.LMARGIN, new_y=YPos.NEXT, border='LR', align='L')

  pdf.set_x(start_x_table)
  pdf.cell(total_cols_width, 0, '', border='T', new_x=XPos.LMARGIN, new_y=YPos.NEXT)

  pdf.ln(10)


  grouped_duplicados_for_pdf = defaultdict(list)
  for item in pdf_info_filtered_for_divergence:
      grouped_duplicados_for_pdf[item['nome_arquivo']].append(item)

  cell_h = 6

  total_records_for_pdf = len(pdf_info_filtered_for_divergence)
  progress_bar_pdf = tqdm(total=total_records_for_pdf, desc="Gerando PDF de Divergência")

  all_pdf_items_to_sort = []
  for file_name, items_in_file in grouped_duplicados_for_pdf.items():
      loc_cols_for_sort = items_in_file[0]['colunas_localizar_usadas']
      comp_cols_for_file = items_in_file[0]['colunas_comparacao_usadas'] # Definir antes do loop de sorting

      items_in_file.sort(key=lambda item: tuple(
          normalizar_texto(item['linha_completa'].get(col, '')) for col in loc_cols_for_sort + comp_cols_for_file
      ))
      all_pdf_items_to_sort.extend(items_in_file)

  current_file_processed = None
  for item_idx, item in enumerate(all_pdf_items_to_sort):
      file_name = item['nome_arquivo']

      is_new_file_section = False
      if file_name != current_file_processed:
          if current_file_processed is not None:
              pdf.ln(10)
          is_new_file_section = True
          current_file_processed = file_name

      related_files_msg_height = 0
      related_files_str = ""
      if item['houve_divergencia_comparacao'] and item['arquivos_relacionados'] and len(item['arquivos_relacionados']) > 1:
          other_files_with_divergence = [f for f in item['arquivos_relacionados'] if f != item['nome_arquivo']]
          if other_files_with_divergence:
              related_files_str = f"Este registro também foi encontrado nos arquivos: {', '.join(other_files_with_divergence)}"
              try:
                  pdf.set_font('DejaVuSans', 'B', 9)
              except RuntimeError:
                  pdf.set_font('Helvetica', 'B', 9)
                  related_files_str = related_files_str.replace(u"\u2022", "-") # Fallback for special chars

              effective_page_width = pdf.w - pdf.l_margin - pdf.r_margin
              # Calculate required height for multi_cell correctly
              # This is an approximation; FPDF's get_string_width only gives single line width
              # We need to estimate lines by dividing total text width by effective cell width.
              # A more precise way would involve FPDF's internal text splitting, but that's complex.
              estimated_text_width_in_cell = pdf.get_string_width(related_files_str)
              num_lines_estimated = (estimated_text_width_in_cell / effective_page_width) + 0.99 # Add a buffer for rounding
              related_files_msg_height = max(cell_h, int(num_lines_estimated) * cell_h + 1) # Ensure at least one line height

      principal_vals_display_parts = []
      for col in item['colunas_localizar_usadas']:
          # Garante que 'nan' não seja exibido aqui também
          val_for_display = "" if pd.isna(item['linha_completa'].get(col)) or \
                                  (isinstance(item['linha_completa'].get(col), str) and \
                                   str(item['linha_completa'].get(col)).lower() == 'nan') \
                            else item['linha_completa'].get(col, '')
          principal_vals_display_parts.append(f"{col}: {val_for_display}")
      principal_vals_display = '; '.join(principal_vals_display_parts)

      # Inicializa divergence_status antes do if, para garantir que sempre exista
      divergence_status = ""
      pdf.set_font('Helvetica', 'B', 10)
      if item['houve_divergencia_comparacao']:
          pdf.set_text_color(255, 0, 0)
          divergence_status = " - [DIVERGÊNCIA DE DADOS NAS COLUNAS DE COMPARAÇÃO]"


      linha_original_text = f"Linha Original: {item['num_linha']} | Valores principais duplicados: '{principal_vals_display}'{divergence_status}"
      effective_page_width = pdf.w - pdf.l_margin - pdf.r_margin
      # Recalcular altura para linha_original_text
      estimated_text_width_line_original = pdf.get_string_width(linha_original_text)
      num_lines_original_estimated = (estimated_text_width_line_original / effective_page_width) + 0.99
      line_original_height = max(cell_h, int(num_lines_original_estimated) * cell_h + 1)

      num_data_rows = len(item['linha_completa'].keys())
      data_table_height = (2 * cell_h) + (num_data_rows * cell_h)

      total_item_display_height = related_files_msg_height + line_original_height + data_table_height + 5

      # Calcula col1_width e col2_width aqui, antes de qualquer possível quebra de página
      all_cols_in_row_dict = item['linha_completa']
      all_cols_in_row_names = list(all_cols_in_row_dict.keys())
      max_col_name_len = 0
      if all_cols_in_row_names:
          max_col_name_len = max(len(str(c)) for c in all_cols_in_row_names)
      col1_width = min(pdf.w * 0.3, max_col_name_len * 2 + 5)
      col2_width = pdf.w - pdf.l_margin - pdf.r_margin - col1_width


      if pdf.get_y() + total_item_display_height > (pdf.h - pdf.b_margin - 10):
          pdf.add_page()
          # Adiciona cabeçalho da tabela de conteúdo da linha se pular de página
          # Garante que `col1_width` e `col2_width` sejam numéricos e válidos.
          if col1_width <= 0: col1_width = pdf.w * 0.3
          if col2_width <= 0: col2_width = pdf.w - pdf.l_margin - pdf.r_margin - col1_width
          
          pdf.set_font('Helvetica', 'B', 10)
          pdf.cell(col1_width, cell_h, 'Campo', border=1, align='C')
          pdf.cell(col2_width, cell_h, 'Valor', new_x=XPos.LMARGIN, new_y=YPos.NEXT, border=1, align='C')
          pdf.set_font('Helvetica', '', 9)
          pdf.cell(0, cell_h, f"Continuação do registro no arquivo: {file_name} (Pág. {pdf.page_no()})", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C')
          pdf.ln(2)

      if is_new_file_section:
          pdf.set_font('Helvetica', 'B', 14)
          pdf.set_fill_color(50, 50, 150); pdf.set_text_color(255, 255, 255)
          pdf.cell(0, 10, f"Arquivo: {file_name}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C', border=0, fill=True)
          pdf.set_text_color(0, 0, 0)
          pdf.ln(2)

          col_loc_for_file = item['colunas_localizar_usadas']
          cols_comp_for_file = item['colunas_comparacao_usadas']

          pdf.set_font('Helvetica', 'B', 10)
          pdf.multi_cell(0, cell_h, f"  Colunas para LOCALIZAR Duplicidade: {', '.join(col_loc_for_file)}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
          pdf.multi_cell(0, cell_h, f"  Colunas para COMPARAÇÃO: {', '.join(cols_comp_for_file) if cols_comp_for_file else '[Nenhuma adicional]'}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
          pdf.ln(2)


      if related_files_str:
          try:
              pdf.set_font('DejaVuSans', 'B', 9)
          except RuntimeError:
              pdf.set_font('Helvetica', 'B', 9)
              related_files_str = related_files_str.replace(u"\u2022", "-") # Fallback for special chars

          pdf.set_text_color(100, 100, 100)
          pdf.multi_cell(0, cell_h, related_files_str, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
          pdf.set_text_color(0, 0, 0)
          pdf.ln(1)

      pdf.set_font('Helvetica', 'B', 10)
      if item['houve_divergencia_comparacao']:
          pdf.set_text_color(255, 0, 0)

      pdf.multi_cell(0, cell_h, linha_original_text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
      pdf.set_text_color(0, 0, 0)
      pdf.set_font('Helvetica', '', 9)

      if all_cols_in_row_names: # Verificação para evitar imprimir tabela vazia
          pdf.cell(col1_width, cell_h, 'Campo', border=1, align='C')
          pdf.cell(col2_width, cell_h, 'Valor', new_x=XPos.LMARGIN, new_y=YPos.NEXT, border=1, align='C')

          for col in all_cols_in_row_names:
              val = all_cols_in_row_dict.get(col, '')
              val_str = str(val) 

              text_width_for_wrap = pdf.get_string_width(val_str)
              effective_cell_width_for_text = col2_width - (2 * pdf.c_margin)
              num_lines_required = (text_width_for_wrap / effective_cell_width_for_text)
              required_height_for_value_cell = max(cell_h, (int(num_lines_required) + (1 if num_lines_required > int(num_lines_required) else 0)) * cell_h)

              if pdf.get_y() + required_height_for_value_cell > (pdf.h - pdf.b_margin - 10):
                  pdf.add_page()
                  pdf.set_font('Helvetica', 'B', 10)
                  pdf.cell(col1_width, cell_h, 'Campo', border=1, align='C')
                  pdf.cell(col2_width, cell_h, 'Valor', new_x=XPos.LMARGIN, new_y=YPos.NEXT, border=1, align='C')
                  pdf.set_font('Helvetica', '', 9)
                  pdf.cell(0, cell_h, f"Continuação do registro no arquivo: {file_name} (Pág. {pdf.page_no()})", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C')
                  pdf.ln(2)

              x_start = pdf.get_x()
              y_start = pdf.get_y()

              fill_color_applied = False
              if col in item['colunas_localizar_usadas']:
                  pdf.set_fill_color(200, 220, 255)
                  fill_color_applied = True
              elif col in item['colunas_comparacao_usadas']:
                  pdf.set_fill_color(230, 230, 230)
                  fill_color_applied = True

              pdf.cell(col1_width, cell_h, str(col), border='L', fill=fill_color_applied, align='L')

              pdf.set_xy(x_start + col1_width, y_start)
              pdf.multi_cell(col2_width, pdf.font_size * 1.2, val_str, border='R', new_x=XPos.LMARGIN, new_y=YPos.NEXT, fill=fill_color_applied, align='L')

              if fill_color_applied:
                  pdf.set_fill_color(255, 255, 255)

          pdf.ln(5) # Linha adicionada para espaçamento após a tabela do item, se ela for impressa
      else:
          pdf.set_font('Helvetica', 'I', 8)
          pdf.cell(0, cell_h, "[Nenhuma coluna com valor para exibir nesta linha]", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C')
          pdf.ln(2)

      progress_bar_pdf.update(1)
  pdf.ln(10)
  progress_bar_pdf.close()

  pdf.output(caminho_completo_saida_pdf); print(f"✅ Relatório PDF de divergência '{caminho_completo_saida_pdf}' gerado com sucesso!")


  input("\nPressione Enter para voltar ao menu principal.")


# Na função _tratar_campos():
def _tratar_campos():
    root = Tk(); root.withdraw(); root.attributes('-topmost', True)
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "="*50)
    print("--- Módulo de Tratamento e Padronização de Dados ---")

    # 1. Seleção do arquivo - AGORA USA A NOVA FUNÇÃO MAIS GENÉRICA
    arquivos_para_tratar = _selecionar_arquivos_para_tratamento() 
    if not arquivos_para_tratar:
        print("\nNenhum arquivo selecionado para tratamento. Retornando.")
        input("Pressione Enter para continuar.")
        return

    # Processar cada arquivo selecionado
    for caminho_original in arquivos_para_tratar:
        nome_arquivo = os.path.basename(caminho_original)
        print(f"\nProcessando arquivo: '{nome_arquivo}'")

        df = None
        # Lógica para carregar o arquivo em um DataFrame (CSV ou Excel)
        if caminho_original.lower().endswith(('.csv')):
            for encoding in ENCODINGS_TO_TRY:
                try:
                    df = pd.read_csv(caminho_original, sep=';', dtype=str, encoding=encoding,
                                     on_bad_lines='skip', engine='python', quoting=csv.QUOTE_NONE, escapechar='\\').fillna('')
                    break
                except UnicodeDecodeError: continue
                except Exception as e: 
                    print(f"❌ Erro ao ler CSV '{nome_arquivo}': {e}. Pulando.")
                    pass # Continue para o próximo encoding
            if df is None: # Se df ainda for None após tentar todos os encodings
                print(f"❌ Erro: Não foi possível ler o CSV '{nome_arquivo}'. Pulando.")
                continue
        elif caminho_original.lower().endswith(('.xlsx', '.xls')):
            try:
                # Carregar o Excel. Para simplicidade, apenas a primeira aba.
                excel_file = pd.ExcelFile(caminho_original)
                sheet_name = excel_file.sheet_names[0] if excel_file.sheet_names else None
                if sheet_name:
                    print(f"  Lendo aba '{sheet_name}' do Excel...")
                    df = pd.read_excel(excel_file, sheet_name=sheet_name, dtype=str).fillna('')
                else:
                    print(f"❌ Erro: Arquivo Excel '{nome_arquivo}' não possui abas. Pulando.")
                    continue
            except Exception as e:
                print(f"❌ Erro: Não foi possível ler o Excel '{nome_arquivo}': {e}. Pulando.")
                continue
        else:
            print(f"❌ Erro: Formato de arquivo não suportado para '{nome_arquivo}'. Pulando.")
            continue

        if df.empty:
            print(f"[AVISO] O arquivo '{nome_arquivo}' está vazio. Pulando.")
            continue

        df_modificado = df.copy() # Criar uma cópia para modificações

        while True:
            os.system('cls' if os.name == 'nt' else 'clear')
            print(f"\n--- Tratando: '{nome_arquivo}' ---")
            print("Colunas Atuais:"); print(list(df_modificado.columns))
            print("\nOpções de Tratamento:")
            print(" 1. Renomear Colunas (interativo)")
            print(" 2. Padronizar Nomes de Colunas (usar regras Oracle)") 
            print(" 3. Limpeza de Texto em Colunas (Ex: remover espaços, caracteres especiais, case)")
            print("\n [p] Pré-visualizar dados (primeiras 5 linhas)")
            print(" [s] Salvar arquivo tratado como novo")
            print(" [v] Voltar ao menu principal (descartar alterações para este arquivo)")

            escolha_tratamento = input("\nDigite sua opção: ").strip().lower()

            if escolha_tratamento == '1':
                # Renomeação Interativa
                print("\n--- Renomear Colunas ---")
                colunas_atuais = list(df_modificado.columns)
                for i, col in enumerate(colunas_atuais):
                    print(f" {i+1}. {col}")
                
                while True:
                    idx_str = input("Digite o NÚMERO da coluna a renomear (ou 'd' para finalizar): ").strip().lower()
                    if idx_str == 'd': break
                    try:
                        idx = int(idx_str) - 1
                        if 0 <= idx < len(colunas_atuais):
                            col_original = colunas_atuais[idx]
                            novo_nome = input(f"Novo nome para '{col_original}': ").strip()
                            if novo_nome:
                                # Verifica se o novo nome já existe (após renomeação anterior) para evitar colisões
                                if novo_nome in df_modificado.columns.drop(col_original, errors='ignore'):
                                    print(f"AVISO: A coluna '{novo_nome}' já existe. Por favor, escolha um nome único.")
                                else:
                                    df_modificado.rename(columns={col_original: novo_nome}, inplace=True)
                                    print(f"Coluna '{col_original}' renomeada para '{novo_nome}'.")
                                    colunas_atuais[idx] = novo_nome 
                            else:
                                print("Nome inválido.")
                        else:
                            print("Número inválido.")
                    except ValueError:
                        print("Entrada inválida.")
                    input("Pressione Enter para continuar...") 
                
            elif escolha_tratamento == '2':
                # Padronizar Nomes de Colunas (regras Oracle)
                print("\n--- Padronizar Nomes de Colunas (Regras Oracle) ---")
                old_to_new_names = {}
                for col in df_modificado.columns:
                    new_col_name = _rename_column_for_oracle(col) # Reutiliza a função existente
                    if col != new_col_name:
                        # Verifica se o nome renomeado já existe após o processamento
                        if new_col_name in df_modificado.columns.drop(col, errors='ignore'):
                            print(f"AVISO: A coluna '{new_col_name}' já existe após padronização de '{col}'. Renomeação para '{col}' ignorada para evitar duplicação.")
                        else:
                            old_to_new_names[col] = new_col_name
                
                if old_to_new_names:
                    df_modificado.rename(columns=old_to_new_names, inplace=True)
                    print("Colunas padronizadas para regras Oracle:")
                    for old, new in old_to_new_names.items():
                        print(f" - '{old}' -> '{new}'")
                else:
                    print("Nenhuma coluna precisou ser padronizada de acordo com as regras Oracle.")
                input("Pressione Enter para continuar...")

            elif escolha_tratamento == '3':
                # Limpeza de Texto em Colunas
                print("\n--- Limpeza de Texto em Colunas ---")
                colunas_atuais = list(df_modificado.columns)
                for i, col in enumerate(colunas_atuais):
                    print(f" {i+1}. {col}")
                
                col_idx_str = input("Digite o NÚMERO da coluna para limpar (ou 'd' para finalizar): ").strip().lower()
                if col_idx_str == 'd': continue
                try:
                    col_idx = int(col_idx_str) - 1
                    if 0 <= col_idx < len(colunas_atuais):
                        col_para_limpar = colunas_atuais[col_idx]
                        
                        while True:
                            os.system('cls' if os.name == 'nt' else 'clear') 
                            print(f"\n--- Limpeza de Texto para a Coluna: '{col_para_limpar}' ---")
                            print("Selecione uma ou mais opções de limpeza (separadas por vírgula, ex: 1,2,5):")
                            print("  1. Remover espaços extras (início, fim, múltiplos)")
                            print("  2. Converter para MAIÚSCULAS")
                            print("  3. Converter para MINÚSCULAS")
                            print("  4. Remover caracteres não alfanuméricos (manter letras, números, espaços)")
                            print("  5. Remover acentos (normalização)")
                            print("  6. Substituir Caractere/Texto (ex: '-' por '_', '@' por 'a')")
                            print("  7. Aplicar TODOS os tratamentos (padrão: MAIÚSCULAS)")
                            print("  8. Truncar caracteres da coluna") # NOVA OPÇÃO AQUI
                            print("\n [d] Finalizar seleção para esta coluna")
                            
                            escolhas_limpeza_str = input("Sua(s) escolha(s): ").strip().lower()

                            if escolhas_limpeza_str == 'd':
                                print(f"Limpeza de '{col_para_limpar}' finalizada.")
                                break 

                            opcoes_selecionadas = [op.strip() for op in escolhas_limpeza_str.split(',') if op.strip().isdigit()]
                            
                            if not opcoes_selecionadas:
                                print("Entrada inválida. Por favor, digite os números das opções separados por vírgula.")
                                time.sleep(1.5)
                                continue

                            # Lógica para a opção "TODOS"
                            if '7' in opcoes_selecionadas:
                                # Define as opções padrão para "TODOS"
                                # Ordem: 5 (acentos) -> 4 (não alfanuméricos) -> 1 (espaços) -> 2 (MAIÚSCULAS) -> 6 (substituição)
                                opcoes_a_aplicar_todos = ['5', '4', '1', '2'] 
                                
                                # Remove 3 (MINÚSCULAS) se 2 (MAIÚSCULAS) estiver em TODOS
                                if '2' in opcoes_a_aplicar_todos and '3' in opcoes_selecionadas:
                                    opcoes_selecionadas.remove('3')
                                    print("  (Opção 3 'MINÚSCULAS' ignorada devido à escolha de 'TODOS' ou 'MAIÚSCULAS'.)")

                                # Combina as opções de 'TODOS' com quaisquer outras escolhidas manualmente,
                                # garantindo a ordem e removendo duplicatas.
                                # Usa um OrderedDict para manter a ordem e a unicidade
                                opcoes_finais_ordenadas_set = OrderedDict()
                                for op in opcoes_a_aplicar_todos:
                                    opcoes_finais_ordenadas_set[op] = True
                                for op in opcoes_selecionadas:
                                    opcoes_finais_ordenadas_set[op] = True
                                
                                # Garante que '7' (TODOS) não seja processado como uma operação, apenas como um gatilho
                                if '7' in opcoes_finais_ordenadas_set:
                                    del opcoes_finais_ordenadas_set['7']

                                opcoes_selecionadas_para_executar = list(opcoes_finais_ordenadas_set.keys())
                                print(f"  (Aplicando TODAS as opções padrão: {opcoes_selecionadas_para_executar})")

                            else: # Se '7' (TODOS) NÃO foi escolhido
                                # Se o usuário selecionou 2 e 3 (MAIÚSCULAS e MINÚSCULAS)
                                if '2' in opcoes_selecionadas and '3' in opcoes_selecionadas:
                                    print("  AVISO: Não é possível aplicar MAIÚSCULAS e MINÚSCULAS simultaneamente. 'MINÚSCULAS' será ignorada.")
                                    opcoes_selecionadas.remove('3') # Remove MINÚSCULAS se MAIÚSCULAS foi escolhida
                                
                                # Garante ordem para as opções individuais
                                # Certifica-se de incluir a nova opção '8' aqui também
                                opcoes_selecionadas_para_executar = sorted([op for op in opcoes_selecionadas if op in ['1', '2', '3', '4', '5', '6', '8']])


                            aplicado_alguma_limpeza = False
                            # Aplica as opções de limpeza na ordem em que são definidas
                            for opcao in opcoes_selecionadas_para_executar: 
                                if opcao == '1':
                                    df_modificado[col_para_limpar] = df_modificado[col_para_limpar].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
                                    print(f" - Espaços extras removidos.")
                                elif opcao == '2':
                                    df_modificado[col_para_limpar] = df_modificado[col_para_limpar].astype(str).str.upper()
                                    print(f" - Convertido para MAIÚSCULAS.")
                                elif opcao == '3':
                                    df_modificado[col_para_limpar] = df_modificado[col_para_limpar].astype(str).str.lower()
                                    print(f" - Convertido para MINÚSCULAS.")
                                elif opcao == '4':
                                    df_modificado[col_para_limpar] = df_modificado[col_para_limpar].astype(str).str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
                                    print(f" - Caracteres não alfanuméricos removidos.")
                                elif opcao == '5':
                                    df_modificado[col_para_limpar] = df_modificado[col_para_limpar].astype(str).apply(normalizar_texto) 
                                    print(f" - Acentos removidos e texto normalizado.")
                                elif opcao == '6': # NOVA LÓGICA: Substituir Caractere/Texto
                                    char_to_replace = input(f"  Digite o caractere/texto a ser substituído na coluna '{col_para_limpar}': ").strip()
                                    replace_with = input(f"  Digite o caractere/texto para substituir '{char_to_replace}' por (deixe vazio para remover): ").strip()
                                    
                                    if char_to_replace:
                                        df_modificado[col_para_limpar] = df_modificado[col_para_limpar].astype(str).str.replace(char_to_replace, replace_with, regex=False) # regex=False para substituição literal
                                        print(f" - '{char_to_replace}' substituído por '{replace_with}' na coluna '{col_para_limpar}'.")
                                    else:
                                        print("  Caractere/texto a ser substituído não informado. Operação de substituição ignorada.")
                                    input("Pressione Enter para continuar...") # Pausa após a substituição
                                elif opcao == '8': # NOVA LÓGICA: Truncar caracteres
                                    while True:
                                        max_len_str = input(f"  Digite a quantidade MÁXIMA de caracteres para a coluna '{col_para_limpar}': ").strip()
                                        try:
                                            max_len = int(max_len_str)
                                            if max_len < 0:
                                                print("A quantidade de caracteres não pode ser negativa. Tente novamente.")
                                            else:
                                                break
                                        except ValueError:
                                            print("Entrada inválida. Por favor, digite um número inteiro.")
                                    df_modificado[col_para_limpar] = df_modificado[col_para_limpar].astype(str).str.slice(0, max_len)
                                    print(f" - Coluna '{col_para_limpar}' truncada para {max_len} caracteres.")
                                    input("Pressione Enter para continuar...") # Pausa após o truncamento
                                else:
                                    print(f"  Opção '{opcao}' inválida.") # Se um número fora do range for digitado
                                    input("Pressione Enter para continuar...")
                                    continue # Continua para a próxima opção selecionada, não quebra o loop

                                aplicado_alguma_limpeza = True
                            
                            if not aplicado_alguma_limpeza:
                                print("Nenhuma opção de limpeza válida foi selecionada.")
                            
                            # Não pedimos Enter aqui para cada opção se múltiplas foram selecionadas,
                            # apenas no final do bloco de seleção múltipla (após a iteração for).
                            if aplicado_alguma_limpeza:
                                print(f"Tratamentos aplicados para '{col_para_limpar}'.")
                            input("Pressione Enter para continuar a limpeza para esta coluna (ou 'd' para finalizar).") 
                        
                    else:
                        print("Número de coluna inválido.")
                        input("Pressione Enter para continuar...") 
                except ValueError:
                    print("Entrada inválida. Por favor, digite um número ou 'd'.")
                    input("Pressione Enter para continuar...") 


            elif escolha_tratamento == 'p':
                print("\n--- Pré-visualização (Primeiras 5 linhas) ---")
                if not df_modificado.empty:
                    # Itera sobre as primeiras 5 linhas e as exibe como tabela
                    for idx, row in df_modificado.head(5).iterrows():
                        print(f"\n--- Linha {idx + 1} ---")
                        _exibir_linha_como_tabela(row.to_dict())
                else:
                    print("O DataFrame está vazio. Nenhuma pré-visualização disponível.")
                input("\nPressione Enter para continuar...")

            elif escolha_tratamento == 's':
                print("\n--- Salvar Arquivo Tratado ---")
                
                # --- Lógica de Criação de Pastas e Nome de Arquivo Automático ---
                base_treated_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Arquivos Tratados")
                os.makedirs(base_treated_folder, exist_ok=True) 

                nome_base_original_sem_ext = os.path.splitext(nome_arquivo)[0]
                specific_treated_folder = os.path.join(base_treated_folder, f"{nome_base_original_sem_ext}_TRATADO")
                os.makedirs(specific_treated_folder, exist_ok=True) 
                
                nome_saida_sugerido = f"{nome_base_original_sem_ext}_TRATADO{os.path.splitext(nome_arquivo)[1]}"
                nome_saida_final = nome_saida_sugerido # Usar o nome sugerido automaticamente

                caminho_saida_completo = os.path.join(specific_treated_folder, nome_saida_final)
                
                # --- IMPLEMENTAÇÃO DO SALVAMENTO AUTOMÁTICO COM BARRA DE PROGRESSO ---
                try:
                    # Barra de progresso para a operação de salvamento
                    with tqdm(total=1, desc=f"Salvando '{nome_saida_final}'", unit="arquivo") as pbar_save:
                        if caminho_original.lower().endswith('.csv'):
                            df_modificado.to_csv(caminho_saida_completo, sep=';', index=False, encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
                        elif caminho_original.lower().endswith(('.xlsx', '.xls')):
                            df_modificado.to_excel(caminho_saida_completo, index=False, engine='openpyxl')
                        pbar_save.update(1) # Completa a barra de progresso

                    # Mensagem de sucesso aprimorada
                    print(f"✅ Arquivo tratado salvo com sucesso!")
                    print(f"   Pasta: '{specific_treated_folder}'")
                    print(f"   Arquivo: '{nome_saida_final}'")

                except Exception as e:
                    print(f"❌ Erro ao salvar o arquivo: {e}")
                input("Pressione Enter para continuar.")
                break # Sai do loop de tratamento para este arquivo, pois ele foi salvo.

            elif escolha_tratamento == 'v':
                print(f"Descartando alterações para '{nome_arquivo}'.")
                input("Pressione Enter para continuar.")
                break 

            else:
                print("Opção inválida. Tente novamente.")
                time.sleep(1)

    print("\nTratamento de campos concluído.")
    input("Pressione Enter para voltar ao menu de validação.")


# --- NOVA FUNÇÃO: _dividir_arquivos() ---
def _dividir_arquivos():
    root = Tk(); root.withdraw(); root.attributes('-topmost', True)
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "="*50)
    print("--- Módulo: Dividir Arquivos ---")

    arquivos_para_dividir = _selecionar_arquivos_para_tratamento() # Reutiliza a função de seleção
    if not arquivos_para_dividir:
        print("\nNenhum arquivo selecionado para divisão. Retornando.")
        input("Pressione Enter para continuar.")
        return

    while True:
        linhas_por_arquivo_str = input("\nDigite o NÚMERO de linhas desejado por arquivo dividido (apenas números inteiros): ").strip()
        try:
            linhas_por_arquivo = int(linhas_por_arquivo_str)
            if linhas_por_arquivo <= 0:
                print("O número de linhas deve ser maior que zero. Tente novamente.")
            else:
                break
        except ValueError:
            print("Entrada inválida. Por favor, digite um número inteiro.")
    
    base_divided_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Arquivos Divididos")
    os.makedirs(base_divided_folder, exist_ok=True)

    total_arquivos_divididos = 0
    
    for caminho_original in tqdm(arquivos_para_dividir, desc="Dividindo arquivos"):
        nome_arquivo = os.path.basename(caminho_original)
        nome_base_sem_ext = os.path.splitext(nome_arquivo)[0]
        extensao = os.path.splitext(nome_arquivo)[1]

        specific_output_folder = os.path.join(base_divided_folder, f"{nome_base_sem_ext}_DIVIDIDO")
        os.makedirs(specific_output_folder, exist_ok=True)

        df = None
        try:
            df = _carregar_arquivo_para_dataframe(caminho_original)
            if df is None or df.empty:
                print(f"  [AVISO] Arquivo '{nome_arquivo}' está vazio ou não pôde ser carregado. Pulando divisão.")
                continue

            num_total_linhas = len(df)
            num_partes = (num_total_linhas + linhas_por_arquivo - 1) // linhas_por_arquivo

            print(f"  Dividindo '{nome_arquivo}' ({num_total_linhas} linhas) em {num_partes} parte(s) de {linhas_por_arquivo} linhas cada...")

            for i in range(num_partes):
                start_row = i * linhas_por_arquivo
                end_row = min((i + 1) * linhas_por_arquivo, num_total_linhas)
                df_parte = df.iloc[start_row:end_row]

                nome_parte = f"{nome_base_sem_ext}_parte_{i+1}{extensao}"
                caminho_parte = os.path.join(specific_output_folder, nome_parte)

                if extensao.lower() == '.csv':
                    df_parte.to_csv(caminho_parte, sep=';', index=False, encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
                elif extensao.lower() in ('.xlsx', '.xls'):
                    df_parte.to_excel(caminho_parte, index=False, engine='openpyxl')
                
                print(f"    Parte {i+1} salva em '{os.path.basename(caminho_parte)}'.")
                total_arquivos_divididos += 1
        
        except Exception as e:
            print(f"  ❌ Erro ao dividir o arquivo '{nome_arquivo}': {e}. Pulando.")
            continue
            
    print(f"\nDivisão de arquivos concluída. Total de {total_arquivos_divididos} arquivos criados.")
    print(f"Os arquivos divididos foram salvos na pasta: '{os.path.abspath(base_divided_folder)}'")
    input("Pressione Enter para voltar ao menu de validação.")


def executar_validador_de_dados():
  root = Tk(); root.withdraw(); root.attributes('-topmost', True)
  while True:
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "="*50); print("--- Módulo de Validação de Dados ---"); print("Selecione uma análise para executar:"); print("-" * 50)
    print(" 1. Procurar por Datas Inválidas")
    print(" 2. Encontrar Duplicidade de Registro")
    print(" 3. Tratar Campos (Limpeza e Padronização)") 
    print(" 4. Dividir Arquivos") # NOVA OPÇÃO AQUI
    print("\n 0. Voltar ao Menu Principal")
    sub_opcao = input("\nDigite a opção desejada: ").strip()
    if sub_opcao == '1': _rodar_validacao_de_datas()
    elif sub_opcao == '2': _verificar_duplicidade_registros()
    elif sub_opcao == '3': _tratar_campos() 
    elif sub_opcao == '4': _dividir_arquivos() # CHAMADA PARA A NOVA FUNÇÃO
    elif sub_opcao == '0': break
    else: print("Opção inválida."); time.sleep(1)

# --- NOVO MÓDULO 4: IMPORTAR CARGA FUNCS ---

# Constantes e Definições de Pastas Base
DB_HISTORY_FILE = "db_connections_history.json"
IMPORT_HISTORY_FILE = "import_history.json"
IMPORT_BASE_FOLDER = "Importar_Carga"
DDL_SUBFOLDER = "DDL_Scripts"
CHECK_BASE_FOLDER = "Checagem_Banco_Resultados"
SQL_LOADER_SAFE_TEMP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlldr_temp")


# buscador_de_dados.py
# ... (código existente acima da função _create_temp_keys_table) ...

# Auxiliar para criar a tabela temporária de chaves
def _create_temp_keys_table(temp_table_name, cols_for_temp_table_ddl, db_conn_info):
    user, password, db = db_conn_info['user'], db_conn_info['password'], db_conn_info['db']
    
    column_definitions = []
    for i, col_name in enumerate(cols_for_temp_table_ddl):
        col_type = "VARCHAR2(4000)" 
        column_definitions.append(f"  \"{col_name}\" {col_type}")
        if i < len(cols_for_temp_table_ddl) - 1:
            column_definitions[-1] += ","

    ddl_content_parts = [
        "SET ECHO ON;",
        "WHENEVER SQLERROR CONTINUE;",
        f"DROP TABLE \"{temp_table_name}\" CASCADE CONSTRAINTS;",
        "COMMIT;", 
        "WHENEVER SQLERROR EXIT FAILURE;",
        f"CREATE GLOBAL TEMPORARY TABLE \"{temp_table_name}\" (",
    ]
    ddl_content_parts.extend(column_definitions)
    ddl_content_parts.extend([
        ") ON COMMIT PRESERVE ROWS;",
        "COMMIT;", 
        "EXIT;"
    ])
    create_temp_table_ddl = "\n".join(ddl_content_parts)

    print(f"  Criando tabela temporária '{temp_table_name}' no banco de dados...")
    temp_log_path = os.path.join(CHECK_BASE_FOLDER, f"{temp_table_name}_create_log.txt")

    # AQUI: Usando o retorno aprimorado de _executar_sql_comando
    success, message = _executar_sql_comando(create_temp_table_ddl, 'sqlplus', user, password, db, log_file_path=temp_log_path)
    if success:
        print(f"  ✅ Tabela temporária '{temp_table_name}' criada/recriada com sucesso.")
        return True
    else:
        print(f"  ❌ Falha ao criar tabela temporária '{temp_table_name}'. {message} Verifique '{temp_log_path}'.")
        return False

# Auxiliar para carregar chaves do arquivo local para a tabela temporária
def _load_keys_to_temp_table(temp_table_name, temp_csv_path, loc_cols_in_temp_table, db_conn_info):
    user, password, db = db_conn_info['user'], db_conn_info['password'], db_conn_info['db']
    
    ctl_file_name = f"{temp_table_name}_load.ctl"
    ctl_file_path = os.path.join(CHECK_BASE_FOLDER, ctl_file_name)
    log_file_path = os.path.join(CHECK_BASE_FOLDER, f"{temp_table_name}_load.log")
    bad_file_path = os.path.join(CHECK_BASE_FOLDER, f"{temp_table_name}_load.bad")

    columns_for_ctl = []
    for col_name_in_temp_table in loc_cols_in_temp_table:
        columns_for_ctl.append(f"  \"{col_name_in_temp_table}\" CHAR(4000)") 

    ctl_content = f"""
LOAD DATA
INFILE '{os.path.abspath(temp_csv_path).replace('\\', '/')}' 
BADFILE '{os.path.abspath(bad_file_path).replace('\\', '/')}'
DISCARDFILE '{os.path.abspath(os.path.join(CHECK_BASE_FOLDER, f'{temp_table_name}.dsc')).replace('\\', '/')}'
INSERT INTO TABLE "{temp_table_name}" 
FIELDS TERMINATED BY ';'
OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
{',\n'.join(columns_for_ctl)}
)
"""
    print(f"  Carregando chaves para '{temp_table_name}' via SQL*Loader...")
    
    try:
        with open(ctl_file_path, 'w', encoding='utf-8') as f:
            f.write(ctl_content.strip())

        sqlldr_command_args_string = f"control='{os.path.abspath(ctl_file_path).replace('\\', '/')}' log='{os.path.abspath(log_file_path).replace('\\', '/')}' bad='{os.path.abspath(bad_file_path).replace('\\', '/')}'"
        
        # AQUI: Usando o retorno aprimorado de _executar_sql_comando
        sqlldr_success_code, exec_message = _executar_sql_comando(sqlldr_command_args_string, 'sqlldr', user, password, db, log_file_path=log_file_path)

        records_loaded = 0
        records_rejected = 0
        status_message = ""

        if os.path.exists(log_file_path) and os.path.getsize(log_file_path) > 0:
            with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f_log:
                log_content = f_log.read()

                match_loaded = re.search(r'(\d+)\s+Rows successfully loaded', log_content)
                if match_loaded:
                    records_loaded = int(match_loaded.group(1))
                
                match_rejected = re.search(r'Total logical records rejected:\s*(\d+)', log_content)
                if match_rejected:
                    records_rejected = int(match_rejected.group(1))

                if sqlldr_success_code: # Verifica se o comando em si teve sucesso
                    if records_loaded > 0 and records_rejected == 0:
                        status_message = f"✅ Chaves carregadas com sucesso para '{temp_table_name}': {records_loaded} registros."
                        return True
                    elif records_loaded > 0 and records_rejected > 0:
                        status_message = f"⚠️ Chaves carregadas para '{temp_table_name}' com avisos/rejeições: {records_loaded} carregados, {records_rejected} rejeitados. Verifique '{log_file_path}'."
                        return True # Considera sucesso parcial para continuar
                    elif records_loaded == 0 and records_rejected == 0:
                        # Se não há registros a carregar (CSV vazio exceto cabeçalho), isso é um sucesso.
                        status_message = f"✅ Chaves carregadas para '{temp_table_name}': 0 registros (arquivo sem dados)."
                        return True
                    else: # sqlldr_success_code é True, mas 0 carregados e rejeições
                        status_message = f"❌ Falha na carga das chaves para '{temp_table_name}'. 0 carregados, {records_rejected} rejeitados. Verifique '{log_file_path}'."
                        return False
                else: # sqlldr_success_code é False (o comando em si falhou)
                    status_message = f"❌ Falha na carga das chaves para '{temp_table_name}'. {exec_message} Verifique '{log_file_path}'."
                    return False
        else:
            status_message = f"❌ ERRO: Log de carga para '{temp_table_name}' não encontrado ou vazio. Carga pode ter falhado."
            return False
    except Exception as e:
        status_message = f"❌ ERRO inesperado ao carregar chaves para '{temp_table_name}': {e}."
        return False
    finally:
        print(f"  {status_message}")

# Auxiliar para extrair dados do banco usando JOIN com a tabela temporária
def _extract_db_data_with_join(table_name_db, temp_table_name, loc_cols_db, comp_cols_db, filter_where_db, db_conn_info):
    user, password, db = db_conn_info['user'], db_conn_info['password'], db_conn_info['db']
    
    # Selecionar todas as colunas necessárias para o SELECT da tabela principal
    # Inclui colunas de localização e comparação do banco
    cols_to_select_db = list(set(loc_cols_db + comp_cols_db))
    if not cols_to_select_db: return pd.DataFrame()

    select_clause = ", ".join([f"T.\"{col}\"" for col in cols_to_select_db])
    
    # Construir a cláusula ON do JOIN para as chaves de localização.
    # Assumimos que a temp table tem as mesmas colunas que loc_cols_db,
    # que é garantido por _create_temp_keys_table
    join_conditions = []
    for col in loc_cols_db:
        join_conditions.append(f"T.\"{col}\" = TEMP.\"{col}\"")
    
    join_clause = " AND ".join(join_conditions)

    sql_query_parts = [
        f"SELECT {select_clause} FROM \"{table_name_db}\" T INNER JOIN \"{temp_table_name}\" TEMP ON ({join_clause})"
    ]
    if filter_where_db:
        sql_query_parts.append(f"WHERE {filter_where_db}")
    
    final_sql_query = "\n".join(sql_query_parts) + ";"

    temp_data_file_path = os.path.join(CHECK_BASE_FOLDER, f"TEMP_DB_EXTRACT_JOIN_{table_name_db}.csv")
    temp_extract_log_path = os.path.join(CHECK_BASE_FOLDER, f"{temp_table_name}_extract_join_log.txt")

    spool_command_parts = [
        "SET HEADING OFF;",
        "SET FEEDBACK OFF;",
        "SET PAGESIZE 0;",
        "SET LINESIZE 32767;", 
        f"SPOOL {os.path.abspath(temp_data_file_path).replace('\\', '/')};",
        final_sql_query,
        "SPOOL OFF;",
        "EXIT;"
    ]
    spool_command = "\n".join(spool_command_parts)
    
    print(f"  Extraindo dados da tabela '{table_name_db}' do banco de dados (via INNER JOIN com chaves únicas)...")

    # AQUI: Usando o retorno aprimorado de _executar_sql_comando
    success, message = _executar_sql_comando(spool_command, 'sqlplus', user, password, db, log_file_path=temp_extract_log_path)
    if success:
        if os.path.exists(temp_data_file_path) and os.path.getsize(temp_data_file_path) > 0:
            print(f"  Dados extraídos para '{os.path.basename(temp_data_file_path)}'. Carregando para DataFrame...")
            try:
                cleaned_lines = []
                with open(temp_data_file_path, 'r', encoding='utf-8', errors='ignore') as f_temp:
                    for line in f_temp:
                        line_stripped = line.strip()
                        if line_stripped and not re.match(r'^(SQL>|SPOOL|Connected\.|Disconnected\.|Enter user-name:|\S+\s+connected\.|^$)', line_stripped):
                            cleaned_lines.append(line)
                
                if cleaned_lines:
                    temp_cleaned_csv_path = os.path.join(CHECK_BASE_FOLDER, f"TEMP_DB_EXTRACT_CLEANED_JOIN_{table_name_db}.csv")
                    with open(temp_cleaned_csv_path, 'w', encoding='utf-8', newline='') as f_clean:
                        writer = csv.writer(f_clean, delimiter=';', quoting=csv.QUOTE_MINIMAL)
                        for line in cleaned_lines:
                            # Split a linha por dois ou mais espaços para simular o comportamento de colunas de SQLPlus
                            parts = re.split(r'\s{2,}', line.strip()) 
                            # Certifica-se de que o número de partes corresponde ao número de colunas esperadas
                            if len(parts) == len(cols_to_select_db):
                                writer.writerow(parts)
                            else:
                                print(f"AVISO: Linha de dados extraída do banco (JOIN) com contagem de colunas inesperada. Ignorada: {line.strip()[:100]}...")

                    df_extracted = pd.read_csv(temp_cleaned_csv_path, sep=';', dtype=str, encoding='utf-8-sig',
                                               on_bad_lines='skip', engine='python', quoting=csv.QUOTE_MINIMAL, escapechar='\\').fillna('')
                    
                    df_extracted.columns = cols_to_select_db # Atribui os nomes das colunas extraídas do banco
                    
                    try:
                        os.remove(temp_data_file_path)
                        os.remove(temp_cleaned_csv_path)
                    except OSError: pass

                    print(f"  Extração otimizada do banco para DataFrame concluída. {len(df_extracted)} registros.")
                    return df_extracted
                else:
                    print(f"  [AVISO] Arquivo de dados extraído de '{table_name_db}' está vazio após a limpeza (JOIN).")

            except Exception as e:
                print(f"  ❌ Erro ao carregar dados extraídos do banco (JOIN) para DataFrame: {e}")
    else:
        print(f"  ❌ Erro na extração de dados da tabela '{table_name_db}' do banco de dados (JOIN). {message} Verifique o log: '{temp_extract_log_path}'.")
    
    return None

# Auxiliar para dropar a tabela temporária de chaves
def _drop_temp_table(temp_table_name, db_conn_info):
    user, password, db = db_conn_info['user'], db_conn_info['password'], db_conn_info['db']
    
    drop_ddl = "\n".join([
        "SET ECHO ON;",
        "WHENEVER SQLERROR CONTINUE;",
        f"DROP TABLE \"{temp_table_name}\" CASCADE CONSTRAINTS;",
        "COMMIT;",
        "EXIT;"
    ])
    temp_log_path = os.path.join(CHECK_BASE_FOLDER, f"{temp_table_name}_drop_log.txt")

    print(f"  Removendo tabela temporária '{temp_table_name}' do banco de dados...")
    # AQUI: Usando o retorno aprimorado de _executar_sql_comando
    success, message = _executar_sql_comando(drop_ddl, 'sqlplus', user, password, db, log_file_path=temp_log_path)
    if success:
        print(f"  ✅ Tabela temporária '{temp_table_name}' removida com sucesso.")
        return True
    else:
        print(f"  ❌ Falha ao remover tabela temporária '{temp_table_name}'. {message} Verifique '{temp_log_path}'.")
        return False

# --- Função de Checagem de Duplicidade Arquivo vs Banco ---
def _verificar_duplicidade_arquivo_vs_banco():
    root = Tk(); root.withdraw(); root.attributes('-topmost', True)
    os.makedirs(CHECK_BASE_FOLDER, exist_ok=True)

    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "="*70)
    print("--- Módulo: Verificar Duplicidade no Arquivo vs. Comparação no Banco ---")
    print("Esta funcionalidade compara a duplicidade do seu arquivo local com o banco de dados.")
    print("É necessário selecionar um arquivo e uma tabela no banco para comparação.")
    print("="*70)

    # 1. Seleção de Arquivo(s) Local
    arquivos_locais = _selecionar_arquivos_para_tratamento()
    if not arquivos_locais:
        print("\nNenhum arquivo local selecionado. Retornando ao menu de checagem.")
        input("Pressione Enter para continuar.")
        return

    if len(arquivos_locais) > 1:
        print("\n[AVISO] Para esta operação, é recomendado processar um arquivo por vez.")
        print("Será utilizada apenas o primeiro arquivo selecionado para a comparação.")
        input("Pressione Enter para continuar...")
    
    arquivo_local_path = arquivos_locais[0]
    nome_arquivo_local = os.path.basename(arquivo_local_path)
    df_local = _carregar_arquivo_para_dataframe(arquivo_local_path)
    if df_local is None or df_local.empty:
        print(f"\n[ERRO] O arquivo '{nome_arquivo_local}' está vazio ou não pôde ser carregado. Não é possível prosseguir.")
        input("Pressione Enter para continuar.")
        return

    # 2. Seleção de Colunas para Localização de Duplicidade no Arquivo
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "="*70)
    print(f"--- Configuração para o Arquivo Local: '{nome_arquivo_local}' ---")
    cols_local_arquivo = list(df_local.columns)

    if not cols_local_arquivo:
        print(f"\n[ERRO] Nenhuma coluna encontrada no arquivo '{nome_arquivo_local}'. Não é possível prosseguir.")
        input("Pressione Enter para continuar.")
        return

    colunas_localizacao_arquivo = _prompt_for_column(
        cols_local_arquivo,
        "Selecione as colunas para LOCALIZAÇÃO DE DUPLICIDADE (Arquivo)",
        "Estas colunas (em conjunto) serão usadas para identificar registros únicos/duplicados no arquivo e para buscar no banco.",
        is_single_selection=False
    )
    if not colunas_localizacao_arquivo:
        print("\nNenhuma coluna de localização de duplicidade selecionada no arquivo. Operação cancelada.")
        input("Pressione Enter para continuar.")
        return
    else:
        print(f"\nColunas de localização selecionadas: {', '.join(colunas_localizacao_arquivo)}")
        time.sleep(1.5)


    # 3. Seleção de Colunas para Exibição/Comparação no Arquivo
    available_for_comp_arquivo = [col for col in cols_local_arquivo if col not in colunas_localizacao_arquivo]
    colunas_comparacao_arquivo = _prompt_for_column(
        available_for_comp_arquivo,
        "Selecione as colunas para COMPARAÇÃO/EXIBIÇÃO (Arquivo)",
        "Estas colunas serão exibidas junto com os registros para ajudar na análise de duplicidade e divergência. (Opcional)",
        is_single_selection=False
    )
    if not colunas_comparacao_arquivo:
        print("Nenhuma coluna de comparação selecionada. Não será possível checar divergência nos dados.")
        time.sleep(1.5)
    else:
        print(f"\nColunas de comparação/exibição selecionadas: {', '.join(colunas_comparacao_arquivo)}")
        time.sleep(1.5)

    # --- CHECAGEM DE DUPLICIDADE E DIVERGÊNCIA NO ARQUIVO LOCAL ---
    print("\n[INFO] Verificando duplicidade e divergência no arquivo local...")

    df_local_analysis = df_local.copy()

    for col in colunas_localizacao_arquivo:
        if col in df_local_analysis.columns:
            df_local_analysis[col] = df_local_analysis[col].astype(str).apply(normalizar_texto)
        else:
            print(f"AVISO: Coluna de localização '{col}' não encontrada no arquivo. Ignorando para checagem de duplicidade.")
            colunas_localizacao_arquivo = [c for c in colunas_localizacao_arquivo if c != col]

    if not colunas_localizacao_arquivo:
        print("\n[ERRO] Nenhuma coluna de localização válida restante no arquivo para checagem de duplicidade. Abortando.")
        input("Pressione Enter para continuar.")
        return

    # NOVO: Pergunta ao usuário se deseja ignorar nulos nas chaves de localização
    print("\n--- Opções de Tratamento de Nulos na Duplicidade ---")
    ignorar_nulos_nas_chaves = input("Deseja ignorar registros com valores vazios/nulos nas COLUNAS DE LOCALIZAÇÃO ao verificar duplicidade? (s/n): ").strip().lower() == 's'

    if ignorar_nulos_nas_chaves:
        print("  ✅ Registros com valores nulos/vazios nas chaves de localização SERÃO IGNORADOS na verificação de duplicidade.")
        # Criar uma máscara booleana para identificar linhas com nulos em qualquer coluna de localização
        mask_has_null_in_key = df_local_analysis[colunas_localizacao_arquivo].apply(lambda x: (x == '').any(), axis=1)

        # Filtrar o DataFrame para remover essas linhas
        df_local_analysis_filtered = df_local_analysis[~mask_has_null_in_key].copy()

        num_linhas_ignoradas = len(df_local_analysis) - len(df_local_analysis_filtered)
        if num_linhas_ignoradas > 0:
            print(f"  [INFO] {num_linhas_ignoradas} registro(s) ignorado(s) devido a valores nulos/vazios nas colunas de localização.")

        # Usar o DataFrame filtrado para a detecção de duplicidade
        df_local_analysis = df_local_analysis_filtered
    else:
        print("  ❌ Registros com valores nulos/vazios nas chaves de localização SERÃO CONSIDERADOS na verificação de duplicidade.")

    # Se o DataFrame ficar vazio após o filtro de nulos, não há o que verificar
    if df_local_analysis.empty:
        print("\n[AVISO] O arquivo ficou vazio após a remoção de registros com nulos nas colunas de localização. Nenhuma duplicidade será verificada.")
        input("Pressione Enter para continuar.")
        return

    df_local_duplicados_all = df_local_analysis[df_local_analysis.duplicated(subset=colunas_localizacao_arquivo, keep=False)].copy()


    df_local_duplicados_all = df_local_analysis[df_local_analysis.duplicated(subset=colunas_localizacao_arquivo, keep=False)].copy()

    if df_local_duplicados_all.empty:
        print("\n🎉 Nenhuma duplicidade encontrada no arquivo local com as colunas de localização selecionadas!")
        print("Não há registros duplicados para comparar com o banco de dados. Retornando.")
        input("Pressione Enter para continuar.")
        return

    df_local_duplicados_com_divergencia = pd.DataFrame()
    if colunas_comparacao_arquivo:
        df_local_duplicados_all['__group_key__'] = df_local_duplicados_all[colunas_localizacao_arquivo].apply(tuple, axis=1)

        df_local_duplicados_all['__comparison_key__'] = ''
        for col_comp in colunas_comparacao_arquivo:
            if col_comp in df_local_analysis.columns:
                df_local_duplicados_all[col_comp] = df_local_duplicados_all[col_comp].astype(str).apply(normalizar_texto)
                df_local_duplicados_all['__comparison_key__'] += df_local_duplicados_all[col_comp] + '|'
            else:
                print(f"AVISO: Coluna de comparação '{col_comp}' não encontrada no arquivo. Ignorando para checagem de divergência.")
        
        df_local_duplicados_all['__comparison_key__'] = df_local_duplicados_all['__comparison_key__'].str.rstrip('|')

        divergent_group_keys = df_local_duplicados_all.groupby('__group_key__')['__comparison_key__'].nunique()
        divergent_group_keys = divergent_group_keys[divergent_group_keys > 1].index.tolist()

        df_local_duplicados_com_divergencia = df_local_duplicados_all[
            df_local_duplicados_all['__group_key__'].isin(divergent_group_keys)
        ].copy()
        
        df_local_duplicados_com_divergencia.drop(columns=['__group_key__', '__comparison_key__'], errors='ignore', inplace=True)
        print(f"  {len(df_local_duplicados_com_divergencia)} registros duplicados com divergência nas colunas de comparação.")
    else:
        print("  Nenhuma coluna de comparação selecionada. Divergência não será checada.")
        df_local_duplicados_com_divergencia = df_local_duplicados_all.copy()


    if df_local_duplicados_com_divergencia.empty:
        print("\n🎉 Nenhuma duplicidade com divergência encontrada no arquivo local!")
        print("Não há registros duplicados e divergentes para comparar com o banco de dados. Retornando.")
        input("Pressione Enter para continuar.")
        return

    print(f"\n✅ {len(df_local_duplicados_com_divergencia)} registros duplicados COM DIVERGÊNCIA encontrados no arquivo local.")
    
    col_para_nome_base = "DUPLICADOS"
    if colunas_localizacao_arquivo:
        col_para_nome_base = colunas_localizacao_arquivo[0]
    nome_base_sanitizado = _rename_column_for_oracle(col_para_nome_base)
    
    nome_base_final = f"TT_REGISTROS_DUPLICADOS_{nome_base_sanitizado}"
    output_file_name_final_csv = f"{nome_base_final}.csv"
    output_path_final_csv = os.path.join(CHECK_BASE_FOLDER, output_file_name_final_csv)
    
    cols_for_final_csv_and_temp_table_ddl = [] 
    final_column_mapping_for_csv_and_temp_table = OrderedDict()

    for col_original in colunas_localizacao_arquivo:
        oracle_name = _rename_column_for_oracle(col_original)
        final_column_mapping_for_csv_and_temp_table[col_original] = oracle_name
        cols_for_final_csv_and_temp_table_ddl.append(oracle_name)

    for col_original in colunas_comparacao_arquivo:
        if col_original not in colunas_localizacao_arquivo:
            oracle_name = _rename_column_for_oracle(col_original)
            original_oracle_name = oracle_name
            counter = 1
            while oracle_name in final_column_mapping_for_csv_and_temp_table.values():
                oracle_name = f"{original_oracle_name[:min(len(original_oracle_name), 27)]}_{counter}"
                counter += 1
                if counter > 99:
                    print(f"AVISO: Excedido limite de renomeação única para coluna '{col_original}'. Pode haver colunas duplicadas.")
                    break
            
            final_column_mapping_for_csv_and_temp_table[col_original] = oracle_name
            cols_for_final_csv_and_temp_table_ddl.append(oracle_name)

    df_for_final_csv = pd.DataFrame()
    for original_col, oracle_col_name in final_column_mapping_for_csv_and_temp_table.items():
        if original_col in df_local.columns:
            df_for_final_csv[oracle_col_name] = df_local.loc[df_local_duplicados_com_divergencia.index, original_col].astype(str)
        else:
            print(f"AVISO: Coluna original '{original_col}' não encontrada no DataFrame local. Será preenchida com vazio no CSV.")
            df_for_final_csv[oracle_col_name] = ""

    df_for_final_csv.fillna('', inplace=True)

    df_for_final_csv.to_csv(output_path_final_csv, sep=';', index=False, encoding='utf-8-sig', quoting=csv.QUOTE_MINIMAL)
    print(f"  Arquivo com registros duplicados locais (com divergência) gerado para carga no banco: '{output_path_final_csv}'")
    
    input("\nPressione Enter para continuar com a conexão ao banco e criação da tabela temporária...")

    # --- FLUXO DE CONEXÃO, CRIAÇÃO DE TABELA TEMP E CARGA ---

    # 4. Conexão e Validação com o Banco de Dados
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "="*70)
    print("--- Conexão com o Banco de Dados ---")
    db_conn_info = None
    while db_conn_info is None:
        db_conn_info = _gerenciar_conexoes_db()
        if db_conn_info is None:
            print("\nConexão com o banco de dados não estabelecida. Retornando ao menu de checagem.")
            input("Pressione Enter para continuar.")
            return

    # Nome da tabela temporária final
    temp_table_name = nome_base_final[:30] 

    print(f"\nNome da tabela temporária sugerido para registros duplicados do arquivo: {temp_table_name}")
    
    # 6. Criação da Tabela Temporária no Banco
    print("\n[PASSO 1/3 Checagem] Criando tabela temporária de chaves no banco...")
    
    if not _create_temp_keys_table(temp_table_name, cols_for_final_csv_and_temp_table_ddl, db_conn_info):
        print("\nFalha na criação da tabela temporária. Abortando checagem.")
        input("Pressione Enter para continuar.") # Adiciona pausa para o usuário ver o erro
        return

    # 7. Preparação e Carga das Chaves do Arquivo para a Tabela Temporária
    print("\n[PASSO 2/3 Checagem] Carregando chaves do arquivo local para a tabela temporária...")
    
    print(f"DEBUG: Caminho do CSV de chaves para carga na temp table: {output_path_final_csv}")
    if os.path.exists(output_path_final_csv):
        print(f"DEBUG: Tamanho do arquivo CSV de chaves: {os.path.getsize(output_path_final_csv)} bytes")
        with open(output_path_final_csv, 'r', encoding='utf-8-sig', errors='ignore') as f:
            print("DEBUG: Conteúdo do CSV de chaves para carga (primeiras 5 linhas):")
            for i, line in enumerate(f):
                if i >= 5: break
                print(f"  {line.strip()}")
    else:
        print(f"DEBUG: O arquivo CSV de chaves '{output_path_final_csv}' NÃO FOI ENCONTRADO.")


    if not _load_keys_to_temp_table(temp_table_name, output_path_final_csv, cols_for_final_csv_and_temp_table_ddl, db_conn_info):
        print("\nFalha no carregamento das chaves para a tabela temporária. Abortando checagem.")
        input("Pressione Enter para continuar.") # Adiciona pausa para o usuário ver o erro
        return
    
    try:
        if os.path.exists(output_path_final_csv):
            os.remove(output_path_final_csv)
            print(f"DEBUG: Arquivo CSV temporário '{output_path_final_csv}' removido após carga.")
    except OSError as e:
        print(f"AVISO: Não foi possível remover o arquivo CSV temporário de chaves '{output_path_final_csv}': {e}")


    print(f"\n✅ Tabela temporária '{temp_table_name}' criada com sucesso e chaves carregadas.")
    input("Pressione Enter para prosseguir com a seleção da tabela do banco de dados e extração...")
    # --- FIM DO FLUXO DE CONEXÃO, CRIAÇÃO DE TABELA TEMP E CARGA ---


    # 5. Seleção da Tabela no Banco de Dados (ESTE PASSO AGORA VEM DEPOIS)
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "="*70)
    print("--- Seleção da Tabela NO BANCO DE DADOS para Comparação ---")
    table_name_db = input("Digite o NOME da tabela principal no banco de dados para comparação (Ex: TB_PESSOA): ").strip().upper()
    if not table_name_db:
        print("\nNome da tabela do banco não fornecido. Operação cancelada.")
        input("Pressione Enter para continuar.")
        return

    # Obter colunas da tabela do banco para o usuário selecionar o mapeamento
    print(f"\nBuscando colunas da tabela '{table_name_db}' no banco de dados...")
    sql_get_db_columns = f"SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE OWNER = USER AND TABLE_NAME = '{table_name_db}' ORDER BY COLUMN_ID;"
    db_columns_temp_file = os.path.join(CHECK_BASE_FOLDER, f"{table_name_db}_columns.csv")
    db_columns_log_file = os.path.join(CHECK_BASE_FOLDER, f"{table_name_db}_get_cols_log.txt")

    spool_command_db_cols = "\n".join([
        "SET HEADING OFF;",
        "SET FEEDBACK OFF;",
        "SET PAGESIZE 0;",
        "SET LINESIZE 32767;",
        f"SPOOL {os.path.abspath(db_columns_temp_file).replace('\\', '/')};",
        sql_get_db_columns,
        "SPOOL OFF;",
        "EXIT;"
    ])

    db_column_names_ordered = []

    # AQUI: Usando o retorno aprimorado de _executar_sql_comando
    success, message = _executar_sql_comando(spool_command_db_cols, 'sqlplus', db_conn_info['user'], db_conn_info['password'], db_conn_info['db'], log_file_path=db_columns_log_file)
    if success:
        if os.path.exists(db_columns_temp_file) and os.path.getsize(db_columns_temp_file) > 0:
            try:
                cleaned_db_col_lines = []
                with open(db_columns_temp_file, 'r', encoding='utf-8', errors='ignore') as f_db_cols:
                    for line in f_db_cols:
                        stripped_line = line.strip()
                        if stripped_line and not re.match(r'^(SQL>|SPOOL|Connected\.|Disconnected\.|Enter user-name:|\S+\s+connected\.|^$)', stripped_line):
                            cleaned_db_col_lines.append(stripped_line.split(' ')[0].strip())

                db_column_names_ordered = [col.upper() for col in cleaned_db_col_lines if col]
                if not db_column_names_ordered:
                    print(f"  [AVISO] Nenhuma coluna encontrada para a tabela '{table_name_db}' no banco de dados. Verifique o nome da tabela e permissões. Operação cancelada.")
                    input("Pressione Enter para continuar.")
                    return
            except Exception as e:
                print(f"  ❌ Erro ao processar colunas da tabela '{table_name_db}' do banco: {e}. Operação cancelada.")
                input("Pressione Enter para continuar.")
                return
            finally:
                try: os.remove(db_columns_temp_file)
                except OSError: pass
        else:
            print(f"  [AVISO] Arquivo de colunas da tabela '{table_name_db}' do banco está vazio ou não gerado. Verifique o log. Operação cancelada.")
            input("Pressione Enter para continuar.")
            return
    else:
        print(f"  ❌ Falha ao obter colunas da tabela '{table_name_db}' do banco de dados. {message} Verifique o log: '{db_columns_log_file}'. Operação cancelada.")
        input("Pressione Enter para continuar.")
        return
    
    # Mapeamento de Colunas de Localização: Arquivo -> Banco
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "="*70)
    print("--- Mapeamento de Colunas de LOCALIZAÇÃO (Arquivo -> Banco Principal) ---")
    print("Para cada coluna de localização selecionada no arquivo, escolha a coluna correspondente na TABELA PRINCIPAL do banco de dados.")
    
    mapping_loc_file_to_db = {}
    colunas_localizacao_db = []

    for file_col in colunas_localizacao_arquivo:
        print(f"\nSelecione a coluna da TABELA PRINCIPAL do banco para mapear '{file_col}' (do arquivo):")
        selected_db_col = _prompt_for_column(
            db_column_names_ordered,
            f"Mapear '{file_col}' (Arquivo) para Coluna da Tabela Principal do Banco",
            "Escolha a coluna da tabela principal do banco que contém os dados correspondentes para localização.",
            is_single_selection=True
        )
        if selected_db_col:
            mapping_loc_file_to_db[file_col] = selected_db_col
            colunas_localizacao_db.append(selected_db_col)
        else:
            print(f"\n[AVISO] Coluna de tabela principal do banco não selecionada para '{file_col}'. O mapeamento para esta coluna será ignorado. A checagem pode não ser precisa.")
            input("Pressione Enter para continuar.")
            return

    if not colunas_localizacao_db:
        print("\n[ERRO] Nenhuma coluna de localização mapeada para a tabela principal do banco. A checagem não pode prosseguir.")
        input("Pressione Enter para continuar.")
        return

    # Mapeamento de Colunas de Comparação/Exibição: Arquivo -> Banco
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "="*70)
    print("--- Mapeamento de Colunas de COMPARAÇÃO/EXIBIÇÃO (Arquivo -> Banco Principal) ---")
    print("Para cada coluna de comparação/exibição selecionada no arquivo, escolha a coluna correspondente na TABELA PRINCIPAL do banco de dados.")
    
    mapping_comp_file_to_db = {}
    colunas_comparacao_db = []

    available_for_comp_db = [col for col in db_column_names_ordered if col not in colunas_localizacao_db]

    for file_col in colunas_comparacao_arquivo:
        if file_col in colunas_localizacao_arquivo:
            continue 

        print(f"\nSelecione a coluna da TABELA PRINCIPAL do banco para mapear '{file_col}' (do arquivo) para comparação:")
        selected_db_col = _prompt_for_column(
            available_for_comp_db,
            f"Mapear '{file_col}' (Arquivo) para Coluna da Tabela Principal do Banco (Comparação)",
            "Escolha a coluna da tabela principal do banco que contém os dados correspondentes para comparação. (Opcional)",
            is_single_selection=True
        )
        if selected_db_col:
            mapping_comp_file_to_db[file_col] = selected_db_col
            colunas_comparacao_db.append(selected_db_col)
        else:
            print(f"\n[AVISO] Coluna de tabela principal do banco não selecionada para '{file_col}' (comparação). Este mapeamento será ignorado.")
            input("Pressione Enter para continuar.")

    all_db_cols_to_extract = list(set(colunas_localizacao_db + colunas_comparacao_db))
    if not all_db_cols_to_extract:
        print("\n[ERRO] Nenhuma coluna da tabela principal do banco selecionada para extração. A checagem não pode prosseguir.")
        input("Pressione Enter para continuar.")
        return

    # --- FILTRO PRINCIPAL NO BANCO E FILTRO ADICIONAL ---
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "="*70)
    print("--- Configuração de Filtro para Extração da Tabela PRINCIPAL do Banco ---")
    
    main_filter_col_db = None
    if all_db_cols_to_extract:
        print("\nSelecione a coluna da TABELA PRINCIPAL do banco de dados para o FILTRO PRINCIPAL (opcional, para limitar a busca):")
        main_filter_col_db = _prompt_for_column(
            all_db_cols_to_extract,
            "Coluna para Filtro Principal na Tabela Principal do Banco",
            "Esta coluna será utilizada para um filtro `IN` na query, se fornecido um valor. (Opcional)"
        )
    
    final_filter_where_db = ""
    if main_filter_col_db:
        main_filter_value = input(f"\nDigite o VALOR (ou valores separados por vírgula) para filtrar na coluna '{main_filter_col_db}' (Ex: 'ATIVO', '01-JAN-2023'). Deixe vazio para não filtrar: ").strip()
        if main_filter_value:
            values = [f"'{v.strip().upper()}'" for v in main_filter_value.split(',')]
            final_filter_where_db = f"T.\"{main_filter_col_db}\" IN ({', '.join(values)})"

    additional_filter_clause = input("\n[Opcional] Deseja adicionar um filtro 'AND' à consulta da TABELA PRINCIPAL do banco? (Ex: CD_EMPRESA_PLANO = 14). Deixe vazio para não adicionar: ").strip()
    if additional_filter_clause:
        if final_filter_where_db:
            final_filter_where_db += f" AND ({additional_filter_clause})"
        else:
            final_filter_where_db = additional_filter_clause
    
    print(f"\nCláusula WHERE final para a TABELA PRINCIPAL do banco: '{final_filter_where_db if final_filter_where_db else '[Nenhum filtro]'}'")
    input("Pressione Enter para iniciar a extração dos dados da tabela principal do banco com este filtro...")

    # 8. Extração de Dados do Banco Usando as Chaves da Tabela Temporária e o filtro
    print("\n[PASSO 3/3 Checagem] Extraindo dados da TABELA PRINCIPAL do banco para comparação...")
    df_db_extracted = _extract_db_data_with_join(
        table_name_db, 
        temp_table_name, 
        cols_for_final_csv_and_temp_table_ddl, 
        all_db_cols_to_extract,
        final_filter_where_db,
        db_conn_info
    )

    if df_db_extracted is None or df_db_extracted.empty:
        print("\n[AVISO] Nenhuns dados foram extraídos do banco de dados para comparação. Pode não haver correspondência para as chaves do arquivo, ou a tabela está vazia após o filtro.")
        print("Finalizando checagem de duplicidade Arquivo vs. Banco.")
        input("Pressione Enter para continuar.")
        return
    
    # --- GERAÇÃO DO ARQUIVO COM DADOS DO BANCO ---
    output_file_name_db_extract = f"{table_name_db}_extraidos.csv"
    output_path_db_extract = os.path.join(CHECK_BASE_FOLDER, output_file_name_db_extract)
    
    df_db_extracted.to_csv(output_path_db_extract, sep=';', index=False, encoding='utf-8-sig', quoting=csv.QUOTE_MINIMAL)
    print(f"\n✅ Arquivo com registros extraídos do banco gerado: '{output_path_db_extract}'")

    print(f"\n✅ Dados do arquivo local e do banco extraídos com sucesso. Pronto para a comparação!")
    print(f"  Registros duplicados no arquivo local: {len(df_for_final_csv)}")
    print(f"  Registros extraídos do banco: {len(df_db_extracted)}")
    print("\nPróximo passo: Comparar os DataFrames e gerar relatórios de divergência. (Em desenvolvimento)")

    print(f"\nNOTA: A tabela temporária '{temp_table_name}' foi criada e carregada no banco de dados e NÃO SERÁ REMOVIDA automaticamente para permitir análises posteriores.")
    input("Pressione Enter para continuar e finalizar a checagem por enquanto.")
    
    
# --- Nova Função Principal para o Módulo de Checagem no Banco ---
def _checagem_no_banco():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "="*50)
    print("--- Módulo: Checagem no Banco ---")
    while True:
        print("\nSelecione uma opção de checagem:")
        print(" 1. Verificar Duplicidade no Arquivo vs. Comparação no Banco")
        print("\n 0. Voltar ao Menu Principal")
        
        escolha_checagem = input("\nDigite sua opção: ").strip()

        if escolha_checagem == '1':
            _verificar_duplicidade_arquivo_vs_banco()
        elif escolha_checagem == '0':
            break
        else:
            print("Opção inválida. Tente novamente.")
            time.sleep(1)
            
# Auxiliar para carregar arquivo (CSV/Excel) em DataFrame (reutiliza lógica do _tratar_campos)
def _carregar_arquivo_para_dataframe(file_path):
    df = None
    file_name = os.path.basename(file_path)
    if file_path.lower().endswith(('.csv')):
        for encoding in ENCODINGS_TO_TRY:
            try:
                df = pd.read_csv(file_path, sep=';', dtype=str, encoding=encoding,
                                 on_bad_lines='skip', engine='python', quoting=csv.QUOTE_NONE, escapechar='\\').fillna('')
                break
            except UnicodeDecodeError: continue
            except Exception as e: print(f"AVISO: Erro ao leer CSV '{file_name}' com '{encoding}': {e}.")
        if df is None:
            print(f"❌ Erro: Não foi possível ler o CSV '{file_name}'.")
    elif file_path.lower().endswith(('.xlsx', '.xls')):
        try:
            excel_file = pd.ExcelFile(file_path)
            sheet_name = excel_file.sheet_names[0] if excel_file.sheet_names else None
            if sheet_name:
                df = pd.read_excel(excel_file, sheet_name=sheet_name, dtype=str).fillna('')
            else:
                print(f"❌ Erro: Arquivo Excel '{file_name}' não possui abas.")
        except Exception as e:
            print(f"❌ Erro: Não foi possível ler o Excel '{file_name}': {e}.")
    else:
        print(f"❌ Erro: Formato de arquivo '{os.path.splitext(file_name)[1]}' não suportado.")
    
    if df is not None:
        df.columns = df.columns.astype(str).str.strip() # Limpa nomes de colunas do DF lido
    return df

def _processar_excel_para_csv_oracle_ready(original_excel_path, base_table_name_candidate, target_folder):
    """
    Lê um arquivo Excel, renomeia as colunas para o padrão Oracle e salva cada aba
    como um arquivo CSV separado na pasta de destino.
    Retorna uma lista de dicionários com info dos arquivos CSV criados.
    """
    processed_excel_sheets_info = []
    
    try:
        excel_file = pd.ExcelFile(original_excel_path)
        sheet_names = excel_file.sheet_names

        if not sheet_names:
            print(f"  [AVISO] O arquivo Excel '{os.path.basename(original_excel_path)}' não possui abas. Pulando.")
            return []

        for sheet_name in sheet_names:
            print(f"    Lendo aba '{sheet_name}' para preparação...")
            try:
                df = pd.read_excel(excel_file, sheet_name=sheet_name, dtype=str).fillna('')
            except Exception as e_read:
                print(f"    [ERRO] Falha ao ler aba '{sheet_name}' do Excel '{os.path.basename(original_excel_path)}': {e_read}. Pulando esta aba.")
                continue

            if df.empty:
                print(f"    [AVISO] A aba '{sheet_name}' do arquivo Excel '{os.path.basename(original_excel_path)}' está vazia. Pulando processamento para esta aba.")
                continue

            # 1. Renomear colunas do DataFrame para o padrão Oracle
            renamed_columns_map = {}
            df.columns = df.columns.astype(str) # Garante que as colunas são strings
            for col in df.columns:
                new_col_name = _rename_column_for_oracle(col)
                # Verifica se o novo nome já existe (após truncamento/normalização)
                original_new_col_name = new_col_name
                counter = 1
                while new_col_name in renamed_columns_map.values():
                    new_col_name = f"{original_new_col_name[:min(len(original_new_col_name), 27)]}_{counter}"
                    counter += 1
                    if counter > 999: # Safety break
                        print(f"AVISO: Excedido limite de renomeação única para coluna '{col}'. Mantendo nome original.")
                        new_col_name = col # Reverte para o nome original se não conseguir renomear
                        break
                renamed_columns_map[col] = new_col_name
            
            df.rename(columns=renamed_columns_map, inplace=True)

            # 2. Gerar o nome da tabela para esta aba (base_table_name_candidate_ABA)
            sheet_table_name_full = f"{base_table_name_candidate}_{sheet_name.replace(' ', '_').upper()}"
            sheet_table_name_final = re.sub(r'[^A-Z0-9_]', '', sheet_table_name_full).strip('_')
            sheet_table_name_final = sheet_table_name_final[:30] # Trunca para 30 chars

            # 3. Salvar o DataFrame (com colunas renomeadas) como CSV na pasta de destino
            copied_csv_file_name = f"{sheet_table_name_final}.csv"
            copied_csv_file_path = os.path.join(target_folder, copied_csv_file_name)
            
            df.to_csv(copied_csv_file_path, sep=';', index=False, encoding='utf-8-sig', quoting=csv.QUOTE_NONE)
            
            print(f"    ✅ Aba '{sheet_name}' de '{os.path.basename(original_excel_path)}' convertida e renomeada para '{copied_csv_file_name}'.")

            processed_excel_sheets_info.append({
                "df": df, # O DataFrame em memória, já renomeado
                "original_path": original_excel_path,
                "copied_file_path": copied_csv_file_path,
                "table_name": sheet_table_name_final,
                "renamed_columns": list(df.columns), # Colunas JÁ renomeadas e usadas no DDL
                "record_count": len(df),
                "original_sheet_name": sheet_name
            })
            
    except Exception as e:
        print(f"  [ERRO GERAL] Falha ao processar arquivo Excel '{os.path.basename(original_excel_path)}': {e}. Pulando.")
        return []
    
    return processed_excel_sheets_info
    
# --- Funções de Leitura/Gravação de Históricos ---
def _carregar_historico_db():
    try:
        if os.path.exists(DB_HISTORY_FILE):
            with open(DB_HISTORY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return []
    return []

def _salvar_historico_db(historico):
    try:
        with open(DB_HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(historico, f, indent=4, ensure_ascii=False)
    except IOError as e:
        print(f"ERRO: Não foi possível salvar o histórico de conexões do banco de dados: {e}")

def _carregar_historico_importacao():
    try:
        if os.path.exists(IMPORT_HISTORY_FILE):
            with open(IMPORT_HISTORY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return []
    return []

def _salvar_historico_importacao(historico):
    try:
        with open(IMPORT_HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(historico, f, indent=4, ensure_ascii=False)
    except IOError as e:
        print(f"ERRO: Não foi possível salvar o histórico de importações: {e}")

# Certifique-se de que não há nenhuma linha como ORACLE_CLIENT_BIN_PATH = r"..."
# definida no topo do seu script, se você não a quiser mais.
# A função abaixo NÃO precisará mais dela.

# Função para executar comandos SQL*Plus/SQL*Loader (melhorada com mensagens amigáveis)
def _executar_sql_comando(comando_completo, tipo_comando, user, password, db, log_file_path=None):
    conn_string = f"{user}/{password}@{db}"
    sql_executable_name = ""
    process_command = []
    stdin_input_for_subprocess = None

    if tipo_comando == 'sqlplus':
        sql_executable_name = "sqlplus.exe"
        process_command = [sql_executable_name, '-S', conn_string]
        stdin_input_for_subprocess = comando_completo
    elif tipo_comando == 'sqlldr':
        sql_executable_name = "sqlldr.exe"
        process_command = [sql_executable_name, conn_string, comando_completo]
        # SQL*Loader recebe argumentos via linha de comando
    else:
        return False, f"ERRO INTERNO: Tipo de comando desconhecido '{tipo_comando}'."

    print(f"  Executando: {sql_executable_name} ...")

    try:
        result = subprocess.run(
            process_command,
            input=stdin_input_for_subprocess,
            capture_output=True,
            text=True,
            check=False,
            shell=False,
            timeout=120 # Adicionado timeout de 120 segundos para evitar travamentos longos
        )

        output = result.stdout + result.stderr

        if log_file_path:
            with open(log_file_path, 'a', encoding='utf-8', errors='ignore') as f:
                f.write(f"\n--- Output do Comando {tipo_comando.upper()} ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---\n")
                f.write(output)
                f.write(f"\n--- Return Code: {result.returncode} ---\n")
                f.write("\n------------------------------------------------------------\n")

        # Análise do resultado
        if result.returncode != 0:
            if "ORA-" in output or "TNS-" in output or "SP2-" in output:
                return False, f"❌ ERRO no banco de dados ou conexão. Detalhes: {output.strip().splitlines()[-1]}" # Pega a última linha relevante
            elif "timeout" in output.lower(): # Verificação explícita para timeout
                return False, f"❌ ERRO de conexão: Tempo limite excedido ao tentar conectar/executar. Verifique a rede e as configurações do banco."
            elif tipo_comando == 'sqlldr' and "SQL*Loader-000" in output: # Erros específicos do SQL*Loader
                return False, f"❌ ERRO no SQL*Loader. Provável problema com arquivo CTL, dados ou permissões. Verifique o log detalhadamente."
            else:
                return False, f"❌ Falha ao executar {sql_executable_name}. Código de saída: {result.returncode}. Verifique o log para mais detalhes."
        else: # returncode == 0
            if tipo_comando == 'sqlplus' and ("Error" in output or "ORA-" in output):
                # Caso o SQL*Plus retorne 0, mas com erros internos (ex: erros de sintaxe SQL)
                return False, f"❌ ERRO: Comando SQL*Plus executado com problemas internos. Detalhes: {output.strip().splitlines()[-1]}"
            return True, f"✅ Comando {tipo_comando.upper()} executado com sucesso."

    except FileNotFoundError:
        return False, f"❌ ERRO: O executável '{sql_executable_name}' não foi encontrado no PATH do sistema. Certifique-se de que o Oracle Client (e as ferramentas SQL*Plus/SQL*Loader) está instalado e configurado na variável de ambiente PATH."
    except subprocess.TimeoutExpired:
        if log_file_path: # Tenta escrever no log que houve um timeout, se o log foi definido
            with open(log_file_path, 'a', encoding='utf-8', errors='ignore') as f:
                f.write(f"\n--- TIMEOUT EXPIRED ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---\n")
                f.write("O comando excedeu o tempo limite de execução e foi encerrado.\n")
        return False, f"❌ ERRO: O comando '{sql_executable_name}' excedeu o tempo limite de 120 segundos. A operação pode estar travada ou demorando muito. Verifique a conectividade e a carga do banco."
    except Exception as e:
        return False, f"❌ ERRO inesperado ao executar comando {tipo_comando.upper()}: {e}"

# Gerenciamento de Conexões (modificado para ser reutilizável)
def _gerenciar_conexoes_db():
    historico_db = _carregar_historico_db()
    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        print("\n" + "="*50)
        print("--- Gerenciamento de Conexões com Banco de Dados ---")
        print("ATENÇÃO: É necessario que o SQL LOADER esteja habilitado para a importação funcionar.")
        print("ATENÇÃO: O usuário precisa estar com permissão para criar tabela no banco.")
        print("----------------------------------------------------")
        print("Conexões salvas:")
        if not historico_db:
            print("  Nenhuma conexão salva.")
        else:
            for i, conn in enumerate(historico_db):
                print(f"  {i+1}. Usuário: {conn['user']}, Database: {conn['db']}")
        print("\nOpções:")
        print(" 1. Adicionar Nova Conexão")
        print(" 2. Usar Conexão do Histórico")
        print(" 3. Excluir Conexão do Histórico")
        print(" 0. Cancelar e Voltar")

        escolha = input("\nDigite sua opção: ").strip()

        if escolha == '1':
            user = input("Digite o usuário do Oracle: ").strip()
            password = input("Digite a senha do Oracle: ").strip()
            db = input("Digite o Database/Service Name (ex: localhost:1521/xe): ").strip()
            if not all([user, password, db]):
                print("Todos os campos (usuário, senha, database) são obrigatórios. Tente novamente.")
                time.sleep(2)
                continue
            conn_info = {'user': user, 'password': password, 'db': db}
            print("\nTestando nova conexão...")
            # AQUI: Usando o retorno aprimorado de _executar_sql_comando
            success, message = _executar_sql_comando('SELECT 1 FROM DUAL;', 'sqlplus', user, password, db)
            print(message) # Exibe a mensagem detalhada
            if success:
                print("✅ Conexão validada com sucesso! Adicionando ao histórico.")
                historico_db.append(conn_info)
                _salvar_historico_db(historico_db)
                input("\nPressione Enter para continuar...")
                return conn_info # Retorna a conexão validada
            else:
                # A mensagem de erro já foi exibida por 'message', apenas pausa
                input("\nPressione Enter para continuar...")

        elif escolha == '2':
            if not historico_db:
                print("Nenhuma conexão no histórico para usar.")
                time.sleep(1.5)
                continue
            try:
                idx = int(input("Digite o NÚMERO da conexão que deseja usar: ")) - 1
                if 0 <= idx < len(historico_db):
                    conn_info = historico_db[idx]
                    print(f"\nTestando conexão selecionada (Usuário: {conn_info['user']}, Database: {conn_info['db']})...")
                    # AQUI: Usando o retorno aprimorado de _executar_sql_comando
                    success, message = _executar_sql_comando('SELECT 1 FROM DUAL;', 'sqlplus', conn_info['user'], conn_info['password'], conn_info['db'])
                    print(message) # Exibe a mensagem detalhada
                    if success:
                        print("✅ Conexão validada com sucesso!")
                        input("\nPressione Enter para continuar...")
                        return conn_info
                    else:
                        # A mensagem de erro já foi exibida por 'message', apenas pausa
                        input("\nPressione Enter para continuar...")
                else:
                    print("Número inválido.")
                    time.sleep(1.5)
            except ValueError:
                print("Entrada inválida.")
                time.sleep(1.5)

        elif escolha == '3':
            if not historico_db:
                print("Nenhuma conexão no histórico para excluir.")
                time.sleep(1.5)
                continue
            try:
                idx = int(input("Digite o NÚMERO da conexão que deseja EXCLUIR: ").strip()) - 1
                if 0 <= idx < len(historico_db):
                    confirm = input(f"Tem certeza que deseja excluir a conexão de {historico_db[idx]['user']}@{historico_db[idx]['db']}? (s/n): ").lower()
                    if confirm == 's':
                        del historico_db[idx]
                        _salvar_historico_db(historico_db)
                        print("Conexão excluída com sucesso.")
                    else:
                        print("Exclusão cancelada.")
                    time.sleep(1.5)
                else:
                    print("Número inválido.")
                    time.sleep(1.5)
            except ValueError:
                print("Entrada inválida.")
                time.sleep(1.5)

        elif escolha == '0':
            print("Operação de conexão cancelada.")
            return None
        else:
            print("Opção inválida.")
            time.sleep(1.5)

# --- Adaptação da Seleção de Arquivos (Reutilizando lógica) ---
def _selecionar_arquivos_para_importacao():
    root = Tk(); root.withdraw(); root.attributes('-topmost', True)
    arquivos_paths_set = set()
    tipo_de_selecao_inicial = None

    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        print("\n--- Seleção de Arquivos para Importação ---")
        print("Como você gostaria de selecionar os arquivos?")
        print(" 1. Selecionar uma Pasta (buscará todos os arquivos CSV na pasta)")
        print(" 2. Selecionar Arquivos Específicos (seleção múltipla permitida de CSVs)")
        print("\n 0. Cancelar e Voltar")

        escolha_selecao = input("\nDigite sua opção: ").strip()

        if escolha_selecao == '1':
            tipo_de_selecao_inicial = 'pasta'
            pasta_selecionada = filedialog.askdirectory(title="Selecione a Pasta para Importação (apenas CSVs)")
            if not pasta_selecionada:
                print("\nNenhuma pasta foi selecionada. Por favor, tente novamente ou cancele.")
                time.sleep(1.5)
                continue
            try:
                # Alterado para buscar apenas .csv
                arquivos_na_pasta = [os.path.join(pasta_selecionada, f) for f in os.listdir(pasta_selecionada) if f.lower().endswith(('.csv'))]
                if not arquivos_na_pasta:
                    print(f"\nNenhum arquivo CSV encontrado na pasta '{pasta_selecionada}'.")
                    resposta = input("Deseja tentar outra seleção (s/n)? ").lower()
                    if resposta == 's': continue
                    else: return []
                arquivos_paths_set.update(arquivos_na_pasta)
                break
            except FileNotFoundError:
                print(f"ERRO: A pasta '{pasta_selecionada}' não foi encontrada. Tente novamente.")
                time.sleep(1.5)
                continue
        elif escolha_selecao == '2':
            tipo_de_selecao_inicial = 'arquivos'
            caminhos_arquivos_selecionados = filedialog.askopenfilenames(
                title="Selecione um ou mais Arquivos CSV para Importação",
                # Restrito a apenas CSV
                filetypes=[("Arquivos CSV", "*.csv")] 
            )
            if not caminhos_arquivos_selecionados:
                print("\nNenhum arquivo foi selecionado. Por favor, tente novamente ou cancele.")
                time.sleep(1.5)
                continue
            arquivos_paths_set.update(caminhos_arquivos_selecionados)
            break
        elif escolha_selecao == '0':
            print("\nOperação de seleção de arquivos cancelada.")
            return []
        else:
            print("Opção inválida. Por favor, digite 1, 2 ou 0.")
            time.sleep(1.5)

    # ... (O restante da função de confirmação/adição/refazer continua o mesmo) ...
    while True: # Loop de Confirmação/Adição/Refazer
        os.system('cls' if os.name == 'nt' else 'clear')
        print("\n" + "="*50)
        print("--- Arquivos Selecionados Atualmente para Importação ---")
        arquivos_atuais = sorted(list(arquivos_paths_set))
        if not arquivos_atuais:
            print("\nNenhum arquivo válido selecionado para importação.")
            resposta = input("Deseja iniciar uma nova seleção (s/n)? ").lower()
            if resposta == 's': return _selecionar_arquivos_para_importacao()
            else: return []
        print(f"\nTotal de arquivos selecionados: {len(arquivos_atuais)}")
        print("Lista de arquivos:"); [print(f" {i + 1}. {os.path.basename(full_path)}") for i, full_path in enumerate(arquivos_atuais)]; print("-" * 50)
        print("\nOpções:"); print(" [c] Confirmar e prosseguir"); print(" [a] Adicionar mais arquivos/pastas"); print(" [r] Refazer toda a seleção"); print(" [n] Cancelar e voltar")
        confirm_option = input("\nEscolha uma opção (c/a/r/n): ").lower().strip()
        if confirm_option == 'c': break
        elif confirm_option == 'a':
            if tipo_de_selecao_inicial == 'pasta':
                nova_pasta = filedialog.askdirectory(title="Adicionar arquivos de outra Pasta (apenas CSVs)")
                if nova_pasta:
                    novos_arquivos = [os.path.join(nova_pasta, f) for f in os.listdir(nova_pasta) if f.lower().endswith(('.csv'))] # Apenas CSV
                    arquivos_paths_set.update(novos_arquivos)
                    print(f"Adicionados {len(novos_arquivos)} arquivos da pasta '{os.path.basename(nova_pasta)}'.")
                else: print("Nenhuma pasta adicional selecionada.")
            else:
                novos_caminhos_arquivos = filedialog.askopenfilenames(
                    title="Adicionar mais Arquivos CSV", filetypes=[("Arquivos CSV", "*.csv")]) # Apenas CSV
                arquivos_paths_set.update(novos_caminhos_arquivos)
                print(f"Adicionados {len(novos_caminhos_arquivos)} arquivos.")
            time.sleep(1.5)
        elif confirm_option == 'r': return _selecionar_arquivos_para_importacao()
        elif confirm_option == 'n': print("\nOperação de seleção cancelada."); return []
        else: print("Opção inválida."); time.sleep(1)
    return list(arquivos_paths_set)


# --- Funções de Processamento e Renomeação ---
def _get_columns_from_df(df_or_excel_path):
    """
    Retorna as colunas de um DataFrame ou de um caminho de arquivo Excel/CSV.
    Trata múltiplos sheets para Excel. Retorna um OrderedDict com colunas por sheet.
    """
    column_map = OrderedDict()
    if isinstance(df_or_excel_path, pd.DataFrame):
        column_map['Sheet1'] = df_or_excel_path.columns.astype(str).str.strip().tolist()
    elif isinstance(df_or_excel_path, str): # Path do arquivo
        file_path = df_or_excel_path
        if file_path.lower().endswith(('.xlsx', '.xls')):
            excel_file = pd.ExcelFile(file_path)
            for sheet_name in excel_file.sheet_names:
                df_temp = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=0, dtype=str)
                column_map[sheet_name] = df_temp.columns.astype(str).str.strip().tolist()
        elif file_path.lower().endswith('.csv'):
            for encoding in ENCODINGS_TO_TRY:
                try:
                    df_temp = pd.read_csv(file_path, sep=';', nrows=0, dtype=str, encoding=encoding,
                                          on_bad_lines='skip', engine='python', quoting=csv.QUOTE_NONE)
                    column_map['Sheet1'] = df_temp.columns.astype(str).str.strip().tolist()
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    print(f"    DEBUG: Erro ao ler colunas do CSV '{os.path.basename(file_path)}' com '{encoding}': {e}")
            if 'Sheet1' not in column_map:
                print(f"    [ERRO] Não foi possível ler as colunas do CSV '{os.path.basename(file_path)}' com nenhuma codificação.")
                return OrderedDict()
    return column_map

def _inferir_tipo_coluna(series, sample_size=5000): # Aumentado sample_size para mais robustez
    """
    Inferir o tipo de dado de uma Series (coluna) com base em seu conteúdo.
    Prioridade: DATE > NUMBER > VARCHAR2.
    """
    ##print(f"DEBUG_INFER: Iniciando inferência para coluna: '{series.name}'") # NOVO PRINT

    # Pegar uma amostra significativa da Series
    sample = series.dropna().astype(str).str.strip().head(sample_size)
    ##print(f"DEBUG_INFER: Amostra da coluna '{series.name}': {sample.tolist()[:5]} (primeiros 5)") # NOVO PRINT
    #print(f"DEBUG_INFER: Tamanho da amostra: {len(sample)}") # NOVO PRINT

    if sample.empty:
        #print(f"DEBUG_INFER: Coluna '{series.name}' vazia ou amostra vazia. Retornando VARCHAR2(500).") # NOVO PRINT
        return "VARCHAR2(500)"

    # --- Tentar inferir como DATA ---
    temp_date_series = pd.to_datetime(sample, errors='coerce', dayfirst=True)
    date_convertible_count = temp_date_series.count()
    #print(f"DEBUG_INFER: Para DATA, '{series.name}': Convertíveis={date_convertible_count}/{len(sample)}, Ratio={date_convertible_count / len(sample):.2f}") # NOVO PRINT

    if date_convertible_count / len(sample) > 0.8:
        current_year = datetime.datetime.now().year
        valid_years_count = temp_date_series[(temp_date_series.dt.year >= 1900) & (temp_date_series.dt.year <= current_year + 5)].count()
        #print(f"DEBUG_INFER: Para DATA, '{series.name}': Anos Válidos={valid_years_count}/{len(sample)}, Ratio={valid_years_count / len(sample):.2f}") # NOVO PRINT
        if valid_years_count / len(sample) > 0.8:
            #print(f"DEBUG_INFER: Coluna '{series.name}' inferida como DATE.") # NOVO PRINT
            return "DATE"

    # --- Tentar inferir como NÚMERO ---
    temp_numeric_series = pd.to_numeric(sample.str.replace(',', '.'), errors='coerce') # Aceita vírgula como separador decimal
    numeric_convertible_count = temp_numeric_series.count()
    #print(f"DEBUG_INFER: Para NUMBER, '{series.name}': Convertíveis={numeric_convertible_count}/{len(sample)}, Ratio={numeric_convertible_count / len(sample):.2f}") # NOVO PRINT

    if numeric_convertible_count / len(sample) > 0.8: # Ajuste o threshold se necessário
        #print(f"DEBUG_INFER: Coluna '{series.name}' inferida como NUMBER.") # NOVO PRINT
        return "NUMBER"

    # --- Fallback para VARCHAR2 ---
    #print(f"DEBUG_INFER: Coluna '{series.name}' inferida como VARCHAR2(500) (fallback).") # NOVO PRINT
    return "VARCHAR2(500)"
    
# buscador_de_dados.py
# Função para renomear colunas para padrão Oracle (NOVA LÓGICA REESCRITA PARA SUFIXOS)
def _rename_column_for_oracle(column_name):
    """
    Renomeia uma coluna seguindo as regras Oracle e padrões definidos.
    Prioridade: Manter prefixo alvo > Mapear palavra-chave > Prefixo CD_.
    Preserva o restante do nome da coluna após o prefixo/mapeamento.
    Truncamento para 30 caracteres.
    """
    #print(f"DEBUG_RENAME: Iniciando renomeação para: '{column_name}'")
    if not isinstance(column_name, str):
        column_name = str(column_name)

    # Limpeza inicial agressiva: apenas letras, números e underscores
    cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '', column_name)
    cleaned_name = cleaned_name.upper().strip()
    original_cleaned = cleaned_name 
    ##print(f"DEBUG_RENAME: Nome limpo: '{cleaned_name}'")

    PREFIXOS_ALVO = {"NM_", "DS_", "QT_", "DT_", "NU_", "CD_", "VL_", "TP_"}
    MAP_PALAVRAS_CHAVE = OrderedDict([
        ("NOME", "NM_"), ("NM", "NM_"),
        ("DESCRICAO", "DS_"), ("DS", "DS_"),
        ("QUANTIDADE", "QT_"), ("QT", "QT_"),
        ("DATA", "DT_"), ("DT", "DT_"),
        ("NUMERO", "NU_"), ("NU", "NU_"),
        ("CODIGO", "CD_"), ("CD", "CD_"),
        ("VALOR", "VL_"), ("VL", "VL_"),
        ("TIPO", "TP_"), ("TP", "TP_")
    ])

    final_name = ""

    # Regra 1: Se já começa com um prefixo alvo, mantém como está.
    for prefix in PREFIXOS_ALVO:
        if original_cleaned.startswith(prefix):
            final_name = original_cleaned
            #print(f"DEBUG_RENAME: Regra 1 aplicada. Nome final provisório: '{final_name}'")
            break

    # Regra 2: Tenta mapear palavras-chave e preservar sufixo
    if not final_name:
        for keyword, prefix_target in MAP_PALAVRAS_CHAVE.items():
            ##print(f"DEBUG_RENAME: Tentando palavra-chave: '{keyword}'")
            # Verifica se o cleaned_name começa com a keyword,
            # E se o que vem depois é um underscore ou o fim da string (limite de palavra)
            match = re.match(rf'^{re.escape(keyword)}(_|$)(.*)', cleaned_name)

            if match:
                # O match.groups() retorna uma tupla com os conteúdos dos grupos de captura explícitos.
                # Nossos grupos são (_|$) (grupo 1) e (.*) (grupo 2).
                # Para acessar o conteúdo do grupo (.*), usamos match.group(2).
                ##print(f"DEBUG_RENAME: Match encontrado para keyword '{keyword}'. Grupos (match.groups()): {match.groups()}")
                # Acessa o conteúdo do segundo grupo de captura, que é o 'resto da string'
                remainder_after_keyword = match.group(2) # <--- CORREÇÃO AQUI: de .groups()[2] para .group(2)

                # Se o prefix_target já tem underscore, usa ele. Senão, adiciona um.
                effective_prefix = prefix_target
                if not effective_prefix.endswith('_') and remainder_after_keyword:
                    effective_prefix += '_'

                final_name = effective_prefix + remainder_after_keyword

                if not remainder_after_keyword and keyword == cleaned_name.upper():
                    final_name = prefix_target + cleaned_name
                else:
                    final_name = final_name.strip('_')
                    
                ##print(f"DEBUG_RENAME: Regra 2 aplicada. Nome final provisório: '{final_name}'")
                break # Sai do loop de palavras-chave se encontrou um match

    # Regra 3: Fallback para prefixo padrão "CD_" se nada foi aplicado
    if not final_name:
        final_name = "CD_" + cleaned_name
        ##print(f"DEBUG_RENAME: Regra 3 aplicada. Nome final provisório: '{final_name}'")

    # Pós-processamento geral para todos os nomes
    final_name = re.sub(r'[^a-zA-Z0-9_]', '', final_name) # Remove caracteres inválidos
    final_name = re.sub(r'_{2,}', '_', final_name)     # Remove múltiplos underscores
    final_name = final_name.strip('_')                 # Remove underscores nas pontas
    ##print(f"DEBUG_RENAME: Nome após pós-processamento: '{final_name}'")

    # Fallback final se o nome ainda estiver vazio (altamente improvável agora)
    if not final_name:
        final_name = original_cleaned
        final_name = re.sub(r'[^A-Z0-9_]', '', final_name).strip('_')
        #print(f"DEBUG_RENAME: Fallback final para nome vazio: '{final_name}'")

    # Truncamento final para 30 caracteres (limite Oracle)
    final_name = final_name[:30]
    ##print(f"DEBUG_RENAME: Nome truncado (30 chars): '{final_name}'")
    return final_name

# --- Funções de Processamento e Renomeação ---
# Removidos prints de debug e ajustadas mensagens de retorno
def _processar_e_renomear_arquivos_e_colunas(arquivos_originais_paths):
    os.makedirs(IMPORT_BASE_FOLDER, exist_ok=True)
    processed_files_info = []

    print("\n[PASSO 3/5] Copiando e Renomeando Arquivos e Colunas...")

    for original_path in tqdm(arquivos_originais_paths, desc="Processando arquivos"):
        original_file_name = os.path.basename(original_path)
        base_name_without_ext = os.path.splitext(original_file_name)[0]
        file_extension = os.path.splitext(original_file_name)[1]

        table_name_candidate_base = "TT_" + base_name_without_ext.replace(' ', '_').upper()
        table_name_candidate_base = re.sub(r'[^A-Z0-9_]', '', table_name_candidate_base).strip('_')
        table_name_candidate_base = table_name_candidate_base[:30]

        if original_file_name.lower().endswith('.csv'):
            print(f"  Iniciando processamento do CSV: '{original_file_name}'...")
            try:
                # NOVO: Detectar propriedades do CSV
                csv_props = _detect_csv_properties(original_path)
                detected_delimiter = csv_props['delimiter']
                detected_quoting = csv_props['quoting']

                df = None
                sucesso_leitura = False
                for encoding in ENCODINGS_TO_TRY:
                    try:
                        df = pd.read_csv(original_path, 
                                         sep=detected_delimiter, # Usar delimitador detectado
                                         dtype=str, 
                                         encoding=encoding,
                                         on_bad_lines='skip', 
                                         engine='python', 
                                         header=0, # NOVO: Forçar a primeira linha como cabeçalho (índice 0)
                                         quoting=detected_quoting, # Usar quoting detectado
                                         escapechar='\\').fillna('')
                        sucesso_leitura = True
                        break
                    except UnicodeDecodeError:
                        continue
                    except Exception as e:
                        print(f"DEBUG: Erro na tentativa de leitura do CSV com {encoding}, sep='{detected_delimiter}', quoting='{detected_quoting}': {e}")
                        pass # Continua para o próximo encoding

                if not sucesso_leitura or df is None:
                    print(f"  ❌ Erro: Não foi possível ler o CSV '{original_file_name}'. Certifique-se de que o arquivo está no formato CSV (delimitado por ';'). Pulando.")
                    continue

                if df.empty:
                    print(f"  [AVISO] O arquivo CSV '{original_file_name}' está vazio. Pulando.")
                    continue

                #print(f"DEBUG: CSV '{original_file_name}' lido. Colunas originais: {list(df.columns)}")

                renamed_columns_map = {}
                renamed_columns_with_types = {}

                df.columns = df.columns.astype(str)
                for col in df.columns:
                    #print(f"DEBUG: Processando coluna '{col}'")
                    new_col_name = _rename_column_for_oracle(col)

                    #print(f"DEBUG: Coluna '{col}' renomeada para '{new_col_name}'")

                    original_new_col_name = new_col_name
                    counter = 1
                    while new_col_name in renamed_columns_map.values():
                        new_col_name = f"{original_new_col_name[:min(len(original_new_col_name), 27)]}_{counter}"
                        counter += 1
                        if counter > 999:
                            print(f"AVISO: Excedido limite de renomeação única para coluna '{col}'. Mantendo nome original.")
                            new_col_name = col
                            break
                    renamed_columns_map[col] = new_col_name

                    # Debug da inferência de tipo
                    try:
                        inferred_type = _inferir_tipo_coluna(df[col])
                        renamed_columns_with_types[new_col_name] = inferred_type
                        #print(f"DEBUG: Tipo inferido para '{new_col_name}': {inferred_type}")
                    except Exception as e_infer:
                        #print(f"DEBUG: Erro ao inferir tipo para coluna '{col}': {e_infer}")
                        renamed_columns_with_types[new_col_name] = "VARCHAR2(500)" # Fallback para continuar

                df.rename(columns=renamed_columns_map, inplace=True)

                #print(f"DEBUG: Colunas do DF após renomeação: {list(df.columns)}")

                # Estas linhas estavam com indentação incorreta e causavam o IndentationError
                table_name_final_for_ddl_and_ctl = table_name_candidate_base
                copied_csv_file_name = f"{table_name_final_for_ddl_and_ctl}.csv"
                copied_csv_file_path = os.path.join(IMPORT_BASE_FOLDER, copied_csv_file_name)

                df.to_csv(copied_csv_file_path, sep=';', index=False, encoding='utf-8-sig', quoting=csv.QUOTE_NONE)

                print(f"  ✅ '{original_file_name}' processado e renomeado para '{os.path.basename(copied_csv_file_path)}'.")

                processed_files_info.append({
                    "original_path": original_path,
                    "copied_file_path": copied_csv_file_path,
                    "table_name": table_name_final_for_ddl_and_ctl,
                    "renamed_columns": list(df.columns),  # Nomes das colunas renomeadas
                    "renamed_columns_with_types": renamed_columns_with_types,  # NOVO: Nomes e Tipos Inferidos
                    "record_count": len(df),
                    "original_sheet_name": None,
                    "ddl_success": False # NOVO: Inicializa o status DDL como False
                })
            except Exception as e:
                # O erro "tuple index out of range" original deve aparecer aqui agora
                print(f"  ❌ Erro inesperado ao processar CSV '{original_file_name}': {e}. Pulando.")
                continue
        else:
            print(f"  [AVISO] Formato de arquivo '{file_extension}' não suportado para importação direta. Por favor, converta para CSV usando a Opção 2 no menu principal. Pulando.")
            continue

    return processed_files_info



# --- Geração e Execução de DDL ---
def _gerar_e_executar_ddl(processed_files_info, db_conn_info, force_create_mode=None):
    os.makedirs(os.path.join(IMPORT_BASE_FOLDER, DDL_SUBFOLDER), exist_ok=True)
    user, password, db = db_conn_info['user'], db_conn_info['password'], db_conn_info['db']

    print("\n[PASSO 4/5] Gerando e Executando Scripts DDL...")

    for file_info in tqdm(processed_files_info, desc="Executando DDLs"):
        table_name = file_info['table_name']
        renamed_columns_with_types = file_info['renamed_columns_with_types'] 
        
        drop_ddl_file_name = f"DROP_TABLE_{table_name}.sql"
        drop_ddl_file_path = os.path.join(IMPORT_BASE_FOLDER, DDL_SUBFOLDER, drop_ddl_file_name)

        create_ddl_file_name = f"CREATE_TABLE_{table_name}.sql"
        create_ddl_file_path = os.path.join(IMPORT_BASE_FOLDER, DDL_SUBFOLDER, create_ddl_file_name)

        column_lines_for_ddl = []
        for i, col_name in enumerate(renamed_columns_with_types.keys()): 
            col_oracle_type = renamed_columns_with_types[col_name]
            
            column_line = f"  \"{col_name}\" {col_oracle_type}"
            if i < len(renamed_columns_with_types.keys()) - 1:
                column_line += "," 
            
            column_lines_for_ddl.append(column_line)
            
        # Adiciona a coluna DT_IMPORTACAO ao final
        if column_lines_for_ddl:
            column_lines_for_ddl[-1] += "," 
        column_lines_for_ddl.append("  \"DT_IMPORTACAO\" DATE DEFAULT SYSDATE")
        
        drop_ddl_content = "\n".join([
            "SET ECHO ON;",
            "WHENEVER SQLERROR CONTINUE;", 
            f"DROP TABLE \"{table_name}\" CASCADE CONSTRAINTS;",
            "COMMIT;",
            "EXIT;"
        ])

        create_ddl_content_parts = [
            "SET ECHO ON;",
            "WHENEVER SQLERROR EXIT FAILURE;", 
            f"CREATE TABLE \"{table_name}\" ("
        ]
        create_ddl_content_parts.extend(column_lines_for_ddl)
        create_ddl_content_parts.extend([
            ");",
            "COMMIT;",
            "EXIT;"
        ])
        create_ddl_content = "\n".join(create_ddl_content_parts)


        try:
            current_create_mode = force_create_mode
            if current_create_mode is None: 
                current_create_mode = 'drop_and_create' 

            if current_create_mode == 'drop_and_create':
                with open(drop_ddl_file_path, 'w', encoding='utf-8') as f:
                    f.write(drop_ddl_content)
                print(f"  Gerado DROP DDL para '{table_name}'.")

                # AQUI: Usando o retorno aprimorado de _executar_sql_comando
                drop_success, drop_message = _executar_sql_comando(drop_ddl_content, 'sqlplus', user, password, db, log_file_path=os.path.join(IMPORT_BASE_FOLDER, f"{table_name}_drop_ddl_log.txt"))
                if not drop_success:
                    print(f"  AVISO: O comando DROP DDL para '{table_name}' pode ter falhado ou encontrado problemas: {drop_message}. Tentando CREATE de qualquer forma.")
                
                with open(create_ddl_file_path, 'w', encoding='utf-8') as f:
                    f.write(create_ddl_content)
                print(f"  Gerado CREATE DDL para '{table_name}'.")

                # AQUI: Usando o retorno aprimorado de _executar_sql_comando
                create_success, create_message = _executar_sql_comando(create_ddl_content, 'sqlplus', user, password, db, log_file_path=os.path.join(IMPORT_BASE_FOLDER, f"{table_name}_create_ddl_log.txt"))
                if create_success:
                    print(f"  ✅ Comando SQLPLUS para CREATE DDL executado. Verificando criação da tabela...")
                    
                    check_table_sql = f"SET HEADING OFF;\nSET FEEDBACK OFF;\nSELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = '{table_name}';"
                    check_log_path = os.path.join(IMPORT_BASE_FOLDER, f"{table_name}_table_check_log.txt")
                    
                    # AQUI: Usando o retorno aprimorado de _executar_sql_comando para verificação de tabela
                    table_exists_success, check_message = _executar_sql_comando(check_table_sql, 'sqlplus', user, password, db, log_file_path=check_log_path)
                    
                    table_found = False
                    if table_exists_success and os.path.exists(check_log_path):
                        with open(check_log_path, 'r', encoding='utf-8', errors='ignore') as f_check_log:
                            check_output = f_check_log.read()
                            all_counts = re.findall(r'^\s*(\d+)\s*$', check_output, re.MULTILINE)
                            if all_counts:
                                try:
                                    count = int(all_counts[-1])
                                    if count > 0:
                                        table_found = True
                                except ValueError:
                                    pass
                    
                    if table_found:
                        print(f"  ✅ Tabela '{table_name}' criada e confirmada no banco de dados.")
                        file_info['ddl_success'] = True
                    else:
                        print(f"  ❌ ERRO: Tabela '{table_name}' NÃO foi criada no banco de dados, ou falha na verificação. {check_message}. Verifique o log de CREATE DDL.")
                        file_info['ddl_success'] = False
                        continue 

                else: 
                    print(f"  ❌ ERRO: Falha na execução do CREATE DDL para '{table_name}'. {create_message} Verifique o log de CREATE DDL.")
                    file_info['ddl_success'] = False
                    continue
            elif current_create_mode == 'append_only':
                print(f"  Modo de carga 'Adicionar' selecionado. Pulando criação/recriação de tabela para '{table_name}'.")
                file_info['ddl_success'] = True 
            else:
                print(f"  [ERRO] Modo de criação de tabela desconhecido: '{current_create_mode}'.")
                file_info['ddl_success'] = False
                continue
            
        except Exception as e:
            print(f"  ❌ ERRO inesperado ao gerar ou executar DDL para '{table_name}': {e}.")
            file_info['ddl_success'] = False
    return processed_files_info

# NOVO: Função para verificar a presença e acessibilidade das ferramentas SQL*Plus e SQL*Loader
def _verificar_ferramentas_oracle_client():
    #print("DEBUG_VERIFICA_CLIENT: Início da função _verificar_ferramentas_oracle_client()") # NOVO PRINT AQUI
    print("\n--- Verificando pré-requisitos do Oracle Client ---")
    ferramentas_ok = True
    mensagens_erro = []

    executaveis_para_checar = {
        "sqlplus.exe": "para conexão e execução de DDLs",
        "sqlldr.exe": "para importação de dados"
    }

    for exec_name, purpose in executaveis_para_checar.items():
        #print(f"DEBUG_VERIFICA_CLIENT: Checando {exec_name}") # NOVO PRINT AQUI
        try:
            # Preferimos usar shutil.which() para uma checagem mais robusta e segura.
            # CERTIFIQUE-SE DE QUE 'import shutil' ESTÁ NO TOPO DO ARQUIVO!
            if shutil.which(exec_name):
                print(f"  ✅ '{exec_name}' encontrado no PATH ({purpose}).")
            else:
                ferramentas_ok = False
                mensagens_erro.append(f"  ❌ '{exec_name}' não encontrado no PATH ({purpose}).")

        except Exception as e:
            #print(f"DEBUG_VERIFICA_CLIENT: Erro na checagem de {exec_name}: {e}") # NOVO PRINT AQUI
            ferramentas_ok = False
            mensagens_erro.append(f"  ❌ Erro ao verificar '{exec_name}': {e}.")

    #print("DEBUG_VERIFICA_CLIENT: Fim do loop de checagem.") # NOVO PRINT AQUI

    if not ferramentas_ok:
        #print("DEBUG_VERIFICA_CLIENT: Ferramentas não OK.") # NOVO PRINT AQUI
        print("\n[ATENÇÃO] Problemas com as ferramentas do Oracle Client foram detectados:")
        for msg in mensagens_erro:
            print(msg)
        print("\nPara que a ferramenta funcione corretamente, certifique-se de que:")
        print("1. O Oracle Instant Client (ou Oracle Client completo) está instalado no seu computador.")
        print("2. O diretório 'bin' da instalação do Oracle Client está adicionado à variável de ambiente 'PATH' do sistema.")
        print("   (Ex: C:\\app\\client\\product\\19.0.0\\client_1\\bin)")
        
        while True:
            escolha = input("\nDeseja tentar continuar mesmo assim (s/n)? (Pode haver mais erros) ").strip().lower()
            if escolha == 's':
                #print("DEBUG_VERIFICA_CLIENT: Usuário escolheu continuar.") # NOVO PRINT AQUI
                print("Continuando... (Aviso: A funcionalidade pode estar comprometida).")
                return True
            elif escolha == 'n':
                #print("DEBUG_VERIFICA_CLIENT: Usuário escolheu não continuar.") # NOVO PRINT AQUI
                print("Operação cancelada. Por favor, configure o ambiente e tente novamente.")
                return False
            else:
                print("Opção inválida. Digite 's' ou 'n'.")
    else:
        #print("DEBUG_VERIFICA_CLIENT: Ferramentas OK.") # NOVO PRINT AQUI
        print("  ✅ Todas as ferramentas do Oracle Client verificadas e encontradas.")
        return True
        
# --- Geração e Execução de SQL Loader (Versão Estável sem progresso em tempo real) ---
def _gerar_e_executar_sqlldr(processed_files_info, db_conn_info, load_mode='append'):
    user, password, db = db_conn_info['user'], db_conn_info['password'], db_conn_info['db']
    load_results = []

    print("\n[PASSO 5/5] Gerando Arquivos .CTL e Executando SQL*Loader...")

    for file_info in processed_files_info:
        if not file_info.get('ddl_success', False):
            print(f"  [PULANDO] Carga para '{file_info['table_name']}' pois a criação da tabela falhou na etapa DDL.")
            load_results.append({
                "table_name": file_info['table_name'],
                "original_file": os.path.basename(file_info['original_path']),
                "status": "Ignorado (falha DDL)",
                "records_loaded": 0,
                "log_file": None,
                "bad_file": None,
                "records_rejected": 0
            })
            continue

        table_name = file_info['table_name']
        copied_file_path = file_info['copied_file_path'] 
        renamed_columns = file_info['renamed_columns']
        total_records_to_load = file_info['record_count']

        load_method = "REPLACE" if load_mode == 'replace' else "APPEND"
        print(f"  Aguardando 2 segundos para garantir visibilidade da tabela '{table_name}' no banco de dados...")
        time.sleep(2) 

        # Ajuste para usar SQL_LOADER_SAFE_TEMP_DIR para todos os arquivos gerados/usados pelo SQL*Loader
        ctl_file_name = f"{table_name}.ctl"
        ctl_file_path = os.path.join(SQL_LOADER_SAFE_TEMP_DIR, ctl_file_name)
        log_file_path = os.path.join(SQL_LOADER_SAFE_TEMP_DIR, f"{table_name}.log") # Este será o log final
        bad_file_path = os.path.join(SQL_LOADER_SAFE_TEMP_DIR, f"{table_name}.bad")
        discard_file_path = os.path.join(SQL_LOADER_SAFE_TEMP_DIR, f"{table_name}.dsc")
        
        # Limpar arquivos temporários antes de cada execução
        for f_path in [ctl_file_path, log_file_path, bad_file_path, discard_file_path]:
            if os.path.exists(f_path):
                try:
                    os.remove(f_path)
                except OSError as e:
                    print(f"AVISO: Não foi possível remover o arquivo {os.path.basename(f_path)}: {e}")

        date_format_in_data = "DD/MM/YYYY HH24:MI:SS"

        column_entries_for_ctl = []
        for col_name in renamed_columns:
            if col_name == "DT_IMPORTACAO":
                continue 
            if col_name.startswith("DT_"):
                column_entries_for_ctl.append(f"  \"{col_name}\" DATE \"{date_format_in_data}\"")
            else:
                column_entries_for_ctl.append(f"  \"{col_name}\" CHAR(4000)") 

        ctl_content = f"""
LOAD DATA
INFILE '{os.path.abspath(copied_file_path).replace('\\', '/')}' 
BADFILE '{os.path.abspath(bad_file_path).replace('\\', '/')}'
DISCARDFILE '{os.path.abspath(discard_file_path).replace('\\', '/')}'
-- REMOVIDO: ROWS 5000 - Removido para estabilizar a carga. O SQL*Loader usará seu padrão de commit.
{load_method} INTO TABLE "{table_name}"
FIELDS TERMINATED BY ';'
OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
{',\n'.join(column_entries_for_ctl)}
)
"""
        # Outer try-except para gerar CTL ou executar SQL*Loader
        try: # <--- INÍCIO DO TRY EXTERNO. TUDO ABAIXO DEVE SER INDENTADO A PARTIR DAQUI.
            with open(ctl_file_path, 'w', encoding='utf-8') as f:
                f.write(ctl_content.strip())
            print(f"  Gerado arquivo CTL para '{table_name}'.")

            sqlldr_command = [
                "sqlldr.exe",
                f"{user}/{password}@{db}",
                f"control='{os.path.abspath(ctl_file_path).replace(os.sep, '/')}'",
                f"bad='{os.path.abspath(bad_file_path).replace(os.sep, '/')}'",
                f"discard='{os.path.abspath(discard_file_path).replace(os.sep, '/')}'"
            ]
            
            print(f"\n  Iniciando importação de '{os.path.basename(copied_file_path)}' para '{table_name}'...")
            print(f"  Acompanhar o andamento desta carga pelo banco.")
            print(f"  SELECT COUNT(*) FROM \"{table_name}\";") # Corrigido para aspas duplas no nome da tabela
            print(f"\n  Importando....';") # Este print parece ter um erro de digitação (aspas soltas e ;')
            
            process = None
            # Inner try-except para a execução do subprocesso SQL*Loader
            try: # <--- INÍCIO DO TRY INTERNO
                process = subprocess.Popen(sqlldr_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=False, env=os.environ)
                
                # Espera o processo terminar e coleta a saída, com timeout
                SQL_LOADER_EXECUTION_TIMEOUT = 900 # 15 minutos de timeout
                stdout, stderr = process.communicate(timeout=SQL_LOADER_EXECUTION_TIMEOUT)

            except FileNotFoundError:
                print(f"❌ ERRO: O executável 'sqlldr.exe' não foi encontrado. Verifique se o Oracle Client está instalado e configurado no PATH.")
                status = "Falha (SQL*Loader não encontrado)"
                load_results.append({
                    "table_name": table_name, "original_file": os.path.basename(file_info['original_path']),
                    "status": status, "records_loaded": 0,
                    "log_file": log_file_path, "bad_file": bad_file_path, "records_rejected": 0, "records_discarded": 0
                })
                continue 
            except subprocess.TimeoutExpired:
                print(f"❌ ERRO: O processo SQL*Loader para '{table_name}' excedeu o tempo limite de {SQL_LOADER_EXECUTION_TIMEOUT} segundos. Forçando término.")
                process.kill()
                stdout, stderr = process.communicate()
                status = "Falha (Tempo Limite)"
                load_results.append({
                    "table_name": table_name, "original_file": os.path.basename(file_info['original_path']),
                    "status": status, "records_loaded": 0,
                    "log_file": log_file_path, "bad_file": bad_file_path, "records_rejected": 0, "records_discarded": 0
                })
                final_log_content_str = stdout + "\n" + stderr
                with open(log_file_path, 'w', encoding='utf-8', errors='ignore') as f:
                    f.write(final_log_content_str)
                continue 

            except Exception as e: # Captura outros erros inesperados do subprocesso
                print(f"❌ ERRO inesperado ao executar SQL*Loader para '{table_name}': {e}.")
                status = "Falha (Erro Subprocesso)"
                load_results.append({
                    "table_name": table_name, "original_file": os.path.basename(file_info['original_path']),
                    "status": status, "records_loaded": 0,
                    "log_file": log_file_path, "bad_file": bad_file_path, "records_rejected": 0, "records_discarded": 0
                })
                continue 

            # TODO: Corrigir o print "Importando....';" para "Importando..."
            # Salva toda a saída do SQL*Loader (stdout e stderr combinados) no log_file_path
            final_log_content_str = stdout + "\n" + stderr 
            with open(log_file_path, 'w', encoding='utf-8', errors='ignore') as f:
                f.write(final_log_content_str)
            # Os arquivos .bad e .dsc já são gerados pelo SQL*Loader se ele conseguir iniciar.

            # Extrai os números finais de registros carregados/rejeitados/descartados do log final
            records_loaded_current = 0 
            records_rejected_current = 0
            records_discarded_current = 0

            match_loaded = re.search(r'(\d+)\s+Rows successfully loaded', final_log_content_str)
            if match_loaded:
                records_loaded_current = int(match_loaded.group(1))
            else: 
                match_loaded_alt = re.search(r'Total rows loaded:\s*(\d+)', final_log_content_str)
                if match_loaded_alt:
                    records_loaded_current = int(match_loaded_alt.group(1))
                
            match_rejected = re.search(r'Total logical records rejected:\s*(\d+)', final_log_content_str)
            if match_rejected:
                records_rejected_current = int(match_rejected.group(1))
            
            match_discarded = re.search(r'Total logical records discarded:\s*(\d+)', final_log_content_str)
            if match_discarded:
                records_discarded_current = int(match_discarded.group(1))

            # Define o status final baseado no returncode e nos registros
            status = "Falha"
            if process.returncode == 0:
                if records_loaded_current > 0 and records_rejected_current == 0 and records_discarded_current == 0:
                    print(f"  ✅ Importação concluída para '{table_name}': {records_loaded_current} registros importados.")
                    status = "Sucesso"
                elif records_loaded_current > 0 and (records_rejected_current > 0 or records_discarded_current > 0):
                    print(f"  ⚠️ Importação para '{table_name}' concluída com avisos. {records_loaded_current} carregados, {records_rejected_current} rejeitados, {records_discarded_current} descartados. Verifique log/bad/discard file.")
                    status = "Sucesso com Aviso"
                elif records_loaded_current == 0 and total_records_to_load > 0 and records_rejected_current == 0 and records_discarded_current == 0:
                    print(f"  ❌ Falha: 0 registros carregados para '{table_name}', mas o arquivo continha dados. Verifique o log e o bad file. (Erro de dados ou CTL inválido).")
                    status = "Falha (Dados/CTL)"
                elif records_loaded_current == 0 and total_records_to_load == 0 and records_rejected_current == 0 and records_discarded_current == 0:
                     print(f"  ✅ Importação concluída para '{table_name}': 0 registros (arquivo vazio ou sem dados).")
                     status = "Sucesso"
                else: 
                    print(f"  ⚠️ Importação para '{table_name}' concluída com avisos/erros. {records_loaded_current} carregados, {records_rejected_current} rejeitados, {records_discarded_current} descartados. Verifique log/bad/discard file.")
                    status = "Sucesso com Aviso"
            elif process.returncode == 1: 
                if records_loaded_current > 0 and (records_rejected_current > 0 or records_discarded_current > 0):
                    print(f"  ⚠️ Importação para '{table_name}' concluída com avisos. Código de saída {process.returncode}. {records_loaded_current} carregados, {records_rejected_current} rejeitados, {records_discarded_current} descartados. Verifique log/bad/discard file.")
                    status = "Sucesso com Aviso" 
                else: 
                    print(f"  ❌ Falha na importação para '{table_name}'. Código de saída do SQL*Loader: {process.returncode}. Registros carregados: {records_loaded_current}, Rejeitados: {records_rejected_current}, Descartados: {records_discarded_current}. Verifique o log: '{log_file_path}'.")
                    if "SQL*Loader-000" in final_log_content_str:
                        status = "Falha (Erro SQL*Loader)"
                    else:
                        status = "Falha (Erro Desconhecido)"
            elif process.returncode == 2: 
                if records_loaded_current == total_records_to_load and records_rejected_current == 0 and records_discarded_current == 0:
                    print(f"  ✅ Importação concluída para '{table_name}': {records_loaded_current} registros importados. (Código de saída SQL*Loader: {process.returncode})")
                    status = "Sucesso" 
                elif records_loaded_current > 0 and (records_rejected_current > 0 or records_discarded_current > 0):
                    print(f"  ⚠️ Importação para '{table_name}' concluída com avisos. Código de saída {process.returncode}. {records_loaded_current} carregados, {records_rejected_current} rejeitados, {records_discarded_current} descartados. Verifique log/bad/discard file.")
                    status = "Sucesso com Aviso"
                elif records_loaded_current == 0 and total_records_to_load > 0 and (records_rejected_current > 0 or records_discarded_current > 0):
                    print(f"  ❌ Falha: 0 registros carregados para '{table_name}', mas com rejeições/descartes. Código de saída {process.returncode}. Verifique log/bad/discard file.")
                    status = "Falha (Dados/CTL)" 
                else: 
                    print(f"  ❌ Falha na importação para '{table_name}'. Código de saída do SQL*Loader: {process.returncode}. Registros carregados: {records_loaded_current}, Rejeitados: {records_rejected_current}, Descartados: {records_discarded_current}. Verifique o log: '{log_file_path}'.")
                    status = "Falha (Erro Desconhecido)" 
            else: # Outros códigos de retorno (maior que 2) são sempre falha
                print(f"  ❌ Falha na importação para '{table_name}'. Código de saída do SQL*Loader: {process.returncode}. Registros carregados: {records_loaded_current}, Rejeitados: {records_rejected_current}, Descartados: {records_discarded_current}. Verifique o log: '{log_file_path}'.")
                status = "Falha (Erro Crítico)"

            load_results.append({
                "table_name": table_name,
                "original_file": os.path.basename(file_info['original_path']),
                "status": status,
                "records_loaded": records_loaded_current,
                "log_file": log_file_path,
                "bad_file": bad_file_path,
                "records_rejected": records_rejected_current,
                "records_discarded": records_discarded_current
            })

        except Exception as e: # <--- FINAL DO TRY EXTERNO, COM SEU EXCEPT
            print(f"  ❌ ERRO inesperado ao gerar CTL ou executar SQL*Loader para '{table_name}': {e}.")
            load_results.append({
                "table_name": table_name,
                "original_file": os.path.basename(file_info['original_path']),
                "status": f"Erro inesperado: {e}",
                "records_loaded": 0,
                "log_file": log_file_path,
                "bad_file": bad_file_path,
                "records_rejected": 0,
                "records_discarded": 0
            })
    
    # Resumo final da carga após processar todos os arquivos
    print("\n" + "="*50)
    print("--- RESUMO DA IMPORTAÇÃO DE CARGA ---")
    if not load_results:
        print("Nenhum arquivo foi importado.")
    else:
        for res in load_results:
            print(f"  Tabela: {res['table_name']}")
            print(f"    Arquivo Original: {res['original_file']}")
            print(f"    Status: {res['status']}")
            print(f"    Registros Importados: {res['records_loaded']:,}".replace(",", "."))
            if 'records_rejected' in res and res['records_rejected'] > 0:
                print(f"    Registros Rejeitados: {res['records_rejected']:,}".replace(",", "."))
            if 'records_discarded' in res and res['records_discarded'] > 0:
                print(f"    Registros Descartados: {res['records_discarded']:,}".replace(",", "."))
            if "Falha" in res['status'] or "Aviso" in res['status'] or res['status'] == 'Falha Crítica':
                print(f"    Detalhes no log: {res['log_file']}")
                if res['bad_file'] and os.path.exists(res['bad_file']) and os.path.getsize(res['bad_file']) > 0:
                     print(f"    Registros ruins (bad file): {res['bad_file']}")
                # Verifica se há descartes E se o discard file realmente existe e não está vazio
                if res['records_discarded'] > 0 and discard_file_path and os.path.exists(discard_file_path) and os.path.getsize(discard_file_path) > 0:
                     print(f"    Registros descartados (discard file): {discard_file_path}") 
                elif res['records_discarded'] > 0 and (not os.path.exists(discard_file_path) or os.path.getsize(discard_file_path) == 0):
                    print(f"    AVISO: Houve descartes mas o discard file '{discard_file_path}' está ausente ou vazio. Verifique.")
            print("-" * 40)
    print("="*50)

    # Bloco de limpeza de arquivos temporários do SQL*Loader (os prints já estão comentados)
    try:
        for file_info_clean in processed_files_info:
            temp_table_name_clean = file_info_clean['table_name']
            
            temp_csv_file = os.path.join(SQL_LOADER_SAFE_TEMP_DIR, f"{temp_table_name_clean}.csv")
            if os.path.exists(temp_csv_file): os.remove(temp_csv_file)

            temp_ctl_file = os.path.join(SQL_LOADER_SAFE_TEMP_DIR, f"{temp_table_name_clean}.ctl")
            if os.path.exists(temp_ctl_file): os.remove(temp_ctl_file)

            temp_log_file = os.path.join(SQL_LOADER_SAFE_TEMP_DIR, f"{temp_table_name_clean}.log")
            if os.path.exists(temp_log_file): os.remove(temp_log_file)

            temp_bad_file = os.path.join(SQL_LOADER_SAFE_TEMP_DIR, f"{temp_table_name_clean}.bad")
            if os.path.exists(temp_bad_file): os.remove(temp_bad_file)

            temp_dsc_file = os.path.join(SQL_LOADER_SAFE_TEMP_DIR, f"{temp_table_name_clean}.dsc")
            if os.path.exists(temp_dsc_file): os.remove(temp_dsc_file)
        
    except Exception as e:
        print(f"AVISO: Erro ao limpar arquivos temporários em '{SQL_LOADER_SAFE_TEMP_DIR}': {e}")

    return load_results

# --- Submenu Final de Histórico e Re-importação ---
def _submenu_historico_importacao(db_conn_info):
    historico_importacao = _carregar_historico_importacao()

    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        print("\n" + "="*50)
        print("--- Histórico de Importações ---")
        if not historico_importacao:
            print("  Nenhum histórico de importação encontrado.")
        else:
            print("Importações Salvas:")
            for i, entry in enumerate(historico_importacao):
                try:
                    table_name = entry.get('table_name', 'N/A')
                    original_file = entry.get('original_file', 'N/A')
                    records_loaded = entry.get('records_loaded', 0)
                    import_date = entry.get('import_date', 'N/A')
                    copied_file_path = entry.get('copied_file_path', '')

                    status_file = "✅ Arquivo OK"
                    if not copied_file_path or not os.path.exists(copied_file_path):
                        status_file = "❌ Arquivo AUSENTE!" 

                    print(f"  {i+1}. Tabela: {table_name}, Arquivo Original: {original_file}, Registros: {records_loaded:,}".replace(",", ".") + f", Data: {import_date} ({status_file})")
                except Exception as e:
                    print(f"  [AVISO] Erro ao carregar entrada {i+1} do histórico: {e}. Entrada ignorada. (Provavelmente um registro antigo com formato diferente)")
                    continue 

        print("\nOpções:")
        print(" 1. Realizar Nova Importação")
        print(" 2. Re-importar do Histórico")
        print(" 3. Limpar Histórico de Importação")
        print(" 0. Voltar ao Menu Principal")

        escolha = input("\nDigite sua opção: ").strip()

        if escolha == '1':
            return 'nova_importacao'
        
        elif escolha == '2':
            if not historico_importacao:
                print("Nenhum histórico para re-importar.")
                time.sleep(1.5)
                continue
            try:
                idx = int(input("Digite o NÚMERO da importação que deseja RE-IMPORTAR: ").strip()) - 1
                if 0 <= idx < len(historico_importacao):
                    entry_to_reimport = historico_importacao[idx]
                    
                    original_file_for_reimport = entry_to_reimport.get('original_file', None)
                    if original_file_for_reimport is None:
                        print(f"❌ Erro: Entrada de histórico selecionada não possui 'original_file'. Por favor, selecione outra ou limpe o histórico.")
                        input("Pressione Enter para continuar...")
                        continue

                    copied_path = entry_to_reimport.get('copied_file_path', '')
                    if not copied_path or not os.path.exists(copied_path): 
                        print(f"❌ Erro: O arquivo copiado '{os.path.basename(copied_path) if copied_path else 'AUSENTE'}' não foi encontrado. Não é possível re-importar. Por favor, regenere a carga original ou use um arquivo válido.")
                        input("Pressione Enter para continuar...")
                        continue
                    
                    print(f"\nIniciando re-importação para a tabela '{entry_to_reimport.get('table_name', 'N/A')}'...")
                    
                    df_reimport_temp = None
                    file_ext_reimport = os.path.splitext(copied_path)[1].lower()
                    if file_ext_reimport == '.csv':
                        # Use a detecção de propriedades CSV para robustez na re-importação também
                        csv_props_reimport = _detect_csv_properties(copied_path)
                        detected_delimiter_reimport = csv_props_reimport['delimiter']
                        detected_quoting_reimport = csv_props_reimport['quoting']
                        
                        for encoding in ENCODINGS_TO_TRY:
                            try:
                                df_reimport_temp = pd.read_csv(copied_path, sep=detected_delimiter_reimport, dtype=str, encoding=encoding,
                                                               on_bad_lines='skip', engine='python', header=0, quoting=detected_quoting_reimport).fillna('')
                                break
                            except UnicodeDecodeError:
                                continue
                            except Exception as e:
                                print(f"    [AVISO] Erro ao ler CSV '{os.path.basename(copied_path)}' para re-importação com '{encoding}': {e}")
                    else:
                        print(f"    [ERRO] Formato de arquivo '{file_ext_reimport}' não suportado para re-importação de histórico. Apenas CSVs copiados são suportados.")
                        input("Pressione Enter para continuar...")
                        continue

                    if df_reimport_temp is None:
                        print(f"❌ Não foi possível ler o arquivo '{os.path.basename(copied_path)}' para re-importação.")
                        input("Pressione Enter para continuar...")
                        continue
                    
                    df_reimport_temp.columns = df_reimport_temp.columns.astype(str).strip() 
                    reimported_cols = list(df_reimport_temp.columns)

                    # Simula processed_files_data para a re-importação
                    # RENAMED_COLUMNS_WITH_TYPES precisa ser preenchido para o DDL
                    renamed_columns_with_types_for_reimport = {}
                    for col_name in reimported_cols:
                        # Re-infere o tipo ou usa um padrão se a inferência for muito cara
                        inferred_type = _inferir_tipo_coluna(df_reimport_temp[col_name]) 
                        renamed_columns_with_types_for_reimport[col_name] = inferred_type
                    
                    processed_files_data_for_reimport = [{
                        "original_path": original_file_for_reimport, 
                        "copied_file_path": copied_path,
                        "table_name": entry_to_reimport.get('table_name', 'N/A'),
                        "renamed_columns": reimported_cols, 
                        "renamed_columns_with_types": renamed_columns_with_types_for_reimport, # NOVO: Preenchido para DDL
                        "record_count": len(df_reimport_temp), # Pegar a contagem real do DF lido
                        "original_sheet_name": entry_to_reimport.get('original_sheet_name', None),
                        "log_file": entry_to_reimport.get('log_file', None), 
                        "bad_file": entry_to_reimport.get('bad_file', None), 
                        "ddl_success": False # Inicializado como False. O DDL irá setar True se for sucesso.
                    }]

                    # --- NOVA LÓGICA DE DECISÃO NA RE-IMPORTAÇÃO ---
                    table_name_to_check = processed_files_data_for_reimport[0]['table_name']
                    
                    table_exists_in_db = False
                    temp_log_path = os.path.join(IMPORT_BASE_FOLDER, f"{table_name_to_check}_check_exists.txt")
                    
                    success_check, message_check = _executar_sql_comando(f"SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = '{table_name_to_check}';", 
                                             'sqlplus', db_conn_info['user'], db_conn_info['password'], db_conn_info['db'], log_file_path=temp_log_path)
                    
                    if success_check:
                        if os.path.exists(temp_log_path):
                            with open(temp_log_path, 'r', encoding='utf-8', errors='ignore') as f_check:
                                check_output = f_check.read()
                                match = re.search(r'^\s*(\d+)\s*$', check_output, re.MULTILINE)
                                if match and int(match.group(1)) > 0:
                                    table_exists_in_db = True
                    else: # Se a verificação do SQL*Plus falhou
                        print(f"❌ Erro ao verificar a existência da tabela '{table_name_to_check}' no banco: {message_check}. Prosseguindo com criação/substituição por segurança.")
                        # Não definimos reimport_create_mode e load_mode aqui para que a escolha abaixo prevaleça
                    
                    reimport_create_mode = 'append_only' # Default para re-importação
                    reimport_load_mode = 'append' # Default para re-importação

                    if table_exists_in_db:
                        print(f"\n--- ATENÇÃO: A tabela '{table_name_to_check}' já existe no banco de dados. ---")
                        print("Deseja: ")
                        print(" 1. Adicionar os novos dados à tabela existente (APPEND)")
                        print(" 2. Dropar a tabela existente e recriá-la com os novos dados (REPLACE)")
                        print(" 0. Cancelar a re-importação")

                        user_choice_reimport_mode = input("\nDigite sua opção: ").strip()

                        if user_choice_reimport_mode == '1':
                            reimport_create_mode = 'append_only' 
                            reimport_load_mode = 'append'
                            print("Modo 'Adicionar nova carga' selecionado para re-importação.")
                        elif user_choice_reimport_mode == '2':
                            reimport_create_mode = 'drop_and_create' 
                            reimport_load_mode = 'replace' 
                            print("Modo 'Dropar e recriar' selecionado para re-importação.")
                        elif user_choice_reimport_mode == '0':
                            print("Re-importação cancelada pelo usuário.")
                            input("Pressione Enter para continuar...")
                            continue # Volta para o submenu de histórico
                        else:
                            print("Opção inválida. Assumindo 'Adicionar nova carga' (APPEND) para re-importação.")
                            reimport_create_mode = 'append_only'
                            reimport_load_mode = 'append'
                    else: # Tabela não existe, sempre cria e substitui
                        print(f"Tabela '{table_name_to_check}' não encontrada no banco. Será criada e preenchida (REPLACE).")
                        reimport_create_mode = 'drop_and_create'
                        reimport_load_mode = 'replace'

                    # Re-executa DDL e SQL Loader com os modos decididos
                    reimport_info_after_ddl = _gerar_e_executar_ddl(processed_files_data_for_reimport, db_conn_info, force_create_mode=reimport_create_mode)
                    
                    if reimport_info_after_ddl and reimport_info_after_ddl[0].get('ddl_success', False):
                        _gerar_e_executar_sqlldr(reimport_info_after_ddl, db_conn_info, load_mode=reimport_load_mode)
                    else:
                        print(f"❌ Re-importação da tabela '{entry_to_reimport.get('table_name', 'N/A')}' falhou na etapa DDL. Verifique os logs.")
                    input("\nPressione Enter para continuar...")
                else:
                    print("Número de seleção inválido.")
                    time.sleep(1.5)
            except ValueError:
                print("Entrada inválida. Por favor, digite um número.")
                time.sleep(1.5)

        elif escolha == '3':
            if not historico_importacao:
                print("Histórico de importação já está vazio.")
                time.sleep(1.5)
                continue
            confirm = input("Tem certeza que deseja LIMPAR TODO o histórico de importação? Esta ação é irreversível. (s/n): ").lower()
            if confirm == 's':
                _salvar_historico_importacao([])
                historico_importacao = [] 
                print("Histórico de importação limpo com sucesso.")
            else:
                print("Limpeza de histórico cancelada.")
            time.sleep(1.5)

        elif escolha == '0':
            print("Voltando ao menu anterior.")
            return 'voltar'
        else:
            print("Opção inválida. Tente novamente.")
            time.sleep(1.5)

# --- Função Principal do Módulo de Importar Carga ---
def _importar_carga():
    """
    Função principal para orquestrar o processo de importação de carga.
    """
    
    ##print("DEBUG_IMPORT_CARGA: Início da função _importar_carga()") # NOVO PRINT AQUI
    
    """if not _verificar_ferramentas_oracle_client():
        #print("DEBUG_IMPORT_CARGA: _verificar_ferramentas_oracle_client() retornou False. Retornando.") # NOVO PRINT AQUI
        return
   
    ##print("DEBUG_IMPORT_CARGA: _verificar_ferramentas_oracle_client() retornou True.") # NOVO PRINT AQUI
    input("\nPressione Enter para continuar com o gerenciamento de conexão...")"""
    os.makedirs(SQL_LOADER_SAFE_TEMP_DIR, exist_ok=True)
    print(f"DEBUG_IMPORT_CARGA: Diretório temporário para SQL*Loader: '{SQL_LOADER_SAFE_TEMP_DIR}'")


    db_conn_info = None
    while db_conn_info is None:
        print("Tentando obter conexão com o banco.") # NOVO PRINT AQUI
        db_conn_info = _gerenciar_conexoes_db()
        if db_conn_info is None:
            ##print("DEBUG_IMPORT_CARGA: Conexão não estabelecida. Retornando.") # NOVO PRINT AQUI
            print("Conexão com o banco de dados não estabelecida. Retornando ao menu principal.")
            return

    print("Conexão com o banco estabelecida.") # NOVO PRINT AQUI
   

    while True:
        load_results = []
        os.system('cls' if os.name == 'nt' else 'clear')
        print("\n" + "="*50)
        print("--- Módulo: Importar Carga ---")
        print(" 1. Iniciar Nova Importação")
        print(" 2. Ver Histórico de Importações e Re-importar")
        print(" 0. Voltar ao Menu Principal")
        import_choice = input("\nDigite sua opção: ").strip()

        if import_choice == '1':
            arquivos_selecionados = _selecionar_arquivos_para_importacao()
            if not arquivos_selecionados:
                print("Nenhum arquivo selecionado para importação. Retornando ao menu de importação.")
                time.sleep(1.5)
                continue 
            
            processed_files_data = _processar_e_renomear_arquivos_e_colunas(arquivos_selecionados)
            if not processed_files_data:
                print("Nenhum arquivo foi processado com sucesso para importação. Verifique os avisos/erros acima. Retornando.")
                input("Pressione Enter para continuar...")
                continue
            
            # NOVO: Exibir o resumo dos arquivos processados
            _exibir_resumo_importacao_inicial(processed_files_data)
            
            # --- LÓGICA DE DECISÃO PARA NOVA CARGA ---
            # Para simplificar o fluxo de decisão, vamos considerar o primeiro arquivo processado
            # para verificar a existência da tabela. Se você tiver múltiplos arquivos
            # que criam tabelas com nomes diferentes, essa lógica precisaria ser expandida
            # para interagir com o usuário para CADA tabela. Por ora, foca no primeiro.
            table_name_to_check = processed_files_data[0]['table_name'] 
            
            table_exists_in_db = False
            temp_log_path = os.path.join(IMPORT_BASE_FOLDER, f"{table_name_to_check}_check_exists.txt")
            
            print(f"\nVerificando se a tabela '{table_name_to_check}' já existe no banco de dados...")
            sql_check_command = f"SET HEADING OFF;\nSET FEEDBACK OFF;\nSELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = '{table_name_to_check}';"
            
            # AQUI: Usando o retorno aprimorado de _executar_sql_comando
            success, message = _executar_sql_comando(sql_check_command, 'sqlplus', db_conn_info['user'], db_conn_info['password'], db_conn_info['db'], log_file_path=temp_log_path)
            if success:
                if os.path.exists(temp_log_path):
                    with open(temp_log_path, 'r', encoding='utf-8', errors='ignore') as f_check:
                        check_output = f_check.read()
                        
                        all_counts = re.findall(r'^\s*(\d+)\s*$', check_output, re.MULTILINE)
                        
                        count = 0
                        if all_counts:
                            try:
                                count = int(all_counts[-1])
                            except ValueError:
                                count = 0
                        
                        #print(f"DEBUG: Tabela '{table_name_to_check}' encontrada {count} vez(es) no USER_TABLES (com base no último COUNT).")
                        if count > 0:
                            table_exists_in_db = True
            else: # Se a verificação do SQL*Plus falhou
                print(f"❌ Erro ao verificar a existência da tabela '{table_name_to_check}' no banco: {message}. Prosseguindo com criação/substituição por segurança.")
                # Apesar da falha na verificação, tentamos criar a tabela.
                # Poderíamos também abortar aqui, dependendo da criticidade.
                # Por ora, mantemos o fluxo para tentar criar/recriar.
                create_mode = 'drop_and_create'
                load_mode = 'replace'
            
            create_mode = 'drop_and_create'
            load_mode = 'replace'

            if table_exists_in_db:
                print(f"\n--- ATENÇÃO: A tabela '{table_name_to_check}' já existe no banco de dados. ---")
                print("Deseja: ")
                print(" 1. Adicionar os novos dados à tabela existente (APPEND)")
                print(" 2. Dropar a tabela existente e recriá-la com os novos dados (REPLACE)")
                print(" 0. Cancelar a importação")

                user_choice_mode = input("\nDigite sua opção: ").strip()

                if user_choice_mode == '1':
                    create_mode = 'append_only' 
                    load_mode = 'append'
                    print("Modo 'Adicionar nova carga' selecionado.")
                elif user_choice_mode == '2':
                    create_mode = 'drop_and_create' 
                    load_mode = 'replace' 
                    print("Modo 'Dropar e recriar' selecionado.")
                elif user_choice_mode == '0':
                    print("Importação cancelada pelo usuário.")
                    input("Pressione Enter para continuar...")
                    continue 
                else:
                    print("Opção inválida. Assumindo 'Adicionar nova carga' (APPEND).")
                    create_mode = 'append_only'
                    load_mode = 'append'
            else: # Tabela não existe, sempre cria e substitui
                print(f"\nTabela '{table_name_to_check}' não encontrada no banco. Será criada e preenchida (REPLACE).")
                create_mode = 'drop_and_create'
                load_mode = 'replace'


            # PASSO 4: Gerar e Executar DDL (passando o modo de criação)
            processed_files_data_after_ddl = _gerar_e_executar_ddl(processed_files_data, db_conn_info, force_create_mode=create_mode)
            
            if not all(f.get('ddl_success', False) for f in processed_files_data_after_ddl):
                print("Falha na etapa de DDL para uma ou mais tabelas. Carga não será realizada para estas.")
            
            # NOVO: Exibir o resumo após a execução do DDL
            _exibir_resumo_pos_ddl(processed_files_data_after_ddl)
            
            # PASSO 5: Gerar CTL e Executar SQL Loader (passando o modo de carregamento)
            load_results = _gerar_e_executar_sqlldr(processed_files_data_after_ddl, db_conn_info, load_mode=load_mode)

            # Salvar resultados no histórico de importação
            current_import_history = _carregar_historico_importacao()
            for res in load_results:
                new_entry = {
                    "table_name": res['table_name'],
                    "original_file_name": res['original_file'], 
                    "copied_file_path": next((f['copied_file_path'] for f in processed_files_data if f['table_name'] == res['table_name']), None),
                    "import_date": datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                    "records_loaded": res['records_loaded'],
                    "status": res['status'],
                    "original_sheet_name": next((f['original_sheet_name'] for f in processed_files_data if f['table_name'] == res['table_name']), None),
                    "log_file": res['log_file'],
                    "bad_file": res['bad_file']
                }
                current_import_history.append(new_entry)
            _salvar_historico_importacao(current_import_history)
            
            print("\nProcesso de importação de carga concluído. Detalhes no resumo final e histórico.")
            input("Pressione Enter para continuar...")
            # Após uma nova importação, direciona o usuário para o submenu de histórico
            # para que ele possa revisar ou re-importar, se desejar.
            _submenu_historico_importacao(db_conn_info) 
            # Se o usuário escolher "Voltar ao Menu Principal" no submenu, ele sai daqui.
            # Se escolher "Nova Importação", o loop externo continuará.
            
        elif import_choice == '2':
            # Chama o submenu de histórico, que gerencia as ações de re-importar/limpar/voltar
            _submenu_historico_importacao(db_conn_info) 
            # O submenu de histórico já lida com o input do usuário e o fluxo de retorno.
            # Se ele retornar, o loop principal do _importar_carga continuará.

        elif import_choice == '0':
            print("Voltando ao Menu Principal.")
            break
        else:
            print("Opção inválida. Tente novamente.")
            time.sleep(1.5)

# Adicione esta nova função em seu script, se ainda não tiver feito
def _selecionar_arquivos_para_tratamento():
    root = Tk(); root.withdraw(); root.attributes('-topmost', True)
    arquivos_paths_set = set()
    tipo_de_selecao_inicial = None

    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        print("\n--- Seleção de Arquivos para Tratamento ---")
        print("Como você gostaria de selecionar os arquivos?")
        print(" 1. Selecionar uma Pasta (buscará todos os arquivos suportados na pasta)")
        print(" 2. Selecionar Arquivos Específicos (seleção múltipla permitida)")
        print("\n 0. Cancelar e Voltar")

        escolha_selecao = input("\nDigite sua opção: ").strip()

        if escolha_selecao == '1':
            tipo_de_selecao_inicial = 'pasta'
            pasta_selecionada = filedialog.askdirectory(title="Selecione a Pasta para Tratamento")
            if not pasta_selecionada:
                print("\nNenhuma pasta foi selecionada. Por favor, tente novamente ou cancele.")
                time.sleep(1.5)
                continue
            try:
                # Permite CSV e Excel para tratamento
                arquivos_na_pasta = [os.path.join(pasta_selecionada, f) for f in os.listdir(pasta_selecionada) if f.lower().endswith(('.csv', '.xlsx', '.xls'))]
                if not arquivos_na_pasta:
                    print(f"\nNenhum arquivo CSV ou Excel encontrado na pasta '{pasta_selecionada}'.")
                    resposta = input("Deseja tentar outra seleção (s/n)? ").lower()
                    if resposta == 's': continue
                    else: return []
                arquivos_paths_set.update(arquivos_na_pasta)
                break
            except FileNotFoundError:
                print(f"ERRO: A pasta '{pasta_selecionada}' não foi encontrada. Tente novamente.")
                time.sleep(1.5)
                continue
        elif escolha_selecao == '2':
            tipo_de_selecao_inicial = 'arquivos'
            caminhos_arquivos_selecionados = filedialog.askopenfilenames(
                title="Selecione um ou mais Arquivos para Tratamento",
                # Permite CSV e Excel para tratamento
                filetypes=[("Arquivos Suportados", "*.csv *.xlsx *.xls")] 
            )
            if not caminhos_arquivos_selecionados:
                print("\nNenhum arquivo foi selecionado. Por favor, tente novamente ou cancele.")
                time.sleep(1.5)
                continue
            arquivos_paths_set.update(caminhos_arquivos_selecionados)
            break
        elif escolha_selecao == '0':
            print("\nOperação de seleção de arquivos cancelada.")
            return []
        else:
            print("Opção inválida. Por favor, digite 1, 2 ou 0.")
            time.sleep(1.5)

    while True: # Loop de Confirmação/Adição/Refazer
        os.system('cls' if os.name == 'nt' else 'clear')
        print("\n" + "="*50)
        print("--- Arquivos Selecionados Atualmente para Tratamento ---")
        arquivos_atuais = sorted(list(arquivos_paths_set))
        if not arquivos_atuais:
            print("\nNenhum arquivo válido selecionado para tratamento.")
            resposta = input("Deseja iniciar uma nova seleção (s/n)? ").lower()
            if resposta == 's': return _selecionar_arquivos_para_tratamento()
            else: return []
        print(f"\nTotal de arquivos selecionados: {len(arquivos_atuais)}")
        print("Lista de arquivos:"); [print(f" {i + 1}. {os.path.basename(full_path)}") for i, full_path in enumerate(arquivos_atuais)]; print("-" * 50)
        print("\nOpções:"); print(" [c] Confirmar e prosseguir"); print(" [a] Adicionar mais arquivos/pastas"); print(" [r] Refazer toda a seleção"); print(" [n] Cancelar e voltar")
        confirm_option = input("\nEscolha uma opção (c/a/r/n): ").lower().strip()
        if confirm_option == 'c': break
        elif confirm_option == 'a':
            if tipo_de_selecao_inicial == 'pasta':
                nova_pasta = filedialog.askdirectory(title="Adicionar arquivos de outra Pasta")
                if nova_pasta:
                    novos_arquivos = [os.path.join(nova_pasta, f) for f in os.listdir(nova_pasta) if f.lower().endswith(('.csv', '.xlsx', '.xls'))]
                    arquivos_paths_set.update(novos_arquivos)
                    print(f"Adicionados {len(novos_arquivos)} arquivos da pasta '{os.path.basename(nova_pasta)}'.")
                else: print("Nenhuma pasta adicional selecionada.")
            else:
                novos_caminhos_arquivos = filedialog.askopenfilenames(
                    title="Adicionar mais Arquivos para Tratamento", filetypes=[("Arquivos Suportados", "*.csv *.xlsx *.xls")])
                arquivos_paths_set.update(novos_caminhos_arquivos)
                print(f"Adicionados {len(novos_caminhos_arquivos)} arquivos.")
            time.sleep(1.5)
        elif confirm_option == 'r': return _selecionar_arquivos_para_tratamento()
        elif confirm_option == 'n': print("\nOperação de seleção cancelada."); return []
        else: print("Opção inválida."); time.sleep(1)
    return list(arquivos_paths_set)

    
def main():
  os.system('cls' if os.name == 'nt' else 'clear'); print("Iniciando.....Aguarde!"); time.sleep(1)
  while True:
    os.system('cls' if os.name == 'nt' else 'clear')
    # Aumentando a versão para refletir as novas funcionalidades de checagem no banco
    print("--- Ferramenta de Análise e Conversão v" + __version__ + " ---") # AGORA MOSTRA A VERSÃO
    print("Selecione uma opção:")
    print(" 1. Buscar Dados em Arquivos")
    print(" 2. Converter Arquivos (CSV <-> Excel)")
    print(" 3. Validador de Qualidade de Dados")
    print(" 4. Importar Carga")
    #print(" 5. Checagem no Banco")
    #print(" 6. Checar/Atualizar Versão") # NOVA OPÇÃO AQUI
    print("\n 0. Sair")
    opcao = input("\nDigite a opção desejada: ").strip()

    if opcao == '1':
      realizar_uma_busca()
    elif opcao == '2':
      executar_conversao_arquivos()
    elif opcao == '3':
      executar_validador_de_dados()
    elif opcao == '4':
      _importar_carga()
    #elif opcao == '5':
    #  _checagem_no_banco()
    #elif opcao == '6': # CHAMADA PARA A NOVA FUNÇÃO DE ATUALIZAÇÃO
    #  _checar_e_atualizar_versao()
    elif opcao == '0':
      break
    else:
      print("Opção inválida."); time.sleep(1)

    # Atualiza as opções válidas para o loop de "outra operação", incluindo a nova opção 6
    if opcao in ['1', '2', '3', '4', '5', '6']:
      resposta = input("\nDeseja realizar outra operação? (s/n): ").lower()
      if resposta == 'n': break

  print("\nObrigado por usar a ferramenta. Programa encerrado.")

if __name__ == '__main__':
  try:
    main()
  except Exception as e:
    print(f"\nOCORREU UM ERRO CRÍTICO INESPERADO: {e}")
    input("Pressione Enter para fechar o programa.")
