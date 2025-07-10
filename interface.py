import tkinter as tk
from tkinter import ttk, messagebox
import subprocess
import os
import threading
import time
import re
import queue
import matplotlib.pyplot as plt

PIPELINE_DIR = "core/pipelines"
RULES_DIR = "rules"

processo_em_execucao = None
start_time = None

total_registros = 0
registros_processados = 0
velocidade_processamento = 0
tempo_estimado = 0

entidade_ativa = None

contagem_ok = 0
contagem_divergente = 0
contagem_outros = 0

stdout_queue = queue.Queue()
stderr_queue = queue.Queue()

def formatar_tempo(segundos):
    if segundos < 60:
        return f"{int(segundos)}s"
    minutos, segundos = divmod(int(segundos), 60)
    if minutos < 60:
        return f"{minutos}m {segundos}s"
    horas, minutos = divmod(minutos, 60)
    return f"{horas}h {minutos}m {segundos}s"

def formatar_numero(numero):
    return f"{numero:,}".replace(',', '.')

def calcular_velocidade():
    global velocidade_processamento, tempo_estimado
    if registros_processados > 0 and start_time:
        tempo_decorrido = time.time() - start_time
        velocidade_processamento = registros_processados / tempo_decorrido
        if velocidade_processamento > 0 and total_registros > 0:
            registros_restantes = total_registros - registros_processados
            tempo_estimado = registros_restantes / velocidade_processamento

def atualizar_progresso_real():
    global registros_processados, total_registros
    if total_registros > 0:
        porcentagem = (registros_processados / total_registros) * 100
        if porcentagem > 100:
            porcentagem = 100  # travar em 100%
        progresso_real.config(value=porcentagem)
        progresso_label.config(text=f"{porcentagem:.1f}% - {formatar_numero(registros_processados)}/{formatar_numero(total_registros)} registros")
    else:
        # Quando não temos total, mostrar apenas os processados
        progresso_real.config(value=0)
        if registros_processados > 0:
            progresso_label.config(text=f"Processados: {formatar_numero(registros_processados)} registros")
        else:
            progresso_label.config(text="Aguardando dados...")

def atualizar_estatisticas():
    if processo_em_execucao and processo_em_execucao.poll() is None:
        tempo_decorrido = time.time() - start_time
        calcular_velocidade()
        status_text = f"⏱ Tempo: {formatar_tempo(tempo_decorrido)}"
        if velocidade_processamento > 0:
            status_text += f" | 🚀 Velocidade: {velocidade_processamento:.1f} reg/s"
        if tempo_estimado > 0:
            status_text += f" | ⏳ Restante: {formatar_tempo(tempo_estimado)}"
        if entidade_ativa:
            status_text += f" | 📋 Entidade: {entidade_ativa}"
        status_var.set(status_text)
        janela.after(1000, atualizar_estatisticas)

def extrair_progresso_do_log(linha):
    global total_registros, registros_processados, entidade_ativa
    global contagem_ok, contagem_divergente, contagem_outros

    # Remover códigos de cor ANSI para processar o texto limpo
    linha_limpa = re.sub(r'\x1b\[[0-9;]*m', '', linha)
    
    # Padrões para capturar o nome da entidade de forma mais robusta
    padroes_entidade = [
        r"Exportado para Google Sheets na aba '([^']+)'",
        r"Validando entidade[:\s]+([^\s\n]+)",
        r"Processando entidade[:\s]+([^\s\n]+)",
        r"Entidade[:\s]+([^\s\n]+)",
        r"Aba[:\s]+([^\s\n]+)",
        r"Tabela[:\s]+([^\s\n]+)"
    ]
    
    for padrao in padroes_entidade:
        if match := re.search(padrao, linha_limpa, re.IGNORECASE):
            entidade_ativa = match.group(1).strip()
            break

    # Padrões específicos para os logs do sistema baseado no exemplo
    # Detecta registros retornados das queries
    if match := re.search(r'Query\s+\w+\s+executada\s+-\s+(\d+)\s+registros\s+retornados', linha_limpa, re.IGNORECASE):
        registros_encontrados = int(match.group(1))
        # Use o maior número encontrado como total (normalmente o SQL Server tem mais registros)
        if registros_encontrados > total_registros:
            total_registros = registros_encontrados
            progresso_real.config(maximum=100)
        return True

    # Detecta início da validação
    if re.search(r'Iniciando validação central', linha_limpa, re.IGNORECASE):
        progresso_label.config(text="Iniciando validação central...")
        return True
    
    # Detecta validação para tabela única
    if re.search(r'Executando validação para uma única tabela', linha_limpa, re.IGNORECASE):
        progresso_label.config(text="Executando validação...")
        return True

    # Padrões para capturar total de registros (mais genérico)
    padroes_total = [
        r'Total de registros[:\s]+(\d+)',
        r'Total[:\s]+(\d+)\s+registros',
        r'(\d+)\s+registros encontrados',
        r'Encontrados\s+(\d+)\s+registros',
        r'Carregados\s+(\d+)\s+registros'
    ]
    
    for padrao in padroes_total:
        if match := re.search(padrao, linha_limpa, re.IGNORECASE):
            total_registros = int(match.group(1))
            progresso_real.config(maximum=100)
            return True

    # Padrões para capturar progresso (mais abrangente)
    padroes_progresso = [
        r'Processando registro (\d+)',
        r'Processados (\d+) registros',
        r'Validando (\d+)/(\d+)',
        r'Registro (\d+)/',
        r'(\d+)/(\d+) processados',
        r'Exportando registro (\d+)',
        r'Salvando registro (\d+)',
        r'Concluído (\d+) de (\d+)',
        r'Validação.*?(\d+)/(\d+)',
        r'Progresso.*?(\d+)/(\d+)'
    ]
    
    for padrao in padroes_progresso:
        if match := re.search(padrao, linha_limpa, re.IGNORECASE):
            grupos = match.groups()
            if len(grupos) == 2:  # Formato "atual/total"
                registros_processados = int(grupos[0])
                if total_registros == 0:
                    total_registros = int(grupos[1])
            else:  # Formato "atual"
                registros_processados = int(grupos[0])
            atualizar_progresso_real()
            return True

    # Padrões para detectar diferentes fases do processo
    fases = [
        r'INICIANDO EXECUÇÃO DO PIPELINE',
        r'SISTEMA DE VALIDACAO DE DADOS - INICIADO',
        r'Iniciando\s+(validação|exportação|processamento)',
        r'Finalizando\s+(validação|exportação|processamento)',
        r'Conectando\s+ao\s+banco',
        r'Preparando\s+dados',
        r'Exportando\s+para\s+planilha',
        r'Salvando\s+resultados'
    ]
    
    for padrao in fases:
        if re.search(padrao, linha_limpa, re.IGNORECASE):
            return True

    # Contadores de status
    if match := re.search(r'Registros OK\s*[:\s]+(\d+)', linha_limpa):
        contagem_ok = int(match.group(1))
        return True

    if match := re.search(r'Divergencias\s*[:\s]+(\d+)', linha_limpa):
        contagem_divergente = int(match.group(1))
        return True

    if match := re.search(r'Erros\s*[:\s]+(\d+)', linha_limpa):
        contagem_outros = int(match.group(1))
        return True

    # Status individuais
    linha_lower = linha_limpa.lower()
    if "status: ok" in linha_lower:
        contagem_ok += 1
        return True
    elif "status: divergente" in linha_lower:
        contagem_divergente += 1
        return True
    elif "status:" in linha_lower:
        contagem_outros += 1
        return True

    return False

def resetar_progresso():
    global total_registros, registros_processados, velocidade_processamento, tempo_estimado
    global entidade_ativa, contagem_ok, contagem_divergente, contagem_outros

    total_registros = 0
    registros_processados = 0
    velocidade_processamento = 0
    tempo_estimado = 0
    entidade_ativa = None
    contagem_ok = 0
    contagem_divergente = 0
    contagem_outros = 0
    progresso_real.config(value=0)
    progresso_label.config(text="Aguardando início...")

def ler_output(pipe, queue_destino):
    for line in iter(lambda: pipe.readline(), ''):
        queue_destino.put(line)
    pipe.close()

def processar_output():
    progresso_atualizado = False
    
    while not stdout_queue.empty():
        linha = stdout_queue.get_nowait()
        if extrair_progresso_do_log(linha):
            progresso_atualizado = True
        timestamp = time.strftime('%H:%M:%S')
        log_text.insert(tk.END, f"[{timestamp}] {linha}")
        log_text.see(tk.END)

    while not stderr_queue.empty():
        linha = stderr_queue.get_nowait()
        timestamp = time.strftime('%H:%M:%S')
        log_text.insert(tk.END, f"[{timestamp}] ❌ {linha}")
        log_text.see(tk.END)

    if processo_em_execucao and processo_em_execucao.poll() is None:
        janela.after(100, processar_output)
    else:
        finalizar_execucao()

def finalizar_execucao():
    global processo_em_execucao, entidade_ativa
    tempo_total = time.time() - start_time
    tempo_formatado = formatar_tempo(tempo_total)
    
    # Atualizar progresso para 100% apenas se temos dados
    if total_registros > 0:
        progresso_real.config(value=100)
        progresso_label.config(text=f"100% - {formatar_numero(total_registros)} registros processados")
    else:
        progresso_real.config(value=0)
        progresso_label.config(text="Processo finalizado")
    
    status_text = f"✅ Concluído! Tempo: {tempo_formatado}"
    if total_registros > 0:
        status_text += f" | Registros: {formatar_numero(total_registros)}"
    if entidade_ativa:
        status_text += f" | Aba planilha: {entidade_ativa}"
    status_var.set(status_text)
    
    log_text.insert(tk.END, f"\n{'='*60}\n")
    log_text.insert(tk.END, f"✅ VALIDAÇÃO CONCLUÍDA!\n")
    log_text.insert(tk.END, f"⏱ Tempo total: {tempo_formatado}\n")
    if total_registros > 0:
        log_text.insert(tk.END, f"📊 Registros processados: {formatar_numero(total_registros)}\n")
    if entidade_ativa:
        log_text.insert(tk.END, f"📋 Entidade processada: {entidade_ativa}\n")
    log_text.insert(tk.END, f"📄 Planilha: https://docs.google.com/spreadsheets/d/16u8KSsLNidNtlZ4daX2Vs-GALqgM7OvESHRpGB9VUI8\n")
    log_text.insert(tk.END, f"{'='*60}\n")
    log_text.see(tk.END)
    
    btn_executar.config(text="Executar", state="normal")
    btn_limpar.config(state="normal")
    btn_parar.config(state="disabled")
    combo_pipeline.config(state="readonly")
    combo_regra.config(state="readonly")
    processo_em_execucao = None

def executar_pipeline():
    global processo_em_execucao, start_time, stdout_queue, stderr_queue

    pipeline = combo_pipeline.get()
    regra = combo_regra.get()

    if not pipeline or not regra:
        messagebox.showwarning("Aviso", "Selecione o pipeline e o arquivo de regras.")
        return

    resetar_progresso()
    btn_executar.config(text="Executando...", state="disabled")
    btn_limpar.config(state="disabled")
    btn_parar.config(state="normal")
    combo_pipeline.config(state="disabled")
    combo_regra.config(state="disabled")

    frame_progresso.pack(fill="x", padx=10, pady=5, before=frame_log)
    status_var.set(f"Iniciando pipeline: {pipeline} com regras: {regra}")

    stdout_queue = queue.Queue()
    stderr_queue = queue.Queue()

    comando = f"python main.py {pipeline} {regra}"

    try:
        processo_em_execucao = subprocess.Popen(
            comando,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            text=True,
            bufsize=1,
            encoding="latin1"
        )
    except Exception as e:
        messagebox.showerror("Erro", f"Falha ao iniciar processo:\n{e}")
        btn_executar.config(text="Executar", state="normal")
        btn_limpar.config(state="normal")
        btn_parar.config(state="disabled")
        combo_pipeline.config(state="readonly")
        combo_regra.config(state="readonly")
        return

    start_time = time.time()
    threading.Thread(target=ler_output, args=(processo_em_execucao.stdout, stdout_queue), daemon=True).start()
    threading.Thread(target=ler_output, args=(processo_em_execucao.stderr, stderr_queue), daemon=True).start()

    atualizar_estatisticas()
    processar_output()

def limpar_log():
    log_text.delete(1.0, tk.END)
    resetar_progresso()
    frame_progresso.pack_forget()
    status_var.set("Log limpo. Pronto.")

def parar_execucao():
    global processo_em_execucao
    if processo_em_execucao and processo_em_execucao.poll() is None:
        processo_em_execucao.terminate()
        try:
            processo_em_execucao.wait(timeout=3)
        except subprocess.TimeoutExpired:
            processo_em_execucao.kill()
        tempo_decorrido = time.time() - start_time if start_time else 0
        status_var.set(f"🛑 Interrompido após {formatar_tempo(tempo_decorrido)}")
        processo_em_execucao = None
        btn_executar.config(text="Executar", state="normal")
        btn_limpar.config(state="normal")
        btn_parar.config(state="disabled")
        combo_pipeline.config(state="readonly")
        combo_regra.config(state="readonly")

def mostrar_grafico_resumo():
    total = contagem_ok + contagem_divergente + contagem_outros
    if total == 0:
        messagebox.showinfo("Gráfico", "Nenhum dado processado para gerar gráfico.")
        return

    labels = ['OK', 'Divergentes', 'Outros']
    valores = [contagem_ok, contagem_divergente, contagem_outros]
    cores = ['#4caf50', '#ff9800', '#9e9e9e']

    plt.figure(figsize=(8, 6))
    bars = plt.bar(labels, valores, color=cores)
    
    # Título mais específico com nome da entidade
    titulo = f"Resumo da Validação"
    if entidade_ativa:
        titulo += f" - {entidade_ativa}"
    plt.title(titulo, fontsize=14, fontweight='bold')
    
    plt.ylabel("Quantidade", fontsize=12)
    plt.xlabel("Status", fontsize=12)
    
    # Adicionar valores nas barras
    for bar, valor in zip(bars, valores):
        if valor > 0:
            plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(valores) * 0.01, 
                    str(valor), ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    # Adicionar informações extras
    plt.figtext(0.02, 0.02, f"Total processado: {formatar_numero(total)} registros", 
                fontsize=10, style='italic')
    
    plt.tight_layout()
    plt.grid(axis='y', alpha=0.3)
    plt.show()

# --- Interface gráfica ---

janela = tk.Tk()
janela.title("Validador de Dados")
janela.geometry("1100x650")

try:
    janela.iconbitmap('assets/icone.ico')
except Exception:
    pass

style = ttk.Style(janela)
style.theme_use('clam')
style.configure("real.Horizontal.TProgressbar", troughcolor='#e0e0e0', background='#4caf50', thickness=20)
style.configure('TButton', font=('Segoe UI', 10), padding=6)
style.configure('TLabel', font=('Segoe UI', 10))
style.configure('TCombobox', font=('Segoe UI', 10))
style.configure('Status.TLabel', relief='sunken', anchor='w', font=('Segoe UI', 9))

frame_topo = ttk.Frame(janela, padding=10)
frame_topo.pack(fill='x')

ttk.Label(frame_topo, text="Pipeline:", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky='w', padx=5)
combo_pipeline = ttk.Combobox(frame_topo, state="readonly", width=20)
combo_pipeline.grid(row=0, column=1, sticky='w', padx=5)

ttk.Label(frame_topo, text="Regras:", font=("Segoe UI", 10, "bold")).grid(row=0, column=2, sticky='w', padx=5)
combo_regra = ttk.Combobox(frame_topo, state="readonly", width=45)
combo_regra.grid(row=0, column=3, sticky='w', padx=5)

btn_executar = ttk.Button(frame_topo, text="▶ Executar", command=executar_pipeline, width=12)
btn_executar.grid(row=0, column=4, padx=5)

btn_parar = ttk.Button(frame_topo, text="⏸ Parar", command=parar_execucao, state="disabled", width=12)
btn_parar.grid(row=0, column=5, padx=5)

btn_limpar = ttk.Button(frame_topo, text="🗑 Limpar", command=limpar_log, width=12)
btn_limpar.grid(row=0, column=6, padx=5)

btn_grafico = ttk.Button(frame_topo, text="📊 Exibir Gráfico", command=mostrar_grafico_resumo, width=14)
btn_grafico.grid(row=0, column=7, padx=5)

# Frame de progresso 
frame_progresso = ttk.Frame(janela, padding=10)
ttk.Label(frame_progresso, text="Progresso do processo:", font=("Segoe UI", 10, "bold")).pack(anchor='w')
progresso_label = ttk.Label(frame_progresso, text="Aguardando início...", font=("Segoe UI", 9))
progresso_label.pack(anchor='w', pady=(0, 5))
progresso_real = ttk.Progressbar(frame_progresso, mode='determinate', style="real.Horizontal.TProgressbar")
progresso_real.pack(fill='x', pady=(0, 5))

frame_log = ttk.Frame(janela, padding=10)
frame_log.pack(fill='both', expand=True)
scrollbar = ttk.Scrollbar(frame_log)
scrollbar.pack(side='right', fill='y')

log_text = tk.Text(frame_log, wrap='word', yscrollcommand=scrollbar.set,
                   font=('Consolas', 10), bg='#f8f9fa', fg='#333333')
log_text.pack(fill='both', expand=True)
scrollbar.config(command=log_text.yview)

status_var = tk.StringVar(value="Pronto - Selecione pipeline e regras para começar")
status_bar = ttk.Label(janela, textvariable=status_var, style='Status.TLabel')
status_bar.pack(fill='x', side='bottom', ipady=5)

# Popular os combos
regras = [f for f in os.listdir(RULES_DIR) if f.endswith(".json")]
combo_regra['values'] = regras
if regras:
    combo_regra.current(0)

prefixos = ("firebird", "mysql", "sql")
pipelines = [f.replace(".py", "") for f in os.listdir(PIPELINE_DIR) if f.endswith(".py") and f.startswith(prefixos)]
combo_pipeline['values'] = pipelines
if pipelines:
    combo_pipeline.current(0)

# Instruções iniciais
log_text.insert(tk.END, "🚀 VALIDADOR DE DADOS\n" + "="*50 + "\n\n")
log_text.insert(tk.END, "💡 INSTRUÇÕES:\n1. Selecione o pipeline e arquivo de regras\n2. Clique em 'Executar'\n3. Acompanhe o progresso\n\n")

# Start
combo_pipeline.focus()
janela.mainloop()