import tkinter as tk
from tkinter import ttk, messagebox
import subprocess
import os
import threading
import time

PIPELINE_DIR = "core/pipelines"
RULES_DIR = "rules"

processo_em_execucao = None
start_time = None

def formatar_tempo(segundos):
    minutos, segundos = divmod(int(segundos), 60)
    return f"{minutos:02d}:{segundos:02d}"

def atualizar_tempo():
    if processo_em_execucao and processo_em_execucao.poll() is None:
        tempo_decorrido = time.time() - start_time
        status_var.set(f"⏱ Tempo decorrido: {formatar_tempo(tempo_decorrido)}")
        janela.after(1000, atualizar_tempo)

def executar_pipeline():
    global processo_em_execucao, start_time

    pipeline = combo_pipeline.get()
    regra = combo_regra.get()

    if not pipeline or not regra:
        messagebox.showwarning("Aviso", "Selecione o pipeline e o arquivo de regras.")
        return

    btn_executar.config(text="Executando...", state="disabled")
    btn_limpar.config(state="disabled")
    btn_parar.config(state="normal")
    combo_pipeline.config(state="disabled")
    combo_regra.config(state="disabled")
    status_var.set(f"Executando pipeline: {pipeline} com regras: {regra}")
    progresso.start(10)
    progresso.pack(fill="x", side="bottom", padx=10, pady=(0,10))

    def run():
        global processo_em_execucao, start_time

        try:
            log_text.insert(tk.END, f"\n▶ Iniciando validação com pipeline: {pipeline}\n")
            log_text.insert(tk.END, f"📄 Usando regras: {regra}\n\n")
            log_text.see(tk.END)

            comando = f"python main.py {pipeline} {regra}"
            start_time = time.time()
            processo_em_execucao = subprocess.Popen(
                comando, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True
            )

            atualizar_tempo()

            while True:
                out_line = processo_em_execucao.stdout.readline()
                err_line = processo_em_execucao.stderr.readline()

                if out_line:
                    log_text.insert(tk.END, out_line)
                    log_text.see(tk.END)

                if err_line:
                    log_text.insert(tk.END, f"❌ {err_line}")
                    log_text.see(tk.END)

                if out_line == '' and err_line == '' and processo_em_execucao.poll() is not None:
                    break

            tempo_total = time.time() - start_time
            tempo_formatado = formatar_tempo(tempo_total)

            log_text.insert(tk.END, "\n✅ Validação concluída!\n")
            log_text.insert(tk.END, f"⏱ Tempo total: {tempo_formatado}\n")
            log_text.insert(tk.END, "📄 Planilha: https://docs.google.com/spreadsheets/d/16u8KSsLNidNtlZ4daX2Vs-GALqgM7OvESHRpGB9VUI8\n")
            log_text.see(tk.END)
            status_var.set(f"✅ Validação concluída! Tempo total: {tempo_formatado}")

        except Exception as e:
            log_text.insert(tk.END, f"\n❌ Erro durante a execução: {e}\n")
            status_var.set("❌ Erro na execução.")

        finally:
            progresso.stop()
            progresso.pack_forget()
            processo_em_execucao = None
            btn_executar.config(text="Executar", state="normal")
            btn_limpar.config(state="normal")
            btn_parar.config(state="disabled")
            combo_pipeline.config(state="readonly")
            combo_regra.config(state="readonly")
            if not status_var.get().startswith("✅ Validação concluída"):
                status_var.set("Pronto")

    threading.Thread(target=run, daemon=True).start()

def limpar_log():
    log_text.delete(1.0, tk.END)
    status_var.set("Log limpo. Pronto.")

def parar_execucao():
    global processo_em_execucao
    if processo_em_execucao and processo_em_execucao.poll() is None:
        processo_em_execucao.terminate()
        try:
            processo_em_execucao.wait(timeout=3)
        except subprocess.TimeoutExpired:
            processo_em_execucao.kill()
        progresso.stop()
        progresso.pack_forget()
        status_var.set("🛑 Execução interrompida.")
        log_text.insert(tk.END, "\n🛑 Execução interrompida pelo usuário.\n")
        log_text.see(tk.END)
        processo_em_execucao = None
        btn_executar.config(text="Executar", state="normal")
        btn_limpar.config(state="normal")
        btn_parar.config(state="disabled")
        combo_pipeline.config(state="readonly")
        combo_regra.config(state="readonly")

# Atalhos com retorno break para evitar conflito
def atalho_executar(event=None):
    if btn_executar['state'] == 'normal':
        executar_pipeline()
    return "break"

def atalho_parar(event=None):
    if btn_parar['state'] == 'normal':
        parar_execucao()
    return "break"

def atalho_limpar(event=None):
    limpar_log()
    return "break"

# Interface
janela = tk.Tk()
janela.title("Validador de Dados")
janela.geometry("900x600")
janela.resizable(True, True)

# Ícone (opcional)
try:
    janela.iconbitmap('assets/icone.ico')
except Exception:
    pass

# Estilo ttk customizado
style = ttk.Style(janela)
style.theme_use('clam')

style.configure("green.Horizontal.TProgressbar",
                troughcolor='#d9d9d9',
                background='#4caf50',
                thickness=20)

style.configure('TButton', font=('Segoe UI', 10), padding=6)
style.configure('TLabel', font=('Segoe UI', 10))
style.configure('TCombobox', font=('Segoe UI', 10))
style.configure('Status.TLabel', relief='sunken', anchor='w', font=('Segoe UI', 9, 'italic'))

frame_topo = ttk.Frame(janela, padding=10)
frame_topo.pack(fill='x')

ttk.Label(frame_topo, text="Pipeline:", font=("Segoe UI", 10)).grid(row=0, column=0, sticky='w', padx=5)
combo_pipeline = ttk.Combobox(frame_topo, state="readonly", width=20)
combo_pipeline.grid(row=0, column=1, sticky='w', padx=5)

ttk.Label(frame_topo, text="Regras:", font=("Segoe UI", 10)).grid(row=0, column=2, sticky='w', padx=5)
combo_regra = ttk.Combobox(frame_topo, state="readonly", width=45)
combo_regra.grid(row=0, column=3, sticky='w', padx=5)

btn_executar = ttk.Button(frame_topo, text="Executar", command=executar_pipeline, width=10)
btn_executar.grid(row=0, column=4, padx=10)
btn_parar = ttk.Button(frame_topo, text="🛑 Parar", command=parar_execucao, state="disabled", width=10)
btn_parar.grid(row=0, column=5, padx=10)
btn_limpar = ttk.Button(frame_topo, text="Limpar Log", command=limpar_log, width=10)
btn_limpar.grid(row=0, column=6, padx=10)

prefixos = ("firebird", "mysql", "sql")
pipelines = [f.replace(".py", "") for f in os.listdir(PIPELINE_DIR) if f.endswith(".py") and f.startswith(prefixos)]
combo_pipeline['values'] = pipelines
if pipelines:
    combo_pipeline.current(0)

regras = [f for f in os.listdir(RULES_DIR) if f.endswith(".json")]
combo_regra['values'] = regras
if regras:
    combo_regra.current(0)

frame_log = ttk.Frame(janela, padding=10)
frame_log.pack(fill='both', expand=True)

scrollbar = ttk.Scrollbar(frame_log)
scrollbar.pack(side='right', fill='y')

log_text = tk.Text(frame_log, wrap='word', yscrollcommand=scrollbar.set, font=('Consolas', 11))
log_text.pack(fill='both', expand=True)
scrollbar.config(command=log_text.yview)

status_var = tk.StringVar(value="Pronto")
status_bar = ttk.Label(janela, textvariable=status_var, style='Status.TLabel')
status_bar.pack(fill='x', side='bottom', ipady=3)

progresso = ttk.Progressbar(janela, mode='indeterminate', style="green.Horizontal.TProgressbar")

# Binds para garantir que atalhos funcionem sempre, mesmo com foco no Text
janela.bind_all('<Control-e>', atalho_executar)
janela.bind_all('<Control-p>', atalho_parar)
janela.bind_all('<Control-l>', atalho_limpar)

log_text.bind('<Control-e>', atalho_executar)
log_text.bind('<Control-p>', atalho_parar)
log_text.bind('<Control-l>', atalho_limpar)

combo_pipeline.focus()

janela.mainloop()
