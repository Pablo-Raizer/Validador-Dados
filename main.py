import importlib
import sys

def executar_pipeline(nome_pipeline, json_file):
    try:
        modulo = importlib.import_module(f"core.pipelines.{nome_pipeline}")
        modulo.executar(json_file)
    except Exception as e:
        print(f"[ERRO] Falha ao executar o pipeline '{nome_pipeline}': {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python main.py <nome_pipeline> <arquivo_de_regras.json>")
    else:
        nome_pipeline = sys.argv[1]
        json_file = sys.argv[2]
        executar_pipeline(nome_pipeline, json_file)
