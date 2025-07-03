import os
import sys
from PySide6.QtWidgets import (
    QApplication, QWidget, QLabel, QComboBox, QPushButton,
    QTextEdit, QVBoxLayout, QHBoxLayout, QStatusBar, QMessageBox
)
from PySide6.QtCore import QProcess, Qt

PIPELINE_DIR = "core/pipelines"
RULES_DIR = "rules"
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/16u8KSsLNidNtlZ4daX2Vs-GALqgM7OvESHRpGB9VUI8"

class ValidadorInterface(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Validador de Dados")
        self.setMinimumSize(850, 560)

        self.process = QProcess(self)
        self.process.readyReadStandardOutput.connect(self.update_log)
        self.process.readyReadStandardError.connect(self.update_log)
        self.process.finished.connect(self.finalizar_execucao)

        self.setup_ui()

    def setup_ui(self):
        # Combos
        self.label_pipeline = QLabel("Pipeline:")
        self.combo_pipeline = QComboBox()
        self.combo_pipeline.addItems(self.listar_pipelines())

        self.label_regra = QLabel("Regras:")
        self.combo_regra = QComboBox()
        self.combo_regra.addItems(self.listar_regras())

        # Botões
        self.btn_executar = QPushButton("Executar")
        self.btn_parar = QPushButton("🛑 Parar")
        self.btn_limpar = QPushButton("Limpar Log")

        self.btn_parar.setEnabled(False)

        # Log
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setStyleSheet("font-family: Consolas, monospace; font-size: 11pt;")

        # Status bar
        self.status = QStatusBar()
        self.status.showMessage("Pronto")

        # Layout topo
        topo = QHBoxLayout()
        topo.addWidget(self.label_pipeline)
        topo.addWidget(self.combo_pipeline)
        topo.addWidget(self.label_regra)
        topo.addWidget(self.combo_regra)
        topo.addWidget(self.btn_executar)
        topo.addWidget(self.btn_parar)
        topo.addWidget(self.btn_limpar)

        # Layout principal
        layout = QVBoxLayout()
        layout.addLayout(topo)
        layout.addWidget(self.log)
        layout.addWidget(self.status)

        self.setLayout(layout)

        # Conexões
        self.btn_executar.clicked.connect(self.executar_pipeline)
        self.btn_parar.clicked.connect(self.parar_execucao)
        self.btn_limpar.clicked.connect(self.limpar_log)

    def listar_pipelines(self):
        prefixos = ("firebird", "mysql", "sql")
        return [f.replace(".py", "") for f in os.listdir(PIPELINE_DIR)
                if f.endswith(".py") and f.startswith(prefixos)]

    def listar_regras(self):
        return [f for f in os.listdir(RULES_DIR) if f.endswith(".json")]

    def executar_pipeline(self):
        pipeline = self.combo_pipeline.currentText()
        regra = self.combo_regra.currentText()

        if not pipeline or not regra:
            QMessageBox.warning(self, "Aviso", "Selecione o pipeline e o arquivo de regras.")
            return

        comando = f"python main.py {pipeline} {regra}"

        self.log.append(f"\n▶ Iniciando validação com pipeline: {pipeline}")
        self.log.append(f"📄 Usando regras: {regra}\n")
        self.status.showMessage("Executando...")

        self.combo_pipeline.setEnabled(False)
        self.combo_regra.setEnabled(False)
        self.btn_executar.setEnabled(False)
        self.btn_parar.setEnabled(True)
        self.btn_limpar.setEnabled(False)

        self.process.start(comando)

    def update_log(self):
        output = self.process.readAllStandardOutput().data().decode()
        if output:
            self.log.append(output.strip())
            self.log.verticalScrollBar().setValue(self.log.verticalScrollBar().maximum())

        error = self.process.readAllStandardError().data().decode()
        if error:
            self.log.append(f"❌ {error.strip()}")

    def finalizar_execucao(self):
        self.status.showMessage("Validação concluída.")
        self.log.append("\n✅ Validação concluída!")
        self.log.append(f"📄 Planilha: {GOOGLE_SHEET_URL}")

        self.combo_pipeline.setEnabled(True)
        self.combo_regra.setEnabled(True)
        self.btn_executar.setEnabled(True)
        self.btn_parar.setEnabled(False)
        self.btn_limpar.setEnabled(True)

    def parar_execucao(self):
        if self.process.state() == QProcess.Running:
            self.process.kill()
            self.status.showMessage("🛑 Execução interrompida.")
            self.log.append("🛑 Execução interrompida pelo usuário.")
            self.combo_pipeline.setEnabled(True)
            self.combo_regra.setEnabled(True)
            self.btn_executar.setEnabled(True)
            self.btn_parar.setEnabled(False)
            self.btn_limpar.setEnabled(True)

    def limpar_log(self):
        self.log.clear()
        self.status.showMessage("Log limpo. Pronto.")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    janela = ValidadorInterface()
    janela.show()
    sys.exit(app.exec())
