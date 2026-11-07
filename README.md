# FlowForge (`flowforge`)

Motor de orquestração de fluxos em etapas (DAGs) e fila de tarefas com prioridades, retentativas configuráveis, *Dead-Letter Queue* (DLQ), agendamento determinístico, trilha de auditoria e API HTTP com **FastAPI + Pydantic + SQLite**.

## Visão Geral

O **FlowForge** permite registrar pipelines declarativos compostos por etapas encadeadas, enfileirar execuções por prioridade (`low`, `normal`, `high`, `critical`), processar tarefas via workers com *backoff* (`fixed`, `linear`, `exponential`), reprocessar falhas via DLQ, agendar disparos únicos ou recorrentes e consultar métricas e eventos de auditoria tanto via **CLI JSON** quanto via **API REST**.

### Estrutura do Projeto

- `flowforge/domain/` — Modelos Pydantic v2, enumerações e máquinas de estado finitas (`RunStatus`, `StepRunStatus`, `QueueItemStatus`).
- `flowforge/storage/` — Gerenciador SQLite (`WAL` + *foreign keys*) e repositórios isolados para workflows, runs, filas, DLQ, agendamentos e auditoria.
- `flowforge/handlers/` — Registro extensível de *handlers* de etapas (projeção/mapeamento de dados, filtros, validação de schema, agregação numérica, deduplicação, templates, *gates* de controle e artefatos JSON em *sandbox*).
- `flowforge/engine/` — Planejador topológico de DAG (detecção de ciclos e ondas de execução), cálculo de *backoff*, executor de etapas com reaproveitamento de etapas já concluídas em retentativas, worker de fila, agendador e telemetria.
- `flowforge/services/` — Fachada unificada (`OrchestratorService`) compartilhada entre CLI e API HTTP.
- `flowforge/api/` — Aplicação FastAPI modular (`/workflows`, `/runs`, `/queue/stats`, `/worker/tick`, `/dlq`, `/schedules`, `/audit`, `/metrics`, `/health`).
- `flowforge/cli/` — Interface de linha de comando com respostas estruturadas em JSON.
- `tests/` — Suíte `pytest` cobrindo validação de DAG, *handlers*, retentativas, DLQ, fila de prioridade, agendador, API FastAPI e CLI.

---

## Configuração Rápida (Windows PowerShell)

```powershell
cd C:\Projetos\flowforge

python -m venv .venv
.\.venv\Scripts\Activate.ps1

python -m pip install -r requirements-dev.txt
```

### Rodando Linter e Testes

```powershell
ruff check .
pytest -v --cov=flowforge --cov-report=term-missing
```

*(Os testes criam bancos SQLite e diretórios de artefatos isolados em pastas temporárias via `tmp_path`, mantendo a árvore do projeto limpa.)*

---

## Exemplos de Uso

### 1. Via CLI (com banco isolado temporário)

```powershell
$TempDir = Join-Path ([System.IO.Path]::GetTempPath()) "flowforge-demo"
$DbFile = Join-Path $TempDir "demo.db"

# Listar handlers disponíveis
python -m flowforge.cli.main --db-path $DbFile handlers

# Criar um workflow em etapas via stdin ('-')
'{"name":"sales-summary","steps":[{"key":"clean","name":"Deduplicate","handler":"aggregate.deduplicate","config":{"key_field":"id"}},{"key":"stats","name":"Summarize","handler":"aggregate.numeric_summary","depends_on":["clean"],"config":{"field":"total"}}]}' | python -m flowforge.cli.main --db-path $DbFile workflow-create -

# Disparar execução com prioridade alta
'{"records":[{"id":1,"total":100},{"id":1,"total":100},{"id":2,"total":250}]}' | python -m flowforge.cli.main --db-path $DbFile run-trigger sales-summary --priority high --inputs -

# Processar a fila e consultar métricas
python -m flowforge.cli.main --db-path $DbFile worker-tick
python -m flowforge.cli.main --db-path $DbFile metrics
```

### 2. Via Servidor HTTP (FastAPI)

```powershell
python -m flowforge.cli.main serve --host 127.0.0.1 --port 8000
```

Documentação interativa OpenAPI disponível em `http://127.0.0.1:8000/docs`.

---

## Variáveis de Ambiente

| Variável | Padrão | Descrição |
| :--- | :--- | :--- |
| `FLOWFORGE_DB_PATH` | `.flowforge/flowforge.db` | Caminho do arquivo SQLite |
| `FLOWFORGE_ARTIFACT_DIR` | `.flowforge/artifacts` | Pasta *sandbox* para leitura/escrita de artefatos JSON |
| `FLOWFORGE_DEFAULT_QUEUE` | `default` | Nome padrão da fila de tarefas |
| `FLOWFORGE_WORKER_BATCH_SIZE` | `10` | Limite padrão de tarefas por ciclo do worker |
