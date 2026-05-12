# Versionamiento de Código con GitHub + DevOps

## Guía Completa para el Proyecto Big Data

---

## 1. Configuración Inicial de Git

### 1.1 Instalación de Git

```bash
# Windows (con Chocolatey)
choco install git

# Linux (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install git

# macOS (con Homebrew)
brew install git

# Verificar instalación
git --version
# Output: git version 2.40.0
```

### 1.2 Configuración Global

```bash
# Configurar identidad
git config --global user.name "Tu Nombre"
git config --global user.email "tu.email@ejemplo.com"

# Configurar editor por defecto
git config --global core.editor "code --wait"  # VS Code

# Configurar line endings (Windows)
git config --global core.autocrlf true

# Ver configuración
git config --list
```

---

## 2. Estructura del Repositorio

### 2.1 Inicialización del Proyecto

```bash
# Navegar al directorio del proyecto
cd AA3-BigData

# Inicializar repositorio Git
git init

# Verificar estado
git status
```

### 2.2 Estructura de Archivos

```
AA3-BigData/
├── .git/                      # Directorio de Git (auto-generado)
├── .gitignore                 # Archivos a ignorar
├── .github/                   # Configuración de GitHub
│   └── workflows/
│       └── ci.yml             # Pipeline CI/CD
├── README.md                  # Documentación principal
├── docker-compose.yml         # Configuración Docker
├── spark-apps/                # Código fuente Spark
│   ├── 01_spark_rdd.py
│   ├── 02_spark_dataframe.py
│   ├── 03_spark_sql.py
│   ├── 04_cargar_mongodb.py
│   ├── 05_batch_completo.py
│   ├── 06_kafka_producer.py
│   └── 07_spark_streaming.py
├── datos/                     # Datos de entrada
├── resultados/                # Outputs generados
├── scripts/                   # Scripts de automatización
└── docs/                      # Documentación
```

### 2.3 Archivo .gitignore

```gitignore
# Crear archivo .gitignore
# AA3-BigData/.gitignore

# Archivos de sistema
.DS_Store
Thumbs.db
desktop.ini

# Dependencias
node_modules/
__pycache__/
*.pyc
.env
.venv/
venv/

# IDEs
.idea/
.vscode/
*.swp
*.swo

# Logs
*.log
logs/

# Resultados generados (opcional - si son muy grandes)
# resultados/

# Archivos temporales
*.tmp
*.temp
.cache/

# Docker
*.tar

# Credenciales (NUNCA commitear)
secrets/
*.key
*.pem
credentials.json
```

---

## 3. Flujo de Trabajo Git

### 3.1 Comandos Básicos

```bash
# Ver estado del repositorio
git status

# Agregar archivos al staging
git add .                      # Todos los archivos
git add spark-apps/            # Directorio específico
git add docker-compose.yml     # Archivo específico

# Crear commit
git commit -m "Mensaje descriptivo del cambio"

# Ver historial
git log --oneline

# Ver diferencias
git diff                       # Cambios no staged
git diff --staged              # Cambios staged
```

### 3.2 Estrategia de Branches

```
┌─────────────────────────────────────────────────────────────────┐
│                    GITFLOW SIMPLIFICADO                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  main (producción)                                              │
│    │                                                            │
│    ├──────────────────────────────────────────────────▶        │
│    │                                                            │
│  develop (desarrollo)                                           │
│    │                                                            │
│    ├────┬────┬────┬───────────────────────────────────▶        │
│         │    │    │                                             │
│         │    │    └── feature/streaming-alertas                 │
│         │    │                                                  │
│         │    └── feature/batch-kpis                             │
│         │                                                       │
│         └── feature/kafka-producer                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 3.3 Trabajar con Branches

```bash
# Crear y cambiar a nueva branch
git checkout -b feature/batch-processing

# Listar branches
git branch -a

# Cambiar de branch
git checkout develop

# Merge de branch
git checkout develop
git merge feature/batch-processing

# Eliminar branch local
git branch -d feature/batch-processing

# Eliminar branch remoto
git push origin --delete feature/batch-processing
```

---

## 4. GitHub - Repositorio Remoto

### 4.1 Crear Repositorio en GitHub

1. Ir a [github.com](https://github.com)
2. Click en "New Repository"
3. Configurar:
   - **Name**: `AA3-BigData`
   - **Description**: `Plataforma de Analytics Big Data con Spark y Kafka`
   - **Visibility**: Private (o Public para proyectos educativos)
   - **Initialize**: NO (ya tenemos repo local)

### 4.2 Conectar Repositorio Local con GitHub

```bash
# Agregar remote origin
git remote add origin https://github.com/tu-usuario/AA3-BigData.git

# Verificar remotes
git remote -v

# Push inicial
git push -u origin main

# Push de todas las branches
git push --all origin
```

### 4.3 Sincronización

```bash
# Descargar cambios sin merge
git fetch origin

# Descargar y merge
git pull origin main

# Push de cambios
git push origin main

# Push de branch específica
git push origin feature/streaming-alertas
```

---

## 5. Convención de Commits

### 5.1 Formato de Mensaje

```
<tipo>(<alcance>): <descripción corta>

[cuerpo opcional]

[footer opcional]
```

### 5.2 Tipos de Commit

| Tipo | Descripción | Ejemplo |
|------|-------------|---------|
| `feat` | Nueva funcionalidad | `feat(batch): agregar procesamiento de KPIs` |
| `fix` | Corrección de bug | `fix(streaming): corregir conexión Kafka` |
| `docs` | Documentación | `docs: actualizar README con instrucciones` |
| `style` | Formato, sin cambio de lógica | `style: formatear código PySpark` |
| `refactor` | Refactorización | `refactor(batch): simplificar limpieza datos` |
| `test` | Tests | `test: agregar tests para batch processing` |
| `chore` | Tareas de mantenimiento | `chore: actualizar docker-compose` |

### 5.3 Ejemplos de Commits

```bash
# Commit simple
git commit -m "feat(batch): implementar lectura de archivos CSV, JSON y TXT"

# Commit con descripción detallada
git commit -m "feat(streaming): agregar detección de anomalías

- Implementar ventanas de 1 minuto
- Detectar actividad alta (>50 eventos)
- Detectar errores excesivos (>5/min)
- Detectar buffering alto (>5000ms)

Closes #15"

# Commit de fix
git commit -m "fix(kafka): corregir timeout de conexión al broker

El producer no se conectaba correctamente cuando Kafka
tardaba más de 5 segundos en iniciar. Aumentado timeout
a 30 segundos.

Fixes #23"
```

---

## 6. GitHub Actions - CI/CD

### 6.1 Workflow de Validación

```yaml
# .github/workflows/ci.yml

name: CI Pipeline

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  validate:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout código
      uses: actions/checkout@v4

    - name: Setup Python
      uses: actions/setup-python@v5
      with:
        python-version: '3.9'

    - name: Instalar dependencias
      run: |
        pip install pyspark==3.1.1
        pip install flake8 pylint

    - name: Lint código Python
      run: |
        flake8 spark-apps/ --max-line-length=120 --ignore=E501,W503
        
    - name: Verificar sintaxis PySpark
      run: |
        python -m py_compile spark-apps/*.py

  docker-build:
    runs-on: ubuntu-latest
    needs: validate
    
    steps:
    - name: Checkout código
      uses: actions/checkout@v4

    - name: Verificar docker-compose
      run: |
        docker-compose config

    - name: Build imágenes (dry-run)
      run: |
        docker-compose build --no-cache || true
```

### 6.2 Workflow de Deploy (Opcional)

```yaml
# .github/workflows/deploy.yml

name: Deploy Pipeline

on:
  push:
    branches: [ main ]
    tags:
      - 'v*'

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout código
      uses: actions/checkout@v4

    - name: Login Docker Hub
      uses: docker/login-action@v3
      with:
        username: ${{ secrets.DOCKER_USERNAME }}
        password: ${{ secrets.DOCKER_TOKEN }}

    - name: Build y Push imágenes
      run: |
        docker-compose build
        docker-compose push

    - name: Notificar deploy exitoso
      run: |
        echo "Deploy completado para tag: ${{ github.ref_name }}"
```

---

## 7. GitHub Features Adicionales

### 7.1 Issues y Project Board

```markdown
# Ejemplo de Issue

## Título: [FEATURE] Implementar detección de anomalías en streaming

### Descripción
Como analista de datos, necesito detectar comportamientos anómalos
en tiempo real para poder tomar acciones correctivas rápidamente.

### Criterios de Aceptación
- [ ] Detectar usuarios con más de 50 eventos por minuto
- [ ] Detectar usuarios con más de 5 errores por minuto
- [ ] Detectar buffering mayor a 5000ms
- [ ] Generar alertas con severidad

### Etiquetas
- enhancement
- streaming
- priority: high
```

### 7.2 Pull Request Template

```markdown
# .github/pull_request_template.md

## Descripción
<!-- Describe los cambios realizados -->

## Tipo de Cambio
- [ ] Bug fix
- [ ] Nueva feature
- [ ] Refactoring
- [ ] Documentación
- [ ] Otro: ___

## Checklist
- [ ] Mi código sigue las convenciones del proyecto
- [ ] He añadido documentación si es necesario
- [ ] He probado los cambios localmente
- [ ] Los tests pasan correctamente

## Screenshots (si aplica)
<!-- Agregar capturas de pantalla -->

## Issues Relacionados
Closes #
```

### 7.3 Releases y Tags

```bash
# Crear tag para release
git tag -a v1.0.0 -m "Release v1.0.0 - Procesamiento Batch y Streaming"

# Push del tag
git push origin v1.0.0

# Push de todos los tags
git push origin --tags

# Listar tags
git tag -l
```

---

## 8. Mejores Prácticas

### 8.1 Commits Atómicos

```bash
# ❌ MAL: Un commit gigante
git add .
git commit -m "Implementar todo el proyecto"

# ✅ BIEN: Commits pequeños y específicos
git add spark-apps/05_batch_completo.py
git commit -m "feat(batch): implementar lectura de archivos"

git add spark-apps/05_batch_completo.py
git commit -m "feat(batch): agregar limpieza de datos"

git add spark-apps/05_batch_completo.py
git commit -m "feat(batch): implementar generación de KPIs"
```

### 8.2 Branches Descriptivas

```bash
# ❌ MAL
git checkout -b fix1
git checkout -b new-stuff

# ✅ BIEN
git checkout -b feature/kafka-producer-events
git checkout -b fix/streaming-watermark-timeout
git checkout -b docs/actualizar-readme-instalacion
```

### 8.3 Code Review

```markdown
# Checklist para Code Review

## Funcionalidad
- [ ] El código cumple con los requisitos
- [ ] Edge cases están manejados
- [ ] No hay regresiones

## Calidad
- [ ] Código legible y bien documentado
- [ ] Sin código duplicado
- [ ] Nombres descriptivos

## Performance
- [ ] No hay operaciones innecesarias
- [ ] Queries optimizadas
- [ ] Uso eficiente de memoria

## Seguridad
- [ ] No hay credenciales hardcodeadas
- [ ] Input validado
- [ ] Sin vulnerabilidades conocidas
```

---

## 9. Integración con DevOps

### 9.1 Flujo DevOps Completo

```
┌─────────────────────────────────────────────────────────────────┐
│                      PIPELINE DEVOPS                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
│  │  CODE    │───▶│  BUILD   │───▶│  TEST    │───▶│  DEPLOY  │  │
│  │          │    │          │    │          │    │          │  │
│  │ Git Push │    │ Docker   │    │ Lint     │    │ Stage/   │  │
│  │ PR       │    │ Compose  │    │ Unit     │    │ Prod     │  │
│  │          │    │          │    │ E2E      │    │          │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘  │
│       │                                               │         │
│       │              FEEDBACK LOOP                    │         │
│       └───────────────────────────────────────────────┘         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 9.2 Ambientes

| Ambiente | Branch | Propósito |
|----------|--------|-----------|
| **Development** | `develop` | Desarrollo activo, tests frecuentes |
| **Staging** | `release/*` | Pre-producción, QA final |
| **Production** | `main` | Ambiente productivo, usuarios finales |

### 9.3 Secrets y Configuración

```bash
# GitHub Secrets (configurar en Settings > Secrets)
DOCKER_USERNAME     # Usuario Docker Hub
DOCKER_TOKEN        # Token de acceso Docker Hub
MONGODB_URI         # URI de MongoDB producción
KAFKA_BROKERS       # Brokers de Kafka producción

# Usar en workflows
${{ secrets.DOCKER_USERNAME }}
${{ secrets.MONGODB_URI }}
```

---

## 10. Comandos Útiles de Referencia

### 10.1 Git Básico

```bash
# Estado y diferencias
git status
git diff
git diff --staged
git log --oneline -10

# Staging y commits
git add <archivo>
git add .
git commit -m "mensaje"
git commit --amend  # Modificar último commit

# Branches
git branch
git checkout -b <nueva-branch>
git checkout <branch>
git merge <branch>
git branch -d <branch>

# Remotes
git remote -v
git fetch origin
git pull origin main
git push origin main
```

### 10.2 Git Avanzado

```bash
# Stash (guardar cambios temporalmente)
git stash
git stash pop
git stash list

# Reset (con cuidado)
git reset --soft HEAD~1   # Deshacer commit, mantener cambios
git reset --hard HEAD~1   # Deshacer commit y cambios (¡PELIGROSO!)

# Rebase (reorganizar commits)
git rebase develop
git rebase -i HEAD~3      # Rebase interactivo

# Cherry-pick (traer commit específico)
git cherry-pick <commit-hash>

# Bisect (encontrar commit problemático)
git bisect start
git bisect bad
git bisect good <commit-hash>
```

### 10.3 GitHub CLI (gh)

```bash
# Instalar GitHub CLI
# https://cli.github.com/

# Autenticarse
gh auth login

# Crear repositorio
gh repo create AA3-BigData --private

# Clonar repositorio
gh repo clone usuario/AA3-BigData

# Crear issue
gh issue create --title "Bug en streaming" --body "Descripción..."

# Crear PR
gh pr create --title "Feature: KPIs batch" --body "Implementación de KPIs"

# Ver PRs
gh pr list
gh pr view 1

# Merge PR
gh pr merge 1
```

---

## 11. Historial de Commits del Proyecto

### Ejemplo de Historial Real

```bash
$ git log --oneline

a1b2c3d (HEAD -> main) docs: crear guía de versionamiento
b2c3d4e docs: agregar documento de data storytelling
c3d4e5f docs: documentar modelo de datos ACID
d4e5f6g feat(streaming): implementar 07_spark_streaming.py
e5f6g7h feat(kafka): crear 06_kafka_producer.py
f6g7h8i feat(batch): implementar 05_batch_completo.py
g7h8i9j chore: agregar servicios Kafka a docker-compose
h8i9j0k feat(mongodb): crear script de carga inicial
i9j0k1l feat(sql): implementar consultas Spark SQL
j0k1l2m feat(dataframe): crear operaciones DataFrame
k1l2m3n feat(rdd): implementar procesamiento RDD básico
l2m3n4o chore: configuración inicial docker-compose
m3n4o5p docs: crear README principal
n4o5p6q init: proyecto inicial AA3-BigData
```

---

## Resumen

```
┌─────────────────────────────────────────────────────────────────┐
│                  VERSIONAMIENTO - CHECKLIST                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ✅ Git configurado correctamente                              │
│  ✅ .gitignore completo                                        │
│  ✅ Repositorio en GitHub                                      │
│  ✅ Estrategia de branches definida                            │
│  ✅ Convención de commits establecida                          │
│  ✅ GitHub Actions para CI/CD                                  │
│  ✅ Templates de PR e Issues                                   │
│  ✅ Releases y tags configurados                               │
│  ✅ Documentación de mejores prácticas                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

*Documento preparado para AA4 - Big Data CERTUS*
*Fecha: Mayo 2026*
