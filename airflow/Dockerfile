FROM astrocrpublic.azurecr.io/runtime:3.1-14

# 1. Troca para ROOT para ter permissão de criar pastas no sistema
USER root

ENV DBT_VENV_PATH=/usr/local/pyenv/dbt-venv

# 2. Cria a pasta e o venv com permissões totais
RUN mkdir -p /usr/local/pyenv && \
    python -m venv $DBT_VENV_PATH && \
    $DBT_VENV_PATH/bin/pip install --no-cache-dir dbt-bigquery dbt-core astronomer-cosmos && \
    chown -R astro:0 /usr/local/pyenv

# 3. VOLTA PARA O USUÁRIO ASTRO (Obrigatório para o Airflow rodar)
USER astro