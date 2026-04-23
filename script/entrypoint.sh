#!/bin/bash

# 1. Instala as bibliotecas extras que vamos usar (Pandas, BeautifulSoup, etc)
if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command -v pip) install --user -r /opt/airflow/requirements.txt
fi

# 2. Inicializa o banco de dados do Airflow (se for a primeira vez)
airflow db init

# 3. Cria o usuário de acesso à interface (Admin)
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin

# 4. Inicia o serviço que foi solicitado pelo docker-compose (Webserver ou Scheduler)
exec airflow db upgrade && airflow webserver & airflow scheduler