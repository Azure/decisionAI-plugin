FROM python:3.7

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY sample/lr/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . /usr/src/app/decisionAI-plugin
WORKDIR /usr/src/app/decisionAI-plugin

EXPOSE 56789

ENTRYPOINT ["gunicorn","-c","gunicorn_config.py","sample.lr.run_server:app"]