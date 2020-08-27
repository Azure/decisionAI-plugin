FROM python:3.6

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . /usr/src/app/decisionAI-plugin
WORKDIR /usr/src/app/decisionAI-plugin

EXPOSE 56789

ENTRYPOINT ["gunicorn","-c","gunicorn_config.py","sample.demo_modeless.run_server:app"]