FROM python:3.12-slim
WORKDIR /app
COPY audience_experience/api/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY live_monitor/__init__.py live_monitor/common.py live_monitor/storage.py /app/live_monitor/
COPY audience_experience/__init__.py /app/audience_experience/__init__.py
COPY audience_experience/api /app/audience_experience/api
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "audience_experience.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
