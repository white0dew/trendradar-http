FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py ./
COPY templates ./templates
COPY static ./static
COPY --from=docker:26.1.4-cli /usr/local/bin/docker /usr/local/bin/docker
EXPOSE 3333
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "3333"]
