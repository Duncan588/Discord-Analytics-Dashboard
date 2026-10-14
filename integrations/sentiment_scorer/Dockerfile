FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENV HF_HOME=/hf PYTHONUNBUFFERED=1
# Bake the model into the image so restarts don't re-download
RUN python -c "from transformers import AutoTokenizer as T,AutoModelForSequenceClassification as M;m='cardiffnlp/twitter-xlm-roberta-base-sentiment';T.from_pretrained(m);M.from_pretrained(m)"
COPY scorer.py .
CMD ["python", "scorer.py"]
