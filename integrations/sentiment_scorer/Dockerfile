FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENV HF_HOME=/hf PYTHONUNBUFFERED=1
# Bake the model files into the image (download only; loading here can hang on transformers' background
# safetensors-conversion check), then run fully offline.
RUN python -c "from huggingface_hub import snapshot_download as s;s('cardiffnlp/twitter-xlm-roberta-base-sentiment',allow_patterns=['*.json','*.bin','*.model','*.txt'])"
ENV HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1
COPY scorer.py .
CMD ["python", "scorer.py"]
