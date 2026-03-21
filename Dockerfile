FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
#CMD ["python", "-c", "print('FoodExpress is ready!')"]
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]