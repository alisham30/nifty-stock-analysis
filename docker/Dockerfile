FROM jupyter/pyspark-notebook:latest

# Set working directory inside container
WORKDIR /home/jovyan/work

# Copy project into container
COPY . /home/jovyan/work

FROM python:3.11-slim

WORKDIR /app

# Install Java (required by Spark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy dependencies
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your app code
COPY . .

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit
CMD ["streamlit", "run", "app.py", "--server.port=$PORT", "--server.address=0.0.0.0"]
