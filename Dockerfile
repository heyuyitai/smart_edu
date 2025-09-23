FROM python:3.12-slim

WORKDIR /app

RUN apt-get install -y adduser

# 创建用户并设置目录权限
RUN adduser --disabled-password --gecos '' zyt && \
    mkdir -p /app && \
    chown -R zyt:zyt /app

COPY --chown=zyt:zyt requirements.txt .
RUN ls -l requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir src
#COPY --chown=zyt:zyt . .
COPY --chown=zyt:zyt src ./src
COPY .env ./src

EXPOSE 5555

CMD ["python","src/app.py"]