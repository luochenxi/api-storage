FROM enkito/base:sp500

ENV LC_ALL=C.UTF-8
ENV PYTHONUNBUFFERED 1

COPY . /usr/src/app
WORKDIR /usr/src/app


CMD ["python3", "/usr/src/app/app.py"]