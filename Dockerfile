FROM python:3.8

COPY ./requirements.txt ./requirements.txt

RUN pip3 install --upgrade pip && pip3 install -r requirements.txt

WORKDIR /app

COPY    main.py     main.py
COPY    ./config/*  config/
COPY    ./src/*     src/

RUN mkdir raw_crypto_data

CMD ["python", "main.py"]
