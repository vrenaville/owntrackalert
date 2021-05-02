FROM python:3.8
WORKDIR /usr/src/retriveot
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD [ "python", "./retriveot.py" ]
