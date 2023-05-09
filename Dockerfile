FROM python:3.11.3-bullseye

COPY /Scripts/ .
COPY /.aws .aws
COPY requirements.txt .

RUN mv /.aws ~

RUN apt-get update
RUN apt-get install g++ unixodbc-dev -y

RUN apt update && \
apt install -y curl apt-transport-https gnupg && \
curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/trusted.gpg.d/microsoft.gpg && \
echo "deb [arch=amd64] https://packages.microsoft.com/debian/11/prod bullseye main" | tee /etc/apt/sources.list.d/msprod.list && \
apt update
RUN ACCEPT_EULA=Y apt install -y --no-install-recommends msodbcsql17
RUN apt-get install nano -y

RUN pip install -r requirements.txt
CMD ["python", "main.py"]