FROM python:3.9
ENV PYTHONUNBUFFERED 1
WORKDIR /app
COPY docker/requirements.txt /app/requirements.txt
RUN pip install --trusted-host=pypi.org --trusted-host=files.pythonhosted.org -r requirements.txt
COPY . /app