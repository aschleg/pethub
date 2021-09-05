# syntax = docker/dockerfile:1.2

FROM python:3.8

COPY . .

RUN pip3 install -r requirements.txt
RUN pip3 install wheel python-dotenv
RUN python setup.py sdist bdist_wheel
RUN pip3 install dist/pethub-0.0.1-py3-none-any.whl

CMD ["python", "pethub/petfinder/main.py"]
