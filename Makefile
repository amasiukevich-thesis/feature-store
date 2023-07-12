install:
	pip3 install --upgrade pip && pip3 install -r requirements.txt

test:
	python3 -m pytest -vv .

format:
	black .

lint:
	pylint --disable=R,C main.py
	pylint --disable=R,C ./src

all:
	install lint format test
