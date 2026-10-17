.PHONY: install test train api up down
install:
	pip install -e ".[dev]"
test:
	pytest -q
train:
	python -m sentinalops.training.train_baseline
api:
	uvicorn sentinalops.serving.app:app --reload
up:
	docker compose up -d
down:
	docker compose down -v
