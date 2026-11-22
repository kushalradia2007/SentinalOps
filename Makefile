.PHONY: install test train api up down record audit features
install:
	pip install -e ".[dev]"
test:
	pytest -q
train:
	python -m sentinalops.training.train_baseline
api:
	uvicorn sentinalops.serving.app:create_app --factory --reload
record:
	python -m sentinalops.data.recorder
audit:
	python -m sentinalops.data.audit_recording --hours 24
features:
	python -m sentinalops.data.pipeline
up:
	docker compose up -d
down:
	docker compose down -v
