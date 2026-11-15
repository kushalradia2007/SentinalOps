.PHONY: install test train api up down record audit
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
up:
	docker compose up -d
down:
	docker compose down -v
