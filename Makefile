.PHONY: local-up local-down test cost-report helm-template tf-fmt lint format

local-up:
	docker compose up --build

local-down:
	docker compose down --volumes

test:
	python -m compileall services scripts tests
	pytest tests/integration

lint:
	ruff check .

format:
	ruff format .

cost-report:
	python scripts/cost_compare.py --output reports/cost-comparison.md

helm-template:
	helm dependency build deploy/helm/event-platform
	helm template event-platform deploy/helm/event-platform -f deploy/environments/local/values.yaml

tf-fmt:
	terraform -chdir=infra/terraform/aws fmt -recursive
	terraform -chdir=infra/terraform/bootstrap fmt -recursive
