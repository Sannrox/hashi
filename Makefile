.PHONY: all test test-integration update validate
all:
	cargo build --all-targets --locked
test:
	cargo test --locked
test-integration:
	cargo test --locked --all-features --tests
update:
	@for s in $$(ls scripts/update-*.sh 2>/dev/null | sort); do bash $$s; done
validate:
	cargo fmt --check
	cargo clippy --all-targets --all-features --locked -- -D warnings
	bash scripts/check-workflows.sh
	python3 -m unittest discover -s scripts/tests -t .
