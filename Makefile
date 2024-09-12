# Makefile

.PHONY: run clean check

run:
	@clear
	@chmod +x clean.sh check.sh 2>/dev/null || true  # Ensure scripts are executable
	@./clean.sh || true                              # Run clean.sh, ignore errors if it doesn't exist
	@RUST_LOG=error cargo run                         # Run the cargo build with logging
	@./check.sh || true                              # Run check.sh, ignore errors if it doesn't exist

clean:
	@./clean.sh || true  # Alias for directly running clean.sh

check:
	@./check.sh || true  # Alias for directly running check.sh

