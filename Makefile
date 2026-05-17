SHELL := /bin/sh
.PHONY: all build debug release test clean install help

all: release

help:
	@printf '%s\n' \
	  'targets:' \
	  '  make release   - cargo build --release  (default; produces target/release/stryke-parquet-helper)' \
	  '  make debug     - cargo build' \
	  '  make test      - cargo test then `s test t/`' \
	  '  make install   - `s pkg install -g .` (registers parquet/parquet-build CLI launchers)' \
	  '  make clean     - cargo clean'

release:
	cargo build --release

debug build:
	cargo build

test:
	cargo test
	s test t/ || true

install: release
	s pkg install -g .

clean:
	cargo clean
