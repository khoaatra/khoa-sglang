<!-- Copilot / AI agent instructions for contributors and automation -->
# SGLang — Quick AI Agent Guide

Purpose: give an AI coding agent the minimal, actionable context to be productive in this repository.

- **Big picture**
  - `sglang/` — main Python runtime and service code (schedulers, `srt/` layers, grpc/fapi entrypoints).
  - `sgl-kernel/` — native C/C++/CUDA kernels (CMake, `build.sh`, high-performance kernels used by `jit_kernel/`).
  - `sgl-model-gateway/` — gateway components (Rust/Cargo and release scripts) that integrate with the Python gRPC server.
  - `docs/`, `benchmark/`, `examples/` — runnable notebooks and performance tests; docs use Jupyter notebooks extensively.

- **Why these boundaries**
  - Performance-sensitive code is split into `sgl-kernel/` (native) and `jit_kernel/` (JIT kernels) to allow independent builds and targeted optimization.
  - Python `sglang/` contains orchestration, scheduler, and protocol code (gRPC/HTTP) so business logic stays in Python while heavy compute stays in native modules.

- **Key integration points** (search these files when changing runtime behavior)
  - `python/sglang/srt/entrypoints/grpc_server.py` — gRPC entrypoint and health/reflection hooks.
  - `python/sglang/srt/grpc/grpc_request_manager.py` — request wiring to scheduler.
  - `sgl-kernel/` — build and kernel sources; see `sgl-kernel/build.sh` and `CMakeLists.txt`.
  - `python/sglang/cli/` (entrypoint `sglang.cli.main:main`) — CLI used for launching servers and tools.

- **Developer workflows & common commands**
  - Install editable Python dev env: `pip install -e .[dev]` from repository root.
  - Build native kernels: `cd sgl-kernel && ./build.sh` (CMake-based, required for GPU/native tests).
  - Run docs locally: `cd docs && pip install -r requirements.txt && bash serve.sh` or `make serve`.
  - Run unit tests: `pytest` (use small models from `docs/` for fast runs, see `docs/` guidelines).
  - Launch local server: use the CLI entrypoint: `python -m sglang.cli.main` or installed `sglang` command.

- **Pinned and fragile dependencies**
  - `python/pyproject.toml` pins many runtime libs (e.g. `grpcio==1.75.1`) — keep proto/grpc versions aligned with `compile_proto.py` and gateway bindings.
  - `flashinfer_*` package versions are tied to Docker and runtime compatibility; check Dockerfiles in `docker/` for matching versions.

- **Project-specific conventions**
  - Docs are executable Jupyter notebooks; prefer relative links and small models for CI speed (see `docs/README.md`).
  - Keep `pre-commit` and notebook outputs clean: run `pre-commit run --all-files` and `nbstripout` before PRs.
  - Native code changes often require rebuilding `sgl-kernel` and bumping `sgl-kernel` version in `pyproject` if releasing.

- **Where to look for examples**
  - Service wiring and gRPC examples: `python/sglang/srt/grpc/` and `python/sglang/srt/entrypoints/grpc_server.py`.
  - Kernel build and tests: `sgl-kernel/` (see `build.sh`, `tests/`).
  - Gateway integration: `sgl-model-gateway/README.md` references the Python gRPC server paths.

If anything here is unclear or you want more depth in a specific area (build matrix, CI, or runtime tracing), tell me which area and I'll expand the file.
