[![CI](https://github.lightsource.ca/kiveln/kv-dict/actions/workflows/ci.yml/badge.svg)](https://github.lightsource.ca/kiveln/kv-dict/actions/workflows/ci.yml)

# kv-dict

KV bacjed dictionary, similar to RedisJSONDict

## Install Package

- pip: `pip install git+https://github.lightsource.ca/kiveln/kv-dict`
- uv: `uv add git+https://github.lightsource.ca/kiveln/kv-dict`
  - add `--optional FEATURE` to add as an optional dependency
- pixi: `pixi add --git https://github.lightsource.ca/kiveln/kv-dict`
  - add `--feature FEATURE` to add as an optional dependency

## Development

This project uses `uv` for managing project dependencies. Installation
instructions may be found [here](https://docs.astral.sh/uv/getting-started/installation/).

```bash
git clone https://github.lightsource.ca/kiveln/kv-dict.git
cd kv-dict
uv sync --all-extras
```
