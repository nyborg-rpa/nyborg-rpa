# Nyborg RPA

This is the workspace of Nyborg RPA.

## Requirements

- [`git`](https://git-scm.com/)
- [`uv`](https://github.com/astral-sh/uv)

## Installation

The workspace consists of a `nyborg_rpa` package, which contains various submodules in the `packages/` directory.

Install the workspace by **recursively** cloning it:

```sh
git clone git@github.com:nyborg-rpa/nyborg-rpa.git --recursive
```

and then setup the project by running:

```sh
cd nyborg-rpa
uv sync
```

## Usage

See [examples/](./examples/) for usage examples.