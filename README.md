# eodash_catalog

[![PyPI - Version](https://img.shields.io/pypi/v/eodash_catalog.svg)](https://pypi.org/project/eodash_catalog)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/eodash_catalog.svg)](https://pypi.org/project/eodash_catalog)

---

**Table of Contents**

- [Installation](#installation)
- [License](#license)

## Installation

```console
pip install eodash_catalog
```

## Testing

Project uses pytest and runs it as part of CI:

```bash
python -m pytest
```

## Testing

Project uses ruff to perform checks on code style and formatting

```bash
ruff check .
```

## Versioning and branches

eodash_catalog adheres to [Semantic Versioning](https://semver.org/) and follows these rules:

Given a version number `MAJOR.MINOR.PATCH`, we increment the:

- `MAJOR` version when we make incompatible API changes
- `MINOR` version when we add functionality in a backward compatible manner
- `PATCH` version when we make backward compatible bug fixes

Active development is followed by the `main` branch.
`
New features or maintenance commits should be done against this branch in the form of a Merge Request of a Feature branch.

## Tagging

This repository uses bump2version for managing tags. To bump a version use

```bash
bump2version <major|minor|patch> # or bump2version --new-version <new_version>
git push && git push --tags
```

Pushing a tag in the repository automatically creates:

- versioned package on pypi

## License

`eodash_catalog` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
