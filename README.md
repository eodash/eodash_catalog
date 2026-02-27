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

## Wiki

eodash_catalog wiki contains meaning of most of configurable parameters of the collections, indicators and catalogs.

For one-time export of the wiki as single PDF, do following:

```bash
git clone https://github.com/eodash/eodash_catalog.wiki.git
cd eodash_catalog.wiki
for f in *.md; do pandoc -V geometry:margin=1in "$f" -o "$f".pdf; done
now=`date +'%Y%m%d'` && pdftk Home.md.pdf Data-integration-steps.md.pdf "Data-definition-‐-first-steps.md.pdf" Auxiliary-layers.md.pdf Colorlegend.md.pdf DataSource.md.pdf Locations.md.pdf Process.md.pdf Projection.md.pdf Provider.md.pdf Reference.md.pdf Resource.md.pdf Service.md.pdf Story.md.pdf output eodash-catalog-wiki-snapshot-$now.pdf
```
Add missing files if new wiki pages have been added in the meantime.
