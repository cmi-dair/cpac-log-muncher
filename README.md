# C-PAC log muncher (`clmunch`)

[![Build](https://github.com/cmi-dair/cpac-log-muncher/actions/workflows/test.yaml/badge.svg?branch=main)](https://github.com/cmi-dair/cpac-log-muncher/actions/workflows/test.yaml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/cmi-dair/cpac-log-muncher/branch/main/graph/badge.svg?token=22HWWFWPW5)](https://codecov.io/gh/cmi-dair/cpac-log-muncher)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![L-GPL License](https://img.shields.io/badge/license-L--GPL-blue.svg)](https://github.com/cmi-dair/cpac-log-muncher/blob/main/LICENSE)
[![pages](https://img.shields.io/badge/api-docs-blue)](https://cmi-dair.github.io/cpac-log-muncher)


## Installation

Get the newest development version via:

```sh
pip install git+https://github.com/cmi-dair/cpac-log-muncher
```

## Usage

```sh
usage: clmunch [-h] [-o OUTPUT] path

Generate a report on CPAC runs.

positional arguments:
  path                  Path to the directory containing the log files.

options:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Path to the output file.
```
