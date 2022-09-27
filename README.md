# data-harmonization
This is the repo to run data harmonization on disparate data sources using entity deduplication

![Flow Diagram](https://github.com/rajdeep07/data-harmonization/blob/main/data_harmonization/main/data/Data_Harmonization.jpg)

Command to generate custom class from config:

`datamodel-codegen --input data_harmonization/main/resources/api.yaml --output data_harmonization/main/code/tiger/model/datamodel.py`

## Overview

This project uses the following tools/libraries/frameworks for running, building,
managing dependencies, versioning, testing, and linting (this does not include any
libraries or dependencies that the actual code in this project uses):

1. [`pyenv`](https://github.com/pyenv/pyenv)
2. [`pytest`](https://github.com/pytest-dev/pytest)
3. [`tox`](https://github.com/tox-dev/tox)
    * [`tox-pyenv`](https://github.com/tox-dev/tox-pyenv)
4. [`pre-commit`](https://github.com/pre-commit/pre-commit)
5. [`black`](https://github.com/psf/black) 
6. [`flake8`](https://github.com/PyCQA/flake8)
7. [`isort`](https://github.com/PyCQA/isort)
8. [`pylint`](https://github.com/PyCQA/pylint)
9. [`mypy`](https://github.com/python/mypy)
10. [`pipx`](https://github.com/pypa/pipx)

## Quick Start

```shell
# 1. pyenv
brew install pyenv

echo 'eval "$(pyenv init --path)"' >> ~/.zprofile
echo 'eval "$(pyenv init -)"' >> ~/.zshrc

source ~/.zshrc

LATEST_38=$(pyenv install -l | sed 's/^ *//' | grep '^3\.8\.' | tail -n 1)
pyenv install ${LATEST_38}
pyenv local ${LATEST_38}

# 2. virtual environment
python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt


# 3. pre-commit
pre-commit install
pre-commit run -a

## Local Development

### Setup

Note that this setup is for Mac.

#### `pyenv` Setup

I highly recommend using [`pyenv`](https://github.com/pyenv/pyenv) for managing 
python versions for local development.

```shell
# Install pyenv
brew install pyenv

# Modify your login shell to initialize pyenv
echo 'eval "$(pyenv init --path)"' >> ~/.zprofile
echo 'eval "$(pyenv init -)"' >> ~/.zshrc

# Reload your shell to initialize pyenv
source ~/.zshrc

# Decide which version of python you want to use...
# You can refer to the `tox.ini` file for which versions
# that we're using for testing.

# In this case I'm going to go with the lowest version that
# we currently support (see `python` in the `tool.poetry.dependencies`
# section in `pyproject.toml`).

# Install python 3.8.*
LATEST_38=$(pyenv install -l | sed 's/^ *//' | grep '^3\.8\.' | tail -n 1)
# 3.8.13 on 2022-09-09
pyenv install ${LATEST_38}

# If you get this error...
#     ERROR: The Python ssl extension was not compiled. Missing the OpenSSL lib?
# ...try running this...
#     PYTHON_BUILD_HOMEBREW_OPENSSL_FORMULA=openssl@1.0 pyenv install 2.6.9
# ...and then try installing again.
# See https://github.com/pyenv/pyenv/issues/1669#issuecomment-729980692

# In this directory, set the python version
# This creates a (git-ignored) .python-version file 
pyenv local ${LATEST_38}
```

#### Virtual Environment Setup

```shell
# In this directory...
python3 -m venv venv
source venv/bin/activate

# Install dev dependencies
pip install -r requirements.txt
```

#### `pre-commit` Setup 

This [`pre-commit`](https://github.com/pre-commit/pre-commit) step is optional
but recommended, since it will catch PEP8 errors before you commit locally, 
versus catching them in the CI/CD pipeline.

```shell
# In this directory, and after the virtual environment setup
source venv/bin/activate

pre-commit install
```

Now when you go to commit, it will run [`black`](https://github.com/psf/black) 
and [`flake8`](https://github.com/PyCQA/flake8) against the python code in
order to format and enforce PEP8 compliance, respectively,
[`isort`](https://github.com/PyCQA/isort) to sort imports,
[`pylint`](https://github.com/PyCQA/pylint) to check for errors and enforce a
coding standard, [`mypy`](https://github.com/python/mypy) for static type
checking. If `black` changes any files, the pre-commit hook will fail the
commit, at which point you can re-add the changed files and attempt to
re-commit your changes. If there's an issue with the python code that `black`
can't fix and/or `flake8`, `pylint`, and `mypy` linting fails, the commit will
fail as well. If `isort` needs to reorder the imports.

You can also proactively run pre-commit manually before attempting commits:

```shell
pre-commit run -a
```

### Updating Dependencies

We use `pip-compile` from [`pip-tools`](https://github.com/jazzband/pip-tools) to
manage and pin our dependencies.

Note that `pip-tools` will be installed in this project's virtual environment when
running the [Virtual Environment Setup](#virtual-environment-setup) step (it's
specified within `serverless/requirements-dev.in` / 
`serverless/requirements-dev.txt`).

1. Define your direct development dependencies in `serverless/requirements.in`,
   using specific versions wherever possible:

   ```
   pre-commit>=1.20.0
   pip-tools==6.8.0
   flake8>=4.0.1
   black>=22.3.0
   pytest==7.1.0
   tox==3.24.5
   pytest-cov==3.0.0
   pytest-html==3.1.1
   pytest-mock==3.7.0
   pytest-xdist==2.5.0
   tox-pyenv==1.1.0
   ```

2. Compile this file to `requirements.txt`. This pins both direct and
   transitive dependencies to specific versions, and also adds comments indicating
   where the dependency came from (dependency tree):

   ```shell
   rm requirements.txt
   pip-compile requirements-dev.in
   # Check in the requirements.in and requirements.txt changes
   ```
   
3. Define your direct application dependencies in `requirements.in`,
   using specific versions wherever possible:

   ```
   pandas==2.4.0
   matplotlib==3.4.0
   ```

See https://github.com/jazzband/pip-tools#updating-requirements for more details
around updating dependencies.

### Coding Conventions

1. Please write tests for your code. For smaller utility functions, you can use
   [`doctest`](https://docs.python.org/3.7/library/doctest.html) for testing if
   you'd like; otherwise please use [`pytest`](https://github.com/pytest-dev/pytest)
   tests in the `tests` directory following the same package structure as the main
   code.

   A benefit of the `doctest` approach is that the usage of your function is
   immediately apparent when looking at the function's docstring, either directly
   in the code or in IDE documentation popup in tools like PyCharm.

2. Please use docstrings on all your packages, modules, classes, and methods/functions.
   And please use the [`reStructuredText`](https://docutils.sourceforge.io/rst.html)
   format.

3. Wherever it's necessary and makes sense, you are welcome to disable certain `pylint`
   rules in the code. But please only disable things that aren't real problems and 
   shouldn't be fixed. Use your judgement.

   Example:
   ```python
   class SnowflakeConfig(BaseSettings):
       # pylint: disable=too-few-public-methods
       """
       The configuration needed to establish a Snowflake connection.
       """

       secret: dict
       account: str = None
       role: str = None
       warehouse: str = None
       database: str = None
       schema_: str = Field(None, alias="schema", env="snowflake_schema")

       @property
       def username(self) -> Optional[str]:
           """
           The Snowflake username.

           :return: the Snowflake username
           """
           return self.secret.get("username")

       @property
       def password(self) -> Optional[str]:
           """
           The Snowflake password.

           :return: the Snowflake password
           """
           return self.secret.get("password")

       class Config:
           # pylint: disable=missing-class-docstring,too-few-public-methods
           env_prefix = "snowflake_"
           allow_population_by_field_name = True
   ```

4. Use type hints wherever you can. We support down to Python 3.7.1, so we have 2 options,
   with the first one being preferred:

   1. Import `from __future__ import annotations` as the first line in your module/script
      and use `list`, `dict`, etc. for as type as specified by 
      [PEP 585](https://peps.python.org/pep-0585/) rather than using the types in the
      `typing` module. This is now supported in PyCharm `2021.2.3` and up, and it's also
      supported in `mypy`.
   
   2. Use [typing compatible with 3.7](https://docs.python.org/3.7/library/typing.html).
      To do this, import `typing.List`, `typing.Dict`, etc. and use them as the type
      annotations.

   See https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html for more information
   about type hints.