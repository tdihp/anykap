How to contribute
=================

Development environment
-----------------------

Fork the repository

Clone the code to local

    git clone https://github.com/<your-name>/anykap
    cd anykap

Create a virtual environment

    python3 -m venv .venv
    . .venv/bin/activate

Install dependencies

    python -m pip install -U pip
    pip install -r requirements.txt

Install anykap for local development

    pip install -e .

Anykap configures [pre-commit](https://pre-commit.com/) for linting and code
style via [black](https://github.com/psf/black) and
[ruff](https://github.com/astral-sh/ruff). To install git pre-commit hooks:

    pre-commit install --install-hooks

For testing Azure blob, 
[install azurite](https://github.com/Azure/Azurite?tab=readme-ov-file#npm)

    npm install -g azurite

For running tests:

    hatch run pytest --cov src/anykap

For building documentation:

    hatch run docs:make -C docs html

You'll find html docs available at `docs/_build/html`. To visit, either open the
`index.html` directly or host it locally by
`python3 -m http.server --directory docs/_build/html`, then visit
[127.0.0.1:8000] in browser.
