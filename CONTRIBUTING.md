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
    pip install pytest pytest-cov pytest-asyncio 

For testing Azure blob, 
[install azurite](https://github.com/Azure/Azurite?tab=readme-ov-file#npm) and
azure-storage-blob

    npm install -g azurite
    pip install azure-storage-blob

For running tests:

    pytest --cov src/anykap
