FROM python:3.12

ENV POETRY_VERSION=1.8 \
    POETRY_VIRTUALENVS_CREATE=false \
    DAGSTER_HOME=/opt/dagster/app

WORKDIR ${DAGSTER_HOME}

RUN pip install "poetry==$POETRY_VERSION"
RUN mkdir -p ${DAGSTER_HOME}
RUN mkdir -p ${DAGSTER_HOME}/data

COPY analytics/ ${DAGSTER_HOME}/analytics
COPY data_transformations/ ${DAGSTER_HOME}/data_transformations
COPY pyproject.toml poetry.lock ${DAGSTER_HOME}

RUN poetry install --no-interaction --no-ansi --no-root --no-dev
