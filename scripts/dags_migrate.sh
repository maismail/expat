#!/bin/bash
set -e

if [ $# -ne 5 ] ; then
    echo "Usage: dags_migrate.sh project project_secret_dir project_user hdfs_superuser hadoop_home"
    exit 1
fi

PROJECT=$1
PROJECT_DAGS_DIR=/srv/hops/airflow/dags/$2
PROJECT_USER=$3
HDFS_SUPERUSER=$4
HADOOP_HOME=$5

if [ -d "$PROJECT_DAGS_DIR" ]; then
  echo "Copying dags for project: $PROJECT"
  for DAG in "$PROJECT_DAGS_DIR"/*; do
    if [ -f $DAG ] ; then
      if [[ "$DAG" == *.py ]]; then
        echo "Found possible dag $DAG"
        # to make it idempotent
        if ! grep -q "access_control" "$DAG"; then
            echo "Writing access dag access_control rule"
            # Put the project to the dag `project` role with read and edit permissions
            sed -i '/default_args = args,/a \ \ \ \ access_control = {\n \ \ \ \ \ \ \ "'"${PROJECT}"'": {"can_dag_read", "can_dag_edit"},\n \ \ \ \ },' $DAG
        fi
      fi
    fi
  done

  # Copy all the file in the project dags to hopsfs
  ${HADOOP_HOME}/bin/hdfs dfs -copyFromLocal -f ${PROJECT_DAGS_DIR}/* /Projects/${PROJECT}/Airflow
  ${HADOOP_HOME}/bin/hdfs dfs -chown -R ${PROJECT_USER}:${PROJECT}__Airflow /Projects/${PROJECT}/Airflow/*
  exit 0
else
  echo "Dags directory for project: $PROJECT, was not configured. So it does not have any dags."
  exit 2
fi
